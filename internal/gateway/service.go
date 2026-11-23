// Package gateway is the PSP core of pix-in-a-box: it owns customer
// wallets in a double-entry ledger (ledger-go) and orchestrates every Pix
// across the DICT and SPI (pix-psp-simulator), the risk engine
// (fraud-rules-go), the charges service (payments-service-go) and merchant
// notifications (webhook-relay-go).
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/victorcondackdev/ledger-go/ledger"
	"github.com/victorcondackdev/ledger-go/ledger/memstore"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/payments"
	"github.com/victorcondackdev/pix-in-a-box/internal/clients/pixsim"
	"github.com/victorcondackdev/pix-in-a-box/internal/clients/relay"
	"github.com/victorcondackdev/pix-in-a-box/internal/clients/riskd"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/idem"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// Service wires the flows together.
type Service struct {
	cfg      Config
	log      *slog.Logger
	ledger   *ledger.Ledger
	books    books
	store    *Store
	sim      *pixsim.Client
	risk     *riskd.Client
	relay    *relay.Client
	payments *payments.Client
	outbox   *outbox
	metrics  *Metrics
	idem     *idem.Store
	now      func() time.Time

	ready     atomic.Bool
	bootErr   atomic.Value // string
	ctx       context.Context
	merchants map[string]ledger.AccountID // normalized Pix key → ledger account
}

// Option customises a Service (tests).
type Option func(*Service)

// WithClock replaces the wall clock.
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// New builds a service. The ledger uses ledger-go's in-memory store; a
// production deployment would plug in a SQL Store implementing the same
// interface (see ledger-go's migrations/postgres.sql).
func New(cfg Config, log *slog.Logger, opts ...Option) *Service {
	m := newMetrics()
	s := &Service{cfg: cfg, log: log, store: NewStore(), metrics: m, now: time.Now, merchants: map[string]ledger.AccountID{}}
	for _, o := range opts {
		o(s)
	}
	s.ledger = ledger.New(memstore.New(),
		ledger.WithClock(func() time.Time { return s.now() }),
		ledger.WithOnCommit(func(tx ledger.Transaction) { m.Postings.Inc(tx.Kind.String()) }))
	s.books = books{l: s.ledger}
	s.sim = pixsim.New(cfg.PixSimURL, m.Downstream)
	s.risk = riskd.New(cfg.RiskdURL, cfg.RiskTimeout, m.Downstream)
	s.relay = relay.New(cfg.RelayURL, cfg.RelayToken, m.Downstream)
	s.payments = payments.New(cfg.PaymentsURL, cfg.PaymentsWebhookSecret, m.Downstream)
	s.outbox = newOutbox(s.relay, log, m)
	s.idem = idem.NewStore(24 * time.Hour)
	s.idem.Hits = func() { m.Replays.Inc() }
	m.Registry.NewGaugeFunc("gateway_outbox_pending", "Merchant notifications waiting to be accepted by relayd.", func() float64 { return float64(s.outbox.depth()) })
	m.Registry.NewGaugeFunc("gateway_ready", "1 once bootstrap (ledger opening balance, relay endpoint) completed.", func() float64 {
		if s.ready.Load() {
			return 1
		}
		return 0
	})
	s.bootErr.Store("")
	return s
}

// Metrics exposes the registry (tests and main).
func (s *Service) Metrics() *Metrics { return s.metrics }

// Ledger exposes the ledger (tests).
func (s *Service) Ledger() *ledger.Ledger { return s.ledger }

// Run bootstraps and then runs background work until ctx ends.
func (s *Service) Run(ctx context.Context) {
	s.ctx = ctx
	go s.outbox.run(ctx)
	go s.bootstrapLoop(ctx)
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.ready.Load() {
				s.pollSubmitted(ctx)
				s.expireChallenges(ctx)
			}
		}
	}
}

func (s *Service) background() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *Service) bootstrapLoop(ctx context.Context) {
	delay := 500 * time.Millisecond
	for {
		bctx := trace.With(ctx, trace.New())
		err := s.Bootstrap(bctx)
		if err == nil {
			s.bootErr.Store("")
			return
		}
		s.bootErr.Store(err.Error())
		s.log.WarnContext(bctx, "bootstrap failed; retrying", slog.Any("error", err), slog.Duration("in", delay))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < 5*time.Second {
			delay *= 2
		}
	}
}

// Bootstrap opens the chart of accounts, books the opening balance read
// from the SPI settlement account and registers the merchant's webhook
// endpoint in relayd. Every step is idempotent.
func (s *Service) Bootstrap(ctx context.Context) error {
	if s.ready.Load() {
		return nil
	}
	if err := s.books.openChart(ctx); err != nil {
		return fmt.Errorf("ledger chart: %w", err)
	}
	acct, err := s.sim.Account(ctx, s.cfg.SPIAccountID)
	if err != nil {
		return fmt.Errorf("reading SPI settlement account %q: %w", s.cfg.SPIAccountID, err)
	}
	if acct.ISPB != s.cfg.ISPB {
		s.log.WarnContext(ctx, "SPI account ISPB differs from GATEWAY_ISPB", slog.String("account_ispb", acct.ISPB), slog.String("gateway_ispb", s.cfg.ISPB))
	}
	if err := s.books.opening(ctx, s.cfg.SPIAccountID, acct.BalanceCents); err != nil {
		return fmt.Errorf("opening balance: %w", err)
	}
	m := MerchantAccount(s.cfg.MerchantID)
	if err := s.books.openWallet(ctx, m, s.cfg.MerchantName, "2.1.02"); err != nil {
		return fmt.Errorf("merchant account: %w", err)
	}
	s.merchants[normalizeKey(s.cfg.MerchantPixKey)] = m
	if s.cfg.MerchantWebhookURL != "" {
		ep, created, err := s.relay.EnsureEndpoint(ctx, relay.EndpointSpec{
			URL: s.cfg.MerchantWebhookURL, Description: s.cfg.MerchantName + " notifications",
			EventTypes: []string{"pix.*", "charge.*"}, Secret: s.cfg.MerchantWebhookSecret, MaxConcurrency: 4,
		})
		if err != nil {
			return fmt.Errorf("relay endpoint: %w", err)
		}
		s.log.InfoContext(ctx, "merchant webhook endpoint", slog.String("endpoint_id", ep.ID), slog.Bool("created", created))
	}
	s.ready.Store(true)
	s.log.InfoContext(ctx, "gateway ready", slog.Int64("opening_reserves_cents", acct.BalanceCents), slog.String("spi_account", s.cfg.SPIAccountID))
	return nil
}

// readiness checks bootstrap and every dependency the request path needs.
func (s *Service) readiness(ctx context.Context) map[string]string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out := map[string]string{}
	check := func(name string, f func(context.Context) error) {
		if err := f(ctx); err != nil {
			out[name] = err.Error()
		} else {
			out[name] = "ok"
		}
	}
	if s.ready.Load() {
		out["bootstrap"] = "ok"
	} else {
		out["bootstrap"] = "pending: " + s.bootErr.Load().(string)
	}
	check("pixsim", s.sim.Healthz)
	check("riskd", s.risk.Healthz)
	check("relayd", s.relay.Healthz)
	check("payments", s.payments.Readyz)
	return out
}

func newID(prefix string) string {
	var b [10]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// errProblem is a domain error with an HTTP mapping.
type errProblem struct {
	status int
	typ    string
	title  string
	detail string
	ext    map[string]any
}

func (e *errProblem) Error() string { return e.title + ": " + e.detail }

func problem(status int, typ, title, detail string) *errProblem {
	return &errProblem{status: status, typ: "/problems/" + typ, title: title, detail: detail}
}

var errNotFound = problem(http.StatusNotFound, "not-found", "Not found", "no such resource")

func asProblem(err error) (*errProblem, bool) {
	var p *errProblem
	ok := errors.As(err, &p)
	return p, ok
}
