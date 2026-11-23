package gateway

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/victorcondackdev/brpay/pix"
	"github.com/victorcondackdev/ledger-go/ledger"
)

// OpenAccountRequest opens a customer wallet.
type OpenAccountRequest struct {
	ID                  string     `json:"id,omitempty"`
	Name                string     `json:"name"`
	Type                string     `json:"type,omitempty"` // PF (default) or PJ
	DayLimitCents       int64      `json:"day_limit_cents,omitempty"`
	NightLimitCents     int64      `json:"night_limit_cents,omitempty"`
	InitialDepositCents int64      `json:"initial_deposit_cents,omitempty"`
	Tags                []string   `json:"tags,omitempty"`
	OpenedAt            *time.Time `json:"opened_at,omitempty"` // KYC date, for accounts migrated from elsewhere
}

// AccountView is a customer with live balances.
type AccountView struct {
	Customer
	BalanceCents   int64 `json:"balance_cents"`
	AvailableCents int64 `json:"available_cents"`
	HeldCents      int64 `json:"held_cents"`
}

var idRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$`)

// OpenAccount creates a customer and books the initial deposit.
func (s *Service) OpenAccount(ctx context.Context, r OpenAccountRequest) (AccountView, error) {
	if r.ID == "" {
		r.ID = newID("cus")
	}
	if !idRe.MatchString(r.ID) {
		return AccountView{}, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid account", "id must be 1-64 characters [A-Za-z0-9_.-]")
	}
	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" || len(r.Name) > 140 {
		return AccountView{}, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid account", "name is required (at most 140 characters)")
	}
	switch r.Type {
	case "":
		r.Type = "PF"
	case "PF", "PJ":
	default:
		return AccountView{}, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid account", "type must be PF or PJ")
	}
	if r.DayLimitCents < 0 || r.NightLimitCents < 0 || r.InitialDepositCents < 0 {
		return AccountView{}, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid account", "limits and deposit must not be negative")
	}
	if r.DayLimitCents == 0 {
		r.DayLimitCents = s.cfg.DefaultDayLimit
	}
	if r.NightLimitCents == 0 {
		r.NightLimitCents = s.cfg.DefaultNight
	}
	opened := s.now().UTC()
	if r.OpenedAt != nil {
		opened = r.OpenedAt.UTC()
	}
	c := &Customer{ID: r.ID, Name: r.Name, Type: r.Type, DayLimitCents: r.DayLimitCents, NightLimitCents: r.NightLimitCents,
		Tags: r.Tags, OpenedAt: opened, LedgerAccount: CustomerAccount(r.ID)}
	if !s.store.putCustomer(c) {
		return AccountView{}, problem(http.StatusConflict, "conflict", "Account exists", "an account with this id already exists")
	}
	if err := s.books.openWallet(ctx, c.LedgerAccount, c.Name, "2.1.01"); err != nil {
		return AccountView{}, err
	}
	if r.InitialDepositCents > 0 {
		if _, err := s.books.cashIn(ctx, "open:"+c.ID, c.LedgerAccount, r.InitialDepositCents); err != nil {
			return AccountView{}, err
		}
	}
	return s.Account(ctx, c.ID)
}

// Account returns a customer with balances.
func (s *Service) Account(ctx context.Context, id string) (AccountView, error) {
	c, ok := s.store.customer(id)
	if !ok {
		return AccountView{}, errNotFound
	}
	b, err := s.ledger.Balance(ctx, c.LedgerAccount)
	if err != nil {
		return AccountView{}, err
	}
	return AccountView{Customer: *c, BalanceCents: b.Posted.Minor(), AvailableCents: b.Available.Minor(), HeldCents: b.Posted.Minor() - b.Available.Minor()}, nil
}

// normalizeKey returns the DICT form of a Pix key (lower-case e-mail,
// +55 phone, digits-only documents) so configured and received keys
// compare equal.
func normalizeKey(k string) string {
	if n, _, err := pix.NormalizeKey(k); err == nil {
		return n
	}
	return strings.ToLower(strings.TrimSpace(k))
}

func isInsufficientFunds(err error) bool { return errors.Is(err, ledger.ErrInsufficientFunds) }
