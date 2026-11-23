// Command notifier bridges payments-service-go's event stream to merchant
// webhooks: it consumes the CloudEvents the service's transactional outbox
// publishes to Kafka (Redpanda here) and hands each one to relayd, which
// signs and delivers it with retries.
//
// Delivery semantics, end to end:
//
//   - payments-service publishes at least once (outbox relay);
//   - the notifier commits a partition's offsets only after relayd has
//     accepted every record it polled, so a crash re-reads (at least once);
//   - every relayd message carries Idempotency-Key "ce:<CloudEvent id>",
//     so re-reads collapse into the original message: merchants get one
//     message per event, each delivered at least once and deduplicated on
//     webhook-id.
//
// Configuration (environment):
//
//	KAFKA_BROKERS     comma-separated seed brokers (required)
//	NOTIFIER_TOPIC    default payments.events.v1
//	NOTIFIER_GROUP    consumer group, default pix-in-a-box-notifier
//	RELAY_URL         relayd base URL (required)
//	RELAY_API_TOKEN   relayd bearer token
//	NOTIFIER_ADDR     health and metrics listener, default :8080
//
// "notifier -healthcheck" probes /healthz and exits.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	brokers    []string
	topic      string
	group      string
	relayURL   string
	relayToken string
	addr       string
}

func loadConfig() (config, error) {
	get := func(k, def string) string {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
		return def
	}
	c := config{
		topic: get("NOTIFIER_TOPIC", "payments.events.v1"), group: get("NOTIFIER_GROUP", "pix-in-a-box-notifier"),
		relayURL: strings.TrimRight(get("RELAY_URL", ""), "/"), relayToken: get("RELAY_API_TOKEN", ""), addr: get("NOTIFIER_ADDR", ":8080"),
	}
	for _, b := range strings.Split(get("KAFKA_BROKERS", ""), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.brokers = append(c.brokers, b)
		}
	}
	if len(c.brokers) == 0 || c.relayURL == "" {
		return c, errors.New("KAFKA_BROKERS and RELAY_URL are required")
	}
	return c, nil
}

func main() {
	health := flag.Bool("healthcheck", false, "probe /healthz and exit")
	flag.Parse()
	if *health {
		addr := os.Getenv("NOTIFIER_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		c := &http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get("http://127.0.0.1:" + addr[strings.LastIndex(addr, ":")+1:] + "/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			fmt.Fprintln(os.Stderr, "unhealthy", err)
			os.Exit(1)
		}
		return
	}
	log := slog.New(traceHandler{slog.NewJSONHandler(os.Stdout, nil)}).With(slog.String("service", "notifier"))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("config", slog.Any("error", err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.Error("notifier stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ClientID("pix-in-a-box-notifier"),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// Local development only: lets the notifier start before the
		// topic exists. Production topics are created by infrastructure.
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()

	m := newMetrics()
	n := &notifier{relay: &http.Client{Timeout: 10 * time.Second}, cfg: cfg, log: log, m: m}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := client.Ping(pctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"kafka": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"kafka": "ok"})
	})
	mux.HandleFunc("GET /metrics", m.serve)
	srv := &http.Server{Addr: cfg.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listener", slog.Any("error", err))
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("notifier consuming", slog.String("topic", cfg.topic), slog.String("group", cfg.group), slog.Any("brokers", cfg.brokers))

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			// Transient broker errors are retried by the client; log them.
			log.Warn("fetch error", slog.String("topic", fe.Topic), slog.Int("partition", int(fe.Partition)), slog.Any("error", fe.Err))
		}
		complete := true
		fetches.EachRecord(func(r *kgo.Record) {
			if !complete {
				return
			}
			if err := n.handle(ctx, r); err != nil {
				complete = false // only ctx cancellation ends handle with an error
			}
		})
		if !complete {
			return nil // uncommitted records are re-read after restart
		}
		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			log.Warn("offset commit failed; records will be re-read and deduplicated by relayd", slog.Any("error", err))
		}
	}
}

// cloudEvent is the structured-mode envelope payments-service publishes
// (internal/outbox.Envelope there).
type cloudEvent struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	Subject     string          `json:"subject"`
	Time        time.Time       `json:"time"`
	Data        json.RawMessage `json:"data"`
}

// eventType maps a CloudEvent type to the merchant-facing event type:
// payments.charge.paid.v1 → charge.paid. The version stays in the payload.
func eventType(ceType string) string {
	t := strings.TrimPrefix(ceType, "payments.")
	if i := strings.LastIndex(t, ".v"); i > 0 {
		t = t[:i]
	}
	return t
}

type notifier struct {
	relay *http.Client
	cfg   config
	log   *slog.Logger
	m     *metrics
}

// handle publishes one record, retrying relayd until it accepts or ctx
// ends. Poison records (not a CloudEvent) are logged and skipped: blocking
// a partition forever on bad data is worse than a gap an operator sees in
// the metrics.
func (n *notifier) handle(ctx context.Context, r *kgo.Record) error {
	var ce cloudEvent
	if err := json.Unmarshal(r.Value, &ce); err != nil || ce.ID == "" || ce.Type == "" {
		n.m.inc("notifier_events_skipped_total", "malformed")
		n.log.Error("skipping record that is not a CloudEvent", slog.String("topic", r.Topic), slog.Int("partition", int(r.Partition)), slog.Int64("offset", r.Offset))
		return nil
	}
	tp := ""
	for _, h := range r.Headers {
		if h.Key == "traceparent" && traceparentRe.Match(h.Value) {
			tp = string(h.Value)
		}
	}
	span := childOf(tp)
	ctx = context.WithValue(ctx, spanKey{}, span)
	et := eventType(ce.Type)
	n.m.inc("notifier_events_consumed_total", et)

	payload, _ := json.Marshal(map[string]any{
		"id": ce.ID, "type": ce.Type, "source": ce.Source, "subject": ce.Subject, "time": ce.Time, "data": ce.Data,
	})
	body, _ := json.Marshal(map[string]any{"event_type": et, "payload": json.RawMessage(payload)})
	backoff := 250 * time.Millisecond
	for {
		status, err := n.publish(ctx, body, "ce:"+ce.ID, span)
		switch {
		case err == nil && (status == http.StatusAccepted || status == http.StatusOK):
			result := "published"
			if status == http.StatusOK {
				result = "replayed" // relayd already had it: a re-read after a crash or rebalance
			}
			n.m.inc("notifier_relay_publish_total", result)
			n.log.InfoContext(ctx, "event relayed", slog.String("ce_id", ce.ID), slog.String("event_type", et),
				slog.String("subject", ce.Subject), slog.Int64("offset", r.Offset), slog.String("result", result))
			return nil
		case err == nil && status >= 400 && status < 500 && status != http.StatusTooManyRequests:
			// relayd refused the message itself (validation): retrying
			// cannot help.
			n.m.inc("notifier_events_skipped_total", "relay_rejected")
			n.log.ErrorContext(ctx, "relayd rejected event; skipping", slog.String("ce_id", ce.ID), slog.Int("status", status))
			return nil
		}
		n.m.inc("notifier_relay_publish_total", "retry")
		n.log.WarnContext(ctx, "relayd publish failed; retrying", slog.String("ce_id", ce.ID), slog.Int("status", status), slog.Any("error", err), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (n *notifier) publish(ctx context.Context, body []byte, key string, span span) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.relayURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("traceparent", span.child().String())
	if n.cfg.relayToken != "" {
		req.Header.Set("Authorization", "Bearer "+n.cfg.relayToken)
	}
	resp, err := n.relay.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// --- trace context (W3C traceparent), kept local so this module has no
// dependency on the rest of the repository.

var traceparentRe = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

type span struct{ trace, id string }

type spanKey struct{}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// childOf continues tp or starts a trace. payments-service does not put
// trace context in its events yet, so in practice each event starts one.
func childOf(tp string) span {
	if tp != "" {
		return span{trace: tp[3:35], id: randHex(8)}
	}
	return span{trace: randHex(16), id: randHex(8)}
}

func (s span) child() span     { return span{trace: s.trace, id: randHex(8)} }
func (s span) String() string  { return "00-" + s.trace + "-" + s.id + "-01" }
func (s span) traceID() string { return s.trace }
func (s span) spanID() string  { return s.id }
func (s span) valid() bool     { return len(s.trace) == 32 }
func (s span) attrs() []any {
	return []any{slog.String("trace_id", s.traceID()), slog.String("span_id", s.spanID())}
}
func spanFrom(ctx context.Context) (span, bool) {
	s, ok := ctx.Value(spanKey{}).(span)
	return s, ok && s.valid()
}

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if s, ok := spanFrom(ctx); ok {
		r.Add(s.attrs()...)
	}
	return h.Handler.Handle(ctx, r)
}
func (h traceHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(as)}
}
func (h traceHandler) WithGroup(n string) slog.Handler { return traceHandler{h.Handler.WithGroup(n)} }

// --- metrics: counters with one label, Prometheus text format.

type metrics struct {
	mu     sync.Mutex
	counts map[string]map[string]float64
}

var metricHelp = map[string]struct{ help, label string }{
	"notifier_events_consumed_total": {"CloudEvents consumed from Kafka, by merchant event type.", "event_type"},
	"notifier_relay_publish_total":   {"Publish attempts to relayd, by result (published, replayed, retry).", "result"},
	"notifier_events_skipped_total":  {"Records skipped without delivery, by reason.", "reason"},
}

func newMetrics() *metrics {
	m := &metrics{counts: map[string]map[string]float64{}}
	for name := range metricHelp {
		m.counts[name] = map[string]float64{}
	}
	return m
}

func (m *metrics) inc(name, label string) {
	m.mu.Lock()
	m.counts[name][label]++
	m.mu.Unlock()
}

func (m *metrics) serve(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.counts))
	for n := range m.counts {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		h := metricHelp[name]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, h.help, name)
		labels := make([]string, 0, len(m.counts[name]))
		for l := range m.counts[name] {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		for _, l := range labels {
			fmt.Fprintf(w, "%s{%s=%q} %g\n", name, h.label, l, m.counts[name][l])
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
