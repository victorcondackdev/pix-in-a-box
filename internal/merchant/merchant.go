// Package merchant is the example merchant backend: it receives the
// notifications relayd delivers, verifies their Standard Webhooks
// signature with webhook-relay-go's signature package, deduplicates on
// webhook-id (deliveries are at-least-once) and keeps what it saw so the
// end-to-end scenarios can assert on it.
package merchant

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/victorcondackdev/webhook-relay-go/signature"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/metrics"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// Event is a verified, deduplicated notification.
type Event struct {
	WebhookID  string          `json:"webhook_id"`
	Type       string          `json:"type"`
	Timestamp  time.Time       `json:"timestamp"`
	Data       json.RawMessage `json:"data"`
	TraceID    string          `json:"trace_id,omitempty"`
	ReceivedAt time.Time       `json:"received_at"`
	// Deliveries counts every verified request for this webhook-id,
	// duplicates and retries included.
	Deliveries int `json:"deliveries"`
}

// Server is the receiver.
type Server struct {
	verifier *signature.Verifier
	log      *slog.Logger
	reg      *metrics.Registry
	received *metrics.CounterVec
	httpm    httpx.ServerMetrics

	mu       sync.Mutex
	events   []*Event
	byID     map[string]*Event
	failNext int
}

// New returns a receiver accepting signatures made with any of secrets
// (several during a rotation).
func New(log *slog.Logger, secrets ...string) (*Server, error) {
	v, err := signature.NewVerifier(secrets...)
	if err != nil {
		return nil, err
	}
	reg := metrics.NewRegistry()
	s := &Server{verifier: v, log: log, reg: reg, byID: map[string]*Event{}, httpm: httpx.NewServerMetrics(reg, "merchant")}
	s.received = reg.NewCounter("merchant_webhooks_received_total", "Webhook requests received, by event type and result (accepted, duplicate, invalid_signature, malformed, injected_failure).", "type", "result")
	return s, nil
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", s.receive)
	mux.HandleFunc("GET /events", s.list)
	mux.HandleFunc("POST /admin/fail-next", s.setFailNext)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", s.reg.Handler())
	return httpx.Instrument(mux, s.httpm, s.log)
}

func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, httpx.MaxBody))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if err := s.verifier.Verify(r.Header, body); err != nil {
		s.received.Inc("unknown", "invalid_signature")
		s.log.WarnContext(r.Context(), "webhook signature rejected", slog.Any("error", err))
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	var env struct {
		Type      string          `json:"type"`
		Timestamp time.Time       `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Type == "" {
		s.received.Inc("unknown", "malformed")
		http.Error(w, "malformed body", http.StatusBadRequest)
		return
	}
	id := r.Header.Get(signature.HeaderID)

	s.mu.Lock()
	if s.failNext > 0 {
		s.failNext--
		s.mu.Unlock()
		// Simulated outage: relayd must retry (5xx is retried on its
		// schedule, and webhook-id stays the same).
		s.received.Inc(env.Type, "injected_failure")
		s.log.InfoContext(r.Context(), "injected failure", slog.String("webhook_id", id), slog.String("type", env.Type))
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	ev, dup := s.byID[id]
	if dup {
		ev.Deliveries++
	} else {
		ev = &Event{WebhookID: id, Type: env.Type, Timestamp: env.Timestamp, Data: env.Data, ReceivedAt: time.Now().UTC(), Deliveries: 1}
		if sp, ok := trace.From(r.Context()); ok {
			ev.TraceID = sp.TraceIDString()
		}
		s.byID[id] = ev
		s.events = append(s.events, ev)
	}
	s.mu.Unlock()

	result := "accepted"
	if dup {
		result = "duplicate"
	}
	s.received.Inc(env.Type, result)
	s.log.InfoContext(r.Context(), "webhook received", slog.String("webhook_id", id), slog.String("type", env.Type),
		slog.String("result", result), slog.String("attempt_timestamp", r.Header.Get(signature.HeaderTimestamp)))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	typ := r.URL.Query().Get("type")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	s.mu.Lock()
	out := []Event{}
	for i := len(s.events) - 1; i >= 0; i-- {
		if typ != "" && s.events[i].Type != typ {
			continue
		}
		out = append(out, *s.events[i])
		if limit > 0 && len(out) == limit {
			break
		}
	}
	s.mu.Unlock()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"events": out})
}

// setFailNext makes the next N deliveries fail with 503, to watch relayd
// retry.
func (s *Server) setFailNext(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count int `json:"count"`
	}
	if !httpx.DecodeJSON(w, r, &req) {
		return
	}
	if req.Count < 0 || req.Count > 100 {
		httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/validation-error", Title: "count must be 0-100", Status: http.StatusUnprocessableEntity})
		return
	}
	s.mu.Lock()
	s.failNext = req.Count
	s.mu.Unlock()
	httpx.WriteJSON(w, http.StatusOK, map[string]int{"fail_next": req.Count})
}
