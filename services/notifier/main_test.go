package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestEventType(t *testing.T) {
	for in, want := range map[string]string{
		"payments.charge.paid.v1":              "charge.paid",
		"payments.charge.created.v1":           "charge.created",
		"payments.charge.cancelled.v2":         "charge.cancelled",
		"payments.boleto_payment.scheduled.v1": "boleto_payment.scheduled",
	} {
		if got := eventType(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}

type fakeRelay struct {
	mu       sync.Mutex
	failures int
	bodies   []map[string]any
	keys     []string
	seen     map[string]bool
}

func (f *fakeRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path != "/v1/messages" || r.Header.Get("Authorization") != "Bearer tok" || !traceparentRe.MatchString(r.Header.Get("traceparent")) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if f.failures > 0 {
		f.failures--
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	key := r.Header.Get("Idempotency-Key")
	f.keys = append(f.keys, key)
	if f.seen[key] {
		w.WriteHeader(http.StatusOK) // relayd replays an existing message
		return
	}
	f.seen[key] = true
	f.bodies = append(f.bodies, body)
	w.WriteHeader(http.StatusAccepted)
}

func newNotifier(t *testing.T, relay http.Handler) *notifier {
	srv := httptest.NewServer(relay)
	t.Cleanup(srv.Close)
	return &notifier{relay: srv.Client(), cfg: config{relayURL: srv.URL, relayToken: "tok"},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), m: newMetrics()}
}

const chargePaid = `{"specversion":"1.0","id":"01a1fec2-9717-7626-b056-1b549bf54411","type":"payments.charge.paid.v1",
 "source":"urn:payments-service","subject":"charge/ch_1","time":"2026-11-21T15:38:47Z","datacontenttype":"application/json",
 "data":{"charge_id":"ch_1","txid":"TX1","amount_cents":1990,"end_to_end_id":"E12345678202611211200000000000001","paid_at":"2026-11-21T15:38:47Z"}}`

func TestHandleRetriesAndDeduplicates(t *testing.T) {
	relay := &fakeRelay{failures: 2, seen: map[string]bool{}}
	n := newNotifier(t, relay)
	rec := &kgo.Record{Topic: "payments.events.v1", Value: []byte(chargePaid)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	// A re-read after a crash maps onto the same relayd message.
	if err := n.handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	relay.mu.Lock()
	defer relay.mu.Unlock()
	if len(relay.bodies) != 1 || relay.keys[0] != "ce:01a1fec2-9717-7626-b056-1b549bf54411" || len(relay.keys) != 2 {
		t.Fatalf("bodies %d keys %v", len(relay.bodies), relay.keys)
	}
	b := relay.bodies[0]
	payload := b["payload"].(map[string]any)
	data := payload["data"].(map[string]any)
	if b["event_type"] != "charge.paid" || payload["type"] != "payments.charge.paid.v1" || data["charge_id"] != "ch_1" {
		t.Fatalf("relay message %v", b)
	}
}

func TestHandleSkipsPoisonRecords(t *testing.T) {
	relay := &fakeRelay{seen: map[string]bool{}}
	n := newNotifier(t, relay)
	if err := n.handle(context.Background(), &kgo.Record{Value: []byte("not json")}); err != nil {
		t.Fatal(err)
	}
	if len(relay.keys) != 0 {
		t.Fatal("poison record reached relayd")
	}
}

func TestHandleStopsOnCancel(t *testing.T) {
	n := newNotifier(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := n.handle(ctx, &kgo.Record{Value: []byte(chargePaid)}); err == nil {
		t.Fatal("handle must give up when the context ends, leaving the offset uncommitted")
	}
}
