package merchant

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/victorcondackdev/webhook-relay-go/signature"
)

const secret = "whsec_cGl4LWluLWEtYm94LXRlc3Qtc2VjcmV0LTEyMzQ="

func post(t *testing.T, h http.Handler, id string, body []byte, sign bool) int {
	t.Helper()
	req := httptest.NewRequest("POST", "/webhooks", bytes.NewReader(body))
	if sign {
		signature.Signer{Secrets: []signature.Secret{signature.MustParseSecret(secret)}}.SetHeaders(req.Header, id, time.Now(), body)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestReceiverVerifiesDeduplicatesAndFails(t *testing.T) {
	s, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), secret)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	body := []byte(`{"type":"charge.paid","timestamp":"2026-11-21T12:00:00Z","data":{"charge_id":"ch_1"}}`)
	if c := post(t, h, "msg_1", body, false); c != 401 {
		t.Fatalf("unsigned: %d", c)
	}
	// An injected outage makes the first delivery fail; the retry succeeds.
	req := httptest.NewRequest("POST", "/admin/fail-next", bytes.NewReader([]byte(`{"count":1}`)))
	h.ServeHTTP(httptest.NewRecorder(), req)
	if c := post(t, h, "msg_1", body, true); c != 503 {
		t.Fatalf("injected failure: %d", c)
	}
	if c := post(t, h, "msg_1", body, true); c != 204 {
		t.Fatalf("delivery: %d", c)
	}
	if c := post(t, h, "msg_1", body, true); c != 204 {
		t.Fatalf("duplicate must still be acknowledged: %d", c)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/events?type=charge.paid", nil))
	var out struct{ Events []Event }
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Events) != 1 || out.Events[0].Deliveries != 2 || out.Events[0].WebhookID != "msg_1" {
		t.Fatalf("events: %s", rec.Body)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{`merchant_webhooks_received_total{type="charge.paid",result="accepted"} 1`, `result="duplicate"} 1`, `result="injected_failure"} 1`} {
		if !bytes.Contains(rec.Body.Bytes(), []byte(want)) {
			t.Errorf("metrics missing %s", want)
		}
	}
}
