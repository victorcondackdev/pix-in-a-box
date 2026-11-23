package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

const (
	payeeKey  = "joao@example.com"
	payeeKey2 = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"
)

func setupPayees(e *env) {
	created := time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)
	e.sim.addKey(payeeKey, "Joao Lima", "10000002", "200001", created)
	e.sim.addKey(payeeKey2, "Maria Mula", "10000009", "900009", created)
}

func TestPayoutHappyPath(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("ana", 1_000_00)

	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	tr, r := e.pay("ana", payeeKey, 125_00, "k1", "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	if r.status != http.StatusAccepted {
		t.Fatalf("status %d: %s", r.status, r.body)
	}
	if r.header.Get(trace.ResponseHeader) != traceID || tr.TraceID != traceID {
		t.Fatalf("trace not continued: header %q transfer %q", r.header.Get(trace.ResponseHeader), tr.TraceID)
	}
	if tr.Risk == nil || tr.Risk.Action != "ALLOW" {
		t.Fatalf("risk: %+v", tr.Risk)
	}
	if !strings.HasPrefix(tr.EndToEndID, "E"+testISPB) || len(tr.EndToEndID) != 32 {
		t.Fatalf("EndToEndId %q must be generated with this PSP's ISPB", tr.EndToEndID)
	}
	if tr.Ledger.Hold == "" {
		t.Fatal("no ledger hold")
	}
	done := e.waitStatus(tr.ID, StatusSettled)
	if done.Ledger.Post == "" || done.FinishedAt == nil {
		t.Fatalf("settled without posting: %+v", done)
	}
	a := e.account("ana")
	if a.BalanceCents != 875_00 || a.AvailableCents != 875_00 {
		t.Fatalf("balance after payout: %+v", a)
	}
	e.eventually("velocity recorded in riskd", func() bool { return e.risk.recordedCount() == 1 })
	e.eventually("merchant notified", func() bool { return len(e.merchantEvents(EventPayoutSettled)) == 1 })
	ev := e.merchantEvents(EventPayoutSettled)[0]
	if ev.TraceID != traceID {
		t.Fatalf("merchant webhook trace %q, want %q (traceparent must flow gateway → relayd → merchant)", ev.TraceID, traceID)
	}
	var payload map[string]any
	_ = json.Unmarshal(ev.Data, &payload)
	if payload["end_to_end_id"] != tr.EndToEndID || payload["status"] != "SETTLED" {
		t.Fatalf("payload: %v", payload)
	}
	// The statement shows the hold, its release and the posting.
	var st StatementView
	e.do("GET", "/v1/ledger/accounts/customer:ana/statement?include_holds=true", nil).decode(t, &st)
	if len(st.Lines) != 4 { // deposit, held, released, posted
		t.Fatalf("statement lines: %+v", st.Lines)
	}
	var rec Reconciliation
	e.do("GET", "/v1/reconciliation/spi", nil).decode(t, &rec)
	if !rec.InSync {
		t.Fatalf("reconciliation: %+v", rec)
	}
	e.assertBooksBalanced()
}

func TestPayoutIdempotency(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("bia", 1_000_00)
	first, r1 := e.pay("bia", payeeKey, 10_00, "same-key")
	second, r2 := e.pay("bia", payeeKey, 10_00, "same-key")
	if r1.status != 202 || r2.status != 202 || first.ID != second.ID || r2.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %d %s %s", r1.status, r2.status, first.ID, second.ID)
	}
	_, r3 := e.pay("bia", payeeKey, 11_00, "same-key")
	if r3.status != http.StatusUnprocessableEntity || !strings.Contains(string(r3.body), "idempotency-key-reused") {
		t.Fatalf("reuse with another body: %d %s", r3.status, r3.body)
	}
	if r := e.do("POST", "/v1/pix/transfers", TransferRequest{AccountID: "bia", Key: payeeKey, AmountCents: 1}); r.status != 400 {
		t.Fatalf("missing key: %d", r.status)
	}
	e.waitStatus(first.ID, StatusSettled)
	e.sim.mu.Lock()
	calls := e.sim.createCalls
	e.sim.mu.Unlock()
	if calls != 1 {
		t.Fatalf("SPI saw %d submissions, want 1", calls)
	}
	if a := e.account("bia"); a.BalanceCents != 990_00 {
		t.Fatalf("balance %d", a.BalanceCents)
	}
}

func TestRiskBlockKeepsFundsAndExplains(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("caio", 100_00)
	tr, r := e.pay("caio", payeeKey, 13_13, "b1")
	if r.status != http.StatusUnprocessableEntity || tr.Status != StatusBlocked {
		t.Fatalf("want 422 BLOCKED, got %d %s", r.status, r.body)
	}
	if tr.Risk == nil || len(tr.Risk.Hits) != 1 || tr.Risk.Hits[0].RuleID != "test.block" || len(tr.Risk.Hits[0].Features) == 0 {
		t.Fatalf("explanation missing: %+v", tr.Risk)
	}
	if tr.Ledger.Hold != "" {
		t.Fatal("blocked transfer must not touch the ledger")
	}
	if a := e.account("caio"); a.AvailableCents != 100_00 {
		t.Fatalf("available %d", a.AvailableCents)
	}
	e.sim.mu.Lock()
	defer e.sim.mu.Unlock()
	if e.sim.createCalls != 0 {
		t.Fatal("blocked transfer reached the SPI")
	}
}

// TestNightATOWithDefaultRuleset runs the demo's account-takeover scenario
// against the real pix-default ruleset: a small first transfer from a
// device seen minutes ago goes through, the next one in the same night
// burst is blocked by ato.new_device.night_drain.
func TestNightATOWithDefaultRuleset(t *testing.T) {
	rs, err := os.ReadFile(filepath.Join("..", "..", "deploy", "riskd", "pix-in-a-box.json"))
	if err != nil {
		t.Fatal(err)
	}
	e := newEnv(t, rs)
	keyCreated := time.Now().Add(-time.Hour)
	e.sim.addKey("mula1@example.com", "Mula Um", "10000009", "900001", keyCreated)
	e.sim.addKey("mula2@example.com", "Mula Dois", "10000009", "900002", keyCreated)
	e.openAccount("vitima", 5_000_00, func(r *OpenAccountRequest) { r.NightLimitCents = 1_000_00; r.DayLimitCents = 5_000_00 })

	night := time.Date(2026, 11, 21, 2, 30, 0, 0, time.FixedZone("BRT", -3*3600))
	device := &DeviceInfo{ID: "dev-new", Trusted: false, FirstSeen: ptr(night.Add(-12 * time.Minute))}
	send := func(key string, cents int64, at time.Time, idemKey string) (Transfer, resp) {
		r := e.do("POST", "/v1/pix/transfers", TransferRequest{AccountID: "vitima", Key: key, AmountCents: cents, Device: device, Channel: "app"},
			"Idempotency-Key", idemKey, "X-Sim-Time", at.Format(time.RFC3339))
		var t Transfer
		_ = json.Unmarshal(r.body, &t)
		return t, r
	}
	first, r := send("mula1@example.com", 180_00, night, "ato-1")
	if r.status != 202 || first.Risk.Action == "BLOCK" {
		t.Fatalf("first transfer: %d %s", r.status, r.body)
	}
	e.waitStatus(first.ID, StatusSettled)
	e.eventually("first transfer recorded in velocity", func() bool { return e.risk.recordedCount() == 1 })
	if got := e.risk.lastEval().Time; !got.Equal(night) {
		t.Fatalf("risk evaluated at %v, want X-Sim-Time %v", got, night)
	}

	second, r := send("mula2@example.com", 450_00, night.Add(3*time.Minute), "ato-2")
	if r.status != http.StatusUnprocessableEntity || second.Status != StatusBlocked {
		t.Fatalf("second transfer: %d %s", r.status, r.body)
	}
	var ato bool
	for _, h := range second.Risk.Hits {
		if h.RuleID == "ato.new_device.night_drain" {
			ato = true
		}
	}
	if !ato {
		t.Fatalf("expected ato.new_device.night_drain among hits: %+v", second.Risk.Hits)
	}
	if a := e.account("vitima"); a.BalanceCents != 5_000_00-180_00 || a.AvailableCents != a.BalanceCents {
		t.Fatalf("balance %+v", a)
	}
	e.assertBooksBalanced()
}

func TestSPIRejectionVoidsHold(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("dani", 500_00)
	tr, r := e.pay("dani", payeeKey, 200_00, "r1", "X-Sim-Fail", "AM04")
	if r.status != 202 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if a := e.account("dani"); a.AvailableCents != 300_00 || a.BalanceCents != 500_00 {
		t.Fatalf("hold not applied: %+v", a)
	}
	done := e.waitStatus(tr.ID, StatusRejected)
	if done.FailureReason != "AM04" || done.Ledger.Void == "" {
		t.Fatalf("rejection: %+v", done)
	}
	if a := e.account("dani"); a.AvailableCents != 500_00 || a.BalanceCents != 500_00 {
		t.Fatalf("hold not released: %+v", a)
	}
	e.sim.mu.Lock()
	fail := e.sim.lastChaosFail
	e.sim.mu.Unlock()
	if fail != "AM04" {
		t.Fatalf("X-Sim-Fail not forwarded: %q", fail)
	}
	e.eventually("rejection notified", func() bool { return len(e.merchantEvents(EventPayoutRejected)) == 1 })
	e.assertBooksBalanced()
}

func TestSimHeadersIgnoredWhenDisabled(t *testing.T) {
	e := newEnv(t, testRuleset, func(c *Config) { c.SimHeaders = false })
	setupPayees(e)
	e.openAccount("edu", 500_00)
	tr, _ := e.pay("edu", payeeKey, 10_00, "s1", "X-Sim-Fail", "AM04")
	e.waitStatus(tr.ID, StatusSettled)
}

func TestHoldThenAnalystRelease(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("fabi", 500_00)
	tr, r := e.pay("fabi", payeeKey, 77_77, "h1")
	if r.status != 202 || tr.Status != StatusHeld || tr.Risk.HoldUntil == nil {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if d := tr.Risk.HoldUntil.Sub(tr.CreatedAt); d != 30*time.Minute {
		t.Fatalf("hold for %v, want riskd's hold_for 30m", d)
	}
	if a := e.account("fabi"); a.AvailableCents != 500_00-77_77 {
		t.Fatalf("funds not held: %+v", a)
	}
	var rec Reconciliation
	e.do("GET", "/v1/reconciliation/spi", nil).decode(t, &rec)
	if rec.UnsubmittedHoldCents != 77_77 || !rec.InSync {
		t.Fatalf("held funds never left the SPI account: %+v", rec)
	}
	e.eventually("held notified", func() bool { return len(e.merchantEvents(EventPayoutHeld)) == 1 })
	if r := e.do("POST", "/v1/pix/transfers/"+tr.ID+"/release", nil); r.status != 202 {
		t.Fatalf("release: %d %s", r.status, r.body)
	}
	e.waitStatus(tr.ID, StatusSettled)
	if r := e.do("POST", "/v1/pix/transfers/"+tr.ID+"/release", nil); r.status != 409 {
		t.Fatalf("second release: %d", r.status)
	}
}

func TestHoldCancelledByAnalyst(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("gil", 500_00)
	tr, _ := e.pay("gil", payeeKey, 77_77, "h2")
	if r := e.do("POST", "/v1/pix/transfers/"+tr.ID+"/cancel", nil); r.status != 200 {
		t.Fatalf("cancel: %d %s", r.status, r.body)
	}
	got := e.transfer(tr.ID)
	if got.Status != StatusCancelled || got.Ledger.Void == "" {
		t.Fatalf("%+v", got)
	}
	if a := e.account("gil"); a.AvailableCents != 500_00 {
		t.Fatalf("%+v", a)
	}
	e.assertBooksBalanced()
}

func TestStepUpConfirmAndExpiry(t *testing.T) {
	now := time.Now()
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	e := newEnv(t, testRuleset, WithClock(clock))
	setupPayees(e)
	e.openAccount("hugo", 500_00)

	tr, r := e.pay("hugo", payeeKey, 66_66, "su1")
	if r.status != 202 || tr.Status != StatusAwaitingConfirmation || tr.Challenge == nil {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/pix/transfers/"+tr.ID+"/confirm", map[string]string{"challenge_id": "wrong"}); r.status != 422 {
		t.Fatalf("wrong challenge: %d", r.status)
	}
	if r := e.do("POST", "/v1/pix/transfers/"+tr.ID+"/confirm", map[string]string{"challenge_id": tr.Challenge.ID}); r.status != 202 {
		t.Fatalf("confirm: %d %s", r.status, r.body)
	}
	e.waitStatus(tr.ID, StatusSettled)

	tr2, _ := e.pay("hugo", payeeKey, 66_66, "su2")
	advance(2 * time.Minute) // StepUpTTL is 1m in tests
	got := e.waitStatus(tr2.ID, StatusCancelled)
	if got.FailureReason != "STEP_UP_EXPIRED" || got.Ledger.Void == "" {
		t.Fatalf("%+v", got)
	}
	if a := e.account("hugo"); a.AvailableCents != 500_00-66_66 || a.BalanceCents != 500_00-66_66 {
		t.Fatalf("%+v", a)
	}
	e.assertBooksBalanced()
}

func TestRiskUnavailable(t *testing.T) {
	t.Run("fails closed by default", func(t *testing.T) {
		e := newEnv(t, testRuleset)
		setupPayees(e)
		e.openAccount("ivo", 100_00)
		e.risk.mu.Lock()
		e.risk.down = true
		e.risk.mu.Unlock()
		_, r := e.pay("ivo", payeeKey, 10_00, "u1")
		if r.status != http.StatusServiceUnavailable || !strings.Contains(string(r.body), "risk-unavailable") {
			t.Fatalf("%d %s", r.status, r.body)
		}
		if a := e.account("ivo"); a.AvailableCents != 100_00 {
			t.Fatalf("%+v", a)
		}
	})
	t.Run("fails open when configured", func(t *testing.T) {
		e := newEnv(t, testRuleset, func(c *Config) { c.RiskFailOpen = true })
		setupPayees(e)
		e.openAccount("ivo", 100_00)
		e.risk.mu.Lock()
		e.risk.down = true
		e.risk.mu.Unlock()
		tr, r := e.pay("ivo", payeeKey, 10_00, "u2")
		if r.status != 202 || !tr.Risk.Unavailable {
			t.Fatalf("%d %s", r.status, r.body)
		}
		e.waitStatus(tr.ID, StatusSettled)
	})
}

func TestValidationAndDICTErrors(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("jana", 10_00)
	cases := []struct {
		name   string
		req    TransferRequest
		status int
		typ    string
	}{
		{"unknown account", TransferRequest{AccountID: "nobody", Key: payeeKey, AmountCents: 1}, 422, "validation-error"},
		{"bad amount", TransferRequest{AccountID: "jana", Key: payeeKey, AmountCents: 0}, 422, "validation-error"},
		{"bad key", TransferRequest{AccountID: "jana", Key: "not a key", AmountCents: 1}, 422, "validation-error"},
		{"key not in DICT", TransferRequest{AccountID: "jana", Key: "ninguem@example.com", AmountCents: 1}, 422, "key-not-found"},
		{"insufficient funds", TransferRequest{AccountID: "jana", Key: payeeKey, AmountCents: 20_00}, 422, "insufficient-funds"},
	}
	for i, c := range cases {
		r := e.do("POST", "/v1/pix/transfers", c.req, "Idempotency-Key", fmt.Sprint("v", i))
		if r.status != c.status || !strings.Contains(string(r.body), "/problems/"+c.typ) {
			t.Errorf("%s: %d %s", c.name, r.status, r.body)
		}
	}
	if r := e.do("POST", "/v1/pix/transfers", map[string]any{"account_id": "jana", "unknown": 1}, "Idempotency-Key", "x"); r.status != 400 {
		t.Errorf("unknown field: %d", r.status)
	}
}

func TestSPIRefusalFailsTransfer(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("kai", 100_00)
	e.sim.mu.Lock()
	e.sim.refuseCreate = 422
	e.sim.mu.Unlock()
	tr, r := e.pay("kai", payeeKey, 10_00, "f1")
	if r.status != 422 || tr.Status != StatusFailed || tr.FailureReason != "INVALID_AMOUNT" || tr.Ledger.Void == "" {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if a := e.account("kai"); a.AvailableCents != 100_00 {
		t.Fatalf("%+v", a)
	}
}

func TestPollerSettlesWhenWebhookIsLost(t *testing.T) {
	e := newEnv(t, testRuleset, func(c *Config) { c.PollAfter = 50 * time.Millisecond })
	setupPayees(e)
	e.openAccount("lia", 100_00)
	e.sim.mu.Lock()
	e.sim.dropWebhooks = true
	e.sim.mu.Unlock()
	tr, _ := e.pay("lia", payeeKey, 10_00, "p1")
	e.waitStatus(tr.ID, StatusSettled)
	e.assertBooksBalanced()
}

func TestPollerFailsTransferTheSPINeverSaw(t *testing.T) {
	e := newEnv(t, testRuleset, func(c *Config) { c.PollAfter = 50 * time.Millisecond })
	setupPayees(e)
	e.openAccount("leo", 100_00)
	e.sim.mu.Lock()
	e.sim.failCreates = 3 // every attempt of the submission fails with 503
	e.sim.mu.Unlock()
	tr, r := e.pay("leo", payeeKey, 10_00, "p2")
	if r.status != 202 || tr.Status != StatusSubmitted {
		t.Fatalf("unknown outcome must stay SUBMITTED: %d %s", r.status, r.body)
	}
	got := e.waitStatus(tr.ID, StatusFailed)
	if got.FailureReason != "NOT_RECEIVED" {
		t.Fatalf("%+v", got)
	}
	if a := e.account("leo"); a.AvailableCents != 100_00 {
		t.Fatalf("%+v", a)
	}
}

func TestSubmissionRetriesTransientErrors(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("lu", 100_00)
	e.sim.mu.Lock()
	e.sim.failCreates = 2
	e.sim.mu.Unlock()
	tr, _ := e.pay("lu", payeeKey, 10_00, "p3")
	e.waitStatus(tr.ID, StatusSettled)
}

func TestSPIWebhookSecurityAndDuplicates(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("mel", 100_00)
	// Unsigned and wrongly signed deliveries are rejected.
	if r := e.do("POST", "/v1/webhooks/spi", map[string]string{"id": "evt_x", "type": "pix.settled"}); r.status != 401 {
		t.Fatalf("unsigned: %d", r.status)
	}
	if r := e.do("POST", "/v1/webhooks/spi", map[string]string{"id": "evt_x"}, "X-Signature", "t=1,v1=00"); r.status != 401 {
		t.Fatalf("bad signature: %d", r.status)
	}
	// Chaos duplicates (same event ID, sent concurrently) settle once.
	tr, _ := e.pay("mel", payeeKey, 10_00, "d1", "X-Sim-Webhook-Duplicates", "3")
	e.waitStatus(tr.ID, StatusSettled)
	e.eventually("duplicates acknowledged", func() bool {
		return e.svc.metrics.SPIWebhooks.Value("pix.settled", "duplicate") == 3
	})
	if v := e.svc.metrics.SPIWebhooks.Value("pix.settled", "processed"); v != 1 {
		t.Fatalf("processed %v times", v)
	}
	if a := e.account("mel"); a.BalanceCents != 90_00 {
		t.Fatalf("%+v", a)
	}
	e.assertBooksBalanced()
}

func TestInboundChargePayment(t *testing.T) {
	e := newEnv(t, testRuleset)
	r := e.do("POST", "/v1/charges", map[string]any{"amount_cents": 49_90, "description": "Pedido 42"}, "Idempotency-Key", "c1")
	if r.status != 201 {
		t.Fatalf("charge: %d %s", r.status, r.body)
	}
	var ch paymentsCharge
	r.decode(t, &ch)
	if r := e.do("POST", "/v1/charges", map[string]any{"amount_cents": 1}); r.status != 400 {
		t.Fatalf("payments-service problems must pass through: %d", r.status)
	}

	// payments-service is briefly down: the webhook asks for redelivery,
	// the ledger credit is not duplicated.
	e.payments.mu.Lock()
	e.payments.failNext = 1
	e.payments.mu.Unlock()
	e2e := "E99999999202611211200abcdefghijk"
	if st := e.sim.receive(e2e, ch.TxID, "Loja@PixInABox.dev", 49_90); st != 200 {
		t.Fatalf("webhook status %d", st)
	}
	in, ok := e.svc.Received(e2e)
	if !ok || in.ForwardResult != "settled" || in.ChargeStatus != "PAID" || in.CreditedTo != MerchantAccount("loja") {
		t.Fatalf("inbound: %+v", in)
	}
	e.payments.mu.Lock()
	hits, bad := e.payments.settleHit, e.payments.badSigs
	e.payments.mu.Unlock()
	if hits != 2 || bad != 0 {
		t.Fatalf("payments-service settle calls %d (bad signatures %d), want 2", hits, bad)
	}
	var st StatementView
	e.do("GET", "/v1/ledger/accounts/merchant:loja/statement", nil).decode(t, &st)
	if st.BalanceCents != 49_90 || len(st.Lines) != 1 {
		t.Fatalf("merchant credited %d in %d lines", st.BalanceCents, len(st.Lines))
	}
	var got paymentsCharge
	e.do("GET", "/v1/charges/"+ch.ID, nil).decode(t, &got)
	if got.Status != "PAID" {
		t.Fatalf("charge %+v", got)
	}
	var rec Reconciliation
	e.do("GET", "/v1/reconciliation/spi", nil).decode(t, &rec)
	if !rec.InSync {
		t.Fatalf("%+v", rec)
	}
	e.assertBooksBalanced()
}

func TestInboundUnknownKeyGoesToSuspense(t *testing.T) {
	e := newEnv(t, testRuleset)
	e2e := "E99999999202611211200zzzzzzzzzzz"
	if st := e.sim.receive(e2e, "", "outra@chave.com", 10_00); st != 200 {
		t.Fatalf("%d", st)
	}
	in, _ := e.svc.Received(e2e)
	if in.CreditedTo != AcctSuspense || in.ForwardResult != "skipped" {
		t.Fatalf("%+v", in)
	}
}

func TestRefundsPartialFullAndWebhookDedup(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("nina", 1_000_00)

	full, _ := e.pay("nina", payeeKey, 125_00, "rf1")
	e.waitStatus(full.ID, StatusSettled)
	r := e.do("POST", "/v1/pix/transfers/"+full.EndToEndID+"/refunds", RefundRequest{AmountCents: 125_00}, "Idempotency-Key", "rf1-r")
	if r.status != 201 {
		t.Fatalf("full refund: %d %s", r.status, r.body)
	}
	var out struct {
		Refund   Refund   `json:"refund"`
		Transfer Transfer `json:"transfer"`
	}
	r.decode(t, &out)
	if out.Refund.LedgerKind != "reversal" || out.Transfer.RefundedCents != 125_00 || out.Refund.Reason != "MD06" {
		t.Fatalf("full refund must be a ledger reversal: %+v", out)
	}

	part, _ := e.pay("nina", payeeKey, 300_00, "rf2")
	e.waitStatus(part.ID, StatusSettled)
	r = e.do("POST", "/v1/pix/transfers/"+part.ID+"/refunds", RefundRequest{AmountCents: 100_00, Reason: "BE08"}, "Idempotency-Key", "rf2-a")
	r.decode(t, &out)
	if r.status != 201 || out.Refund.LedgerKind != "compensating" {
		t.Fatalf("partial: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/pix/transfers/"+part.ID+"/refunds", RefundRequest{AmountCents: 250_00}, "Idempotency-Key", "rf2-b"); r.status != 422 {
		t.Fatalf("over-refund: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/pix/transfers/"+part.ID+"/refunds", RefundRequest{AmountCents: 1, Reason: "FR01"}, "Idempotency-Key", "rf2-c"); r.status != 422 {
		t.Fatalf("FR01 outside MED: %d", r.status)
	}
	// refund.completed webhooks for refunds already booked are no-ops.
	e.eventually("refund webhooks processed", func() bool {
		return e.svc.metrics.SPIWebhooks.Value("refund.completed", "duplicate") >= 2
	})
	if a := e.account("nina"); a.BalanceCents != 1_000_00-300_00+100_00 {
		t.Fatalf("balance %d", a.BalanceCents)
	}
	e.eventually("refund notifications", func() bool { return len(e.merchantEvents(EventPayoutRefunded)) == 2 })
	var rec Reconciliation
	e.do("GET", "/v1/reconciliation/spi", nil).decode(t, &rec)
	if !rec.InSync {
		t.Fatalf("%+v", rec)
	}
	e.assertBooksBalanced()
}

func TestRefundStartedOutsideTheGatewayIsBooked(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("olga", 100_00)
	tr, _ := e.pay("olga", payeeKey, 50_00, "o1")
	e.waitStatus(tr.ID, StatusSettled)
	// The payee's PSP returns funds on its own: only the webhook tells us.
	e.sim.mu.Lock()
	ref, _, err := e.sim.refundLocked(tr.EndToEndID, 20_00, "MD06", "")
	id := e.sim.nextEvent()
	e.sim.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if st := e.sim.deliver(id, "refund.completed", ref); st != 200 {
		t.Fatalf("%d", st)
	}
	if a := e.account("olga"); a.BalanceCents != 70_00 {
		t.Fatalf("%+v", a)
	}
}

func TestMEDReturnsFundsAndMarksBeneficiary(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("paulo", 1_000_00)
	tr, _ := e.pay("paulo", payeeKey2, 300_00, "m1")
	e.waitStatus(tr.ID, StatusSettled)
	r := e.do("POST", "/v1/pix/transfers/"+tr.ID+"/refunds", RefundRequest{AmountCents: 100_00}, "Idempotency-Key", "m1-r")
	if r.status != 201 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	r = e.do("POST", "/v1/pix/transfers/"+tr.ID+"/med", map[string]string{"details": "golpe do falso parente"}, "Idempotency-Key", "m1-med")
	if r.status != 201 {
		t.Fatalf("MED: %d %s", r.status, r.body)
	}
	var res MEDResult
	r.decode(t, &res)
	if res.Infraction.Status != "REFUNDED" || res.Refund == nil || res.Refund.Reason != "FR01" || res.Refund.AmountCents != 200_00 || res.FraudMarks != 1 {
		t.Fatalf("MED result: %+v", res)
	}
	if res.Transfer.RefundedCents != 300_00 {
		t.Fatalf("refunded %d", res.Transfer.RefundedCents)
	}
	if a := e.account("paulo"); a.BalanceCents != 1_000_00 {
		t.Fatalf("balance %d", a.BalanceCents)
	}
	// The MED report feeds the next risk decision for that beneficiary.
	next, _ := e.pay("paulo", payeeKey2, 10_00, "m2")
	if next.Risk == nil || next.Risk.Action != "REVIEW" || e.risk.lastEval().Beneficiary.FraudMarks != 1 {
		t.Fatalf("fraud mark not used: %+v", next.Risk)
	}
	e.waitStatus(next.ID, StatusSettled)
	e.assertBooksBalanced()
}

func TestReadinessAndMetrics(t *testing.T) {
	e := newEnv(t, testRuleset)
	r := e.do("GET", "/readyz", nil)
	if r.status != 200 {
		t.Fatalf("readyz %d %s", r.status, r.body)
	}
	setupPayees(e)
	e.openAccount("quim", 100_00)
	tr, _ := e.pay("quim", payeeKey, 10_00, "mt1")
	e.waitStatus(tr.ID, StatusSettled)
	m := string(e.do("GET", "/metrics", nil).body)
	for _, want := range []string{
		`gateway_pix_transfers_total{status="SETTLED"} 1`,
		`gateway_risk_decisions_total{action="ALLOW"} 1`,
		`gateway_ledger_postings_total{kind="pending"} 1`,
		`gateway_ledger_postings_total{kind="post_pending"} 1`,
		`gateway_spi_webhooks_total{type="pix.settled",result="processed"} 1`,
		`gateway_http_request_duration_seconds_bucket{route="POST /v1/pix/transfers",le="+Inf"} 1`,
		`gateway_downstream_requests_total{service="riskd",code="200"}`,
		"gateway_ready 1",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// TestConcurrentPayouts hammers one wallet: the ledger never lets it go
// negative, every accepted transfer settles, and the books balance.
func TestConcurrentPayouts(t *testing.T) {
	e := newEnv(t, testRuleset)
	setupPayees(e)
	e.openAccount("rui", 100_00)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := []string{}
	insufficient := 0
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, r := e.pay("rui", payeeKey, 7_00, fmt.Sprint("cc", i))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.status == 202:
				accepted = append(accepted, tr.ID)
			case strings.Contains(string(r.body), "insufficient-funds"):
				insufficient++
			default:
				t.Errorf("unexpected %d %s", r.status, r.body)
			}
		}(i)
	}
	wg.Wait()
	if len(accepted) != 14 || insufficient != 16 { // 14 × 7,00 = 98,00 ≤ 100,00
		t.Fatalf("accepted %d, insufficient %d", len(accepted), insufficient)
	}
	for _, id := range accepted {
		e.waitStatus(id, StatusSettled)
	}
	if a := e.account("rui"); a.BalanceCents != 2_00 {
		t.Fatalf("%+v", a)
	}
	e.assertBooksBalanced()
}
