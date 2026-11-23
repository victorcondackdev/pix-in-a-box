package gateway

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/fraud-rules-go/engine"
	"github.com/victorcondackdev/fraud-rules-go/velocity"
	relayapi "github.com/victorcondackdev/webhook-relay-go/api"
	relayeng "github.com/victorcondackdev/webhook-relay-go/relay"

	"github.com/victorcondackdev/pix-psp-simulator/webhook"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/pixsim"
	"github.com/victorcondackdev/pix-in-a-box/internal/merchant"
)

const (
	testISPB        = "13370001"
	testSPIAccount  = "pib"
	testSimSecret   = "pixsim-test-secret"
	testPaySecret   = "payments-test-secret"
	testMerchantKey = "loja@pixinabox.dev"
	testWhsec       = "whsec_cGl4LWluLWEtYm94LXRlc3Qtc2VjcmV0LTEyMzQ=" // base64("pix-in-a-box-test-secret-1234")
	testRelayToken  = "relay-test-token"
)

// ---------------------------------------------------------------------
// fakeSim mimics pix-psp-simulator's HTTP API (internal/api/server.go):
// same routes, same JSON shapes, strict decoding where the simulator is
// strict, ACSP then an asynchronous signed webhook with ACSC or RJCT.

type simTransferRequest struct { // internal/sim.TransferRequest
	FromAccountID string          `json:"from_account_id"`
	Key           string          `json:"key,omitempty"`
	ToAccountID   string          `json:"to_account_id,omitempty"`
	ToAccount     json.RawMessage `json:"to_account,omitempty"`
	AmountCents   int64           `json:"amount_cents"`
	EndToEndID    string          `json:"end_to_end_id,omitempty"`
	Description   string          `json:"description,omitempty"`
}

type simRefundRequest struct { // internal/sim.RefundRequest
	AmountCents int64  `json:"amount_cents"`
	Reason      string `json:"reason,omitempty"`
	Description string `json:"description,omitempty"`
}

type fakeSim struct {
	t   *testing.T
	srv *httptest.Server

	mu           sync.Mutex
	webhookURL   string
	balance      int64 // the PSP's settlement account
	keys         map[string]pixsim.KeyLookup
	payeeBalance map[string]int64
	transfers    map[string]*pixsim.Transfer
	idem         map[string]string
	refunds      map[string][]pixsim.Refund
	infractions  map[string]*pixsim.Infraction
	seq          int

	dropWebhooks   bool // never send webhooks (exercise the poller)
	failCreates    int  // answer 503 to the next N POST /pix/transfers
	lose           bool // with failCreates: the transfer is never stored
	refuseCreate   int  // non-zero: answer this status (with code invalid_amount)
	createCalls    int
	webhooksSent   int
	lastChaosFail  string
	duplicatesSent int
}

func newFakeSim(t *testing.T) *fakeSim {
	f := &fakeSim{t: t, balance: 100_000_000, keys: map[string]pixsim.KeyLookup{}, payeeBalance: map[string]int64{},
		transfers: map[string]*pixsim.Transfer{}, idem: map[string]string{}, refunds: map[string][]pixsim.Refund{}, infractions: map[string]*pixsim.Infraction{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeTestJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /accounts/{id}", f.account)
	mux.HandleFunc("GET /dict/keys/{key}", f.lookup)
	mux.HandleFunc("POST /pix/transfers", f.createTransfer)
	mux.HandleFunc("GET /pix/transfers/{e2eid}", f.getTransfer)
	mux.HandleFunc("POST /pix/transfers/{e2eid}/refunds", f.createRefund)
	mux.HandleFunc("GET /pix/transfers/{e2eid}/refunds/{id}", f.getRefund)
	mux.HandleFunc("POST /med/infractions", f.reportInfraction)
	mux.HandleFunc("POST /med/infractions/{id}/{action}", f.actOnInfraction)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// addKey registers a payee key at another PSP.
func (f *fakeSim) addKey(key, owner, ispb, number string, created time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kt := "EMAIL"
	if !strings.Contains(key, "@") {
		kt = "EVP"
	}
	f.keys[key] = pixsim.KeyLookup{Key: key, KeyType: kt, OwnerName: owner, OwnerTaxID: "***.444.777-**", OwnerType: "NATURAL_PERSON",
		ISPB: ispb, Branch: "0001", Number: number, AccountType: "CACC", CreatedAt: created, AccountID: "acc_" + number}
	f.payeeBalance["acc_"+number] = 0
}

func simProblem(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "urn:pixsim:problem:" + code, "status": status, "code": code, "detail": detail})
}

func strictDecode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (f *fakeSim) account(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") != testSPIAccount {
		simProblem(w, 404, "account_not_found", "account not found")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	writeTestJSON(w, 200, pixsim.Account{ID: testSPIAccount, ISPB: testISPB, Branch: "0001", Number: "100001", Type: "CACC", OwnerName: "Pix in a Box IP", Status: "ACTIVE", BalanceCents: f.balance})
}

func (f *fakeSim) lookup(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Payer-Id") == "" {
		f.t.Errorf("DICT lookup without X-Payer-Id")
	}
	f.mu.Lock()
	lk, ok := f.keys[r.PathValue("key")]
	f.mu.Unlock()
	if !ok {
		simProblem(w, 404, "key_not_found", "pix key not found in DICT")
		return
	}
	writeTestJSON(w, 200, lk)
}

func (f *fakeSim) nextEvent() string { f.seq++; return fmt.Sprintf("evt_%06d", f.seq) }

func (f *fakeSim) createTransfer(w http.ResponseWriter, r *http.Request) {
	var req simTransferRequest
	if err := strictDecode(r, &req); err != nil {
		simProblem(w, 400, "invalid_json", err.Error())
		return
	}
	f.mu.Lock()
	f.createCalls++
	if f.failCreates > 0 {
		f.failCreates--
		f.mu.Unlock()
		simProblem(w, 503, "unavailable", "spi unavailable")
		return
	}
	if f.refuseCreate != 0 {
		st := f.refuseCreate
		f.mu.Unlock()
		simProblem(w, st, "invalid_amount", "refused")
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if e2e, ok := f.idem[key]; ok {
			t := *f.transfers[e2e]
			f.mu.Unlock()
			w.Header().Set("Idempotent-Replayed", "true")
			writeTestJSON(w, 200, t)
			return
		}
		f.idem[key] = req.EndToEndID
	}
	if req.FromAccountID != testSPIAccount {
		f.mu.Unlock()
		simProblem(w, 404, "account_not_found", "payer not found")
		return
	}
	if _, dup := f.transfers[req.EndToEndID]; dup {
		f.mu.Unlock()
		simProblem(w, 409, "duplicate_end_to_end_id", "duplicate")
		return
	}
	lk, ok := f.keys[req.Key]
	if !ok {
		f.mu.Unlock()
		simProblem(w, 404, "key_not_found", "pix key not found in DICT")
		return
	}
	fail := strings.ToUpper(r.Header.Get("X-Sim-Fail"))
	f.lastChaosFail = fail
	dups := 0
	fmt.Sscan(r.Header.Get("X-Sim-Webhook-Duplicates"), &dups)
	tr := &pixsim.Transfer{EndToEndID: req.EndToEndID, Initiation: "DICT", Status: pixsim.StatusPending, AmountCents: req.AmountCents,
		Description: req.Description, CreatedAt: time.Now().UTC(),
		Payer: pixsim.Party{AccountID: testSPIAccount, ISPB: testISPB, Name: "Pix in a Box IP"},
		Payee: pixsim.Party{AccountID: lk.AccountID, ISPB: lk.ISPB, Name: lk.OwnerName, Key: lk.Key}}
	if fail == "" {
		f.balance -= req.AmountCents // reserved at ACSP, as the simulator does
	}
	f.transfers[tr.EndToEndID] = tr
	out := *tr
	f.mu.Unlock()
	w.Header().Set("Location", "/pix/transfers/"+tr.EndToEndID)
	writeTestJSON(w, http.StatusAccepted, out)
	go f.settle(tr.EndToEndID, fail, dups)
}

func (f *fakeSim) settle(e2e, fail string, dups int) {
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	tr := f.transfers[e2e]
	now := time.Now().UTC()
	tr.SettledAt = &now
	typ := webhook.EventPixSettled
	if fail != "" {
		tr.Status, tr.Reason, tr.ReasonDescription = pixsim.StatusRejected, fail, "Insufficient funds"
		typ = webhook.EventPixRejected
	} else {
		tr.Status = pixsim.StatusSettled
		f.payeeBalance[tr.Payee.AccountID] += tr.AmountCents
	}
	snap := *tr
	id := f.nextEvent()
	drop := f.dropWebhooks
	f.mu.Unlock()
	if drop {
		return
	}
	var wg sync.WaitGroup
	for i := 0; i <= dups; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); f.deliver(id, typ, snap) }()
	}
	wg.Wait()
}

// deliver posts a signed event and retries non-2xx a few times, like the
// simulator's dispatcher.
func (f *fakeSim) deliver(id, typ string, data any) int {
	raw, _ := json.Marshal(data)
	body, _ := json.Marshal(webhook.Event{ID: id, Type: typ, CreatedAt: time.Now().UTC(), AccountID: testSPIAccount, Data: raw})
	status := 0
	for attempt := 1; attempt <= 4; attempt++ {
		req, _ := http.NewRequest(http.MethodPost, f.webhookURL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(webhook.SignatureHeader, webhook.Sign([]byte(testSimSecret), time.Now(), body))
		req.Header.Set(webhook.EventIDHeader, id)
		req.Header.Set(webhook.EventTypeHeader, typ)
		req.Header.Set("X-Delivery-Attempt", fmt.Sprint(attempt))
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			status = resp.StatusCode
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			f.mu.Lock()
			f.webhooksSent++
			f.mu.Unlock()
			if status >= 200 && status < 300 {
				return status
			}
		}
		time.Sleep(time.Duration(attempt*30) * time.Millisecond)
	}
	return status
}

// receive simulates an external payer paying this PSP (pix.received).
func (f *fakeSim) receive(e2e, txid, key string, cents int64) int {
	f.mu.Lock()
	f.balance += cents
	tr := pixsim.Transfer{EndToEndID: e2e, TxID: txid, Initiation: "QRES", Status: pixsim.StatusSettled, AmountCents: cents, CreatedAt: time.Now().UTC(),
		Payer: pixsim.Party{AccountID: "maria", ISPB: "99999999", Name: "Maria Souza"},
		Payee: pixsim.Party{AccountID: testSPIAccount, ISPB: testISPB, Name: "Pix in a Box IP", Key: key}}
	f.transfers[e2e] = &tr
	id := f.nextEvent()
	f.mu.Unlock()
	return f.deliver(id, webhook.EventPixReceived, tr)
}

func (f *fakeSim) getTransfer(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	tr, ok := f.transfers[r.PathValue("e2eid")]
	var out pixsim.Transfer
	if ok {
		out = *tr
	}
	f.mu.Unlock()
	if !ok {
		simProblem(w, 404, "transfer_not_found", "transfer not found")
		return
	}
	writeTestJSON(w, 200, out)
}

func (f *fakeSim) refundLocked(e2e string, cents int64, reason, infraction string) (pixsim.Refund, *pixsim.Transfer, error) {
	tr, ok := f.transfers[e2e]
	if !ok {
		return pixsim.Refund{}, nil, fmt.Errorf("transfer_not_found")
	}
	if tr.Status != pixsim.StatusSettled {
		return pixsim.Refund{}, nil, fmt.Errorf("transfer_not_settled")
	}
	if cents <= 0 || cents > tr.AmountCents-tr.RefundedCents {
		return pixsim.Refund{}, nil, fmt.Errorf("refund_exceeds_amount")
	}
	f.seq++
	ref := pixsim.Refund{ID: fmt.Sprintf("D%s202611211200%011d", testISPB, f.seq), EndToEndID: e2e, AmountCents: cents, ReturnReason: reason,
		Status: pixsim.StatusSettled, CreatedAt: time.Now().UTC(), InfractionID: infraction}
	tr.RefundedCents += cents
	f.balance += cents
	f.refunds[e2e] = append(f.refunds[e2e], ref)
	return ref, tr, nil
}

func (f *fakeSim) createRefund(w http.ResponseWriter, r *http.Request) {
	var req simRefundRequest
	if err := strictDecode(r, &req); err != nil {
		simProblem(w, 400, "invalid_json", err.Error())
		return
	}
	if req.Reason == "" {
		req.Reason = "MD06"
	}
	f.mu.Lock()
	ref, _, err := f.refundLocked(r.PathValue("e2eid"), req.AmountCents, req.Reason, "")
	id := f.nextEvent()
	f.mu.Unlock()
	if err != nil {
		simProblem(w, 422, err.Error(), err.Error())
		return
	}
	writeTestJSON(w, http.StatusCreated, ref)
	go f.deliver(id, webhook.EventRefundCompleted, ref)
}

func (f *fakeSim) getRefund(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ref := range f.refunds[r.PathValue("e2eid")] {
		if ref.ID == r.PathValue("id") {
			writeTestJSON(w, 200, ref)
			return
		}
	}
	simProblem(w, 404, "refund_not_found", "refund not found")
}

func (f *fakeSim) reportInfraction(w http.ResponseWriter, r *http.Request) {
	var req struct { // internal/sim.InfractionRequest
		EndToEndID string `json:"end_to_end_id"`
		Reason     string `json:"reason,omitempty"`
		Details    string `json:"details,omitempty"`
	}
	if err := strictDecode(r, &req); err != nil {
		simProblem(w, 400, "invalid_json", err.Error())
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if tr, ok := f.transfers[req.EndToEndID]; !ok || tr.Status != pixsim.StatusSettled {
		simProblem(w, 409, "transfer_not_settled", "not settled")
		return
	}
	f.seq++
	inf := &pixsim.Infraction{ID: fmt.Sprintf("inf_%06d", f.seq), EndToEndID: req.EndToEndID, Reason: req.Reason, Details: req.Details, Status: "OPEN", History: []pixsim.Step{}}
	f.infractions[inf.ID] = inf
	writeTestJSON(w, 201, inf)
}

func (f *fakeSim) actOnInfraction(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	inf, ok := f.infractions[r.PathValue("id")]
	if !ok {
		f.mu.Unlock()
		simProblem(w, 404, "infraction_not_found", "not found")
		return
	}
	next := map[string]map[string]string{"OPEN": {"block": "BLOCKED"}, "BLOCKED": {"analyze": "ANALYSIS"}, "ANALYSIS": {"accept": "REFUNDED", "reject": "CLOSED"}}
	to, ok := next[inf.Status][r.PathValue("action")]
	if !ok {
		f.mu.Unlock()
		simProblem(w, 409, "invalid_transition", "invalid transition")
		return
	}
	tr := f.transfers[inf.EndToEndID]
	var after func()
	switch r.PathValue("action") {
	case "block":
		hold := tr.AmountCents - tr.RefundedCents
		if b := f.payeeBalance[tr.Payee.AccountID]; b < hold {
			hold = b
		}
		f.payeeBalance[tr.Payee.AccountID] -= hold
		inf.HeldCents = hold
	case "accept":
		if inf.HeldCents > 0 {
			ref, _, err := f.refundLocked(inf.EndToEndID, inf.HeldCents, "FR01", inf.ID)
			if err == nil {
				inf.RefundID = ref.ID
				id := f.nextEvent()
				after = func() { f.deliver(id, webhook.EventRefundCompleted, ref) }
			}
		}
		inf.HeldCents = 0
	}
	inf.History = append(inf.History, pixsim.Step{From: inf.Status, Event: r.PathValue("action"), To: to, At: time.Now().UTC()})
	inf.Status = to
	out := *inf
	f.mu.Unlock()
	writeTestJSON(w, 200, out)
	if after != nil {
		go after()
	}
}

func (f *fakeSim) spiBalance() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.balance
}

// ---------------------------------------------------------------------
// fakePayments mimics payments-service-go's webhook and charge endpoints
// (internal/httpapi/server.go): HMAC "sha256=<hex>" in X-Webhook-Signature,
// strict decoding, 404 for unknown txid, duplicate on a repeated
// EndToEndId.

type fakePayments struct {
	srv *httptest.Server

	mu        sync.Mutex
	charges   map[string]*paymentsCharge // by txid
	failNext  int
	settleHit int
	badSigs   int
}

type paymentsCharge struct {
	ID          string `json:"id"`
	TxID        string `json:"txid"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
	EndToEndID  string `json:"end_to_end_id,omitempty"`
	BRCode      string `json:"br_code"`
}

func newFakePayments(t *testing.T) *fakePayments {
	p := &fakePayments{charges: map[string]*paymentsCharge{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /v1/charges", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") == "" {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"type":"/problems/idempotency-key-missing","status":400}`))
			return
		}
		var req struct {
			AmountCents int64  `json:"amount_cents"`
			Description string `json:"description"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		p.mu.Lock()
		c := &paymentsCharge{ID: fmt.Sprintf("ch_%d", len(p.charges)+1), TxID: fmt.Sprintf("TX%023d", len(p.charges)+1), Status: "PENDING", AmountCents: req.AmountCents, BRCode: "000201..."}
		p.charges[c.TxID] = c
		out := *c
		p.mu.Unlock()
		writeTestJSON(w, 201, out)
	})
	mux.HandleFunc("GET /v1/charges/{id}", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.charges {
			if c.ID == r.PathValue("id") {
				writeTestJSON(w, 200, c)
				return
			}
		}
		w.WriteHeader(404)
	})
	mux.HandleFunc("POST /v1/webhooks/pix", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mac := hmac.New(sha256.New, []byte(testPaySecret))
		mac.Write(body)
		if r.Header.Get("X-Webhook-Signature") != "sha256="+hex.EncodeToString(mac.Sum(nil)) {
			p.mu.Lock()
			p.badSigs++
			p.mu.Unlock()
			w.WriteHeader(401)
			return
		}
		var req struct { // pixWebhookRequest
			TxID        string `json:"txid"`
			EndToEndID  string `json:"end_to_end_id"`
			AmountCents int64  `json:"amount_cents"`
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			w.WriteHeader(400)
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.settleHit++
		if p.failNext > 0 {
			p.failNext--
			w.WriteHeader(503)
			return
		}
		c, ok := p.charges[req.TxID]
		if !ok {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"type":"/problems/not-found","title":"Not found","status":404}`))
			return
		}
		if c.AmountCents != req.AmountCents {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(422)
			_, _ = w.Write([]byte(`{"type":"/problems/amount-mismatch","title":"Amount mismatch","status":422}`))
			return
		}
		dup := c.Status == "PAID" && c.EndToEndID == req.EndToEndID
		c.Status, c.EndToEndID = "PAID", req.EndToEndID
		writeTestJSON(w, 200, map[string]any{"charge": c, "duplicate": dup})
	})
	p.srv = httptest.NewServer(mux)
	t.Cleanup(p.srv.Close)
	return p
}

// ---------------------------------------------------------------------
// riskd: the real fraud-rules-go engine behind the same two routes riskd
// serves (decoding with DisallowUnknownFields, as internal/server does).

type riskServer struct {
	srv      *httptest.Server
	eng      *engine.Engine
	mu       sync.Mutex
	recorded []engine.Transaction
	evals    []engine.Transaction
	down     bool
}

func newRiskServer(t *testing.T, ruleset []byte) *riskServer {
	store, err := velocity.New(velocity.Config{})
	if err != nil {
		t.Fatal(err)
	}
	rs := &riskServer{eng: engine.New(store, engine.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})}
	if _, err := rs.eng.LoadJSON(ruleset); err != nil {
		t.Fatalf("ruleset: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeTestJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /v1/evaluate", func(w http.ResponseWriter, r *http.Request) {
		rs.mu.Lock()
		down := rs.down
		rs.mu.Unlock()
		if down {
			w.WriteHeader(503)
			return
		}
		var tx engine.Transaction
		if err := strictDecode(r, &tx); err != nil {
			t.Errorf("riskd would reject the transaction: %v", err)
			writeTestJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		rs.mu.Lock()
		rs.evals = append(rs.evals, tx)
		rs.mu.Unlock()
		d, err := rs.eng.Evaluate(r.Context(), &tx)
		if err != nil {
			writeTestJSON(w, 503, map[string]string{"error": err.Error()})
			return
		}
		writeTestJSON(w, 200, d)
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		var txs []engine.Transaction
		if err := json.NewDecoder(r.Body).Decode(&txs); err != nil {
			t.Errorf("events: %v", err)
			w.WriteHeader(400)
			return
		}
		rs.mu.Lock()
		for i := range txs {
			rs.eng.Record(&txs[i])
			rs.recorded = append(rs.recorded, txs[i])
		}
		rs.mu.Unlock()
		writeTestJSON(w, 202, map[string]int{"recorded": len(txs)})
	})
	rs.srv = httptest.NewServer(mux)
	t.Cleanup(rs.srv.Close)
	return rs
}

func (rs *riskServer) recordedCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.recorded)
}

func (rs *riskServer) lastEval() engine.Transaction {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.evals[len(rs.evals)-1]
}

// testRuleset gives tests exact control over decisions by amount.
var testRuleset = []byte(`{
  "name": "gateway-test", "version": "1",
  "rules": [
    {"id": "test.block", "expr": "tx.amount == 13_13", "action": "BLOCK", "score": 100},
    {"id": "test.hold", "expr": "tx.amount == 77_77", "action": "HOLD", "hold_for": "30m", "score": 10},
    {"id": "test.step_up", "expr": "tx.amount == 66_66", "action": "STEP_UP", "score": 10},
    {"id": "test.review", "expr": "tx.amount == 55_55", "action": "REVIEW", "score": 10},
    {"id": "test.fraud_marks", "expr": "beneficiary.fraud_marks >= 1", "action": "REVIEW", "score": 35}
  ]
}`)

// ---------------------------------------------------------------------
// relayd: the real webhook-relay-go engine and HTTP API, in memory.

func newRelayServer(t *testing.T) *httptest.Server {
	eng, err := relayeng.New(relayeng.Config{
		Store: relayeng.NewMemStore(),
		Guard: &relayeng.Guard{AllowHTTP: true, Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(relayapi.New(eng, relayapi.Options{Token: testRelayToken}))
	t.Cleanup(func() {
		srv.Close()
		cancel()
		sctx, c2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer c2()
		_ = eng.Shutdown(sctx)
	})
	return srv
}

// ---------------------------------------------------------------------

type env struct {
	t        *testing.T
	svc      *Service
	gw       *httptest.Server
	sim      *fakeSim
	payments *fakePayments
	risk     *riskServer
	merchant *merchant.Server
	merchSrv *httptest.Server
	cancel   context.CancelFunc
}

type envOpt func(*Config)

// newEnv accepts envOpt (config changes) and Option (service options).
func newEnv(t *testing.T, ruleset []byte, opts ...any) *env {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if os.Getenv("GATEWAY_TEST_LOGS") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	e := &env{t: t, sim: newFakeSim(t), payments: newFakePayments(t), risk: newRiskServer(t, ruleset)}
	relaySrv := newRelayServer(t)
	m, err := merchant.New(log, testWhsec)
	if err != nil {
		t.Fatal(err)
	}
	e.merchant = m
	e.merchSrv = httptest.NewServer(m.Handler())
	t.Cleanup(e.merchSrv.Close)

	cfg := Config{
		Addr: ":0", ISPB: testISPB, SPIAccountID: testSPIAccount,
		PixSimURL: e.sim.srv.URL, PixSimWebhookSecret: testSimSecret,
		RiskdURL: e.risk.srv.URL, RiskTimeout: 2 * time.Second,
		RelayURL: relaySrv.URL, RelayToken: testRelayToken,
		PaymentsURL: e.payments.srv.URL, PaymentsWebhookSecret: testPaySecret,
		MerchantID: "loja", MerchantName: "Loja Exemplo", MerchantPixKey: testMerchantKey,
		MerchantWebhookURL: e.merchSrv.URL + "/webhooks", MerchantWebhookSecret: testWhsec,
		SimHeaders: true, StepUpTTL: time.Minute, PollInterval: 20 * time.Millisecond, PollAfter: time.Hour,
		DefaultDayLimit: 5_000_00, DefaultNight: 1_000_00,
	}
	var svcOpts []Option
	for _, o := range opts {
		switch o := o.(type) {
		case envOpt:
			o(&cfg)
		case func(*Config):
			o(&cfg)
		case Option:
			svcOpts = append(svcOpts, o)
		default:
			t.Fatalf("unknown option %T", o)
		}
	}
	e.svc = New(cfg, log, svcOpts...)
	e.gw = httptest.NewServer(e.svc.Handler())
	t.Cleanup(e.gw.Close)
	e.sim.webhookURL = e.gw.URL + "/v1/webhooks/spi"
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	t.Cleanup(cancel)
	go e.svc.Run(ctx)
	e.eventually("gateway ready", func() bool { return e.svc.ready.Load() })
	return e
}

func (e *env) eventually(what string, f func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.t.Fatalf("timed out waiting for %s", what)
}

type resp struct {
	status int
	header http.Header
	body   []byte
}

func (r resp) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

func (e *env) do(method, path string, body any, headers ...string) resp {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.gw.URL+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{status: res.StatusCode, header: res.Header, body: b}
}

func (e *env) openAccount(id string, deposit int64, extra ...func(*OpenAccountRequest)) AccountView {
	e.t.Helper()
	req := OpenAccountRequest{ID: id, Name: "Customer " + id, InitialDepositCents: deposit, OpenedAt: ptr(time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC))}
	for _, f := range extra {
		f(&req)
	}
	r := e.do("POST", "/v1/accounts", req, "Idempotency-Key", "open-"+id)
	if r.status != http.StatusCreated {
		e.t.Fatalf("open account: %d %s", r.status, r.body)
	}
	var v AccountView
	r.decode(e.t, &v)
	return v
}

func (e *env) account(id string) AccountView {
	e.t.Helper()
	var v AccountView
	e.do("GET", "/v1/accounts/"+id, nil).decode(e.t, &v)
	return v
}

func (e *env) transfer(id string) Transfer {
	e.t.Helper()
	var v Transfer
	e.do("GET", "/v1/pix/transfers/"+id, nil).decode(e.t, &v)
	return v
}

func (e *env) waitStatus(id string, want Status) Transfer {
	e.t.Helper()
	var t Transfer
	e.eventually("transfer "+id+" "+string(want), func() bool { t = e.transfer(id); return t.Status == want })
	return t
}

func (e *env) pay(account, key string, cents int64, idemKey string, headers ...string) (Transfer, resp) {
	e.t.Helper()
	h := append([]string{"Idempotency-Key", idemKey}, headers...)
	r := e.do("POST", "/v1/pix/transfers", TransferRequest{AccountID: account, Key: key, AmountCents: cents, Description: "test",
		Device: &DeviceInfo{ID: "dev-1", Trusted: true, FirstSeen: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))}}, h...)
	var t Transfer
	if r.status < 300 || r.status == http.StatusUnprocessableEntity {
		_ = json.Unmarshal(r.body, &t)
	}
	return t, r
}

func (e *env) merchantEvents(typ string) []merchant.Event {
	e.t.Helper()
	req, _ := http.NewRequest("GET", e.merchSrv.URL+"/events?type="+typ, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	var out struct {
		Events []merchant.Event `json:"events"`
	}
	_ = json.NewDecoder(res.Body).Decode(&out)
	return out.Events
}

func (e *env) assertBooksBalanced() {
	e.t.Helper()
	var tb TrialBalanceView
	e.do("GET", "/v1/ledger/trial-balance", nil).decode(e.t, &tb)
	if !tb.Balanced || tb.DifferenceCents != 0 || tb.PendingDifferenceCents != 0 {
		e.t.Fatalf("trial balance: %+v", tb)
	}
	r := e.do("GET", "/v1/ledger/verify", nil)
	var cv ChainView
	r.decode(e.t, &cv)
	if r.status != 200 || !cv.Valid || !cv.BalancesMatch {
		e.t.Fatalf("chain: %d %+v", r.status, cv)
	}
}

func ptr[T any](v T) *T { return &v }

func writeTestJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
