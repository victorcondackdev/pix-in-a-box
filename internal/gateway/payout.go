package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/victorcondackdev/brpay/pix"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/pixsim"
	"github.com/victorcondackdev/pix-in-a-box/internal/clients/riskd"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// TransferRequest is POST /v1/pix/transfers.
type TransferRequest struct {
	AccountID   string `json:"account_id"`
	Key         string `json:"key"`
	AmountCents int64  `json:"amount_cents"`
	Description string `json:"description,omitempty"`
	// Channel and Device describe how the customer initiated the payment;
	// they feed the risk rules (new device, API channel at night...).
	Channel string        `json:"channel,omitempty"` // app (default), web, api, open_finance
	Device  *DeviceInfo   `json:"device,omitempty"`
	IP      string        `json:"ip,omitempty"`
	sim     simDirectives // from X-Sim-* headers, sandbox only
}

// DeviceInfo is the initiating device as known to the PSP's device
// binding.
type DeviceInfo struct {
	ID        string     `json:"id"`
	Trusted   bool       `json:"trusted"`
	FirstSeen *time.Time `json:"first_seen,omitempty"`
}

// simDirectives are sandbox-only request options.
type simDirectives struct {
	chaos http.Header // forwarded X-Sim-* headers
	at    *time.Time  // X-Sim-Time: transaction time for risk evaluation
}

func simDirectivesFrom(h http.Header, enabled bool) (simDirectives, error) {
	var d simDirectives
	if !enabled {
		return d, nil
	}
	for k, vs := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), "X-Sim-") && http.CanonicalHeaderKey(k) != "X-Sim-Time" {
			if d.chaos == nil {
				d.chaos = http.Header{}
			}
			d.chaos[http.CanonicalHeaderKey(k)] = append([]string(nil), vs...)
		}
	}
	if v := h.Get("X-Sim-Time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return d, problem(http.StatusBadRequest, "malformed-request", "Invalid X-Sim-Time", "use RFC 3339, e.g. 2026-11-21T03:10:00-03:00")
		}
		d.at = &t
	}
	return d, nil
}

// CreateTransfer runs the outgoing Pix flow up to the SPI submission:
//
//	DICT lookup → risk evaluation → ledger hold → SPI submission
//
// Settlement completes asynchronously (webhook or poll). The returned
// status is 202 for anything still in flight and 422 for a risk block.
func (s *Service) CreateTransfer(ctx context.Context, r TransferRequest) (Transfer, int, error) {
	if !s.ready.Load() {
		return Transfer{}, 0, problem(http.StatusServiceUnavailable, "not-ready", "Gateway starting", "bootstrap has not completed")
	}
	cust, ok := s.store.customer(r.AccountID)
	if !ok {
		return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "validation-error", "Unknown account", "account_id does not exist")
	}
	if r.AmountCents <= 0 || r.AmountCents > pix.MaxCents {
		return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid amount", "amount_cents must be positive")
	}
	if len(r.Description) > 140 {
		return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid description", "description must be at most 140 characters")
	}
	key, _, err := pix.NormalizeKey(r.Key)
	if err != nil {
		return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "validation-error", "Invalid Pix key", err.Error())
	}

	// 1. DICT.
	lk, err := s.sim.LookupKey(ctx, key, cust.ID)
	if err != nil {
		if e, ok := httpx.AsError(err); ok {
			switch {
			case e.Status == http.StatusNotFound:
				return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "key-not-found", "Pix key not found", "the DICT has no entry for this key")
			case e.Status == http.StatusTooManyRequests:
				return Transfer{}, 0, problem(http.StatusTooManyRequests, "dict-rate-limited", "DICT lookup rate limit", "too many key lookups for this customer; retry later")
			}
		}
		return Transfer{}, 0, problem(http.StatusBadGateway, "dict-unavailable", "DICT lookup failed", err.Error())
	}
	if lk.ISPB == s.cfg.ISPB && lk.AccountID == s.cfg.SPIAccountID {
		return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "on-us-not-supported", "On-us transfer", "the key belongs to this PSP; book transfers are out of scope for this demo")
	}

	now := s.now().UTC()
	ctx, span := trace.Ensure(ctx)
	t := &record{Transfer: Transfer{
		ID: newID("pix"), AccountID: cust.ID, Key: key, AmountCents: r.AmountCents, Description: r.Description,
		Beneficiary: beneficiaryOf(lk), Refunds: []Refund{}, CreatedAt: now, UpdatedAt: now, TraceID: span.TraceIDString(),
	}, traceparent: span.String(), chaos: r.sim.chaos}
	if t.EndToEndID, err = pix.NewEndToEndID(s.cfg.ISPB, now, nil); err != nil {
		return Transfer{}, 0, err
	}

	// 2. Risk.
	t.riskTx = s.riskTransaction(t, cust, lk, r)
	dec, err := s.risk.Evaluate(ctx, t.riskTx)
	if err != nil {
		if !s.cfg.RiskFailOpen {
			s.metrics.Risk.Inc("UNAVAILABLE_CLOSED")
			return Transfer{}, 0, problem(http.StatusServiceUnavailable, "risk-unavailable", "Risk engine unavailable", "the transfer was not attempted (risk fails closed)")
		}
		s.log.WarnContext(ctx, "riskd unavailable; failing open", slog.Any("error", err))
		s.metrics.Risk.Inc("UNAVAILABLE")
		dec = riskd.Decision{Action: riskd.Review, Reasons: []string{"risk engine unavailable: failed open"}}
		t.Risk = &Risk{Action: dec.Action, Reasons: dec.Reasons, Unavailable: true}
	} else {
		s.metrics.Risk.Inc(dec.Action)
		t.Risk = &Risk{Action: dec.Action, Score: dec.Score, Reasons: dec.Reasons, Hits: dec.Hits, HoldFor: dec.HoldFor,
			Ruleset: dec.Ruleset, Revision: dec.Revision, Degraded: dec.Degraded}
	}
	s.log.InfoContext(ctx, "risk decision", slog.String("transfer_id", t.ID), slog.String("action", dec.Action), slog.Int("score", dec.Score), slog.Any("reasons", dec.Reasons))

	if dec.Action == riskd.Block {
		t.Status = StatusBlocked
		t.FailureReason = "RISK_BLOCKED"
		t.FailureDescription = strings.Join(dec.Reasons, "; ")
		t.FinishedAt = &now
		s.store.putTransfer(t)
		s.metrics.Transfers.Inc(string(t.Status))
		snap := t.snapshot()
		s.outbox.enqueue(ctx, EventPayoutBlocked, t.ID+":blocked", payoutEvent(snap))
		return snap, http.StatusUnprocessableEntity, nil
	}

	// 3. Ledger hold: the customer's available balance drops now, whatever
	// happens next (step-up, cautionary hold, SPI).
	hold, err := s.books.holdPayout(ctx, t, cust.LedgerAccount)
	if err != nil {
		if isInsufficientFunds(err) {
			return Transfer{}, 0, problem(http.StatusUnprocessableEntity, "insufficient-funds", "Insufficient funds", "the available balance does not cover the amount")
		}
		return Transfer{}, 0, err
	}
	t.Ledger.Hold = hold.ID

	t.mu.Lock()
	defer t.mu.Unlock()
	switch dec.Action {
	case riskd.StepUp:
		t.Status = StatusAwaitingConfirmation
		t.Challenge = &Challenge{ID: newID("chl"), ExpiresAt: now.Add(s.cfg.StepUpTTL)}
		s.store.putTransfer(t)
		s.metrics.Transfers.Inc(string(t.Status))
		return t.snapshot(), http.StatusAccepted, nil
	case riskd.Hold:
		d, perr := riskd.ParseDuration(dec.HoldFor)
		if perr != nil || d <= 0 {
			d = time.Hour
		}
		until := now.Add(d)
		t.Risk.HoldUntil = &until
		t.Status = StatusHeld
		s.store.putTransfer(t)
		s.metrics.Transfers.Inc(string(t.Status))
		id := t.ID
		t.timer = time.AfterFunc(d, func() { s.releaseHeld(id, "cautionary hold elapsed") })
		snap := t.snapshot()
		s.outbox.enqueue(ctx, EventPayoutHeld, t.ID+":held", payoutEvent(snap))
		return snap, http.StatusAccepted, nil
	}
	// ALLOW, REVIEW (flagged for the analyst queue, not stopped) and a
	// failed-open evaluation go straight to the SPI.
	s.store.putTransfer(t)
	s.submitLocked(ctx, t)
	if t.Status == StatusFailed {
		return t.snapshot(), http.StatusUnprocessableEntity, nil
	}
	return t.snapshot(), http.StatusAccepted, nil
}

// beneficiaryOf builds the payee view and its opaque risk identifier.
func beneficiaryOf(lk pixsim.KeyLookup) Beneficiary {
	h := sha256.Sum256([]byte(lk.ISPB + "|" + lk.Branch + "|" + lk.Number))
	pt := "PF"
	if lk.OwnerType == "LEGAL_PERSON" {
		pt = "PJ"
	}
	return Beneficiary{Name: lk.OwnerName, TaxIDMasked: lk.OwnerTaxID, PersonType: pt, ISPB: lk.ISPB, Branch: lk.Branch,
		Account: lk.Number, AccountType: lk.AccountType, KeyType: strings.ToLower(lk.KeyType), RiskID: "ben_" + hex.EncodeToString(h[:8])}
}

// riskTransaction maps the transfer to riskd's schema. The transaction
// time is the gateway clock, or X-Sim-Time in the sandbox, so scenarios
// that depend on the hour (night limits) are reproducible at any time of
// day: riskd computes every feature in event time.
func (s *Service) riskTransaction(t *record, c *Customer, lk pixsim.KeyLookup, r TransferRequest) riskd.Transaction {
	at := t.CreatedAt
	if r.sim.at != nil {
		at = *r.sim.at
	}
	opened := c.OpenedAt
	keyCreated := lk.CreatedAt
	channel := r.Channel
	if channel == "" {
		channel = "app"
	}
	dev := riskd.Device{ID: "unknown"}
	if r.Device != nil {
		dev = riskd.Device{ID: r.Device.ID, Trusted: r.Device.Trusted, FirstSeen: r.Device.FirstSeen}
	}
	ben := riskd.Beneficiary{ID: t.Beneficiary.RiskID, Type: t.Beneficiary.PersonType, Bank: lk.ISPB, KeyType: t.Beneficiary.KeyType,
		FraudMarks: s.store.fraudMarksOf(t.Beneficiary.RiskID)}
	if !keyCreated.IsZero() {
		ben.KeyCreated = &keyCreated
	}
	return riskd.Transaction{
		ID: t.ID, Time: at, Amount: t.AmountCents, Channel: channel, Initiation: "key", Description: t.Description,
		Payer: riskd.Payer{ID: c.ID, Type: c.Type, AccountCreated: &opened, DayLimit: c.DayLimitCents, NightLimit: c.NightLimitCents,
			Device: dev, IP: r.IP, Tags: c.Tags},
		Beneficiary: ben,
	}
}

// submitLocked sends the transfer to the SPI. t.mu must be held; it is
// released during the network calls (a settlement webhook may arrive
// before the submission returns, and must not wait on this lock) and held
// again on return. The EndToEndId is generated by this PSP (the payer's
// PSP owns it) and, with the transfer ID as Idempotency-Key, deduplicates
// on the SPI side: a retried submission can never create a second payment.
func (s *Service) submitLocked(ctx context.Context, t *record) {
	now := s.now().UTC()
	t.Status, t.SubmittedAt, t.UpdatedAt, t.Challenge = StatusSubmitted, &now, now, nil
	s.metrics.Transfers.Inc(string(StatusSubmitted))
	req := pixsim.TransferRequest{FromAccountID: s.cfg.SPIAccountID, Key: t.Key, AmountCents: t.AmountCents, EndToEndID: t.EndToEndID, Description: t.Description}
	id, chaos := t.ID, t.chaos
	t.submitting = true
	t.mu.Unlock()
	var lastErr error
	refused := false
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(100<<attempt) * time.Millisecond)
		}
		_, err := s.sim.CreateTransfer(ctx, req, id, chaos)
		if err == nil {
			lastErr = nil
			break
		}
		lastErr = err
		e, isHTTP := httpx.AsError(err)
		if isHTTP && e.Status == http.StatusConflict && e.Code == "duplicate_end_to_end_id" {
			lastErr = nil // an earlier attempt got through
			break
		}
		if isHTTP && !e.Retryable() {
			refused = true
			break
		}
	}
	t.mu.Lock()
	t.submitting = false
	switch {
	case lastErr == nil:
		s.log.InfoContext(ctx, "transfer submitted to SPI", slog.String("transfer_id", id), slog.String("end_to_end_id", t.EndToEndID))
	case refused && t.Status == StatusSubmitted:
		// The SPI refused the instruction outright: the payment never
		// existed, release the funds.
		e, _ := httpx.AsError(lastErr)
		s.failLocked(ctx, t, strings.ToUpper(firstNonEmpty(e.Code, "SPI_REFUSED")), e.Detail)
	default:
		// Unknown outcome (timeouts, 5xx): stay SUBMITTED; the poller asks
		// the SPI and either finds the transfer or fails it.
		s.log.WarnContext(ctx, "SPI submission outcome unknown; will reconcile", slog.String("transfer_id", id), slog.Any("error", lastErr))
	}
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// failLocked voids the hold of a transfer that never reached the SPI.
func (s *Service) failLocked(ctx context.Context, t *record, code, detail string) {
	tx, err := s.books.voidPayout(ctx, t, code)
	if err != nil {
		s.log.ErrorContext(ctx, "void failed", slog.String("transfer_id", t.ID), slog.Any("error", err))
		return
	}
	now := s.now().UTC()
	t.Ledger.Void = tx.ID
	t.Status, t.FailureReason, t.FailureDescription, t.FinishedAt, t.UpdatedAt = StatusFailed, code, detail, &now, now
	s.metrics.Transfers.Inc(string(StatusFailed))
	s.outbox.enqueue(ctx, EventPayoutRejected, t.ID+":rejected", payoutEvent(t.snapshot()))
}

// Settle applies the SPI's final status (from a webhook or a poll). It is
// idempotent: only a SUBMITTED transfer moves, exactly once.
func (s *Service) Settle(ctx context.Context, e2e, status, reason, reasonDesc, source string) bool {
	t, ok := s.store.transfer(e2e)
	if !ok {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusSubmitted {
		return false
	}
	ctx = trace.Continue(ctx, t.traceparent)
	now := s.now().UTC()
	switch status {
	case pixsim.StatusSettled:
		tx, err := s.books.postPayout(ctx, t)
		if err != nil {
			s.log.ErrorContext(ctx, "posting settled Pix failed", slog.String("transfer_id", t.ID), slog.Any("error", err))
			return false
		}
		t.Ledger.Post = tx.ID
		t.Status = StatusSettled
		rtx := t.riskTx
		go func() {
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := s.risk.Record(rctx, rtx); err != nil {
				s.log.WarnContext(rctx, "recording settled transfer in riskd failed", slog.String("transfer_id", rtx.ID), slog.Any("error", err))
			}
		}()
	case pixsim.StatusRejected:
		tx, err := s.books.voidPayout(ctx, t, reason)
		if err != nil {
			s.log.ErrorContext(ctx, "voiding rejected Pix failed", slog.String("transfer_id", t.ID), slog.Any("error", err))
			return false
		}
		t.Ledger.Void = tx.ID
		t.Status, t.FailureReason, t.FailureDescription = StatusRejected, reason, reasonDesc
	default:
		return false
	}
	t.FinishedAt, t.UpdatedAt = &now, now
	s.metrics.Transfers.Inc(string(t.Status))
	if t.SubmittedAt != nil {
		s.metrics.Settlement.Observe(now.Sub(*t.SubmittedAt).Seconds(), string(t.Status))
	}
	s.log.InfoContext(ctx, "transfer finished", slog.String("transfer_id", t.ID), slog.String("end_to_end_id", e2e),
		slog.String("status", string(t.Status)), slog.String("reason", reason), slog.String("source", source))
	ev := EventPayoutSettled
	if t.Status == StatusRejected {
		ev = EventPayoutRejected
	}
	s.outbox.enqueue(ctx, ev, t.ID+":"+strings.ToLower(string(t.Status)), payoutEvent(t.snapshot()))
	return true
}

// pollSubmitted reconciles transfers whose webhook has not arrived: the SPI
// is asked directly, and a transfer it has never heard of is failed.
func (s *Service) pollSubmitted(ctx context.Context) {
	cutoff := s.now().Add(-s.cfg.PollAfter)
	for _, t := range s.store.transfersWhere(func(t *record) bool {
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.Status == StatusSubmitted && !t.submitting && t.SubmittedAt != nil && t.SubmittedAt.Before(cutoff)
	}) {
		pctx := trace.Continue(ctx, t.traceparent)
		st, err := s.sim.Transfer(pctx, t.EndToEndID)
		switch {
		case httpx.IsStatus(err, http.StatusNotFound):
			t.mu.Lock()
			if t.Status == StatusSubmitted && !t.submitting {
				s.failLocked(pctx, t, "NOT_RECEIVED", "the SPI has no record of this transfer")
			}
			t.mu.Unlock()
		case err != nil:
			s.log.WarnContext(pctx, "settlement poll failed", slog.String("transfer_id", t.ID), slog.Any("error", err))
		case st.Status == pixsim.StatusSettled || st.Status == pixsim.StatusRejected:
			s.Settle(pctx, t.EndToEndID, st.Status, st.Reason, st.ReasonDescription, "poll")
		}
	}
}

// Confirm completes a step-up challenge and submits the transfer.
func (s *Service) Confirm(ctx context.Context, id, challengeID string) (Transfer, error) {
	t, ok := s.store.transfer(id)
	if !ok {
		return Transfer{}, errNotFound
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusAwaitingConfirmation {
		return t.snapshot(), problem(http.StatusConflict, "invalid-state-transition", "Not awaiting confirmation", "transfer is "+string(t.Status))
	}
	if t.Challenge == nil || t.Challenge.ID != challengeID {
		return t.snapshot(), problem(http.StatusUnprocessableEntity, "challenge-mismatch", "Wrong challenge", "challenge_id does not match")
	}
	if !s.now().Before(t.Challenge.ExpiresAt) {
		s.cancelLocked(ctx, t, "STEP_UP_EXPIRED", "step-up challenge expired")
		return t.snapshot(), problem(http.StatusConflict, "challenge-expired", "Challenge expired", "the transfer was cancelled")
	}
	s.submitLocked(trace.Continue(ctx, t.traceparent), t)
	return t.snapshot(), nil
}

// Release submits a held transfer before its cautionary delay ends (an
// analyst cleared it).
func (s *Service) Release(ctx context.Context, id string) (Transfer, error) {
	t, ok := s.store.transfer(id)
	if !ok {
		return Transfer{}, errNotFound
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusHeld {
		return t.snapshot(), problem(http.StatusConflict, "invalid-state-transition", "Not held", "transfer is "+string(t.Status))
	}
	if t.timer != nil {
		t.timer.Stop()
	}
	s.submitLocked(trace.Continue(ctx, t.traceparent), t)
	return t.snapshot(), nil
}

func (s *Service) releaseHeld(id, why string) {
	t, ok := s.store.transfer(id)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusHeld {
		return
	}
	ctx := trace.Continue(s.background(), t.traceparent)
	s.log.InfoContext(ctx, "releasing held transfer", slog.String("transfer_id", id), slog.String("why", why))
	s.submitLocked(ctx, t)
}

// Cancel abandons a held or step-up transfer and releases the funds.
func (s *Service) Cancel(ctx context.Context, id string) (Transfer, error) {
	t, ok := s.store.transfer(id)
	if !ok {
		return Transfer{}, errNotFound
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusHeld && t.Status != StatusAwaitingConfirmation {
		return t.snapshot(), problem(http.StatusConflict, "invalid-state-transition", "Cannot cancel", "transfer is "+string(t.Status))
	}
	s.cancelLocked(trace.Continue(ctx, t.traceparent), t, "CANCELLED", "cancelled before submission")
	return t.snapshot(), nil
}

func (s *Service) cancelLocked(ctx context.Context, t *record, code, detail string) {
	if t.timer != nil {
		t.timer.Stop()
	}
	tx, err := s.books.voidPayout(ctx, t, code)
	if err != nil {
		s.log.ErrorContext(ctx, "void failed", slog.String("transfer_id", t.ID), slog.Any("error", err))
		return
	}
	now := s.now().UTC()
	t.Ledger.Void = tx.ID
	t.Status, t.FailureReason, t.FailureDescription, t.FinishedAt, t.UpdatedAt, t.Challenge = StatusCancelled, code, detail, &now, now, nil
	s.metrics.Transfers.Inc(string(StatusCancelled))
	s.outbox.enqueue(ctx, EventPayoutRejected, t.ID+":cancelled", payoutEvent(t.snapshot()))
}

func (s *Service) expireChallenges(ctx context.Context) {
	now := s.now()
	for _, t := range s.store.transfersWhere(func(t *record) bool {
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.Status == StatusAwaitingConfirmation && t.Challenge != nil && !now.Before(t.Challenge.ExpiresAt)
	}) {
		t.mu.Lock()
		if t.Status == StatusAwaitingConfirmation {
			s.cancelLocked(trace.Continue(ctx, t.traceparent), t, "STEP_UP_EXPIRED", "step-up challenge expired")
		}
		t.mu.Unlock()
	}
}

// Transfer returns a transfer by ID or EndToEndId.
func (s *Service) Transfer(id string) (Transfer, bool) {
	t, ok := s.store.transfer(id)
	if !ok {
		return Transfer{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshot(), true
}

// Transfers lists transfers, optionally of one account.
func (s *Service) Transfers(accountID string) []Transfer {
	ts := s.store.transfersWhere(func(t *record) bool { return accountID == "" || t.AccountID == accountID })
	out := make([]Transfer, 0, len(ts))
	for _, t := range ts {
		t.mu.Lock()
		out = append(out, t.snapshot())
		t.mu.Unlock()
	}
	return out
}

// payoutEvent is the merchant-facing notification payload: no internal
// ledger IDs, the risk decision reduced to its action and reasons.
func payoutEvent(t Transfer) map[string]any {
	ev := map[string]any{
		"id": t.ID, "end_to_end_id": t.EndToEndID, "account_id": t.AccountID, "amount_cents": t.AmountCents,
		"status": t.Status, "refunded_cents": t.RefundedCents, "beneficiary": map[string]string{"name": t.Beneficiary.Name, "ispb": t.Beneficiary.ISPB},
		"created_at": t.CreatedAt,
	}
	if t.FailureReason != "" {
		ev["failure_reason"], ev["failure_description"] = t.FailureReason, t.FailureDescription
	}
	if t.Risk != nil {
		ev["risk"] = map[string]any{"action": t.Risk.Action, "reasons": t.Risk.Reasons}
	}
	return ev
}
