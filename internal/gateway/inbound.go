package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/victorcondackdev/pix-psp-simulator/webhook"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/payments"
	"github.com/victorcondackdev/pix-in-a-box/internal/clients/pixsim"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// errRetry asks the SPI to redeliver the webhook (any non-2xx is retried
// with backoff by the simulator).
var errRetry = errors.New("transient failure, redelivery requested")

// HandleSPIEvent processes one verified webhook event from the SPI. The
// result is "processed", "duplicate" or "ignored"; an error wrapping
// errRetry means the caller must answer non-2xx so the event is redelivered.
func (s *Service) HandleSPIEvent(ctx context.Context, ev webhook.Event) (string, error) {
	if !s.store.claimEvent(ev.ID) {
		return "duplicate", nil
	}
	var result string
	var err error
	processed := false
	defer func() { s.store.finishEvent(ev.ID, processed, s.now()) }()
	switch ev.Type {
	case webhook.EventPixSettled, webhook.EventPixRejected:
		var tr pixsim.Transfer
		if err := json.Unmarshal(ev.Data, &tr); err != nil {
			return "", fmt.Errorf("decoding %s: %w", ev.Type, err)
		}
		if tr.Payer.AccountID != s.cfg.SPIAccountID {
			return "ignored", nil
		}
		if s.Settle(ctx, tr.EndToEndID, tr.Status, tr.Reason, tr.ReasonDescription, "webhook") {
			result = "processed"
		} else {
			result = "ignored" // unknown, or already final (the poll got there first)
		}
	case webhook.EventPixReceived:
		var tr pixsim.Transfer
		if err := json.Unmarshal(ev.Data, &tr); err != nil {
			return "", fmt.Errorf("decoding %s: %w", ev.Type, err)
		}
		if tr.Payee.AccountID != s.cfg.SPIAccountID {
			return "ignored", nil
		}
		result, err = s.Receive(ctx, tr)
	case webhook.EventRefundCompleted:
		var ref pixsim.Refund
		if err := json.Unmarshal(ev.Data, &ref); err != nil {
			return "", fmt.Errorf("decoding %s: %w", ev.Type, err)
		}
		result, err = s.refundCompleted(ctx, ref)
	default:
		result = "ignored"
	}
	if err != nil {
		return "", err
	}
	processed = true
	return result, nil
}

// Receive handles a Pix credited to the settlement account: it credits the
// owner of the key in the ledger and, when the Pix pays a charge, reports
// the settlement to payments-service, which marks the charge PAID and
// publishes payments.charge.paid.v1 through its outbox.
//
// Ordering: the ledger credit is idempotent on the EndToEndId and happens
// first, because the money is in the settlement account whatever
// payments-service says. A transport failure towards payments-service
// returns errRetry: the SPI redelivers, the ledger replays, the forward is
// retried, and payments-service deduplicates on the EndToEndId.
func (s *Service) Receive(ctx context.Context, tr pixsim.Transfer) (string, error) {
	if !s.ready.Load() {
		return "", fmt.Errorf("not ready: %w", errRetry)
	}
	to := AcctSuspense
	merchant := false
	if acct, ok := s.merchants[normalizeKey(tr.Payee.Key)]; ok && tr.Payee.Key != "" {
		to, merchant = acct, true
	}
	tx, err := s.books.inbound(ctx, tr.EndToEndID, to, tr.AmountCents, tr.Payer.Name)
	if err != nil {
		return "", fmt.Errorf("crediting inbound Pix: %w", err)
	}
	in := &Inbound{EndToEndID: tr.EndToEndID, TxID: tr.TxID, AmountCents: tr.AmountCents, Key: tr.Payee.Key,
		PayerName: tr.Payer.Name, PayerISPB: tr.Payer.ISPB, CreditedTo: to, LedgerTx: tx.ID, ReceivedAt: s.now().UTC(), ForwardResult: "skipped"}
	if prev, ok := s.store.inboundByE2E(tr.EndToEndID); ok {
		in.RefundedCents = prev.RefundedCents
	}
	result := "credited_suspense"
	if merchant {
		result = "credited_merchant"
	}
	if merchant && tr.TxID != "" && tr.TxID != "***" {
		res, err := s.payments.Settle(ctx, payments.Settlement{TxID: tr.TxID, EndToEndID: tr.EndToEndID, AmountCents: tr.AmountCents})
		var he *httpx.Error
		switch {
		case err == nil:
			in.ChargeID, in.ChargeStatus = res.Charge.ID, res.Charge.Status
			in.ForwardResult = "settled"
			if res.Duplicate {
				in.ForwardResult = "duplicate"
			}
			result = "charge_" + in.ForwardResult
		case errors.As(err, &he) && he.Status == http.StatusNotFound:
			in.ForwardResult = "not_a_charge"
		case errors.As(err, &he) && !he.Retryable():
			// Amount mismatch, cancelled charge, EndToEndId reuse: the money
			// stays with the merchant and the case goes to operations.
			in.ForwardResult = "rejected:" + he.Code
			s.log.WarnContext(ctx, "payments-service rejected the settlement", slog.String("end_to_end_id", tr.EndToEndID), slog.String("problem", he.Code), slog.String("detail", he.Detail))
		default:
			s.store.putInbound(in)
			s.metrics.Inbound.Inc("forward_retry")
			return "", fmt.Errorf("forwarding settlement to payments-service: %v: %w", err, errRetry)
		}
	}
	s.store.putInbound(in)
	s.metrics.Inbound.Inc(result)
	s.log.InfoContext(ctx, "Pix received", slog.String("end_to_end_id", tr.EndToEndID), slog.String("txid", tr.TxID),
		slog.Int64("amount_cents", tr.AmountCents), slog.String("credited_to", string(to)), slog.String("forward", in.ForwardResult))
	return "processed", nil
}

// Received returns an inbound Pix by EndToEndId.
func (s *Service) Received(e2e string) (Inbound, bool) {
	in, ok := s.store.inboundByE2E(e2e)
	if !ok {
		return Inbound{}, false
	}
	return *in, true
}

// refundCompleted books a devolução reported by the SPI. It covers refunds
// the gateway requested (already booked synchronously: a no-op here),
// refunds started elsewhere, and MED returns.
func (s *Service) refundCompleted(ctx context.Context, ref pixsim.Refund) (string, error) {
	if ref.Status != pixsim.StatusSettled {
		return "ignored", nil
	}
	if t, ok := s.store.transfer(ref.EndToEndID); ok {
		t.mu.Lock()
		defer t.mu.Unlock()
		ctx = trace.Continue(ctx, t.traceparent)
		if _, booked, err := s.bookRefundLocked(ctx, t, ref); err != nil {
			return "", err
		} else if !booked {
			return "duplicate", nil
		}
		return "processed", nil
	}
	if in, ok := s.store.inboundByE2E(ref.EndToEndID); ok {
		if !s.store.markRefund(ref.ID) {
			return "duplicate", nil
		}
		if _, err := s.books.refundInbound(ctx, in, ref.ID, ref.AmountCents); err != nil {
			s.store.unmarkRefund(ref.ID)
			return "", err
		}
		in.RefundedCents += ref.AmountCents
		return "processed", nil
	}
	return "ignored", nil
}

// bookRefundLocked books a settled refund of an outgoing Pix exactly once;
// t.mu must be held. It returns the refund record and whether this call
// booked it.
func (s *Service) bookRefundLocked(ctx context.Context, t *record, ref pixsim.Refund) (Refund, bool, error) {
	for _, r := range t.Refunds {
		if r.ID == ref.ID {
			return r, false, nil
		}
	}
	rec := Refund{ID: ref.ID, AmountCents: ref.AmountCents, Reason: ref.ReturnReason, Status: ref.Status,
		RejectReason: ref.Reason, InfractionID: ref.InfractionID, CreatedAt: ref.CreatedAt}
	if ref.Status != pixsim.StatusSettled {
		s.metrics.Refunds.Inc(rec.Reason, ref.Status)
		return rec, false, nil
	}
	if t.Status != StatusSettled || t.Ledger.Post == "" {
		return rec, false, fmt.Errorf("refund %s for transfer %s in status %s", ref.ID, t.ID, t.Status)
	}
	cust, ok := s.store.customer(t.AccountID)
	if !ok {
		return rec, false, fmt.Errorf("customer %s not found", t.AccountID)
	}
	tx, kind, err := s.books.refundPayout(ctx, t, cust.LedgerAccount, rec)
	if err != nil {
		return rec, false, err
	}
	s.store.markRefund(ref.ID)
	rec.LedgerTx, rec.LedgerKind, rec.BookedTo = tx.ID, kind, cust.LedgerAccount
	t.Refunds = append(t.Refunds, rec)
	t.RefundedCents += rec.AmountCents
	t.UpdatedAt = s.now().UTC()
	s.metrics.Refunds.Inc(rec.Reason, ref.Status)
	s.log.InfoContext(ctx, "refund booked", slog.String("transfer_id", t.ID), slog.String("return_id", ref.ID),
		slog.Int64("amount_cents", ref.AmountCents), slog.String("reason", rec.Reason), slog.String("ledger_kind", kind))
	s.outbox.enqueue(ctx, EventPayoutRefunded, t.ID+":refund:"+ref.ID, payoutEvent(t.snapshot()))
	return rec, true, nil
}

// RefundRequest is POST /v1/pix/transfers/{id}/refunds.
type RefundRequest struct {
	AmountCents int64  `json:"amount_cents"`
	Reason      string `json:"reason,omitempty"` // MD06 (default), BE08, SL02
	Description string `json:"description,omitempty"`
}

// RequestRefund asks the payee side to return funds of a settled outgoing
// Pix (in this sandbox, the simulator plays the receiving PSP) and books
// the return in the ledger when it settles.
func (s *Service) RequestRefund(ctx context.Context, id string, r RefundRequest, chaos http.Header) (Refund, Transfer, error) {
	t, ok := s.store.transfer(id)
	if !ok {
		return Refund{}, Transfer{}, errNotFound
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusSettled {
		return Refund{}, t.snapshot(), problem(http.StatusConflict, "invalid-state-transition", "Not settled", "only settled transfers can be refunded")
	}
	if r.AmountCents <= 0 || r.AmountCents > t.AmountCents-t.RefundedCents {
		return Refund{}, t.snapshot(), problem(http.StatusUnprocessableEntity, "validation-error", "Invalid amount",
			fmt.Sprintf("amount_cents must be between 1 and the refundable %d", t.AmountCents-t.RefundedCents))
	}
	if r.Reason == "FR01" {
		return Refund{}, t.snapshot(), problem(http.StatusUnprocessableEntity, "validation-error", "Use the MED endpoint", "FR01 returns come from a MED infraction: POST /v1/pix/transfers/{id}/med")
	}
	ctx = trace.Continue(ctx, t.traceparent)
	ref, err := s.sim.CreateRefund(ctx, t.EndToEndID, pixsim.RefundRequest{AmountCents: r.AmountCents, Reason: r.Reason, Description: r.Description}, chaos)
	if err != nil {
		if e, ok := httpx.AsError(err); ok && !e.Retryable() {
			return Refund{}, t.snapshot(), &errProblem{status: http.StatusUnprocessableEntity, typ: "/problems/refund-refused", title: "Refund refused by the SPI", detail: e.Detail, ext: map[string]any{"spi_code": e.Code}}
		}
		return Refund{}, t.snapshot(), problem(http.StatusBadGateway, "spi-unavailable", "Refund request failed", err.Error())
	}
	rec, _, err := s.bookRefundLocked(ctx, t, ref)
	if err != nil {
		return Refund{}, t.snapshot(), err
	}
	return rec, t.snapshot(), nil
}

// MEDResult reports a MED case run end to end.
type MEDResult struct {
	Infraction pixsim.Infraction `json:"infraction"`
	Refund     *Refund           `json:"refund,omitempty"`
	Transfer   Transfer          `json:"transfer"`
	FraudMarks int64             `json:"beneficiary_fraud_marks"`
}

// ReportFraud opens a MED (Mecanismo Especial de Devolução) case for a
// settled outgoing Pix and drives it to the end. In production the payer's
// PSP only reports the infraction; blocking, analysis and acceptance are
// the receiving PSP's steps, which the sandbox lets us play. The report also
// becomes a fraud mark on the beneficiary, so riskd sees it in the next
// evaluation (beneficiary.fraud_marks).
func (s *Service) ReportFraud(ctx context.Context, id, details string) (MEDResult, error) {
	t, ok := s.store.transfer(id)
	if !ok {
		return MEDResult{}, errNotFound
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Status != StatusSettled {
		return MEDResult{}, problem(http.StatusConflict, "invalid-state-transition", "Not settled", "MED applies to settled transfers")
	}
	ctx = trace.Continue(ctx, t.traceparent)
	inf, err := s.sim.ReportInfraction(ctx, t.EndToEndID, "FRAUD", details)
	if err != nil {
		if e, ok := httpx.AsError(err); ok && !e.Retryable() {
			return MEDResult{}, &errProblem{status: http.StatusUnprocessableEntity, typ: "/problems/med-refused", title: "MED report refused", detail: e.Detail, ext: map[string]any{"spi_code": e.Code}}
		}
		return MEDResult{}, problem(http.StatusBadGateway, "spi-unavailable", "MED report failed", err.Error())
	}
	marks := s.store.addFraudMark(t.Beneficiary.RiskID)
	for _, step := range []string{"block", "analyze", "accept"} {
		if inf, err = s.sim.ActOnInfraction(ctx, inf.ID, step); err != nil {
			return MEDResult{}, problem(http.StatusBadGateway, "spi-unavailable", "MED step "+step+" failed", err.Error())
		}
	}
	res := MEDResult{Infraction: inf, FraudMarks: marks}
	if inf.RefundID != "" {
		ref, err := s.sim.Refund(ctx, t.EndToEndID, inf.RefundID)
		if err != nil {
			return MEDResult{}, problem(http.StatusBadGateway, "spi-unavailable", "Reading MED refund failed", err.Error())
		}
		rec, _, err := s.bookRefundLocked(ctx, t, ref)
		if err != nil {
			return MEDResult{}, err
		}
		res.Refund = &rec
	}
	s.log.InfoContext(ctx, "MED case closed", slog.String("transfer_id", t.ID), slog.String("infraction_id", inf.ID),
		slog.String("status", inf.Status), slog.Int64("beneficiary_fraud_marks", marks))
	res.Transfer = t.snapshot()
	return res, nil
}

// webhookTolerance bounds the age of an SPI webhook signature.
const webhookTolerance = 5 * time.Minute
