package gateway

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/victorcondackdev/ledger-go/ledger"
	"github.com/victorcondackdev/ledger-go/money"
)

// StatementLine is one movement on a ledger account.
type StatementLine struct {
	Seq            uint64      `json:"seq"`
	TxID           ledger.TxID `json:"tx_id"`
	Kind           string      `json:"kind"`
	Phase          string      `json:"phase"`
	Time           time.Time   `json:"time"`
	Description    string      `json:"description"`
	Direction      string      `json:"direction"`
	AmountCents    int64       `json:"amount_cents"`
	BalanceCents   int64       `json:"balance_cents"`
	AvailableCents int64       `json:"available_cents"`
}

// StatementView is a page of a statement with current balances.
type StatementView struct {
	Account        ledger.AccountID `json:"account"`
	Name           string           `json:"name,omitempty"`
	Type           string           `json:"type"`
	BalanceCents   int64            `json:"balance_cents"`
	AvailableCents int64            `json:"available_cents"`
	PendingCents   int64            `json:"pending_cents"`
	Lines          []StatementLine  `json:"lines"`
	NextCursor     string           `json:"next_cursor,omitempty"`
}

// Statement returns a page of an account's movements.
func (s *Service) Statement(ctx context.Context, id string, opt ledger.StatementOptions) (StatementView, error) {
	st, err := s.ledger.Statement(ctx, ledger.AccountID(id), opt)
	if err != nil {
		if errors.Is(err, ledger.ErrAccountNotFound) {
			return StatementView{}, errNotFound
		}
		if errors.Is(err, ledger.ErrInvalidCursor) {
			return StatementView{}, problem(http.StatusBadRequest, "malformed-request", "Invalid cursor", err.Error())
		}
		return StatementView{}, err
	}
	b, err := s.ledger.Balance(ctx, st.Account.ID)
	if err != nil {
		return StatementView{}, err
	}
	v := StatementView{Account: st.Account.ID, Name: st.Account.Name, Type: st.Account.Type.String(), BalanceCents: b.Posted.Minor(),
		AvailableCents: b.Available.Minor(), PendingCents: b.Pending.Minor(), Lines: []StatementLine{}, NextCursor: st.NextCursor}
	for _, l := range st.Lines {
		v.Lines = append(v.Lines, StatementLine{Seq: l.Seq, TxID: l.TxID, Kind: l.Kind.String(), Phase: l.Phase.String(), Time: l.Time,
			Description: l.Description, Direction: l.Direction.String(), AmountCents: l.Amount.Minor(), BalanceCents: l.Balance.Minor(), AvailableCents: l.Available.Minor()})
	}
	return v, nil
}

// TrialBalanceRow is one account's totals.
type TrialBalanceRow struct {
	Account             ledger.AccountID `json:"account"`
	Type                string           `json:"type"`
	DebitsPostedCents   int64            `json:"debits_posted_cents"`
	CreditsPostedCents  int64            `json:"credits_posted_cents"`
	DebitsPendingCents  int64            `json:"debits_pending_cents"`
	CreditsPendingCents int64            `json:"credits_pending_cents"`
	BalanceCents        int64            `json:"balance_cents"`
}

// TrialBalanceView is the whole ledger at one instant.
type TrialBalanceView struct {
	Balanced bool `json:"balanced"`
	// DifferenceCents is posted debits minus posted credits in BRL (zero
	// in a correct ledger); PendingDifferenceCents is the same for holds.
	DifferenceCents        int64             `json:"difference_cents"`
	PendingDifferenceCents int64             `json:"pending_difference_cents"`
	Error                  string            `json:"error,omitempty"`
	Rows                   []TrialBalanceRow `json:"rows"`
}

// TrialBalance snapshots every account and checks debits equal credits.
func (s *Service) TrialBalance(ctx context.Context) (TrialBalanceView, error) {
	tb, err := s.ledger.TrialBalance(ctx)
	if err != nil {
		return TrialBalanceView{}, err
	}
	v := TrialBalanceView{Balanced: true, Rows: []TrialBalanceRow{}}
	if err := tb.Check(); err != nil {
		v.Balanced, v.Error = false, err.Error()
	}
	t := tb.Totals[money.BRL]
	v.DifferenceCents = t.DebitsPosted - t.CreditsPosted
	v.PendingDifferenceCents = t.DebitsPending - t.CreditsPending
	for _, r := range tb.Rows {
		v.Rows = append(v.Rows, TrialBalanceRow{Account: r.Account.ID, Type: r.Account.Type.String(), DebitsPostedCents: r.DebitsPosted.Minor(),
			CreditsPostedCents: r.CreditsPosted.Minor(), DebitsPendingCents: r.DebitsPending.Minor(), CreditsPendingCents: r.CreditsPending.Minor(),
			BalanceCents: r.Balance.Minor()})
	}
	return v, nil
}

// ChainView is the result of verifying the hash chain.
type ChainView struct {
	Valid    bool      `json:"valid"`
	Seq      uint64    `json:"seq"`
	HeadHash string    `json:"head_hash,omitempty"`
	HeadTime time.Time `json:"head_time,omitempty"`
	Error    string    `json:"error,omitempty"`
	// BalancesMatch is VerifyBalances: stored counters equal a replay of
	// every entry.
	BalancesMatch bool `json:"balances_match"`
}

// VerifyChain recomputes the audit chain and the balances.
func (s *Service) VerifyChain(ctx context.Context) ChainView {
	head, err := s.ledger.VerifyChain(ctx)
	v := ChainView{Valid: err == nil, Seq: head.Seq, HeadTime: head.Time}
	if err != nil {
		v.Error = err.Error()
	} else {
		v.HeadHash = hex.EncodeToString(head.Hash[:])
	}
	if err := s.ledger.VerifyBalances(ctx); err != nil {
		if v.Error == "" {
			v.Error = err.Error()
		}
	} else {
		v.BalancesMatch = true
	}
	return v
}

// Reconciliation compares the ledger with the SPI settlement account.
type Reconciliation struct {
	SPIAccount           string `json:"spi_account"`
	SPIBalanceCents      int64  `json:"spi_balance_cents"`
	LedgerPostedCents    int64  `json:"ledger_posted_cents"`
	InFlightCents        int64  `json:"in_flight_cents"`
	UnsubmittedHoldCents int64  `json:"unsubmitted_hold_cents"`
	// ExpectedSPICents is what the SPI should report: posted reserves
	// minus transfers already submitted (the SPI debits those at ACSP).
	ExpectedSPICents int64 `json:"expected_spi_cents"`
	DifferenceCents  int64 `json:"difference_cents"`
	InSync           bool  `json:"in_sync"`
}

// Reconcile reads the SPI balance and compares it with the ledger. A
// non-zero difference while traffic is flowing is normal (a webhook in
// transit); a persistent one is an incident.
func (s *Service) Reconcile(ctx context.Context) (Reconciliation, error) {
	r := Reconciliation{SPIAccount: s.cfg.SPIAccountID}
	for _, t := range s.store.transfersWhere(func(*record) bool { return true }) {
		t.mu.Lock()
		switch t.Status {
		case StatusSubmitted:
			r.InFlightCents += t.AmountCents
		case StatusHeld, StatusAwaitingConfirmation:
			r.UnsubmittedHoldCents += t.AmountCents
		}
		t.mu.Unlock()
	}
	b, err := s.ledger.Balance(ctx, AcctReserves)
	if err != nil {
		return r, err
	}
	acct, err := s.sim.Account(ctx, s.cfg.SPIAccountID)
	if err != nil {
		return r, problem(http.StatusBadGateway, "spi-unavailable", "Reading the SPI balance failed", err.Error())
	}
	r.SPIBalanceCents = acct.BalanceCents
	r.LedgerPostedCents = b.Posted.Minor()
	r.ExpectedSPICents = r.LedgerPostedCents - r.InFlightCents
	r.DifferenceCents = r.SPIBalanceCents - r.ExpectedSPICents
	r.InSync = r.DifferenceCents == 0
	return r, nil
}
