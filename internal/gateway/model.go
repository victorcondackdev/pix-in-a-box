package gateway

import (
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/victorcondackdev/ledger-go/ledger"

	"github.com/victorcondackdev/pix-in-a-box/internal/clients/riskd"
)

// Status is the lifecycle state of an outgoing Pix.
//
//	                     ┌─ BLOCKED                       (risk)
//	                     ├─ AWAITING_CONFIRMATION ─┬─► SUBMITTED   (step-up confirmed)
//	request ─► risk ─────┤                         └─► CANCELLED   (expired / declined)
//	                     ├─ HELD ──────────────────┬─► SUBMITTED   (cautionary delay over / analyst release)
//	                     │                         └─► CANCELLED   (analyst rejects)
//	                     └─ SUBMITTED ─────────────┬─► SETTLED     (ACSC: hold posted)
//	                                               ├─► REJECTED    (RJCT: hold voided)
//	                                               └─► FAILED      (never reached the SPI: hold voided)
type Status string

// Transfer statuses.
const (
	StatusAwaitingConfirmation Status = "AWAITING_CONFIRMATION"
	StatusHeld                 Status = "HELD"
	StatusSubmitted            Status = "SUBMITTED"
	StatusSettled              Status = "SETTLED"
	StatusRejected             Status = "REJECTED"
	StatusBlocked              Status = "BLOCKED"
	StatusCancelled            Status = "CANCELLED"
	StatusFailed               Status = "FAILED"
)

// Terminal reports whether no further transition is possible (refunds of
// settled transfers are separate records, not status changes).
func (s Status) Terminal() bool {
	switch s {
	case StatusSettled, StatusRejected, StatusBlocked, StatusCancelled, StatusFailed:
		return true
	}
	return false
}

// Customer is an account holder of this PSP.
type Customer struct {
	ID              string           `json:"id"`
	Name            string           `json:"name"`
	Type            string           `json:"type"` // PF or PJ
	DayLimitCents   int64            `json:"day_limit_cents"`
	NightLimitCents int64            `json:"night_limit_cents"`
	Tags            []string         `json:"tags,omitempty"`
	OpenedAt        time.Time        `json:"opened_at"`
	LedgerAccount   ledger.AccountID `json:"ledger_account"`
}

// Beneficiary is the DICT view of the payee, with the owner's tax ID
// already masked by the DICT.
type Beneficiary struct {
	Name        string `json:"name"`
	TaxIDMasked string `json:"tax_id_masked"`
	PersonType  string `json:"person_type"`
	ISPB        string `json:"ispb"`
	Branch      string `json:"branch"`
	Account     string `json:"account"`
	AccountType string `json:"account_type"`
	KeyType     string `json:"key_type"`
	// RiskID is the opaque identifier sent to riskd (a hash of the
	// account coordinates, never a document).
	RiskID string `json:"risk_id"`
}

// Risk summarises the decision that admitted, held or blocked a transfer:
// the explanation goes back to the API client and into the audit trail.
type Risk struct {
	Action    string      `json:"action"`
	Score     int         `json:"score"`
	Reasons   []string    `json:"reasons"`
	Hits      []riskd.Hit `json:"hits,omitempty"`
	HoldFor   string      `json:"hold_for,omitempty"`
	HoldUntil *time.Time  `json:"hold_until,omitempty"`
	Ruleset   string      `json:"ruleset,omitempty"`
	Revision  int64       `json:"revision,omitempty"`
	Degraded  bool        `json:"degraded,omitempty"`
	// Unavailable is set when riskd could not be reached and the gateway
	// failed open.
	Unavailable bool `json:"unavailable,omitempty"`
}

// Challenge is a pending step-up authentication.
type Challenge struct {
	ID        string    `json:"id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Refund is a devolução of (part of) a settled transfer.
type Refund struct {
	ID           string           `json:"id"` // the SPI return identification ("D...")
	AmountCents  int64            `json:"amount_cents"`
	Reason       string           `json:"reason"` // MD06, BE08, SL02, FR01 (MED)
	Status       string           `json:"status"` // ACSC or RJCT
	RejectReason string           `json:"reject_reason,omitempty"`
	InfractionID string           `json:"infraction_id,omitempty"`
	LedgerTx     ledger.TxID      `json:"ledger_tx,omitempty"`
	LedgerKind   string           `json:"ledger_kind,omitempty"` // reversal or compensating
	CreatedAt    time.Time        `json:"created_at"`
	BookedTo     ledger.AccountID `json:"booked_to,omitempty"`
}

// LedgerRefs are the ledger transactions of a transfer.
type LedgerRefs struct {
	Hold ledger.TxID `json:"hold,omitempty"`
	Post ledger.TxID `json:"post,omitempty"`
	Void ledger.TxID `json:"void,omitempty"`
}

// Transfer is an outgoing Pix.
type Transfer struct {
	ID                 string      `json:"id"`
	EndToEndID         string      `json:"end_to_end_id"`
	AccountID          string      `json:"account_id"`
	Key                string      `json:"key"`
	AmountCents        int64       `json:"amount_cents"`
	Description        string      `json:"description,omitempty"`
	Status             Status      `json:"status"`
	FailureReason      string      `json:"failure_reason,omitempty"`
	FailureDescription string      `json:"failure_description,omitempty"`
	Beneficiary        Beneficiary `json:"beneficiary"`
	Risk               *Risk       `json:"risk,omitempty"`
	Challenge          *Challenge  `json:"challenge,omitempty"`
	Ledger             LedgerRefs  `json:"ledger"`
	RefundedCents      int64       `json:"refunded_cents"`
	Refunds            []Refund    `json:"refunds"`
	TraceID            string      `json:"trace_id,omitempty"`
	CreatedAt          time.Time   `json:"created_at"`
	UpdatedAt          time.Time   `json:"updated_at"`
	SubmittedAt        *time.Time  `json:"submitted_at,omitempty"`
	FinishedAt         *time.Time  `json:"finished_at,omitempty"`
}

// record is the mutable, lock-protected state behind a Transfer.
type record struct {
	Transfer
	traceparent string            // span to resume for asynchronous work
	riskTx      riskd.Transaction // recorded into riskd on settlement
	chaos       http.Header       // X-Sim-* headers, for deferred submissions
	mu          sync.Mutex        // serialises state transitions
	timer       *time.Timer       // cautionary hold release
	submitting  bool              // a submission call is in flight (t.mu released)
}

// snapshot returns a deep copy safe to serialise without the lock.
func (t *record) snapshot() Transfer {
	c := t.Transfer
	c.Refunds = append([]Refund{}, t.Refunds...)
	if t.Risk != nil {
		r := *t.Risk
		r.Reasons = append([]string(nil), t.Risk.Reasons...)
		r.Hits = append([]riskd.Hit(nil), t.Risk.Hits...)
		c.Risk = &r
	}
	if t.Challenge != nil {
		ch := *t.Challenge
		c.Challenge = &ch
	}
	return c
}

// Inbound is a Pix received into this PSP's settlement account.
type Inbound struct {
	EndToEndID    string           `json:"end_to_end_id"`
	TxID          string           `json:"txid,omitempty"`
	AmountCents   int64            `json:"amount_cents"`
	Key           string           `json:"key,omitempty"`
	PayerName     string           `json:"payer_name,omitempty"`
	PayerISPB     string           `json:"payer_ispb,omitempty"`
	CreditedTo    ledger.AccountID `json:"credited_to"`
	LedgerTx      ledger.TxID      `json:"ledger_tx"`
	ChargeID      string           `json:"charge_id,omitempty"`
	ChargeStatus  string           `json:"charge_status,omitempty"`
	ForwardResult string           `json:"forward_result"` // settled, duplicate, not_a_charge, rejected:<type>, skipped
	RefundedCents int64            `json:"refunded_cents"`
	ReceivedAt    time.Time        `json:"received_at"`
}

// Store is the gateway's in-memory state. The ledger is the system of
// record for money; this holds workflow state around it.
type Store struct {
	mu         sync.RWMutex
	customers  map[string]*Customer
	transfers  map[string]*record // by ID
	byE2E      map[string]*record
	inbound    map[string]*Inbound // by EndToEndId
	refunds    map[string]bool     // booked SPI return IDs
	fraudMarks map[string]int64    // beneficiary RiskID → MED reports
	events     map[string]time.Time
	inflight   map[string]bool
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		customers: map[string]*Customer{}, transfers: map[string]*record{}, byE2E: map[string]*record{},
		inbound: map[string]*Inbound{}, refunds: map[string]bool{}, fraudMarks: map[string]int64{}, events: map[string]time.Time{}, inflight: map[string]bool{},
	}
}

func (s *Store) customer(id string) (*Customer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.customers[id]
	return c, ok
}

func (s *Store) putCustomer(c *Customer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.customers[c.ID]; dup {
		return false
	}
	s.customers[c.ID] = c
	return true
}

func (s *Store) putTransfer(t *record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transfers[t.ID] = t
	s.byE2E[t.EndToEndID] = t
}

// transfer finds a transfer by ID or EndToEndId.
func (s *Store) transfer(id string) (*record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.transfers[id]; ok {
		return t, true
	}
	t, ok := s.byE2E[id]
	return t, ok
}

func (s *Store) transfersWhere(f func(*record) bool) []*record {
	s.mu.RLock()
	all := make([]*record, 0, len(s.transfers))
	for _, t := range s.transfers {
		all = append(all, t)
	}
	s.mu.RUnlock()
	out := all[:0]
	for _, t := range all {
		if f(t) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *Store) inboundByE2E(e2e string) (*Inbound, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	in, ok := s.inbound[e2e]
	return in, ok
}

func (s *Store) putInbound(in *Inbound) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inbound[in.EndToEndID] = in
}

// markRefund records a booked refund; it returns false if it was already
// booked.
func (s *Store) markRefund(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refunds[id] {
		return false
	}
	s.refunds[id] = true
	return true
}

func (s *Store) unmarkRefund(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.refunds, id)
}

func (s *Store) addFraudMark(riskID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fraudMarks[riskID]++
	return s.fraudMarks[riskID]
}

func (s *Store) fraudMarksOf(riskID string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fraudMarks[riskID]
}

// claimEvent reserves a webhook event ID for processing. It returns false
// when the event was already processed or is being processed by a
// concurrent delivery (the simulator sends chaos duplicates in parallel).
func (s *Store) claimEvent(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, done := s.events[id]; done || s.inflight[id] {
		return false
	}
	s.inflight[id] = true
	return true
}

// finishEvent releases a claim; processed events are remembered for a day
// (the simulator's retry schedule is far shorter).
func (s *Store) finishEvent(id string, processed bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, id)
	if !processed {
		return
	}
	s.events[id] = now
	if len(s.events) > 100_000 {
		for k, at := range s.events {
			if now.Sub(at) > 24*time.Hour {
				delete(s.events, k)
			}
		}
	}
}
