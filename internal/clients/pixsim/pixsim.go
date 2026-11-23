// Package pixsim is a client for pix-psp-simulator, which plays the SPI
// (settlement), the DICT (key directory) and the counterparty PSPs.
//
// The types mirror the simulator's JSON (internal/sim and internal/dict in
// that repository) for the fields the gateway uses; unknown fields are
// ignored so the simulator can grow without breaking this client.
package pixsim

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
)

// ISO 20022 transaction statuses used by the simulator.
const (
	StatusPending  = "ACSP"
	StatusSettled  = "ACSC"
	StatusRejected = "RJCT"
)

// Client calls the simulator.
type Client struct{ c *httpx.Client }

// New returns a client for baseURL.
func New(baseURL string, m httpx.Downstream) *Client {
	return &Client{c: httpx.NewClient("pixsim", baseURL, 10*time.Second, m)}
}

// Account is a simulator account (internal/ledger.Account).
type Account struct {
	ID           string    `json:"id"`
	ISPB         string    `json:"ispb"`
	Branch       string    `json:"branch"`
	Number       string    `json:"number"`
	Type         string    `json:"type"`
	OwnerName    string    `json:"owner_name"`
	Status       string    `json:"status"`
	BalanceCents int64     `json:"balance_cents"`
	CreatedAt    time.Time `json:"created_at"`
}

// KeyLookup is GET /dict/keys/{key} (internal/dict.LookupResult).
type KeyLookup struct {
	Key         string    `json:"key"`
	KeyType     string    `json:"key_type"` // CPF, CNPJ, EMAIL, PHONE, EVP
	OwnerName   string    `json:"owner_name"`
	OwnerTaxID  string    `json:"owner_tax_id_masked"`
	OwnerType   string    `json:"owner_type"` // NATURAL_PERSON or LEGAL_PERSON
	ISPB        string    `json:"ispb"`
	Branch      string    `json:"branch"`
	Number      string    `json:"account_number"`
	AccountType string    `json:"account_type"`
	CreatedAt   time.Time `json:"created_at"`
	AccountID   string    `json:"account_id"`
}

// Party is one side of a transfer (internal/sim.Party).
type Party struct {
	AccountID   string `json:"account_id,omitempty"`
	ISPB        string `json:"ispb"`
	Branch      string `json:"branch"`
	Number      string `json:"account_number"`
	AccountType string `json:"account_type,omitempty"`
	Name        string `json:"name,omitempty"`
	TaxIDMasked string `json:"tax_id_masked,omitempty"`
	Key         string `json:"key,omitempty"`
}

// Transfer is a Pix transfer (internal/sim.Transfer).
type Transfer struct {
	EndToEndID        string     `json:"end_to_end_id"`
	TxID              string     `json:"txid,omitempty"`
	Initiation        string     `json:"initiation"`
	Status            string     `json:"status"`
	Reason            string     `json:"reason,omitempty"`
	ReasonDescription string     `json:"reason_description,omitempty"`
	AmountCents       int64      `json:"amount_cents"`
	RefundedCents     int64      `json:"refunded_cents"`
	Description       string     `json:"description,omitempty"`
	Payer             Party      `json:"payer"`
	Payee             Party      `json:"payee"`
	CreatedAt         time.Time  `json:"created_at"`
	SettledAt         *time.Time `json:"settled_at,omitempty"`
}

// TransferRequest is POST /pix/transfers (internal/sim.TransferRequest).
type TransferRequest struct {
	FromAccountID string `json:"from_account_id"`
	Key           string `json:"key,omitempty"`
	AmountCents   int64  `json:"amount_cents"`
	EndToEndID    string `json:"end_to_end_id,omitempty"`
	Description   string `json:"description,omitempty"`
}

// Refund is a devolução (internal/sim.Refund).
type Refund struct {
	ID                string    `json:"id"`
	EndToEndID        string    `json:"end_to_end_id"`
	AmountCents       int64     `json:"amount_cents"`
	ReturnReason      string    `json:"return_reason"`
	Status            string    `json:"status"`
	Reason            string    `json:"reason,omitempty"`
	ReasonDescription string    `json:"reason_description,omitempty"`
	Description       string    `json:"description,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	InfractionID      string    `json:"infraction_id,omitempty"`
}

// RefundRequest is POST /pix/transfers/{e2eid}/refunds.
type RefundRequest struct {
	AmountCents int64  `json:"amount_cents"`
	Reason      string `json:"reason,omitempty"`
	Description string `json:"description,omitempty"`
}

// Infraction is a MED infraction report (internal/sim.Infraction).
type Infraction struct {
	ID         string    `json:"id"`
	EndToEndID string    `json:"end_to_end_id"`
	Reason     string    `json:"reason"`
	Details    string    `json:"details,omitempty"`
	Status     string    `json:"status"`
	HeldCents  int64     `json:"held_cents"`
	RefundID   string    `json:"refund_id,omitempty"`
	Allowed    []string  `json:"allowed_events"`
	History    []Step    `json:"history"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Step is one MED transition.
type Step struct {
	From  string    `json:"from"`
	Event string    `json:"event"`
	To    string    `json:"to"`
	At    time.Time `json:"at"`
	Note  string    `json:"note,omitempty"`
}

// Account returns an account, used to read the PSP's settlement balance.
func (s *Client) Account(ctx context.Context, id string) (Account, error) {
	var a Account
	_, err := s.c.JSON(ctx, http.MethodGet, "/accounts/"+url.PathEscape(id), nil, nil, &a)
	return a, err
}

// LookupKey resolves a Pix key on behalf of payerID (the DICT rate limit is
// per payer, sent as X-Payer-Id).
func (s *Client) LookupKey(ctx context.Context, key, payerID string) (KeyLookup, error) {
	var r KeyLookup
	h := http.Header{}
	h.Set("X-Payer-Id", payerID)
	_, err := s.c.JSON(ctx, http.MethodGet, "/dict/keys/"+url.PathEscape(key), nil, h, &r)
	return r, err
}

// CreateTransfer submits a transfer to the SPI. idempotencyKey makes the
// call safe to retry; chaos carries optional X-Sim-* headers.
func (s *Client) CreateTransfer(ctx context.Context, req TransferRequest, idempotencyKey string, chaos http.Header) (Transfer, error) {
	h := chaos.Clone()
	if h == nil {
		h = http.Header{}
	}
	if idempotencyKey != "" {
		h.Set("Idempotency-Key", idempotencyKey)
	}
	var t Transfer
	_, err := s.c.JSON(ctx, http.MethodPost, "/pix/transfers", req, h, &t)
	return t, err
}

// Transfer reads a transfer by EndToEndId.
func (s *Client) Transfer(ctx context.Context, e2e string) (Transfer, error) {
	var t Transfer
	_, err := s.c.JSON(ctx, http.MethodGet, "/pix/transfers/"+url.PathEscape(e2e), nil, nil, &t)
	return t, err
}

// CreateRefund asks the payee side to return funds of a settled transfer.
// The simulator settles refunds synchronously: ACSC or RJCT.
func (s *Client) CreateRefund(ctx context.Context, e2e string, req RefundRequest, chaos http.Header) (Refund, error) {
	var r Refund
	_, err := s.c.JSON(ctx, http.MethodPost, "/pix/transfers/"+url.PathEscape(e2e)+"/refunds", req, chaos, &r)
	return r, err
}

// Refund reads one refund of a transfer.
func (s *Client) Refund(ctx context.Context, e2e, id string) (Refund, error) {
	var r Refund
	_, err := s.c.JSON(ctx, http.MethodGet, "/pix/transfers/"+url.PathEscape(e2e)+"/refunds/"+url.PathEscape(id), nil, nil, &r)
	return r, err
}

// ReportInfraction opens a MED infraction on a settled transfer.
func (s *Client) ReportInfraction(ctx context.Context, e2e, reason, details string) (Infraction, error) {
	var inf Infraction
	body := map[string]string{"end_to_end_id": e2e, "reason": reason, "details": details}
	_, err := s.c.JSON(ctx, http.MethodPost, "/med/infractions", body, nil, &inf)
	return inf, err
}

// ActOnInfraction applies block, analyze, accept, reject or cancel.
func (s *Client) ActOnInfraction(ctx context.Context, id, action string) (Infraction, error) {
	var inf Infraction
	_, err := s.c.JSON(ctx, http.MethodPost, "/med/infractions/"+url.PathEscape(id)+"/"+url.PathEscape(action), nil, nil, &inf)
	return inf, err
}

// Healthz probes the simulator.
func (s *Client) Healthz(ctx context.Context) error {
	_, err := s.c.Do(ctx, http.MethodGet, "/healthz", nil, nil)
	return err
}
