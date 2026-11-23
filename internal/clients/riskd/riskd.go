// Package riskd is a client for the fraud-rules-go risk service.
//
// Transaction mirrors engine.Transaction field for field: riskd decodes
// POST /v1/evaluate with DisallowUnknownFields, so this struct must never
// carry a field the engine does not know. Optional times are pointers so a
// missing value is omitted instead of being sent as year 1.
package riskd

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
)

// Actions, from least to most severe.
const (
	Allow  = "ALLOW"
	Review = "REVIEW"
	StepUp = "STEP_UP"
	Hold   = "HOLD"
	Block  = "BLOCK"
)

// Transaction is a transfer awaiting a decision (engine.Transaction).
type Transaction struct {
	ID          string      `json:"id"`
	Time        time.Time   `json:"time"`
	Amount      int64       `json:"amount"`
	Channel     string      `json:"channel,omitempty"`
	Initiation  string      `json:"initiation,omitempty"`
	Description string      `json:"description,omitempty"`
	Payer       Payer       `json:"payer"`
	Beneficiary Beneficiary `json:"beneficiary"`
}

// Payer is the sending side (engine.Payer).
type Payer struct {
	ID             string     `json:"id"`
	Type           string     `json:"type,omitempty"`
	AccountCreated *time.Time `json:"account_created,omitempty"`
	DayLimit       int64      `json:"day_limit,omitempty"`
	NightLimit     int64      `json:"night_limit,omitempty"`
	Device         Device     `json:"device"`
	IP             string     `json:"ip,omitempty"`
	Tags           []string   `json:"tags,omitempty"`
}

// Device is the initiating device (engine.Device).
type Device struct {
	ID        string     `json:"id"`
	Trusted   bool       `json:"trusted"`
	FirstSeen *time.Time `json:"first_seen,omitempty"`
}

// Beneficiary is the receiving side (engine.Beneficiary).
type Beneficiary struct {
	ID             string     `json:"id"`
	Type           string     `json:"type,omitempty"`
	Bank           string     `json:"bank,omitempty"`
	KeyType        string     `json:"key_type,omitempty"`
	KeyCreated     *time.Time `json:"key_created,omitempty"`
	AccountCreated *time.Time `json:"account_created,omitempty"`
	FraudMarks     int64      `json:"fraud_marks,omitempty"`
}

// Feature is one value a matched rule read: the explanation.
type Feature struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Display string `json:"display,omitempty"`
}

// Hit is a matched rule (engine.Hit).
type Hit struct {
	RuleID       string    `json:"rule_id"`
	Description  string    `json:"description,omitempty"`
	Action       string    `json:"action"`
	Score        int       `json:"score"`
	Mode         string    `json:"mode"`
	HoldFor      string    `json:"hold_for,omitempty"`
	Features     []Feature `json:"features"`
	FailedClosed bool      `json:"failed_closed,omitempty"`
}

// Outcome is an aggregated action (engine.Outcome).
type Outcome struct {
	Action  string `json:"action"`
	Score   int    `json:"score"`
	HoldFor string `json:"hold_for,omitempty"`
}

// Decision is the response of POST /v1/evaluate (engine.Decision, which
// embeds Outcome, so action/score/hold_for are top-level members).
type Decision struct {
	TransactionID string   `json:"transaction_id"`
	Action        string   `json:"action"`
	Score         int      `json:"score"`
	HoldFor       string   `json:"hold_for,omitempty"`
	Reasons       []string `json:"reasons"`
	Hits          []Hit    `json:"hits"`
	ShadowHits    []Hit    `json:"shadow_hits,omitempty"`
	Shadow        *Outcome `json:"shadow,omitempty"`
	Degraded      bool     `json:"degraded,omitempty"`
	Ruleset       string   `json:"ruleset"`
	Revision      int64    `json:"revision"`
	ElapsedNs     int64    `json:"elapsed_ns"`
}

// Client calls riskd.
type Client struct{ c *httpx.Client }

// New returns a client. The timeout is the gateway's whole risk budget
// including the network; riskd itself enforces a per-evaluation budget.
func New(baseURL string, timeout time.Duration, m httpx.Downstream) *Client {
	return &Client{c: httpx.NewClient("riskd", baseURL, timeout, m)}
}

// Evaluate asks for a decision.
func (c *Client) Evaluate(ctx context.Context, tx Transaction) (Decision, error) {
	var d Decision
	_, err := c.c.JSON(ctx, http.MethodPost, "/v1/evaluate", tx, nil, &d)
	if err == nil && d.Action == "" {
		err = fmt.Errorf("riskd: decision without action")
	}
	return d, err
}

// Record feeds settled transfers into the velocity store. riskd records on
// settlement, not on evaluation, so blocked attempts never inflate a
// victim's counters.
func (c *Client) Record(ctx context.Context, txs ...Transaction) error {
	_, err := c.c.Do(ctx, http.MethodPost, "/v1/events", txs, nil)
	return err
}

// Healthz is 200 once a ruleset is active.
func (c *Client) Healthz(ctx context.Context) error {
	_, err := c.c.Do(ctx, http.MethodGet, "/healthz", nil, nil)
	return err
}

// ParseDuration parses riskd's duration spelling (expr.FormatDuration):
// a sequence of <n><unit> with units d, h, m, s and ms, e.g. "1d2h30m".
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	var total time.Duration
	units := []struct {
		suffix string
		d      time.Duration
	}{{"ms", time.Millisecond}, {"d", 24 * time.Hour}, {"h", time.Hour}, {"m", time.Minute}, {"s", time.Second}}
	rest := s
	for rest != "" {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("riskd: invalid duration %q", s)
		}
		n, err := strconv.ParseInt(rest[:i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("riskd: invalid duration %q", s)
		}
		rest = rest[i:]
		matched := false
		for _, u := range units {
			if strings.HasPrefix(rest, u.suffix) {
				total += time.Duration(n) * u.d
				rest = rest[len(u.suffix):]
				matched = true
				break
			}
		}
		if !matched {
			return 0, fmt.Errorf("riskd: invalid duration unit in %q", s)
		}
	}
	return total, nil
}
