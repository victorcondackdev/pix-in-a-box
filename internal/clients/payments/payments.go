// Package payments is a client for payments-service-go: charge creation
// (proxied as-is, the service owns idempotency and the BR Code) and the
// PSP settlement webhook, which the gateway calls when the SPI reports a
// payment of one of its charges.
package payments

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
)

// SignatureHeader carries "sha256=<hex HMAC-SHA256(secret, raw body)>"
// (httpapi.WebhookSignatureHeader in payments-service-go).
const SignatureHeader = "X-Webhook-Signature"

// Client calls payments-service.
type Client struct {
	c      *httpx.Client
	secret []byte
}

// New returns a client. secret is payments-service's WEBHOOK_SECRET.
func New(baseURL, webhookSecret string, m httpx.Downstream) *Client {
	return &Client{c: httpx.NewClient("payments", baseURL, 10*time.Second, m), secret: []byte(webhookSecret)}
}

// Charge is the subset of payments-service's charge resource the gateway
// reads.
type Charge struct {
	ID          string `json:"id"`
	TxID        string `json:"txid"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
	EndToEndID  string `json:"end_to_end_id,omitempty"`
}

// Settlement is pixWebhookRequest; the service decodes it strictly.
type Settlement struct {
	TxID        string `json:"txid"`
	EndToEndID  string `json:"end_to_end_id"`
	AmountCents int64  `json:"amount_cents"`
}

// SettlementResult is pixWebhookResponse.
type SettlementResult struct {
	Charge    Charge `json:"charge"`
	Duplicate bool   `json:"duplicate"`
}

// Forward proxies a request verbatim (charge creation and reads) and
// returns the upstream response, including problem documents.
func (p *Client) Forward(ctx context.Context, method, path string, body []byte, idempotencyKey string) (*httpx.Response, error) {
	h := http.Header{}
	if idempotencyKey != "" {
		h.Set("Idempotency-Key", idempotencyKey)
	}
	var b any
	if body != nil {
		b = body
	}
	resp, err := p.c.Do(ctx, method, path, b, h)
	if _, isHTTP := httpx.AsError(err); isHTTP {
		return resp, nil // the upstream answered; the caller relays it
	}
	return resp, err
}

// ChargePath returns the path of a charge resource.
func ChargePath(id string) string { return "/v1/charges/" + url.PathEscape(id) }

// Settle reports a settled Pix for a charge. The body is signed exactly as
// sent, which is why it is marshalled here and passed as bytes.
func (p *Client) Settle(ctx context.Context, s Settlement) (SettlementResult, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return SettlementResult{}, err
	}
	h := http.Header{}
	if len(p.secret) > 0 {
		mac := hmac.New(sha256.New, p.secret)
		mac.Write(raw)
		h.Set(SignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	var out SettlementResult
	_, err = p.c.JSON(ctx, http.MethodPost, "/v1/webhooks/pix", raw, h, &out)
	return out, err
}

// Readyz probes the service (it depends only on the database).
func (p *Client) Readyz(ctx context.Context) error {
	_, err := p.c.Do(ctx, http.MethodGet, "/readyz", nil, nil)
	return err
}
