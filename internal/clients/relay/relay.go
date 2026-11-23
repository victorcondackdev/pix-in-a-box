// Package relay is a client for webhook-relay-go's relayd: endpoint
// registration and message publishing. relayd owns retries, the WAL, the
// dead-letter queue and Standard Webhooks signing; producers only need to
// get a message accepted once (Idempotency-Key makes that safe to retry).
package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
)

// Endpoint is the subset of relayd's endpoint view the gateway uses.
type Endpoint struct {
	ID         string   `json:"id"`
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
	Disabled   bool     `json:"disabled"`
}

// EndpointSpec is POST /v1/endpoints.
type EndpointSpec struct {
	URL            string   `json:"url"`
	Description    string   `json:"description,omitempty"`
	EventTypes     []string `json:"event_types,omitempty"`
	Secret         string   `json:"secret,omitempty"`
	Ordered        bool     `json:"ordered,omitempty"`
	MaxConcurrency int      `json:"max_concurrency,omitempty"`
}

// Delivery is one fan-out target of a message.
type Delivery struct {
	ID         string `json:"id"`
	EndpointID string `json:"endpoint_id"`
	State      string `json:"state"`
	Attempts   int    `json:"attempts"`
}

// Message is relayd's message view.
type Message struct {
	ID         string     `json:"id"`
	EventType  string     `json:"event_type"`
	CreatedAt  time.Time  `json:"created_at"`
	Deliveries []Delivery `json:"deliveries"`
}

// Client calls relayd.
type Client struct{ c *httpx.Client }

// New returns a client; token, when set, is sent as a bearer token
// (relayd's RELAY_API_TOKEN).
func New(baseURL, token string, m httpx.Downstream) *Client {
	c := httpx.NewClient("relayd", baseURL, 10*time.Second, m)
	if token != "" {
		c.Headers.Set("Authorization", "Bearer "+token)
	}
	return &Client{c: c}
}

// Endpoints lists registered endpoints.
func (r *Client) Endpoints(ctx context.Context) ([]Endpoint, error) {
	var out struct {
		Data []Endpoint `json:"data"`
	}
	_, err := r.c.JSON(ctx, http.MethodGet, "/v1/endpoints", nil, nil, &out)
	return out.Data, err
}

// CreateEndpoint registers an endpoint.
func (r *Client) CreateEndpoint(ctx context.Context, spec EndpointSpec) (Endpoint, error) {
	var ep Endpoint
	_, err := r.c.JSON(ctx, http.MethodPost, "/v1/endpoints", spec, nil, &ep)
	return ep, err
}

// EnsureEndpoint returns the endpoint registered for spec.URL, creating it
// when absent. It is safe to call on every start: relayd keeps endpoints in
// its WAL across restarts.
func (r *Client) EnsureEndpoint(ctx context.Context, spec EndpointSpec) (Endpoint, bool, error) {
	eps, err := r.Endpoints(ctx)
	if err != nil {
		return Endpoint{}, false, err
	}
	for _, ep := range eps {
		if ep.URL == spec.URL {
			return ep, false, nil
		}
	}
	ep, err := r.CreateEndpoint(ctx, spec)
	return ep, err == nil, err
}

// Publish hands a message to relayd, which fans it out to every endpoint
// subscribed to eventType. The trace context in ctx is stored with the
// message and forwarded on every delivery attempt.
func (r *Client) Publish(ctx context.Context, eventType, idempotencyKey string, payload any) (Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Message{}, err
	}
	body := struct {
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
	}{eventType, raw}
	h := http.Header{}
	if idempotencyKey != "" {
		h.Set("Idempotency-Key", idempotencyKey)
	}
	var m Message
	_, err = r.c.JSON(ctx, http.MethodPost, "/v1/messages", body, h, &m)
	return m, err
}

// Healthz probes relayd.
func (r *Client) Healthz(ctx context.Context) error {
	_, err := r.c.Do(ctx, http.MethodGet, "/healthz", nil, nil)
	return err
}
