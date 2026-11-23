// Package httpx holds the HTTP plumbing shared by the services: a JSON
// client that propagates trace context and records downstream metrics, and
// server middleware for metrics, access logs, panic recovery and RFC 9457
// problem responses.
package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/metrics"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// RequestIDHeader is set to the trace ID on every outgoing call. The
// simulator and payments-service do not speak W3C Trace Context but log a
// client-supplied X-Request-Id, so their log lines carry the same ID.
const RequestIDHeader = "X-Request-Id"

// Downstream records calls to other services. Both fields may be nil.
type Downstream struct {
	Requests *metrics.CounterVec   // labels: service, code
	Duration *metrics.HistogramVec // labels: service
}

// NewDownstream registers the downstream metric families on reg.
func NewDownstream(reg *metrics.Registry, prefix string) Downstream {
	return Downstream{
		Requests: reg.NewCounter(prefix+"_downstream_requests_total", "Calls to other services by service and status code (\"error\" for transport failures).", "service", "code"),
		Duration: reg.NewHistogram(prefix+"_downstream_request_duration_seconds", "Latency of calls to other services.", metrics.DefaultBuckets, "service"),
	}
}

// Client calls one downstream service.
type Client struct {
	Service string // label for metrics and logs, e.g. "riskd"
	BaseURL string
	HTTP    *http.Client
	Headers http.Header // added to every request (e.g. Authorization)
	Metrics Downstream
}

// NewClient returns a client with a sane timeout.
func NewClient(service, baseURL string, timeout time.Duration, m Downstream) *Client {
	return &Client{Service: service, BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: timeout}, Headers: http.Header{}, Metrics: m}
}

// Response is a fully read response.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// JSON decodes the body into v.
func (r *Response) JSON(v any) error {
	if err := json.Unmarshal(r.Body, v); err != nil {
		return fmt.Errorf("decoding %d response: %w", r.Status, err)
	}
	return nil
}

// Error is a non-2xx response.
type Error struct {
	Service string
	Method  string
	Path    string
	Status  int
	Body    []byte
	// Code is the "code" (simulator) or "type" (RFC 9457) member of a
	// problem body, when present.
	Code   string
	Detail string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("%s %s %s: HTTP %d", e.Service, e.Method, e.Path, e.Status)
	if e.Code != "" {
		s += " " + e.Code
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Retryable reports whether the failure is worth retrying: 5xx, 429 and
// 408. Any other 4xx is a definitive answer.
func (e *Error) Retryable() bool {
	return e.Status >= 500 || e.Status == http.StatusTooManyRequests || e.Status == http.StatusRequestTimeout
}

// AsError returns the *Error in err's chain.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}

// IsStatus reports whether err is an HTTP error with the given status.
func IsStatus(err error, status int) bool {
	e, ok := AsError(err)
	return ok && e.Status == status
}

// Do sends body (JSON-encoded unless it is a []byte) and returns the
// response. Non-2xx responses are returned as *Error together with the
// response, so callers can still inspect it.
func (c *Client) Do(ctx context.Context, method, path string, body any, h http.Header) (*Response, error) {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
	if err != nil {
		return nil, err
	}
	for k, vs := range c.Headers {
		req.Header[k] = append([]string(nil), vs...)
	}
	for k, vs := range h {
		req.Header[k] = append([]string(nil), vs...)
	}
	if rd != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	span := trace.Inject(ctx, req.Header)
	req.Header.Set(RequestIDHeader, span.TraceIDString())

	start := time.Now()
	resp, err := c.HTTP.Do(req)
	elapsed := time.Since(start).Seconds()
	if c.Metrics.Duration != nil {
		c.Metrics.Duration.Observe(elapsed, c.Service)
	}
	if err != nil {
		if c.Metrics.Requests != nil {
			c.Metrics.Requests.Inc(c.Service, "error")
		}
		return nil, fmt.Errorf("%s %s %s: %w", c.Service, method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if c.Metrics.Requests != nil {
		c.Metrics.Requests.Inc(c.Service, strconv.Itoa(resp.StatusCode))
	}
	if err != nil {
		return nil, fmt.Errorf("%s %s %s: reading body: %w", c.Service, method, path, err)
	}
	out := &Response{Status: resp.StatusCode, Header: resp.Header, Body: raw}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		e := &Error{Service: c.Service, Method: method, Path: path, Status: resp.StatusCode, Body: raw}
		var p struct {
			Code   string `json:"code"`
			Type   string `json:"type"`
			Detail string `json:"detail"`
			Error  string `json:"error"`
		}
		if json.Unmarshal(raw, &p) == nil {
			e.Code = p.Code
			if e.Code == "" {
				e.Code = p.Type
			}
			e.Detail = p.Detail
			if e.Detail == "" {
				e.Detail = p.Error
			}
		}
		return out, e
	}
	return out, nil
}

// JSON is Do followed by decoding a 2xx body into out (when not nil).
func (c *Client) JSON(ctx context.Context, method, path string, body any, h http.Header, out any) (*Response, error) {
	resp, err := c.Do(ctx, method, path, body, h)
	if err != nil {
		return resp, err
	}
	if out != nil && len(resp.Body) > 0 {
		if err := resp.JSON(out); err != nil {
			return resp, fmt.Errorf("%s %s %s: %w", c.Service, method, path, err)
		}
	}
	return resp, nil
}
