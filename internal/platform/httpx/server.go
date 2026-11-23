package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/metrics"
	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// MaxBody bounds request bodies.
const MaxBody = 1 << 20

// Problem is an RFC 9457 problem detail. Extensions are merged into the
// top-level object.
type Problem struct {
	Type       string         `json:"type"`
	Title      string         `json:"title"`
	Status     int            `json:"status"`
	Detail     string         `json:"detail,omitempty"`
	Instance   string         `json:"instance,omitempty"`
	TraceID    string         `json:"trace_id,omitempty"`
	Extensions map[string]any `json:"-"`
}

// WriteProblem writes p as application/problem+json.
func WriteProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	if p.Instance == "" {
		p.Instance = r.URL.Path
	}
	if s, ok := trace.From(r.Context()); ok {
		p.TraceID = s.TraceIDString()
	}
	m := map[string]any{}
	for k, v := range p.Extensions {
		m[k] = v
	}
	m["type"], m["title"], m["status"] = p.Type, p.Title, p.Status
	if p.Detail != "" {
		m["detail"] = p.Detail
	}
	m["instance"] = p.Instance
	if p.TraceID != "" {
		m["trace_id"] = p.TraceID
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(m)
}

// WriteJSON writes v with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// DecodeJSON reads a strict JSON body (unknown fields rejected) into v and
// writes a 400 problem on failure.
func DecodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		WriteProblem(w, r, Problem{Type: "/problems/unsupported-media-type", Title: "Use Content-Type: application/json", Status: http.StatusUnsupportedMediaType})
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("empty body")
		}
		WriteProblem(w, r, Problem{Type: "/problems/malformed-request", Title: "Malformed JSON body", Status: http.StatusBadRequest, Detail: err.Error()})
		return false
	}
	if dec.More() {
		WriteProblem(w, r, Problem{Type: "/problems/malformed-request", Title: "Malformed JSON body", Status: http.StatusBadRequest, Detail: "trailing data after JSON value"})
		return false
	}
	return true
}

// Recorder captures the status code.
type Recorder struct {
	http.ResponseWriter
	Status int
	wrote  bool
}

// WriteHeader records the first status.
func (r *Recorder) WriteHeader(code int) {
	if !r.wrote {
		r.Status, r.wrote = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *Recorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.Status, r.wrote = http.StatusOK, true
	}
	return r.ResponseWriter.Write(b)
}

// ServerMetrics are the inbound HTTP metric families.
type ServerMetrics struct {
	Requests *metrics.CounterVec   // route, code
	Duration *metrics.HistogramVec // route
}

// NewServerMetrics registers prefix_http_requests_total and
// prefix_http_request_duration_seconds.
func NewServerMetrics(reg *metrics.Registry, prefix string) ServerMetrics {
	return ServerMetrics{
		Requests: reg.NewCounter(prefix+"_http_requests_total", "HTTP requests by route pattern and status code.", "route", "code"),
		Duration: reg.NewHistogram(prefix+"_http_request_duration_seconds", "HTTP request latency by route pattern.", metrics.DefaultBuckets, "route"),
	}
}

// Instrument wraps a ServeMux: trace context, panic recovery, metrics
// labelled by the matched route pattern (bounded cardinality) and one
// access log line per request.
func Instrument(mux *http.ServeMux, m ServerMetrics, log *slog.Logger) http.Handler {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" {
			pattern = "unmatched"
		}
		rec := &Recorder{ResponseWriter: w, Status: http.StatusOK}
		start := time.Now()
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.ErrorContext(r.Context(), "panic", slog.String("panic", fmt.Sprint(v)), slog.String("route", pattern))
				if !rec.wrote {
					WriteProblem(rec, r, Problem{Type: "/problems/internal-error", Title: "Internal error", Status: http.StatusInternalServerError})
				}
			}
			d := time.Since(start)
			m.Requests.Inc(pattern, strconv.Itoa(rec.Status))
			m.Duration.Observe(d.Seconds(), pattern)
			if pattern != "GET /metrics" && pattern != "GET /healthz" && pattern != "GET /readyz" {
				log.InfoContext(r.Context(), "http request", slog.String("route", pattern), slog.Int("status", rec.Status),
					slog.Float64("duration_ms", float64(d.Microseconds())/1000))
			}
		}()
		mux.ServeHTTP(rec, r)
	})
	return trace.Middleware(inner)
}

// Probe GETs url and returns nil on a 2xx. Distroless images have no curl,
// so each binary implements its own -healthcheck flag with this.
func Probe(url string, timeout time.Duration) error {
	c := &http.Client{Timeout: timeout}
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return nil
}

// ProbeAddr builds a loopback URL for a listen address such as ":8080".
func ProbeAddr(addr, path string) string {
	host, port := "127.0.0.1", strings.TrimPrefix(addr, ":")
	if i := strings.LastIndex(addr, ":"); i > 0 {
		host, port = addr[:i], addr[i+1:]
		if host == "0.0.0.0" || host == "" {
			host = "127.0.0.1"
		}
	}
	return "http://" + host + ":" + port + path
}
