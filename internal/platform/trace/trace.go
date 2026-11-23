// Package trace implements the subset of W3C Trace Context
// (https://www.w3.org/TR/trace-context/) the services in this repository
// need: parsing and emitting the traceparent header, deriving child spans
// for outgoing calls and carrying the current span in a context.Context.
//
// There is deliberately no exporter. Every service logs trace_id and
// span_id on each line, so a single Pix can be followed across the gateway,
// the relay and the merchant with nothing more than grep. Swapping this for
// the OpenTelemetry SDK is a drop-in change at the edges (middleware and
// HTTP client).
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

// Header is the W3C traceparent header.
const Header = "traceparent"

// ResponseHeader returns the trace ID to callers so they can search logs.
const ResponseHeader = "X-Trace-Id"

// Span identifies one unit of work inside a trace.
type Span struct {
	TraceID [16]byte
	SpanID  [8]byte
	Flags   byte
}

// New starts a new sampled trace.
func New() Span {
	var s Span
	_, _ = rand.Read(s.TraceID[:])
	_, _ = rand.Read(s.SpanID[:])
	s.Flags = 0x01
	return s
}

// Parse decodes a version-00 traceparent value. Invalid values, including
// the all-zero trace and span IDs the specification forbids, return false.
func Parse(v string) (Span, bool) {
	v = strings.TrimSpace(v)
	parts := strings.Split(v, "-")
	if len(parts) < 4 || len(parts[0]) != 2 || parts[0] == "ff" {
		return Span{}, false
	}
	if parts[0] == "00" && len(parts) != 4 {
		return Span{}, false
	}
	var s Span
	if !decode(parts[1], s.TraceID[:]) || !decode(parts[2], s.SpanID[:]) {
		return Span{}, false
	}
	var f [1]byte
	if !decode(parts[3], f[:]) {
		return Span{}, false
	}
	s.Flags = f[0]
	if s.TraceID == [16]byte{} || s.SpanID == [8]byte{} {
		return Span{}, false
	}
	return s, true
}

func decode(s string, dst []byte) bool {
	if len(s) != 2*len(dst) || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.Decode(dst, []byte(s))
	return err == nil
}

// Child returns a new span in the same trace.
func (s Span) Child() Span {
	c := Span{TraceID: s.TraceID, Flags: s.Flags}
	_, _ = rand.Read(c.SpanID[:])
	return c
}

// Valid reports whether s has a trace ID.
func (s Span) Valid() bool { return s.TraceID != [16]byte{} }

// TraceIDString is the 32-character hex trace ID.
func (s Span) TraceIDString() string { return hex.EncodeToString(s.TraceID[:]) }

// SpanIDString is the 16-character hex span ID.
func (s Span) SpanIDString() string { return hex.EncodeToString(s.SpanID[:]) }

// String encodes s as a traceparent value.
func (s Span) String() string {
	return "00-" + s.TraceIDString() + "-" + s.SpanIDString() + "-" + hex.EncodeToString([]byte{s.Flags})
}

type ctxKey struct{}

// With returns a context carrying s.
func With(ctx context.Context, s Span) context.Context { return context.WithValue(ctx, ctxKey{}, s) }

// From returns the span carried by ctx.
func From(ctx context.Context) (Span, bool) {
	s, ok := ctx.Value(ctxKey{}).(Span)
	return s, ok && s.Valid()
}

// Ensure returns ctx and its span, starting a new trace when ctx has none.
func Ensure(ctx context.Context) (context.Context, Span) {
	if s, ok := From(ctx); ok {
		return ctx, s
	}
	s := New()
	return With(ctx, s), s
}

// Continue returns a context with a child of the span encoded in
// traceparent, or a new trace when the value is empty or invalid. Services
// use it to resume a trace stored with an entity, for example when a
// settlement webhook arrives for a transfer created earlier.
func Continue(ctx context.Context, traceparent string) context.Context {
	if p, ok := Parse(traceparent); ok {
		return With(ctx, p.Child())
	}
	return With(ctx, New())
}

// Middleware makes every request a server span: a child of the caller's
// traceparent when valid, otherwise the root of a new trace. The trace ID
// is echoed in X-Trace-Id.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := Continue(r.Context(), r.Header.Get(Header))
		s, _ := From(ctx)
		w.Header().Set(ResponseHeader, s.TraceIDString())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Inject sets traceparent on an outgoing request to a new child of the
// span in ctx (starting a trace if needed) and returns that child.
func Inject(ctx context.Context, h http.Header) Span {
	_, parent := Ensure(ctx)
	c := parent.Child()
	h.Set(Header, c.String())
	return c
}
