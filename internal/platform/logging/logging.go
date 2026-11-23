// Package logging builds the JSON slog logger shared by the services and
// stamps every record with the trace and span IDs found in its context.
package logging

import (
	"context"
	"io"
	"log/slog"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/trace"
)

// New returns a JSON logger at level that adds trace_id and span_id to
// every record logged with a context carrying a span.
func New(w io.Writer, level slog.Level, service string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(traceHandler{h}).With(slog.String("service", service))
}

// ParseLevel parses debug, info, warn or error; anything else is info.
func ParseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if s, ok := trace.From(ctx); ok {
		r.AddAttrs(slog.String("trace_id", s.TraceIDString()), slog.String("span_id", s.SpanIDString()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(as)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}
