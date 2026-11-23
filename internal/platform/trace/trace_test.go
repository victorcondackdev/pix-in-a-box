package trace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParse(t *testing.T) {
	ok := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	s, valid := Parse(ok)
	if !valid || s.String() != ok || s.TraceIDString() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("parse %q: %v %s", ok, valid, s)
	}
	for _, bad := range []string{
		"", "garbage", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace ID
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero span ID
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01", // upper case
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // forbidden version
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
	} {
		if _, valid := Parse(bad); valid {
			t.Errorf("accepted %q", bad)
		}
	}
	if _, valid := Parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-future"); !valid {
		t.Error("future versions may append fields")
	}
}

func TestMiddlewareAndInject(t *testing.T) {
	var seen Span
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, _ = From(r.Context())
		out := http.Header{}
		child := Inject(r.Context(), out)
		if child.TraceID != seen.TraceID || child.SpanID == seen.SpanID {
			t.Errorf("outgoing span must be a child: %s vs %s", child, seen)
		}
		if p, ok := Parse(out.Get(Header)); !ok || p != child {
			t.Errorf("injected header %q", out.Get(Header))
		}
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(Header, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if seen.TraceIDString() != "4bf92f3577b34da6a3ce929d0e0e4736" || rec.Header().Get(ResponseHeader) != seen.TraceIDString() {
		t.Fatalf("trace not continued: %s", seen)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if !seen.Valid() || rec.Header().Get(ResponseHeader) == "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatal("a request without traceparent must start a new trace")
	}
}

func TestContinueAndEnsure(t *testing.T) {
	root := New()
	ctx := Continue(context.Background(), root.String())
	s, ok := From(ctx)
	if !ok || s.TraceID != root.TraceID || s.SpanID == root.SpanID {
		t.Fatal("Continue must create a child span")
	}
	if _, s2 := Ensure(ctx); s2 != s {
		t.Fatal("Ensure must keep an existing span")
	}
	if _, s3 := Ensure(context.Background()); !s3.Valid() {
		t.Fatal("Ensure must start a trace")
	}
	if s4, _ := From(Continue(context.Background(), "bogus")); !s4.Valid() {
		t.Fatal("invalid traceparent must start a new trace")
	}
}
