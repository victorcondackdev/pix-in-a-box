package idem

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSemantics(t *testing.T) {
	s := NewStore(time.Hour)
	var calls atomic.Int32
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		if strings.Contains(string(b), "boom") {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"n":` + string(rune('0'+calls.Load())) + `}`))
	})
	do := func(key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/v1/x", strings.NewReader(body))
		if key != "" {
			req.Header.Set(Header, key)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}
	if r := do("", "{}"); r.Code != 400 {
		t.Fatalf("missing key: %d", r.Code)
	}
	a := do("k", `{"a":1}`)
	b := do("k", `{"a":1}`)
	if a.Code != 201 || b.Code != 201 || a.Body.String() != b.Body.String() || b.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %s / %d %s", a.Code, a.Body, b.Code, b.Body)
	}
	if r := do("k", `{"a":2}`); r.Code != 422 {
		t.Fatalf("reuse: %d", r.Code)
	}
	if r := do("e", `"boom"`); r.Code != 500 {
		t.Fatal(r.Code)
	}
	if r := do("e", `{"ok":1}`); r.Code != 201 {
		t.Fatalf("5xx must release the key: %d", r.Code)
	}
	if calls.Load() != 3 {
		t.Fatalf("handler ran %d times", calls.Load())
	}
}

func TestConcurrentSameKeyRunsOnce(t *testing.T) {
	s := NewStore(time.Hour)
	var calls atomic.Int32
	release := make(chan struct{})
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		w.WriteHeader(201)
	})
	var wg sync.WaitGroup
	codes := make(chan int, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/v1/x", strings.NewReader("{}"))
			req.Header.Set(Header, "same")
			rec := httptest.NewRecorder()
			h(rec, req)
			codes <- rec.Code
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(codes)
	n201, n409 := 0, 0
	for c := range codes {
		switch c {
		case 201:
			n201++
		case 409:
			n409++
		}
	}
	if calls.Load() != 1 || n201+n409 != 10 || n201 < 1 {
		t.Fatalf("calls %d, 201s %d, 409s %d", calls.Load(), n201, n409)
	}
}
