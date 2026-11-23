// Package idem implements Idempotency-Key handling for mutating HTTP
// endpoints, with the semantics payments-service-go uses:
//
//   - same key, same request: the stored response is replayed with
//     Idempotent-Replayed: true;
//   - same key, different request (method, path or body): 422;
//   - same key while the first request is still running: 409 with
//     Retry-After;
//   - 5xx responses release the key so the client can retry.
//
// Entries live in memory with a TTL. That is enough for a single gateway
// replica; production would keep them in the same database transaction as
// the side effect, as payments-service-go does.
package idem

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/victorcondackdev/pix-in-a-box/internal/platform/httpx"
)

// Header is the request header carrying the key.
const Header = "Idempotency-Key"

type entry struct {
	hash    [32]byte
	done    bool
	status  int
	header  http.Header
	body    []byte
	created time.Time
}

// Store holds keys for TTL.
type Store struct {
	mu   sync.Mutex
	ttl  time.Duration
	m    map[string]*entry
	now  func() time.Time
	Hits func() // called on every replay; optional (metrics)
}

// NewStore returns a store keeping keys for ttl.
func NewStore(ttl time.Duration) *Store {
	return &Store{ttl: ttl, m: map[string]*entry{}, now: time.Now}
}

type capture struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (c *capture) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}
	c.ResponseWriter.WriteHeader(code)
}

func (c *capture) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	c.buf.Write(b)
	return c.ResponseWriter.Write(b)
}

// Wrap enforces the key on h. Keys are scoped by method and path pattern
// implicitly, because the request hash includes method and path.
func (s *Store) Wrap(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(Header)
		if key == "" || len(key) > 255 {
			httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/idempotency-key-missing", Title: "Idempotency-Key header required", Status: http.StatusBadRequest,
				Detail: "send a unique Idempotency-Key (at most 255 characters) on every POST"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpx.MaxBody))
		if err != nil {
			httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/payload-too-large", Title: "Request body too large", Status: http.StatusRequestEntityTooLarge})
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		hh := sha256.New()
		hh.Write([]byte(r.Method + " " + r.URL.Path + "\n"))
		hh.Write(body)
		var hash [32]byte
		copy(hash[:], hh.Sum(nil))

		s.mu.Lock()
		now := s.now()
		for k, e := range s.m {
			if e.done && now.Sub(e.created) > s.ttl {
				delete(s.m, k)
			}
		}
		e, seen := s.m[key]
		switch {
		case seen && e.hash != hash:
			s.mu.Unlock()
			httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/idempotency-key-reused", Title: "Idempotency-Key reused with a different request", Status: http.StatusUnprocessableEntity})
			return
		case seen && !e.done:
			s.mu.Unlock()
			w.Header().Set("Retry-After", "1")
			httpx.WriteProblem(w, r, httpx.Problem{Type: "/problems/idempotency-key-in-progress", Title: "A request with this Idempotency-Key is in progress", Status: http.StatusConflict})
			return
		case seen:
			st, hdr, b := e.status, e.header.Clone(), append([]byte(nil), e.body...)
			s.mu.Unlock()
			if s.Hits != nil {
				s.Hits()
			}
			for k, vs := range hdr {
				w.Header()[k] = vs
			}
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(st)
			_, _ = w.Write(b)
			return
		}
		e = &entry{hash: hash, created: now}
		s.m[key] = e
		s.mu.Unlock()

		c := &capture{ResponseWriter: w}
		completed := false
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !completed || c.status >= 500 || c.status == 0 {
				delete(s.m, key)
				return
			}
			e.done, e.status, e.body = true, c.status, c.buf.Bytes()
			e.header = http.Header{}
			for _, k := range []string{"Content-Type", "Location"} {
				if v := w.Header().Get(k); v != "" {
					e.header.Set(k, v)
				}
			}
		}()
		h(c, r)
		completed = true
	}
}
