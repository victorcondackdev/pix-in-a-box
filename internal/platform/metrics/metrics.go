// Package metrics is a small, dependency-free Prometheus text exposition
// (format 0.0.4) with labelled counters, histograms and gauge functions,
// written in the same spirit as the hand-written exporters of the component
// services. A larger codebase would use the official client or OTel.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metric families.
type Registry struct {
	mu   sync.Mutex
	fams map[string]family
}

type family interface{ write(w io.Writer) }

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{fams: map[string]family{}} }

func (r *Registry) add(name string, f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.fams[name]; dup {
		panic("metrics: duplicate family " + name)
	}
	r.fams[name] = f
}

// Write renders every family sorted by name.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	names := make([]string, 0, len(r.fams))
	for n := range r.fams {
		names = append(names, n)
	}
	sort.Strings(names)
	fams := make([]family, len(names))
	for i, n := range names {
		fams[i] = r.fams[n]
	}
	r.mu.Unlock()
	for _, f := range fams {
		f.write(w)
	}
}

// Handler serves the registry.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

// series stores per-label-set values keyed by the joined label values.
type series[T any] struct {
	mu     sync.Mutex
	labels []string
	vals   map[string]*T
	newT   func() *T
}

func (s *series[T]) get(values []string) *T {
	if len(values) != len(s.labels) {
		panic(fmt.Sprintf("metrics: want %d label values, got %d", len(s.labels), len(values)))
	}
	k := strings.Join(values, "\xff")
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vals[k]
	if !ok {
		v = s.newT()
		s.vals[k] = v
	}
	return v
}

func (s *series[T]) keys() []string {
	ks := make([]string, 0, len(s.vals))
	for k := range s.vals {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// CounterVec is a counter family.
type CounterVec struct {
	name, help string
	s          series[float64]
}

// NewCounter registers a counter family with the given label names.
func (r *Registry) NewCounter(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, s: series[float64]{labels: labels, vals: map[string]*float64{}, newT: func() *float64 { return new(float64) }}}
	r.add(name, c)
	return c
}

// Add adds v (which must not be negative) to the series for values.
func (c *CounterVec) Add(v float64, values ...string) {
	if v < 0 {
		return
	}
	p := c.s.get(values)
	c.s.mu.Lock()
	*p += v
	c.s.mu.Unlock()
}

// Inc adds one.
func (c *CounterVec) Inc(values ...string) { c.Add(1, values...) }

// Value returns the current value of a series, for tests.
func (c *CounterVec) Value(values ...string) float64 {
	p := c.s.get(values)
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return *p
}

func (c *CounterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, escHelp(c.help), c.name)
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	for _, k := range c.s.keys() {
		fmt.Fprintf(w, "%s%s %s\n", c.name, labelStr(c.s.labels, k, "", ""), fmtFloat(*c.s.vals[k]))
	}
}

// DefaultBuckets suit HTTP and settlement latencies, in seconds.
var DefaultBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type hist struct {
	counts []uint64
	sum    float64
	count  uint64
}

// HistogramVec is a histogram family.
type HistogramVec struct {
	name, help string
	bounds     []float64
	s          series[hist]
}

// NewHistogram registers a histogram family with ascending bucket bounds.
func (r *Registry) NewHistogram(name, help string, bounds []float64, labels ...string) *HistogramVec {
	if !sort.Float64sAreSorted(bounds) {
		panic("metrics: histogram bounds must be sorted")
	}
	h := &HistogramVec{name: name, help: help, bounds: bounds}
	h.s = series[hist]{labels: labels, vals: map[string]*hist{}, newT: func() *hist { return &hist{counts: make([]uint64, len(bounds))} }}
	r.add(name, h)
	return h
}

// Observe records v in the series for values.
func (h *HistogramVec) Observe(v float64, values ...string) {
	p := h.s.get(values)
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for i, b := range h.bounds {
		if v <= b {
			p.counts[i]++
		}
	}
	p.sum += v
	p.count++
}

func (h *HistogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, escHelp(h.help), h.name)
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for _, k := range h.s.keys() {
		p := h.s.vals[k]
		for i, b := range h.bounds {
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelStr(h.s.labels, k, "le", fmtFloat(b)), p.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelStr(h.s.labels, k, "le", "+Inf"), p.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelStr(h.s.labels, k, "", ""), fmtFloat(p.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelStr(h.s.labels, k, "", ""), p.count)
	}
}

type gaugeFunc struct {
	name, help string
	f          func() float64
}

// NewGaugeFunc registers a gauge whose value is read at scrape time.
func (r *Registry) NewGaugeFunc(name, help string, f func() float64) {
	r.add(name, &gaugeFunc{name: name, help: help, f: f})
}

func (g *gaugeFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %s\n", g.name, escHelp(g.help), g.name, g.name, fmtFloat(g.f()))
}

func labelStr(names []string, key, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	if len(names) > 0 {
		vals := strings.Split(key, "\xff")
		for i, n := range names {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s=\"%s\"", n, escLabel(vals[i]))
		}
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=\"%s\"", extraName, extraValue)
	}
	b.WriteByte('}')
	return b.String()
}

func fmtFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

var (
	labelEsc = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	helpEsc  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
)

func escLabel(s string) string { return labelEsc.Replace(s) }
func escHelp(s string) string  { return helpEsc.Replace(s) }
