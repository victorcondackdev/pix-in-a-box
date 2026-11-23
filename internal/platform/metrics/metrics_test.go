package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("pix_total", "Pix by status.", "status")
	h := r.NewHistogram("lat_seconds", "Latency.", []float64{0.1, 1}, "route")
	r.NewGaugeFunc("up", "Up.", func() float64 { return 1 })
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Inc("SETTLED"); h.Observe(0.5, "GET /x") }()
	}
	wg.Wait()
	c.Inc(`we"ird`)
	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE pix_total counter", `pix_total{status="SETTLED"} 50`, `pix_total{status="we\"ird"} 1`,
		"# TYPE lat_seconds histogram",
		`lat_seconds_bucket{route="GET /x",le="0.1"} 0`, `lat_seconds_bucket{route="GET /x",le="1"} 50`,
		`lat_seconds_bucket{route="GET /x",le="+Inf"} 50`, `lat_seconds_sum{route="GET /x"} 25`, `lat_seconds_count{route="GET /x"} 50`,
		"# TYPE up gauge\nup 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Index(out, "lat_seconds") > strings.Index(out, "pix_total") {
		t.Error("families must be sorted by name")
	}
}

func TestDuplicateFamilyPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	r := NewRegistry()
	r.NewCounter("x", "x")
	r.NewCounter("x", "x")
}
