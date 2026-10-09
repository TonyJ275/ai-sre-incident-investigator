package demo

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	ten := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

	tests := []struct {
		name   string
		sorted []float64
		p      float64
		want   float64
	}{
		{"p50 of ten", ten, 0.50, 50},
		{"p95 of ten", ten, 0.95, 100},
		{"p90 of ten", ten, 0.90, 90},
		{"single value, any percentile", []float64{42}, 0.95, 42},
		{"p0 clamps to first value", ten, 0, 10},
		{"even count picks lower middle, no interpolation", []float64{10, 20}, 0.50, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(tc.sorted, tc.p); got != tc.want {
				t.Errorf("percentile(p=%v) = %v, want %v", tc.p, got, tc.want)
			}
		})
	}
}

func TestCollectorFlush(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	c := NewCollector("checkout-api", t0)

	// unsorted
	latencies := []float64{100, 10, 90, 20, 80, 30, 70, 40, 60, 50}
	for i, l := range latencies {
		status := 200
		if i < 2 { // the first two requests are server errors
			status = 500
		}
		c.Record(l, status)
	}

	s := c.Flush(t0.Add(5*time.Second), 3, 50)

	if s.Requests != 10 || s.Errors != 2 {
		t.Errorf("counts: got %d requests, %d errors", s.Requests, s.Errors)
	}
	if s.ErrorRate != 0.2 {
		t.Errorf("error rate = %v, want 0.2", s.ErrorRate)
	}
	if s.P50LatencyMS != 50 || s.P95LatencyMS != 100 {
		t.Errorf("percentiles: p50=%v p95=%v, want 50 and 100", s.P50LatencyMS, s.P95LatencyMS)
	}
	if s.WindowSeconds != 5 {
		t.Errorf("window = %v, want 5", s.WindowSeconds)
	}
	if s.DBPoolInUse != 3 || s.DBPoolMax != 50 || s.Service != "checkout-api" {
		t.Errorf("gauges/metadata wrong: %+v", s)
	}

	// The window must reset: the next flush covers only the new interval.
	next := c.Flush(t0.Add(10*time.Second), 0, 50)
	if next.Requests != 0 || next.WindowSeconds != 5 {
		t.Errorf("window did not reset: %+v", next)
	}
}

func TestCollectorEmptyWindow(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	c := NewCollector("checkout-api", t0)

	s := c.Flush(t0.Add(5*time.Second), 0, 50) // must not panic on zero requests

	if s.Requests != 0 || s.ErrorRate != 0 || s.P95LatencyMS != 0 {
		t.Errorf("empty window should be all zeros: %+v", s)
	}
}

func TestCollectorConcurrentRecord(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	c := NewCollector("checkout-api", t0)

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.Record(float64(i), 200)
			}
		}()
	}
	wg.Wait()

	if s := c.Flush(t0.Add(time.Second), 0, 50); s.Requests != 5000 {
		t.Errorf("requests = %d, want 5000 (lost updates mean a data race)", s.Requests)
	}
}

func TestCollectorErrorDefinition(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	c := NewCollector("checkout-api", t0)

	// 499 is the last non-error, 500 the first error.
	for _, status := range []int{200, 404, 499, 500, 503} {
		c.Record(10, status)
	}

	s := c.Flush(t0.Add(5*time.Second), 0, 50)
	if s.Requests != 5 || s.Errors != 2 {
		t.Errorf("got %d requests, %d errors; want 5 and 2", s.Requests, s.Errors)
	}
}

func TestCollectorExcludesInvalidLatencies(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	c := NewCollector("checkout-api", t0)

	c.Record(math.NaN(), 200)
	c.Record(math.Inf(1), 500) // invalid latency, but this request really was an error
	c.Record(-5, 200)
	c.Record(10, 200)
	c.Record(0, 200) // zero is a valid latency

	s := c.Flush(t0.Add(5*time.Second), 0, 50)

	if s.Requests != 5 || s.Errors != 1 || s.InvalidLatencySamples != 3 {
		t.Errorf("counts: %+v", s)
	}
	if s.ErrorRate != 0.2 {
		t.Errorf("error rate = %v, want 0.2 (1 error / 5 requests)", s.ErrorRate)
	}
	// Valid latencies are [0, 10]: nearest-rank p95 is the larger one.
	if s.P95LatencyMS != 10 {
		t.Errorf("p95 = %v, want 10; invalid values leaked into percentiles", s.P95LatencyMS)
	}
}
