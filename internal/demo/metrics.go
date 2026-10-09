package demo

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

// Collector accumulates request outcomes for the current window and turns
// them into a MetricSample on Flush. It is safe for concurrent use.
type Collector struct {
	mu          sync.Mutex
	service     string
	windowStart time.Time
	requests    int
	errors      int
	invalid     int
	latencies   []float64 // valid latencies only
}

// NewCollector starts the first window at start.
func NewCollector(service string, start time.Time) *Collector {
	return &Collector{service: service, windowStart: start}
}

// Record is called once per finished request, from many goroutines.
func (c *Collector) Record(latencyMS float64, status int) {
	valid := !math.IsNaN(latencyMS) && !math.IsInf(latencyMS, 0) && latencyMS >= 0

	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	if models.IsServerError(status) {
		c.errors++
	}
	if valid {
		c.latencies = append(c.latencies, latencyMS)
	} else {
		c.invalid++
	}
}

// Flush closes the current window, returns its summary and starts a new window at now.
func (c *Collector) Flush(now time.Time, poolInUse, poolMax int) models.MetricSample {
	// Hold the lock only long enough to take the data and reset.
	c.mu.Lock()
	reqs, errs, invalid, lat, start := c.requests, c.errors, c.invalid, c.latencies, c.windowStart
	c.requests, c.errors, c.invalid, c.latencies, c.windowStart = 0, 0, 0, nil, now
	c.mu.Unlock()

	s := models.MetricSample{
		Time:                  now.UTC(),
		Service:               c.service,
		WindowSeconds:         now.Sub(start).Seconds(),
		Requests:              reqs,
		Errors:                errs,
		InvalidLatencySamples: invalid,
		DBPoolInUse:           poolInUse,
		DBPoolMax:             poolMax,
	}
	if reqs > 0 {
		s.ErrorRate = round(float64(errs)/float64(reqs), 4)
	}
	if len(lat) > 0 {
		sort.Float64s(lat) // we own lat now, so sorting in place is safe
		s.P50LatencyMS = round(percentile(lat, 0.50), 2)
		s.P95LatencyMS = round(percentile(lat, 0.95), 2)
	}
	return s
}

// percentile returns the p-th percentile (0..1) using the nearest-rank
// method defined on models.MetricSample.
func percentile(sorted []float64, p float64) float64 {
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func round(v float64, places int) float64 {
	pow := math.Pow(10, float64(places))
	return math.Round(v*pow) / pow
}
