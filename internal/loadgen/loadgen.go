// Package loadgen sends steady, open-loop HTTP traffic to a service.
package loadgen

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Endpoint is one kind of request, chosen with probability proportional to Weight.
type Endpoint struct {
	Method string
	Path   string
	Weight int
}

// DefaultEndpoints is the mix of the demo checkout service: mostly browsing,
// some purchases, a few health probes.
func DefaultEndpoints() []Endpoint {
	return []Endpoint{
		{http.MethodGet, "/products", 60},
		{http.MethodPost, "/checkout", 35},
		{http.MethodGet, "/healthz", 5},
	}
}

const (
	// MinRate and MaxRate bound Config.Rate. Below MinRate (one request per
	// 100 seconds on average) the generator is useless for a demo, and far
	// below it the gaps overflow time.Duration. Above MaxRate the single
	// scheduling goroutine would spin without ever sleeping, mostly producing
	// drops: the in-flight cap and the service's own capacity are far lower.
	MinRate = 0.01
	MaxRate = 10000.0

	// maxInterArrival caps one gap. At MinRate the longest gap that can
	// actually occur is about an hour, so this is a safety net, not a limit
	// anyone should hit.
	maxInterArrival = 24 * time.Hour
)

// Config describes one load run.
type Config struct {
	BaseURL     string        // e.g. http://127.0.0.1:8080
	Rate        float64       // average requests per second
	Duration    time.Duration // 0 means run until the context is cancelled
	Endpoints   []Endpoint
	MaxInFlight int           // requests beyond this are dropped, not queued
	Timeout     time.Duration // per-request client timeout
	Seed        uint64        // same seed, same sequence of choices
}

func DefaultConfig(baseURL string) Config {
	return Config{
		BaseURL:     baseURL,
		Rate:        50,
		Endpoints:   DefaultEndpoints(),
		MaxInFlight: 500,
		Timeout:     5 * time.Second,
		Seed:        1,
	}
}

func (c Config) Validate() error {
	if err := validateBaseURL(c.BaseURL); err != nil {
		return err
	}
	if !(c.Rate >= MinRate && c.Rate <= MaxRate) { // also rejects NaN and infinities
		return fmt.Errorf("loadgen: rate must be between %v and %v requests/s, got %v", MinRate, MaxRate, c.Rate)
	}
	if c.Duration < 0 {
		return fmt.Errorf("loadgen: duration must not be negative, got %v", c.Duration)
	}
	if c.MaxInFlight <= 0 {
		return fmt.Errorf("loadgen: max in-flight must be positive, got %d", c.MaxInFlight)
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("loadgen: timeout must be positive, got %v", c.Timeout)
	}
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("loadgen: at least one endpoint is required")
	}
	for _, e := range c.Endpoints {
		switch {
		case e.Method == "":
			return fmt.Errorf("loadgen: endpoint %q has no method", e.Path)
		case !strings.HasPrefix(e.Path, "/"):
			return fmt.Errorf("loadgen: endpoint path must start with '/', got %q", e.Path)
		case e.Weight <= 0:
			return fmt.Errorf("loadgen: endpoint %s %s needs a positive weight, got %d", e.Method, e.Path, e.Weight)
		}
	}
	return nil
}

// validateBaseURL enforces the contract that the base URL is an origin with
// an optional path prefix. A query string or fragment would be corrupted when
// the endpoint path is appended, silently sending every request to the wrong
// place.
func validateBaseURL(raw string) error {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		return fmt.Errorf("loadgen: base URL is not valid: %w", err)
	case u.Scheme != "http" && u.Scheme != "https":
		return fmt.Errorf("loadgen: base URL must start with http:// or https://, got %q", raw)
	case u.Hostname() == "": // u.Host alone would accept "http://:8080"
		return fmt.Errorf("loadgen: base URL has no host name: %q", raw)
	case u.User != nil:
		return fmt.Errorf("loadgen: base URL must not contain credentials")
	case strings.ContainsAny(raw, "?#"):
		return fmt.Errorf("loadgen: base URL must be an origin with an optional path prefix, with no query or fragment: %q", raw)
	}
	return nil
}

// Stats is the outcome of a run. Sent = Success + ServerErrors + OtherStatus +
// TransportErrors. Dropped requests were never sent.
type Stats struct {
	Sent            int64
	Dropped         int64 // skipped because MaxInFlight was reached
	Success         int64 // 2xx
	ServerErrors    int64 // 5xx
	OtherStatus     int64 // anything else (1xx, 3xx, 4xx)
	TransportErrors int64 // no HTTP response at all: refused, reset, timed out
	FirstError      string
}

type counters struct {
	sent, dropped, success, serverErrors, otherStatus, transportErrors atomic.Int64

	mu       sync.Mutex
	firstErr string
}

func (c *counters) transportError(err error) {
	c.transportErrors.Add(1)
	c.mu.Lock()
	if c.firstErr == "" {
		c.firstErr = err.Error()
	}
	c.mu.Unlock()
}

func (c *counters) snapshot() Stats {
	c.mu.Lock()
	first := c.firstErr
	c.mu.Unlock()
	return Stats{
		Sent: c.sent.Load(), Dropped: c.dropped.Load(), Success: c.success.Load(),
		ServerErrors: c.serverErrors.Load(), OtherStatus: c.otherStatus.Load(),
		TransportErrors: c.transportErrors.Load(), FirstError: first,
	}
}

// interArrival converts a uniform random u in [0,1) into the gap before the
// next request. Exponentially distributed gaps are the standard model of
// independent users: the average gap is 1/rate, with bursts and lulls.
//
// The result is always within [0, maxInterArrival], whatever the inputs:
// converting an out-of-range float64 to a Duration is implementation-defined
// in Go (on amd64 it typically yields a large negative number), so the value
// is checked before it is converted.
func interArrival(rate, u float64) time.Duration {
	secs := -math.Log1p(-u) / rate
	switch {
	case secs <= 0: // u = 0, or an out-of-range u: no gap
		return 0
	case !(secs < maxInterArrival.Seconds()): // too large, +Inf, or NaN
		return maxInterArrival
	}
	return time.Duration(secs * float64(time.Second))
}

// picker chooses endpoints in proportion to their weights.
type picker struct {
	endpoints []Endpoint
	cum       []int // running total of weights
	total     int
}

func newPicker(eps []Endpoint) *picker {
	p := &picker{endpoints: eps}
	for _, e := range eps {
		p.total += e.Weight
		p.cum = append(p.cum, p.total)
	}
	return p
}

// pick maps u in [0,1) onto the cumulative weights.
func (p *picker) pick(u float64) Endpoint {
	target := u * float64(p.total)
	for i, c := range p.cum {
		if target < float64(c) {
			return p.endpoints[i]
		}
	}
	return p.endpoints[len(p.endpoints)-1] // only reachable through float rounding
}

// Generator runs one load test. Use Run once.
type Generator struct {
	cfg       Config
	baseURL   string
	pick      *picker
	rng       *rand.Rand // used only by the scheduling goroutine
	client    *http.Client
	transport *http.Transport
	stats     counters
}

func New(cfg Config) (*Generator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Keep as many idle connections as we may have in flight. The default of
	// 2 per host would force a new TCP connection for most requests.
	tr := &http.Transport{
		MaxIdleConns:        cfg.MaxInFlight,
		MaxIdleConnsPerHost: cfg.MaxInFlight,
		IdleConnTimeout:     30 * time.Second,
	}
	return &Generator{
		cfg:       cfg,
		baseURL:   strings.TrimRight(cfg.BaseURL, "/"),
		pick:      newPicker(cfg.Endpoints),
		rng:       rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9E3779B97F4A7C15)),
		client:    &http.Client{Transport: tr, Timeout: cfg.Timeout},
		transport: tr,
	}, nil
}

// Run sends requests until the duration elapses or ctx is cancelled, then
// waits for in-flight requests to finish and returns the totals.
func (g *Generator) Run(ctx context.Context) Stats {
	if g.cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.cfg.Duration)
		defer cancel()
	}
	defer g.transport.CloseIdleConnections()

	slots := make(chan struct{}, g.cfg.MaxInFlight)
	var wg sync.WaitGroup

	// Each arrival is scheduled against absolute time (previous arrival plus a
	// random gap), so a late wake-up is made up for instead of slowing us down.
	next := time.Now()
	for {
		next = next.Add(interArrival(g.cfg.Rate, g.rng.Float64()))
		if !sleepUntil(ctx, next) {
			break
		}
		ep := g.pick.pick(g.rng.Float64())

		select {
		case slots <- struct{}{}:
			g.stats.sent.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				g.do(ep)
			}()
		default:
			g.stats.dropped.Add(1) // at the cap: skip rather than queue
		}
	}
	wg.Wait()
	return g.stats.snapshot()
}

// sleepUntil waits until t and reports whether the caller should carry on.
// It returns false as soon as ctx is done.
func sleepUntil(ctx context.Context, t time.Time) bool {
	if d := time.Until(t); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return false
		}
	}
	return ctx.Err() == nil
}

// do sends one request. It deliberately does not use Run's context: when the
// run stops, requests already in flight finish normally instead of being
// abandoned (the service would log those as client cancellations).
func (g *Generator) do(ep Endpoint) {
	req, err := http.NewRequest(ep.Method, g.baseURL+ep.Path, nil)
	if err != nil {
		g.stats.transportError(err)
		return
	}
	resp, err := g.client.Do(req)
	if err != nil {
		g.stats.transportError(err)
		return
	}
	// Read the body to the end before closing, or the connection cannot be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		g.stats.success.Add(1)
	case resp.StatusCode >= 500:
		g.stats.serverErrors.Add(1)
	default:
		g.stats.otherStatus.Add(1)
	}
}

// Check applies the command's exit policy. A nil result means the generator
// did its job, whatever the service answered: producing load against a
// failing service is the whole point, so 4xx and 5xx responses are not
// failures of the generator.
//
// It fails when nothing reached the service, or when a run that was not
// interrupted sent nothing. Arrivals are random (a Poisson process), so the
// chance of sending nothing is e^-(rate*duration). When that chance is high
// the parameters were too small to reliably produce traffic; when it is tiny,
// zero requests is not bad luck and the message says so.
func (s Stats) Check(cfg Config, interrupted bool) error {
	switch {
	case s.Sent > 0 && s.TransportErrors == s.Sent:
		return fmt.Errorf("no request reached the service at %s (%s) - is it running?", cfg.BaseURL, s.FirstError)
	case s.Sent == 0 && !interrupted:
		expected := cfg.Rate * cfg.Duration.Seconds()
		chance := math.Exp(-expected) // probability of zero arrivals
		if chance < 0.01 {
			return fmt.Errorf("no requests were sent although about %.0f were expected (%v at %g req/s); "+
				"that is statistically very unlikely, so something is wrong with the generator",
				expected, cfg.Duration, cfg.Rate)
		}
		return fmt.Errorf("no requests were sent: the run (%v at %g req/s, about %.2g expected requests) "+
			"was too short or the rate too low to reliably generate traffic; "+
			"the chance of sending nothing was %.1f%%; raise -rate or -duration",
			cfg.Duration, cfg.Rate, expected, chance*100)
	}
	return nil
}
