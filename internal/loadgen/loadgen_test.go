package loadgen

import (
	"context"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInterArrival(t *testing.T) {
	if got := interArrival(100, 0); got != 0 {
		t.Errorf("u=0 should give no gap, got %v", got)
	}

	// u = 1 - 1/e is exactly one mean interval, 1/rate: 10ms at 100 req/s.
	got := interArrival(100, 1-math.Exp(-1))
	if d := got - 10*time.Millisecond; d < -time.Microsecond || d > time.Microsecond {
		t.Errorf("got %v, want 10ms", got)
	}

	// The mean of many samples converges to 1/rate (seeded, so deterministic).
	rng := rand.New(rand.NewPCG(7, 8))
	const n = 100000
	var sum time.Duration
	for i := 0; i < n; i++ {
		sum += interArrival(200, rng.Float64())
	}
	mean, want := sum/n, 5*time.Millisecond
	if mean < want*98/100 || mean > want*102/100 {
		t.Errorf("mean gap = %v, want about %v", mean, want)
	}
}

func TestPicker(t *testing.T) {
	p := newPicker(DefaultEndpoints()) // weights: /products 60, /checkout 35, /healthz 5

	tests := []struct {
		u    float64
		want string
	}{
		{0, "/products"}, {0.59, "/products"},
		{0.61, "/checkout"}, {0.94, "/checkout"},
		{0.96, "/healthz"}, {0.999999, "/healthz"},
	}
	for _, tc := range tests {
		if got := p.pick(tc.u).Path; got != tc.want {
			t.Errorf("pick(%v) = %s, want %s", tc.u, got, tc.want)
		}
	}

	rng := rand.New(rand.NewPCG(1, 2))
	counts := map[string]int{}
	const n = 100000
	for i := 0; i < n; i++ {
		counts[p.pick(rng.Float64()).Path]++
	}
	for path, want := range map[string]float64{"/products": 0.60, "/checkout": 0.35, "/healthz": 0.05} {
		if got := float64(counts[path]) / n; math.Abs(got-want) > 0.015 {
			t.Errorf("%s share = %.3f, want about %.2f", path, got, want)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{"defaults are valid", func(c *Config) {}, false},
		{"empty base URL", func(c *Config) { c.BaseURL = "" }, true},
		{"base URL without scheme", func(c *Config) { c.BaseURL = "localhost:8080" }, true},
		{"zero rate", func(c *Config) { c.Rate = 0 }, true},
		{"negative rate", func(c *Config) { c.Rate = -1 }, true},
		{"NaN rate", func(c *Config) { c.Rate = math.NaN() }, true},
		{"infinite rate", func(c *Config) { c.Rate = math.Inf(1) }, true},
		{"negative duration", func(c *Config) { c.Duration = -time.Second }, true},
		{"zero in-flight cap", func(c *Config) { c.MaxInFlight = 0 }, true},
		{"zero timeout", func(c *Config) { c.Timeout = 0 }, true},
		{"no endpoints", func(c *Config) { c.Endpoints = nil }, true},
		{"zero weight", func(c *Config) { c.Endpoints = []Endpoint{{"GET", "/x", 0}} }, true},
		{"path without slash", func(c *Config) { c.Endpoints = []Endpoint{{"GET", "x", 1}} }, true},
		{"empty method", func(c *Config) { c.Endpoints = []Endpoint{{"", "/x", 1}} }, true},
		{"base URL with a query string", func(c *Config) { c.BaseURL = "http://127.0.0.1:8080?debug=1" }, true},
		{"base URL with an empty query", func(c *Config) { c.BaseURL = "http://127.0.0.1:8080?" }, true},
		{"base URL with a fragment", func(c *Config) { c.BaseURL = "http://127.0.0.1:8080#top" }, true},
		{"base URL with credentials", func(c *Config) { c.BaseURL = "http://user:pw@127.0.0.1:8080" }, true},
		{"base URL without a host name", func(c *Config) { c.BaseURL = "http://:8080" }, true},
		{"base URL with a trailing slash", func(c *Config) { c.BaseURL = "http://127.0.0.1:8080/" }, false},
		{"base URL with a path prefix", func(c *Config) { c.BaseURL = "https://example.com/api" }, false},
		{"rate below the minimum", func(c *Config) { c.Rate = MinRate / 2 }, true},
		{"rate at the minimum", func(c *Config) { c.Rate = MinRate }, false},
		{"rate above the maximum", func(c *Config) { c.Rate = MaxRate * 2 }, true},
		{"rate at the maximum", func(c *Config) { c.Rate = MaxRate }, false},
		{"subnormal rate", func(c *Config) { c.Rate = 5e-324 }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig("http://127.0.0.1:8080")
			tc.mutate(&cfg)
			if err := cfg.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestRunAccountsForEveryRequest(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.Method+" "+r.URL.Path]++
		mu.Unlock()
		if r.URL.Path == "/checkout" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := DefaultConfig(srv.URL)
	cfg.Rate = 300
	cfg.Duration = 400 * time.Millisecond
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	st := g.Run(context.Background())

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for key, n := range hits {
		total += n
		switch key {
		case "GET /products", "POST /checkout", "GET /healthz":
		default:
			t.Errorf("unexpected request %q", key)
		}
	}
	if int64(total) != st.Sent {
		t.Errorf("server saw %d requests, generator says it sent %d", total, st.Sent)
	}
	if st.Success != int64(hits["GET /products"]+hits["GET /healthz"]) || st.ServerErrors != int64(hits["POST /checkout"]) {
		t.Errorf("status classification wrong: %+v vs hits %v", st, hits)
	}
	if st.Sent != st.Success+st.ServerErrors+st.OtherStatus+st.TransportErrors {
		t.Errorf("outcomes do not add up to Sent: %+v", st)
	}
	if st.TransportErrors != 0 || st.Dropped != 0 {
		t.Errorf("unexpected transport errors or drops: %+v", st)
	}
	// 300 req/s for 0.4s is about 120 requests; the bounds are wide on purpose.
	if st.Sent < 48 || st.Sent > 192 {
		t.Errorf("sent %d requests, want roughly 120", st.Sent)
	}
	if hits["GET /products"] == 0 || hits["POST /checkout"] == 0 {
		t.Errorf("request mix not exercised: %v", hits)
	}
}

func TestRunDropsRequestsAtInFlightCap(t *testing.T) {
	release := make(chan struct{})
	var cur, peak atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release // hold the request open
		cur.Add(-1)
	}))
	defer srv.Close()
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce() // runs before srv.Close, which would otherwise wait forever

	cfg := DefaultConfig(srv.URL)
	cfg.Rate = 500
	cfg.Duration = 200 * time.Millisecond
	cfg.MaxInFlight = 3
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan Stats, 1)
	go func() { done <- g.Run(context.Background()) }()
	time.Sleep(400 * time.Millisecond) // sending stopped at 200ms; 3 requests are still held
	releaseOnce()
	st := <-done

	if st.Sent != 3 || st.Success != 3 {
		t.Errorf("sent=%d success=%d, want exactly 3 each: %+v", st.Sent, st.Success, st)
	}
	if st.Dropped < 20 {
		t.Errorf("dropped = %d, want many (about 100 attempts, 3 accepted)", st.Dropped)
	}
	if peak.Load() != 3 {
		t.Errorf("server saw %d concurrent requests, want exactly the cap of 3", peak.Load())
	}
}

func TestRunCountsTransportErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler) // drop the connection without answering
	}))
	defer srv.Close()

	cfg := DefaultConfig(srv.URL)
	cfg.Rate = 200
	cfg.Duration = 200 * time.Millisecond
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	st := g.Run(context.Background())

	if st.Sent == 0 || st.TransportErrors != st.Sent || st.Success != 0 {
		t.Errorf("every request should be a transport error: %+v", st)
	}
	if st.FirstError == "" {
		t.Error("FirstError should describe what went wrong")
	}
}

func TestRunStopsOnCancelAndLetsInFlightRequestsFinish(t *testing.T) {
	var served, cancelled atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(100 * time.Millisecond):
			served.Add(1)
		case <-r.Context().Done(): // the client abandoned the request
			cancelled.Add(1)
		}
	}))
	defer srv.Close()

	cfg := DefaultConfig(srv.URL)
	cfg.Rate = 100 // Duration left at zero: run until cancelled
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Stats, 1)
	go func() { done <- g.Run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case st := <-done:
		if cancelled.Load() != 0 {
			t.Errorf("%d in-flight requests were abandoned; they must be allowed to finish", cancelled.Load())
		}
		if st.Sent == 0 || st.Success != st.Sent || served.Load() != st.Sent {
			t.Errorf("sent=%d success=%d served=%d, want all equal and non-zero", st.Sent, st.Success, served.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestInterArrivalIsAlwaysWithinBounds(t *testing.T) {
	rates := []float64{1e-300, 1e-15, 1e-9, MinRate, 1, 1e6, MaxRate, 1e12}
	us := []float64{-0.5, 0, 1e-12, 0.5, 1 - 1e-16, 1, math.NaN()}
	for _, rate := range rates {
		for _, u := range us {
			if got := interArrival(rate, u); got < 0 || got > maxInterArrival {
				t.Errorf("interArrival(%v, %v) = %v, want within [0, %v]", rate, u, got, maxInterArrival)
			}
		}
	}
	// An absurdly low rate must saturate at the cap, not overflow time.Duration.
	if got := interArrival(1e-15, 0.5); got != maxInterArrival {
		t.Errorf("got %v, want the cap %v", got, maxInterArrival)
	}
}

func TestBaseURLPathPrefixIsPreserved(t *testing.T) {
	var mu sync.Mutex
	paths := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.URL.Path]++
		mu.Unlock()
	}))
	defer srv.Close()

	cfg := DefaultConfig(srv.URL + "/api/") // the trailing slash must not produce "//"
	cfg.Rate = 300
	cfg.Duration = 200 * time.Millisecond
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	st := g.Run(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if st.Sent == 0 {
		t.Fatal("nothing was sent")
	}
	for p := range paths {
		if p != "/api/products" && p != "/api/checkout" && p != "/api/healthz" {
			t.Errorf("unexpected path %q", p)
		}
	}
}

func TestStatsCheck(t *testing.T) {
	cfg := DefaultConfig("http://127.0.0.1:8080")
	cfg.Duration = 30 * time.Second

	tests := []struct {
		name        string
		st          Stats
		interrupted bool
		wantErr     string // substring of the expected error; empty means success
	}{
		{"healthy run", Stats{Sent: 100, Success: 100}, false, ""},
		{"a service answering 503 to everything is still a successful run", Stats{Sent: 100, ServerErrors: 100}, false, ""},
		{"a service answering 404 to everything is still a successful run", Stats{Sent: 100, OtherStatus: 100}, false, ""},
		{"some transport errors", Stats{Sent: 100, Success: 60, TransportErrors: 40}, false, ""},
		{"nothing reached the service", Stats{Sent: 100, TransportErrors: 100, FirstError: "connection refused"}, false, "connection refused"},
		{"nothing reached the service, even when interrupted", Stats{Sent: 5, TransportErrors: 5, FirstError: "connection refused"}, true, "connection refused"},
		{"a completed run that sent nothing, though traffic was expected", Stats{}, false, "statistically very unlikely"},
		{"interrupted before the first request", Stats{}, true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.st.Check(cfg, tc.interrupted)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestCheckZeroRequestsMessageMatchesTheOdds(t *testing.T) {
	t.Run("low expected count: the parameters were too small", func(t *testing.T) {
		cfg := DefaultConfig("http://127.0.0.1:8080")
		cfg.Rate, cfg.Duration = 0.01, time.Second // P(no arrivals) = e^-0.01, about 99%

		err := Stats{}.Check(cfg, false)
		if err == nil {
			t.Fatal("want an error for a completed run that sent nothing")
		}
		msg := err.Error()
		for _, want := range []string{"too short or the rate too low", "99.0%", "raise -rate or -duration"} {
			if !strings.Contains(msg, want) {
				t.Errorf("message %q should contain %q", msg, want)
			}
		}
		if strings.Contains(msg, "statistically very unlikely") {
			t.Errorf("a 99%% outcome must not be called unlikely: %q", msg)
		}
	})

	t.Run("high expected count: not a fluke, so not the user's parameters", func(t *testing.T) {
		cfg := DefaultConfig("http://127.0.0.1:8080")
		cfg.Rate, cfg.Duration = 50, 30*time.Second // about 1500 expected requests

		err := Stats{}.Check(cfg, false)
		if err == nil {
			t.Fatal("want an error for a completed run that sent nothing")
		}
		msg := err.Error()
		if !strings.Contains(msg, "statistically very unlikely") {
			t.Errorf("message %q should say the outcome is very unlikely", msg)
		}
		if strings.Contains(msg, "too short or the rate too low") {
			t.Errorf("must not blame the parameters when 1500 requests were expected: %q", msg)
		}
	})
}
