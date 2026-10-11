package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

func fastCfg() DBConfig {
	return DBConfig{
		MaxConns: 5, AcquireTimeout: time.Second, SlowAcquire: time.Hour,
		MinWork: time.Millisecond, MaxWork: time.Millisecond,
	}
}

// exhaustedCfg has a one-connection pool and a short acquire timeout, so a
// held connection makes every other query fail quickly.
func exhaustedCfg() DBConfig {
	return DBConfig{
		MaxConns: 1, AcquireTimeout: 50 * time.Millisecond, SlowAcquire: time.Hour,
		MinWork: 300 * time.Millisecond, MaxWork: 300 * time.Millisecond,
	}
}

// testServer wires the handlers and the DB to the same log buffer, so tests
// see the DB's lines and the handler's lines interleaved, as in production.
func testServer(cfg DBConfig) (*Server, *DB, *Collector, *syncBuffer) {
	db, buf := testDB(cfg)
	c := NewCollector("test-svc", time.Now())
	return NewServer(db.logger, db, c), db, c, buf
}

func do(srv *Server, method, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

// holdConnection occupies the only pool connection for the work duration.
func holdConnection(t *testing.T, db *DB) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- db.Query(context.Background(), "holder") }()
	waitFor(t, func() bool { return inUse(db) == 1 })
	return done
}

func flush(c *Collector) models.MetricSample {
	return c.Flush(time.Now().Add(time.Second), 0, 5)
}

func TestHealthzReturnsOKAndLogsOneLine(t *testing.T) {
	srv, _, _, buf := testServer(fastCfg())

	w := do(srv, http.MethodGet, "/healthz")

	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("got %d %q", w.Code, w.Body.String())
	}
	logs := logEntries(t, buf)
	if len(logs) != 1 || logs[0].Endpoint != "/healthz" || logs[0].Level != "INFO" || logs[0].Msg != "request completed" {
		t.Errorf("unexpected logs: %+v", logs)
	}
}

func TestHealthzDoesNotTouchTheDatabase(t *testing.T) {
	srv, db, _, _ := testServer(exhaustedCfg())
	holder := holdConnection(t, db)

	w := do(srv, http.MethodGet, "/healthz") // pool is fully used

	if w.Code != http.StatusOK {
		t.Errorf("healthz = %d while the pool is exhausted, want 200", w.Code)
	}
	<-holder
}

func TestProductsSuccess(t *testing.T) {
	srv, _, c, buf := testServer(fastCfg())

	w := do(srv, http.MethodGet, "/products")

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	id := w.Header().Get("X-Request-ID")
	if id == "" {
		t.Fatal("missing X-Request-ID header")
	}
	logs := logEntries(t, buf)
	if len(logs) != 1 {
		t.Fatalf("want 1 log line, got %d: %+v", len(logs), logs)
	}
	e := logs[0]
	if e.Level != "INFO" || e.Msg != "request completed" || e.RequestID != id ||
		e.Endpoint != "/products" || e.Method != "GET" || e.Status != 200 || e.LatencyMS <= 0 {
		t.Errorf("unexpected log entry: %+v", e)
	}
	if s := flush(c); s.Requests != 1 || s.Errors != 0 {
		t.Errorf("metrics = %+v, want 1 request and 0 errors", s)
	}
}

func TestCheckoutPoolExhaustedReturns503AndCorrelatedLogs(t *testing.T) {
	srv, db, c, buf := testServer(exhaustedCfg())
	holder := holdConnection(t, db)

	w := do(srv, http.MethodPost, "/checkout")
	<-holder

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	id := w.Header().Get("X-Request-ID")

	// The client must not learn internals, only the status and the request ID.
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["error"] != "Service Unavailable" || body["request_id"] != id {
		t.Errorf("body = %v", body)
	}
	if strings.Contains(strings.ToLower(w.Body.String()), "pool") {
		t.Errorf("response leaks internal details: %s", w.Body.String())
	}

	// The DB line and the handler line must share the request ID.
	var related []models.LogEntry
	for _, e := range logEntries(t, buf) {
		if e.RequestID == id {
			related = append(related, e)
		}
	}
	if len(related) != 2 {
		t.Fatalf("want 2 log lines for request %s, got %+v", id, related)
	}
	if related[0].Msg != "db connection acquire failed" || related[0].PoolInUse != 1 || related[0].PoolMax != 1 {
		t.Errorf("first line should be the DB failure with pool state: %+v", related[0])
	}
	if related[1].Msg != "request failed" || related[1].Level != "ERROR" || related[1].Status != 503 ||
		!strings.Contains(related[1].Error, "pool exhausted") {
		t.Errorf("second line should be the request failure: %+v", related[1])
	}

	if s := flush(c); s.Requests != 1 || s.Errors != 1 {
		t.Errorf("metrics = %+v, want 1 request and 1 error", s)
	}
}

func TestClientDisconnectIsNotAServiceError(t *testing.T) {
	cfg := exhaustedCfg()
	cfg.AcquireTimeout = 2 * time.Second // the client gives up long before this
	srv, db, c, buf := testServer(cfg)
	holder := holdConnection(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/checkout", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	<-holder

	if w.Code != 499 {
		t.Errorf("status = %d, want 499", w.Code)
	}
	for _, e := range logEntries(t, buf) {
		if e.Level == "ERROR" {
			t.Errorf("client disconnect logged as an error: %+v", e)
		}
	}
	if s := flush(c); s.Requests != 1 || s.Errors != 0 {
		t.Errorf("metrics = %+v, want 1 request and 0 errors", s)
	}
}

func TestCancellationClassification(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want int
	}{
		{"client left and the error is a context error", cancelled, fmt.Errorf("q: %w", context.Canceled), 499},
		{"client left but the real failure was pool exhaustion", cancelled, fmt.Errorf("q: %w", ErrPoolExhausted), 503},
		{"internal timeout while the client is still waiting", context.Background(), fmt.Errorf("q: %w", context.DeadlineExceeded), 500},
		{"pool exhaustion, client still waiting", context.Background(), fmt.Errorf("q: %w", ErrPoolExhausted), 503},
		{"work finished, client left afterwards", cancelled, nil, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _, _ := testServer(fastCfg())
			h := srv.wrap("/x", func(r *http.Request) (result, error) {
				if tc.err != nil {
					return result{}, tc.err
				}
				return result{Status: http.StatusOK, Body: map[string]string{}}, nil
			})

			w := httptest.NewRecorder()
			h(w, httptest.NewRequest(http.MethodGet, "/x", nil).WithContext(tc.ctx))

			if w.Code != tc.want {
				t.Errorf("status = %d, want %d", w.Code, tc.want)
			}
		})
	}
}
