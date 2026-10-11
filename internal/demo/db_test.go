package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

// syncBuffer is a bytes.Buffer safe for concurrent writes, so tests can
// capture log output produced by many goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func testDB(cfg DBConfig) (*DB, *syncBuffer) {
	buf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil)).With("service", "test-svc")
	return NewDB(cfg, logger), buf
}

// logEntries parses captured logs with the real models.LogEntry, so these
// tests also verify the logging contract the agent will rely on.
func logEntries(t *testing.T, buf *syncBuffer) []models.LogEntry {
	t.Helper()
	var out []models.LogEntry
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var e models.LogEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func inUse(db *DB) int { n, _ := db.Stats(); return n }

func TestQuerySucceedsAndReleasesConnection(t *testing.T) {
	db, buf := testDB(DBConfig{
		MaxConns: 2, AcquireTimeout: time.Second, SlowAcquire: time.Second,
		MinWork: time.Millisecond, MaxWork: time.Millisecond,
	})

	if err := db.Query(context.Background(), "select"); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if n, max := db.Stats(); n != 0 || max != 2 {
		t.Errorf("stats = (%d in use, %d max), want (0, 2)", n, max)
	}
	if got := logEntries(t, buf); len(got) != 0 {
		t.Errorf("healthy query should not log, got %+v", got)
	}
}

func TestPoolExhaustionTimesOutAndLogsEvidence(t *testing.T) {
	db, buf := testDB(DBConfig{
		MaxConns: 1, AcquireTimeout: 50 * time.Millisecond, SlowAcquire: time.Hour,
		MinWork: 300 * time.Millisecond, MaxWork: 300 * time.Millisecond,
	})

	holder := make(chan error, 1)
	go func() { holder <- db.Query(context.Background(), "holder") }()
	waitFor(t, func() bool { return inUse(db) == 1 })

	ctx := WithRequestID(context.Background(), "req-1")
	err := db.Query(ctx, "insert order")
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("err = %v, want ErrPoolExhausted", err)
	}
	if err := <-holder; err != nil {
		t.Fatalf("holder failed: %v", err)
	}

	got := logEntries(t, buf)
	if len(got) != 1 {
		t.Fatalf("want exactly 1 log line, got %d: %+v", len(got), got)
	}
	e := got[0]
	if e.Level != "ERROR" || e.Msg != "db connection acquire failed" {
		t.Errorf("level/msg = %s / %q", e.Level, e.Msg)
	}
	if e.Query != "insert order" || e.RequestID != "req-1" {
		t.Errorf("query/request_id = %q / %q", e.Query, e.RequestID)
	}
	if e.PoolInUse != 1 || e.PoolMax != 1 {
		t.Errorf("pool = %d/%d, want 1/1", e.PoolInUse, e.PoolMax)
	}
	if e.WaitMS < 50 {
		t.Errorf("wait_ms = %v, want >= 50", e.WaitMS)
	}
}

func TestWaiterGetsConnectionAfterRelease(t *testing.T) {
	db, buf := testDB(DBConfig{
		MaxConns: 1, AcquireTimeout: 2 * time.Second, SlowAcquire: 10 * time.Millisecond,
		MinWork: 50 * time.Millisecond, MaxWork: 50 * time.Millisecond,
	})

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errs <- db.Query(context.Background(), "select") }()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("query %d failed: %v", i, err)
		}
	}

	var slow []models.LogEntry
	for _, e := range logEntries(t, buf) {
		if e.Level == "WARN" && e.Msg == "db connection acquire slow" {
			slow = append(slow, e)
		}
	}
	if len(slow) != 1 || slow[0].WaitMS < 10 {
		t.Errorf("want exactly 1 slow-acquire warning with wait_ms >= 10, got %+v", slow)
	}
}

func TestCallerCancellationIsNotAPoolFailure(t *testing.T) {
	db, buf := testDB(DBConfig{
		MaxConns: 1, AcquireTimeout: 2 * time.Second, SlowAcquire: time.Hour,
		MinWork: 300 * time.Millisecond, MaxWork: 300 * time.Millisecond,
	})

	holder := make(chan error, 1)
	go func() { holder <- db.Query(context.Background(), "holder") }()
	waitFor(t, func() bool { return inUse(db) == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	err := db.Query(ctx, "select")
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("err = %v, want context.Canceled and not ErrPoolExhausted", err)
	}
	<-holder
	for _, e := range logEntries(t, buf) {
		if e.Level == "ERROR" {
			t.Errorf("client hang-up must not log a DB error: %+v", e)
		}
	}
}

func TestSetMaxWakesWaiters(t *testing.T) {
	db, _ := testDB(DBConfig{
		MaxConns: 1, AcquireTimeout: 2 * time.Second, SlowAcquire: time.Hour,
		MinWork: 300 * time.Millisecond, MaxWork: 300 * time.Millisecond,
	})

	done := make(chan error, 2)
	go func() { done <- db.Query(context.Background(), "holder") }()
	waitFor(t, func() bool { return inUse(db) == 1 })
	go func() { done <- db.Query(context.Background(), "waiter") }()
	time.Sleep(20 * time.Millisecond) // let the waiter start waiting

	db.SetMax(2)

	// If the waiter was not woken, inUse would stay at 1 until the holder finished.
	waitFor(t, func() bool { return inUse(db) == 2 })
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Errorf("query failed: %v", err)
		}
	}
}

func TestPoolNeverExceedsMaxUnderConcurrency(t *testing.T) {
	db, _ := testDB(DBConfig{
		MaxConns: 3, AcquireTimeout: 5 * time.Second, SlowAcquire: time.Hour,
		MinWork: time.Millisecond, MaxWork: 3 * time.Millisecond,
	})

	stop := make(chan struct{})
	violation := make(chan string, 1)
	go func() { // observer: keeps checking the invariant while load runs
		for {
			select {
			case <-stop:
				return
			default:
				if n, max := db.Stats(); n > max {
					select {
					case violation <- "in use exceeded max":
					default:
					}
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 60; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if err := db.Query(context.Background(), "select"); err != nil {
					t.Errorf("query failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)

	select {
	case v := <-violation:
		t.Fatal(v)
	default:
	}
	if n := inUse(db); n != 0 {
		t.Errorf("connections leaked: %d still in use", n)
	}
}
