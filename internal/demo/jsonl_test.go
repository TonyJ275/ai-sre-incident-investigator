package demo

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

// newTestWriter creates a writer in a temp dir. The cleanup closes it before
// the dir is removed: Windows cannot delete a file that is still open.
func newTestWriter(t *testing.T) (*JSONLWriter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out.jsonl")
	w, err := NewJSONLWriter(path)
	if err != nil {
		t.Fatalf("NewJSONLWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return lines
}

type rec struct {
	G int    `json:"g"`
	N int    `json:"n"`
	S string `json:"s,omitempty"`
}

func TestAppendWritesOneJSONLinePerRecord(t *testing.T) {
	w, path := newTestWriter(t)

	for i := 1; i <= 3; i++ {
		if err := w.Append(rec{N: i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	lines := readLines(t, path)
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3", len(lines))
	}
	for i, line := range lines {
		var r rec
		if err := json.Unmarshal([]byte(line), &r); err != nil || r.N != i+1 {
			t.Errorf("line %d = %q (err %v), want n=%d", i, line, err, i+1)
		}
	}
}

func TestAppendKeepsEmbeddedNewlinesOnOneLine(t *testing.T) {
	w, path := newTestWriter(t)

	if err := w.Append(rec{S: "line one\nline two"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("a newline inside a value must not split the record: got %d lines", len(lines))
	}
	var r rec
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil || r.S != "line one\nline two" {
		t.Errorf("round trip failed: %+v, %v", r, err)
	}
}

func TestAppendEncodeErrorWritesNothing(t *testing.T) {
	w, path := newTestWriter(t)

	if err := w.Append(make(chan int)); err == nil { // channels cannot be marshalled
		t.Fatal("want an encode error, got nil")
	}
	if lines := readLines(t, path); len(lines) != 0 {
		t.Errorf("failed Append left data behind: %q", lines)
	}
	if err := w.Append(rec{N: 1}); err != nil { // writer must still be usable
		t.Errorf("writer unusable after an encode error: %v", err)
	}
}

func TestNewJSONLWriterTruncatesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	if err := os.WriteFile(path, []byte("old run\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	w, err := NewJSONLWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })

	if lines := readLines(t, path); len(lines) != 0 {
		t.Errorf("old content survived: %q", lines)
	}
}

func TestConcurrentAppendsNeverInterleave(t *testing.T) {
	w, path := newTestWriter(t)

	const goroutines, perGoroutine = 50, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < perGoroutine; n++ {
				if err := w.Append(rec{G: g, N: n}); err != nil {
					t.Errorf("Append: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	lines := readLines(t, path)
	if len(lines) != goroutines*perGoroutine {
		t.Fatalf("got %d lines, want %d", len(lines), goroutines*perGoroutine)
	}
	for i, line := range lines {
		var r rec
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("line %d is corrupt (interleaved write?): %q", i, line)
		}
	}
}

func TestWritesAfterCloseFailAndCloseIsIdempotent(t *testing.T) {
	w, _ := newTestWriter(t)

	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close should be a no-op, got %v", err)
	}
	if err := w.Append(rec{N: 1}); !errors.Is(err, ErrWriterClosed) {
		t.Errorf("Append after Close = %v, want ErrWriterClosed", err)
	}
	if _, err := w.Write([]byte("x\n")); !errors.Is(err, ErrWriterClosed) {
		t.Errorf("Write after Close = %v, want ErrWriterClosed", err)
	}
}

// This is the real contract: what slog writes must parse as models.LogEntry.
func TestSlogOutputParsesAsLogEntry(t *testing.T) {
	w, path := newTestWriter(t)
	logger := slog.New(slog.NewJSONHandler(w, nil)).With("service", "checkout-api")

	logger.Error("request failed", "request_id", "r1", "status", 500, "latency_ms", 512.5, "error", "boom")
	logger.Info("request completed", "request_id", "r2", "status", 200)

	lines := readLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), lines)
	}
	var e models.LogEntry
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
		t.Fatal(err)
	}
	if e.Level != "ERROR" || e.Service != "checkout-api" || e.RequestID != "r1" ||
		e.Status != 500 || e.LatencyMS != 512.5 || e.Error != "boom" || e.Time.IsZero() {
		t.Errorf("slog line did not map onto LogEntry: %+v", e)
	}
	if strings.Contains(lines[1], "\n") {
		t.Errorf("line contains a raw newline: %q", lines[1])
	}
}
