package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// ErrWriterClosed is returned for writes after Close.
var ErrWriterClosed = errors.New("jsonl: writer is closed")

// Compile-time checks: JSONLWriter must satisfy both interfaces it exists for.
var (
	_ Appender  = (*JSONLWriter)(nil)
	_ io.Writer = (*JSONLWriter)(nil)
)

// JSONLWriter writes newline-delimited JSON to a file. It is safe for
// concurrent use and unbuffered, so every completed write is visible to
// readers immediately and survives a crash of this process.
type JSONLWriter struct {
	mu     sync.Mutex // guards f and closed
	f      *os.File
	closed bool
}

// NewJSONLWriter creates the file at path, TRUNCATING it if it exists: each
// demo run starts from clean data. The directory must already exist.
func NewJSONLWriter(path string) (*JSONLWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return &JSONLWriter{f: f}, nil
}

// Append encodes v as one JSON line. Encoding happens before the lock is
// taken and before anything is written, so a value that cannot be encoded
// leaves the file untouched.
func (w *JSONLWriter) Append(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("jsonl: encode: %w", err)
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// Write implements io.Writer so slog can log straight into the file. It
// expects p to be one or more complete lines, which is how slog's JSON
// handler behaves: one Write call per record.
func (w *JSONLWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWriterClosed
	}
	return w.f.Write(p)
}

// Close closes the file. Calling it again is a no-op.
func (w *JSONLWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.f.Close()
}
