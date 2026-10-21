package demo

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

// sampleSink is an Appender that delivers each written MetricSample on a channel.
type sampleSink struct {
	mu  sync.Mutex
	err error
	ch  chan models.MetricSample
}

func newSampleSink() *sampleSink { return &sampleSink{ch: make(chan models.MetricSample, 16)} }

func (s *sampleSink) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *sampleSink) Append(v any) error {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.ch <- v.(models.MetricSample)
	return nil
}

type flusherHarness struct {
	t0     time.Time
	c      *Collector
	ticks  chan time.Time
	sink   *sampleSink
	buf    *syncBuffer
	cancel context.CancelFunc
	done   chan struct{}
}

// newFlusherHarness starts a flusher whose pool gauges are always (2, 5) and
// whose shutdown clock reads finalNow.
func newFlusherHarness(t *testing.T, finalNow time.Time) *flusherHarness {
	t.Helper()
	h := &flusherHarness{
		t0:    time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC),
		ticks: make(chan time.Time),
		sink:  newSampleSink(),
		buf:   &syncBuffer{},
		done:  make(chan struct{}),
	}
	h.c = NewCollector("test-svc", h.t0)
	f := &MetricsFlusher{
		Collector: h.c,
		Stats:     func() (int, int) { return 2, 5 },
		Out:       h.sink,
		Logger:    slog.New(slog.NewJSONHandler(h.buf, nil)),
		Now:       func() time.Time { return finalNow },
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(cancel)
	go func() {
		defer close(h.done)
		f.Run(ctx, h.ticks)
	}()
	return h
}

func (h *flusherHarness) next(t *testing.T) models.MetricSample {
	t.Helper()
	select {
	case s := <-h.sink.ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("no sample written within 2s")
		return models.MetricSample{}
	}
}

func (h *flusherHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		t.Fatal("flusher did not stop within 2s")
	}
}

func TestFlusherWritesOneSamplePerTickIncludingIdleWindows(t *testing.T) {
	h := newFlusherHarness(t, time.Time{})
	h.c.Record(10, 200)
	h.c.Record(20, 200)
	h.c.Record(30, 500)

	tick1 := h.t0.Add(5 * time.Second)
	h.ticks <- tick1
	s := h.next(t)
	if s.Requests != 3 || s.Errors != 1 || s.Service != "test-svc" || !s.Time.Equal(tick1) {
		t.Errorf("first sample wrong: %+v", s)
	}
	if s.WindowSeconds != 5 || s.DBPoolInUse != 2 || s.DBPoolMax != 5 {
		t.Errorf("window/gauges wrong: %+v", s)
	}

	h.ticks <- h.t0.Add(10 * time.Second) // no traffic in this window
	idle := h.next(t)
	if idle.Requests != 0 || idle.WindowSeconds != 5 {
		t.Errorf("an idle window must still be written: %+v", idle)
	}
	h.stop(t)
}

func TestFlusherFinalFlushOnShutdown(t *testing.T) {
	finalNow := time.Date(2026, 10, 9, 14, 0, 7, 0, time.UTC)
	h := newFlusherHarness(t, finalNow)
	h.c.Record(10, 200)
	h.c.Record(20, 200)

	h.stop(t)

	s := h.next(t)
	if s.Requests != 2 || !s.Time.Equal(finalNow) || s.WindowSeconds != 7 {
		t.Errorf("final sample wrong: %+v", s)
	}
}

func TestFlusherSkipsEmptyFinalWindow(t *testing.T) {
	h := newFlusherHarness(t, time.Date(2026, 10, 9, 14, 0, 1, 0, time.UTC))

	h.stop(t)

	select {
	case s := <-h.sink.ch:
		t.Errorf("empty final window should not be written, got %+v", s)
	default:
	}
}

func TestFlusherSurvivesWriteErrors(t *testing.T) {
	h := newFlusherHarness(t, time.Time{})

	h.sink.setErr(errors.New("disk full"))
	h.ticks <- h.t0.Add(5 * time.Second)
	waitFor(t, func() bool { return strings.Contains(h.buf.String(), "write metrics failed") })

	h.sink.setErr(nil)
	h.c.Record(10, 200)
	h.ticks <- h.t0.Add(10 * time.Second)
	if s := h.next(t); s.Requests != 1 {
		t.Errorf("flusher did not recover after a failed write: %+v", s)
	}
	h.stop(t)
}

// With a tick pending and the context already cancelled, both select cases
// are ready and Go picks either at random. Whichever order occurs, the
// requests must appear in exactly one sample: no duplicates, no empty extras.
func TestFlusherTickAndShutdownTogetherWritesRequestsExactlyOnce(t *testing.T) {
	for i := 0; i < 200; i++ {
		t0 := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
		c := NewCollector("svc", t0)
		c.Record(10, 200)
		c.Record(20, 200)

		sink := newSampleSink()
		f := &MetricsFlusher{
			Collector: c,
			Stats:     func() (int, int) { return 0, 5 },
			Out:       sink,
			Logger:    slog.New(slog.NewJSONHandler(&syncBuffer{}, nil)),
			Now:       func() time.Time { return t0.Add(6 * time.Second) },
		}
		ticks := make(chan time.Time, 1)
		ticks <- t0.Add(5 * time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		f.Run(ctx, ticks)

		var got []models.MetricSample
		for len(sink.ch) > 0 {
			got = append(got, <-sink.ch)
		}
		total := 0
		for _, s := range got {
			total += s.Requests
		}
		if len(got) != 1 || total != 2 {
			t.Fatalf("iteration %d: %d samples with %d requests, want 1 sample with 2: %+v", i, len(got), total, got)
		}
	}
}
