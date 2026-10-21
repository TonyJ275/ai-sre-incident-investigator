package demo

import (
	"context"
	"log/slog"
	"time"
)

// MetricsFlusher turns the Collector's current window into a MetricSample on
// every tick and writes it out.
type MetricsFlusher struct {
	Collector *Collector
	Stats     func() (inUse, poolMax int) // pool gauges, read at flush time
	Out       Appender
	Logger    *slog.Logger
	Now       func() time.Time // used for the final flush at shutdown
}

// Run blocks until ctx is cancelled. Each value received on ticks ends a
// window; the tick's timestamp is the window end. ticks must not be closed:
// time.Ticker's channel never is.
func (f *MetricsFlusher) Run(ctx context.Context, ticks <-chan time.Time) {
	for {
		select {
		case t := <-ticks:
			f.flush(t, true)
		case <-ctx.Done():
			// Capture the partial window so shutdown does not lose requests,
			// but skip it when empty: a near-zero-length window of zero
			// requests would read as "traffic stopped".
			f.flush(f.Now(), false)
			return
		}
	}
}

func (f *MetricsFlusher) flush(at time.Time, writeIfEmpty bool) {
	inUse, poolMax := f.Stats()
	s := f.Collector.Flush(at, inUse, poolMax)
	if !writeIfEmpty && s.Requests == 0 {
		return
	}
	if err := f.Out.Append(s); err != nil {
		// Losing one sample is better than stopping monitoring altogether.
		f.Logger.Error("write metrics failed", "error", err.Error())
	}
}
