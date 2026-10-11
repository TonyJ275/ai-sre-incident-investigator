package demo

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrPoolExhausted is returned when a caller waited the full acquire timeout without getting a connection.
var ErrPoolExhausted = errors.New("db: connection pool exhausted, timed out waiting for a free connection")

// DBConfig holds the knobs of the simulated database.
type DBConfig struct {
	MaxConns       int           // pool size at startup
	AcquireTimeout time.Duration // how long a caller waits for a free connection
	SlowAcquire    time.Duration // waits longer than this are logged as warnings
	MinWork        time.Duration // each query holds a connection for a random time
	MaxWork        time.Duration // between MinWork and MaxWork
}

// DefaultDBConfig is a healthy production-like setup.
func DefaultDBConfig() DBConfig {
	return DBConfig{
		MaxConns:       50,
		AcquireTimeout: 500 * time.Millisecond,
		SlowAcquire:    50 * time.Millisecond,
		MinWork:        5 * time.Millisecond,
		MaxWork:        25 * time.Millisecond,
	}
}

// DB simulates a database with a bounded, resizable connection pool.
type DB struct {
	cfg    DBConfig
	logger *slog.Logger

	mu      sync.Mutex    // guards everything below
	max     int           // current pool size; SetMax can change it
	inUse   int           // connections currently held
	changed chan struct{} // closed and replaced whenever max or inUse changes
}

func NewDB(cfg DBConfig, logger *slog.Logger) *DB {
	return &DB{cfg: cfg, logger: logger, max: cfg.MaxConns, changed: make(chan struct{})}
}

// SetMax resizes the pool. Lowering it below the number of connections
// currently in use does not kill them; new callers just wait until enough
// are released.
func (d *DB) SetMax(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.max = n
	d.notifyLocked()
}

// Stats returns a consistent snapshot of the pool gauges.
func (d *DB) Stats() (inUse, poolMax int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inUse, d.max
}

// notifyLocked wakes every waiter: closing a channel unblocks all receivers.
// The caller must hold d.mu.
func (d *DB) notifyLocked() {
	close(d.changed)
	d.changed = make(chan struct{})
}

// acquire blocks until a connection is free or ctx ends.
func (d *DB) acquire(ctx context.Context) error {
	for {
		d.mu.Lock()
		if d.inUse < d.max {
			d.inUse++
			d.mu.Unlock()
			return nil
		}
		changed := d.changed // grab the current signal before unlocking
		d.mu.Unlock()

		select {
		case <-changed: // something changed: loop and check again
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *DB) release() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inUse--
	d.notifyLocked()
}

// Query simulates one database call: wait for a connection, hold it while
// "working", release it.
func (d *DB) Query(ctx context.Context, name string) error {
	log := d.loggerFor(ctx)
	start := time.Now()

	actx, cancel := context.WithTimeout(ctx, d.cfg.AcquireTimeout)
	err := d.acquire(actx)
	cancel()
	waited := time.Since(start)

	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // the caller gave up; the database is not at fault
		}
		inUse, poolMax := d.Stats()
		log.Error("db connection acquire failed",
			"query", name, "wait_ms", ms(waited),
			"pool_in_use", inUse, "pool_max", poolMax,
			"error", ErrPoolExhausted.Error())
		return ErrPoolExhausted
	}
	defer d.release()

	if waited > d.cfg.SlowAcquire {
		inUse, poolMax := d.Stats()
		log.Warn("db connection acquire slow",
			"query", name, "wait_ms", ms(waited),
			"pool_in_use", inUse, "pool_max", poolMax)
	}

	t := time.NewTimer(d.workDuration())
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *DB) workDuration() time.Duration {
	lo, hi := d.cfg.MinWork, d.cfg.MaxWork
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int64N(int64(hi-lo)))
}

func (d *DB) loggerFor(ctx context.Context) *slog.Logger {
	if id := RequestIDFromContext(ctx); id != "" {
		return d.logger.With("request_id", id)
	}
	return d.logger
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
