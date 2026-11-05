package demo

import (
	"context"
	"errors"
	"fmt"
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

// MaxQueryWork bounds DBConfig.MinWork and MaxWork. Together with
// MaxWorkFactor it guarantees that the largest hold time, MaxQueryWork x
// MaxWorkFactor (10s x 1000 = 10,000s), is nowhere near the range limit of
// time.Duration (about 292 years). Overflow is impossible by construction,
// so nothing is clamped and nothing is silently altered.
const MaxQueryWork = 10 * time.Second

// Validate reports whether the config is usable. The work bounds also make
// MaxWork-MinWork in workDuration safe from overflow.
func (c DBConfig) Validate() error {
	switch {
	case c.MaxConns < 1:
		return fmt.Errorf("demo: MaxConns must be at least 1, got %d", c.MaxConns)
	case c.AcquireTimeout <= 0:
		return fmt.Errorf("demo: AcquireTimeout must be positive, got %v", c.AcquireTimeout)
	case c.SlowAcquire < 0:
		return fmt.Errorf("demo: SlowAcquire must not be negative, got %v", c.SlowAcquire)
	case c.MinWork < 0:
		return fmt.Errorf("demo: MinWork must not be negative, got %v", c.MinWork)
	case c.MaxWork < c.MinWork:
		return fmt.Errorf("demo: MaxWork (%v) must not be below MinWork (%v)", c.MaxWork, c.MinWork)
	case c.MaxWork > MaxQueryWork:
		return fmt.Errorf("demo: MaxWork must not exceed %v, got %v", MaxQueryWork, c.MaxWork)
	}
	return nil
}

// DB simulates a database with a bounded, resizable connection pool.
type DB struct {
	cfg    DBConfig
	logger *slog.Logger

	mu         sync.Mutex    // guards everything below
	max        int           // current pool size; SetMax can change it
	inUse      int           // connections currently held
	changed    chan struct{} // closed and replaced whenever max or inUse changes
	workFactor float64       // multiplies how long a query holds its connection; 1 is normal
}

// NewDB panics if cfg is invalid: a DBConfig is code, not user input, so an
// invalid one is a programming error. Callers that build a config from user
// input should call cfg.Validate and report the error themselves.
func NewDB(cfg DBConfig, logger *slog.Logger) *DB {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return &DB{cfg: cfg, logger: logger, max: cfg.MaxConns, workFactor: 1, changed: make(chan struct{})}
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

// MaxWorkFactor bounds SetWorkFactor so that a typo cannot overflow
// time.Duration or make a query hold its connection for hours.
const MaxWorkFactor = 1000.0

// SetWorkFactor makes every query hold its connection f times as long as
// normal. This simulates slow queries: slower queries keep connections busy
// longer, so the pool saturates even though its size never changed.
func (d *DB) SetWorkFactor(f float64) error {
	if !(f > 0 && f <= MaxWorkFactor) { // also rejects NaN
		return fmt.Errorf("demo: work factor must be within (0, %v], got %v", MaxWorkFactor, f)
	}
	d.mu.Lock()
	d.workFactor = f
	d.mu.Unlock()
	return nil
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

// workDuration picks a random hold time in [MinWork, MaxWork] and scales it
// by the work factor. Config validation (MaxWork <= MaxQueryWork) and
// SetWorkFactor (factor <= MaxWorkFactor) bound both operands, so neither the
// subtraction nor the float-to-Duration conversion can overflow.
func (d *DB) workDuration() time.Duration {
	base := d.cfg.MinWork
	if d.cfg.MaxWork > d.cfg.MinWork {
		base += time.Duration(rand.Int64N(int64(d.cfg.MaxWork - d.cfg.MinWork)))
	}
	d.mu.Lock()
	f := d.workFactor
	d.mu.Unlock()
	return time.Duration(float64(base) * f)
}

func (d *DB) loggerFor(ctx context.Context) *slog.Logger {
	if id := RequestIDFromContext(ctx); id != "" {
		return d.logger.With("request_id", id)
	}
	return d.logger
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
