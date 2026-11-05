package demo

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"
)

func TestSetWorkFactorValidation(t *testing.T) {
	tests := []struct {
		name    string
		factor  float64
		wantErr bool
	}{
		{"normal", 1, false},
		{"faster than normal", 0.5, false},
		{"the maximum", MaxWorkFactor, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"above the maximum", MaxWorkFactor + 1, true},
		{"NaN", math.NaN(), true},
		{"infinite", math.Inf(1), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := testDB(fastCfg())
			if err := db.SetWorkFactor(tc.factor); (err != nil) != tc.wantErr {
				t.Errorf("SetWorkFactor(%v) error = %v, wantErr %v", tc.factor, err, tc.wantErr)
			}
		})
	}
}

func TestSetWorkFactorSlowsQueries(t *testing.T) {
	db, _ := testDB(DBConfig{
		MaxConns: 2, AcquireTimeout: time.Second, SlowAcquire: time.Hour,
		MinWork: 20 * time.Millisecond, MaxWork: 20 * time.Millisecond,
	})
	if err := db.SetWorkFactor(5); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := db.Query(context.Background(), "select"); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// 20ms x 5 = 100ms. A timer never fires early, so a lower bound is safe.
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Errorf("query took %v, want at least about 100ms", elapsed)
	}
}

func TestDBConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *DBConfig)
		wantErr bool
	}{
		{"default config is valid", func(c *DBConfig) {}, false},
		{"zero connections", func(c *DBConfig) { c.MaxConns = 0 }, true},
		{"negative connections", func(c *DBConfig) { c.MaxConns = -1 }, true},
		{"zero acquire timeout", func(c *DBConfig) { c.AcquireTimeout = 0 }, true},
		{"negative acquire timeout", func(c *DBConfig) { c.AcquireTimeout = -time.Second }, true},
		{"negative slow-acquire threshold", func(c *DBConfig) { c.SlowAcquire = -1 }, true},
		{"negative MinWork", func(c *DBConfig) { c.MinWork = -time.Millisecond }, true},
		{"very negative MinWork (would overflow MaxWork-MinWork)", func(c *DBConfig) { c.MinWork = time.Duration(math.MinInt64) }, true},
		{"MaxWork below MinWork", func(c *DBConfig) { c.MinWork, c.MaxWork = 10*time.Millisecond, 5*time.Millisecond }, true},
		{"both works zero", func(c *DBConfig) { c.MinWork, c.MaxWork = 0, 0 }, false},
		{"MaxWork at the limit", func(c *DBConfig) { c.MaxWork = MaxQueryWork }, false},
		{"MaxWork above the limit", func(c *DBConfig) { c.MaxWork = MaxQueryWork + 1 }, true},
		{"enormous MaxWork", func(c *DBConfig) { c.MaxWork = time.Duration(math.MaxInt64) }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultDBConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewDBPanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewDB accepted an invalid config; it should panic")
		}
	}()
	NewDB(DBConfig{}, slog.Default()) // zero MaxConns and AcquireTimeout
}

// The largest config Validate accepts, with the largest factor, must produce
// the exact integer product: proof that the bounds make overflow impossible.
func TestWorstCaseWorkDurationIsRepresentable(t *testing.T) {
	db, _ := testDB(DBConfig{
		MaxConns: 1, AcquireTimeout: time.Second, SlowAcquire: time.Second,
		MinWork: MaxQueryWork, MaxWork: MaxQueryWork,
	})
	if err := db.SetWorkFactor(MaxWorkFactor); err != nil {
		t.Fatal(err)
	}

	got, want := db.workDuration(), MaxQueryWork*1000
	if got <= 0 || got != want {
		t.Errorf("workDuration() = %v, want %v", got, want)
	}
}

// Under `go test -race` this fails if the work factor is read or written
// without the mutex.
func TestWorkFactorCanChangeWhileQueriesRun(t *testing.T) {
	db, _ := testDB(DBConfig{
		MaxConns: 4, AcquireTimeout: 5 * time.Second, SlowAcquire: time.Hour,
		MinWork: time.Millisecond, MaxWork: 2 * time.Millisecond,
	})

	stop := make(chan struct{})
	var changer sync.WaitGroup
	changer.Add(1)
	go func() {
		defer changer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.SetWorkFactor(float64(1 + i%3)); err != nil {
				t.Errorf("SetWorkFactor: %v", err)
				return
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	var queries sync.WaitGroup
	for g := 0; g < 8; g++ {
		queries.Add(1)
		go func() {
			defer queries.Done()
			for i := 0; i < 20; i++ {
				if err := db.Query(context.Background(), "select"); err != nil {
					t.Errorf("query failed: %v", err)
					return
				}
			}
		}()
	}
	queries.Wait()
	close(stop)
	changer.Wait()

	if n := inUse(db); n != 0 {
		t.Errorf("connections leaked: %d still in use", n)
	}
}
