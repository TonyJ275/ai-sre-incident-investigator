package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/loadgen"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := loadgen.DefaultConfig("http://127.0.0.1:8080")
	flag.StringVar(&cfg.BaseURL, "url", cfg.BaseURL, "base URL of the service")
	flag.Float64Var(&cfg.Rate, "rate", cfg.Rate, "average requests per second")
	flag.DurationVar(&cfg.Duration, "duration", 0, "how long to run, e.g. 30s (0 = until Ctrl+C)")
	flag.IntVar(&cfg.MaxInFlight, "max-in-flight", cfg.MaxInFlight, "requests beyond this many concurrent ones are dropped")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "per-request timeout")
	flag.Uint64Var(&cfg.Seed, "seed", cfg.Seed, "random seed (same seed, same request mix)")
	flag.Parse()

	gen, err := loadgen.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("sending about %.0f requests/s to %s - press Ctrl+C to stop\n", cfg.Rate, cfg.BaseURL)
	st := gen.Run(ctx)

	fmt.Printf("sent %d  |  2xx %d  |  5xx %d  |  other %d  |  no response %d  |  dropped %d\n",
		st.Sent, st.Success, st.ServerErrors, st.OtherStatus, st.TransportErrors, st.Dropped)

	// ctx is cancelled only by Ctrl+C or SIGTERM; the -duration limit lives
	// inside Run.
	return st.Check(cfg, ctx.Err() != nil)
}
