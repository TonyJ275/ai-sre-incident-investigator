package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/demo"
)

type config struct {
	addr            string
	dataDir         string
	service         string
	metricsInterval time.Duration
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.addr, "addr", "127.0.0.1:8080", "listen address (loopback by default: this is a demo service)")
	flag.StringVar(&c.dataDir, "data-dir", "data", "directory for logs.jsonl and metrics.jsonl; existing files are truncated")
	flag.StringVar(&c.service, "service", "checkout-api", "service name recorded in every log line and metric")
	flag.DurationVar(&c.metricsInterval, "metrics-interval", 5*time.Second, "how often a metrics sample is written")
	flag.Parse()
	return c
}

func (c config) validate() error {
	if c.metricsInterval <= 0 {
		return fmt.Errorf("-metrics-interval must be positive, got %v", c.metricsInterval)
	}
	if c.service == "" {
		return errors.New("-service must not be empty")
	}
	return nil
}

func main() {
	if err := run(parseFlags()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// shutdownTimeout bounds how long in-flight requests get to finish after
// Ctrl+C. Our handlers take well under a second, so reaching it means
// something is badly wrong.
const shutdownTimeout = 5 * time.Second

func run(cfg config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	// Register for signals before anything else starts. Before this line
	// Ctrl+C would use the OS default and kill the process instantly. If the
	// signal arrives during setup, sigCtx is already done by the time we
	// reach the select below, and we shut down gracefully.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return err
	}

	logs, err := demo.NewJSONLWriter(filepath.Join(cfg.dataDir, "logs.jsonl"))
	if err != nil {
		return err
	}
	defer logs.Close()
	metricsOut, err := demo.NewJSONLWriter(filepath.Join(cfg.dataDir, "metrics.jsonl"))
	if err != nil {
		return err
	}
	defer metricsOut.Close()

	logger := slog.New(slog.NewJSONHandler(logs, nil)).With("service", cfg.service)
	db := demo.NewDB(demo.DefaultDBConfig(), logger)
	collector := demo.NewCollector(cfg.service, time.Now())
	srv := demo.NewServer(logger, db, collector)

	ticker := time.NewTicker(cfg.metricsInterval)
	defer ticker.Stop()
	flusher := &demo.MetricsFlusher{
		Collector: collector, Stats: db.Stats, Out: metricsOut, Logger: logger, Now: time.Now,
	}
	flushCtx, stopFlusher := context.WithCancel(context.Background())
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		flusher.Run(flushCtx, ticker.C)
	}()
	// Deferred AFTER the file closes, so on every return path (early errors
	// included) it runs BEFORE them: the flusher finishes its final write
	// while the file is still open. On the normal path, Shutdown has already
	// drained requests by now, so the final flush includes them.
	defer func() {
		stopFlusher()
		<-flusherDone
	}()

	// Listen before announcing: a busy port fails here, loudly.
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()

	logger.Info("demo app started", "addr", cfg.addr)
	fmt.Println("demo app listening on", cfg.addr, "- press Ctrl+C to stop")

	select {
	case err := <-serveErr:
		// Serve returned without us asking, so this is a real failure. It does
		// not close existing connections; Close does. The Close error is
		// ignored because we are already returning a more important one.
		_ = httpSrv.Close()
		return fmt.Errorf("http server failed: %w", err)
	case <-sigCtx.Done():
	}
	fmt.Println("shutting down...")

	var shutdownErr error
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		// Requests were still running at the deadline. Close drops their
		// connections but does not wait for the handlers, so a late log line
		// may be lost: acceptable for a demo, and reported, not hidden.
		logger.Warn("graceful shutdown timed out, forcing close", "error", err.Error())
		_ = httpSrv.Close()
		shutdownErr = fmt.Errorf("graceful shutdown incomplete: %w", err)
	}

	// Serve returns as soon as Shutdown/Close is called. Here ErrServerClosed
	// is the expected result; anything else is a real server failure.
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server failed: %w", err)
	}
	if shutdownErr == nil {
		logger.Info("demo app stopped")
	}
	return shutdownErr
}
