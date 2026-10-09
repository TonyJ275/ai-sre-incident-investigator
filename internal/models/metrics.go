package models

import "time"

// MetricSample summarises one service's traffic over a short window.
// The demo service writes one sample per window to metrics.jsonl.
type MetricSample struct {
	Time          time.Time `json:"time"` // end of the window
	Service       string    `json:"service"`
	WindowSeconds float64   `json:"window_seconds"` // length of the window this sample covers

	// Counts over the window. They reset every flush.
	Requests int `json:"requests"`
	Errors   int `json:"errors"` // responses with status >= 500

	// Derived from the counts. Zero when there was no traffic in the window.
	ErrorRate    float64 `json:"error_rate"` // Errors / Requests, 0..1
	P50LatencyMS float64 `json:"p50_latency_ms"`
	P95LatencyMS float64 `json:"p95_latency_ms"`

	// Gauges: the state of the connection pool at the moment of the flush.
	DBPoolInUse int `json:"db_pool_in_use"`
	DBPoolMax   int `json:"db_pool_max"`
}
