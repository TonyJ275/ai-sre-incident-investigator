package models

import "time"

// MetricSample summarises one service's traffic over a short window.
// The demo service writes one sample per window to metrics.jsonl.

// Percentile definition (project-wide): nearest-rank. For n sorted values the
// p-th percentile is the value at position ceil(p*n), counting from 1.
// It is always a value that was actually observed; there is no interpolation.
// Percentiles cannot be averaged or merged across samples.
type MetricSample struct {
	Time          time.Time `json:"time"` // end of the window
	Service       string    `json:"service"`
	WindowSeconds float64   `json:"window_seconds"` // length of the window this sample covers

	// Counts over the window. They reset every flush.
	Requests int `json:"requests"`
	Errors   int `json:"errors"` // see IsServerError

	// Derived from the counts. Zero when there was no usable data in the window.
	ErrorRate    float64 `json:"error_rate"` // Errors / Requests, 0..1
	P50LatencyMS float64 `json:"p50_latency_ms"`
	P95LatencyMS float64 `json:"p95_latency_ms"`

	// InvalidLatencySamples counts requests whose latency was NaN, infinite or
	// negative. They are included in Requests and Errors but excluded from the
	// percentiles. Non-zero means the percentiles are based on incomplete data.
	InvalidLatencySamples int `json:"invalid_latency_samples,omitempty"`

	// Gauges: the state of the connection pool at the moment of the flush.
	DBPoolInUse int `json:"db_pool_in_use"`
	DBPoolMax   int `json:"db_pool_max"`
}

// IsServerError is the single definition of "an error" for metrics:
// responses with status 500 or above. Client errors (4xx) are not counted
// because they do not indicate a failure of the service.
func IsServerError(status int) bool { return status >= 500 }
