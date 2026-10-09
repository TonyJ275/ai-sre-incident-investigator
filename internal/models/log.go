package models

import "time"

// LogEntry is one structured log line emitted by the demo service.
// The json tags must match the keys that Go's slog JSON handler writes,
// because the same struct is used to read the lines back.
type LogEntry struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"` // slog writes "INFO", "WARN", "ERROR"
	Msg     string    `json:"msg"`
	Service string    `json:"service"`

	// Request context. omitempty keeps these out of lines that are not about a request.
	RequestID string  `json:"request_id,omitempty"`
	Endpoint  string  `json:"endpoint,omitempty"`
	Method    string  `json:"method,omitempty"`
	Status    int     `json:"status,omitempty"`
	LatencyMS float64 `json:"latency_ms,omitempty"`
	Error     string  `json:"error,omitempty"`

	// Database context, present on DB-related lines.
	Query     string  `json:"query,omitempty"`
	WaitMS    float64 `json:"wait_ms,omitempty"`
	PoolInUse int     `json:"pool_in_use,omitempty"`
	PoolMax   int     `json:"pool_max,omitempty"`
}
