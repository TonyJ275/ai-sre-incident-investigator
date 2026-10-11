package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMetricSampleParsesJSONLine(t *testing.T) {
	line := `{
		"time": "2026-10-09T14:32:05Z",
		"service": "checkout-api",
		"window_seconds": 5,
		"requests": 412,
		"errors": 37,
		"error_rate": 0.0898,
		"p50_latency_ms": 24.5,
		"p95_latency_ms": 1840.2,
		"db_pool_in_use": 5,
		"db_pool_max": 5
	}`

	var m MetricSample
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if m.Requests != 412 || m.Errors != 37 || m.WindowSeconds != 5 {
		t.Errorf("counts wrong: %+v", m)
	}
	if m.P95LatencyMS != 1840.2 || m.ErrorRate != 0.0898 {
		t.Errorf("derived fields wrong: %+v", m)
	}
	if m.DBPoolInUse != 5 || m.DBPoolMax != 5 {
		t.Errorf("pool gauges wrong: %+v", m)
	}
	want := time.Date(2026, 10, 9, 14, 32, 5, 0, time.UTC)
	if !m.Time.Equal(want) {
		t.Errorf("time = %v, want %v", m.Time, want)
	}
}
func TestIsServerError(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   bool
	}{
		{"OK", 200, false},
		{"Not Found", 404, false},
		{"just below server error", 499, false},
		{"Internal Server Error", 500, true},
		{"Bad Gateway", 502, true},
		{"Service Unavailable", 503, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsServerError(tt.status)

			if got != tt.want {
				t.Errorf(
					"IsServerError(%d) = %v, want %v",
					tt.status,
					got,
					tt.want,
				)
			}
		})
	}
}
