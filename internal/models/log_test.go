package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLogEntryParsesSlogLine(t *testing.T) {
	line := `{
		"time": "2026-10-09T14:32:01.123456789Z",
		"level": "ERROR",
		"msg": "db connection acquire failed",
		"service": "checkout-api",
		"query": "insert order",
		"wait_ms": 500.2,
		"pool_in_use": 5,
		"pool_max": 5,
		"error": "db: connection pool exhausted"
	}`

	var e LogEntry
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if e.Level != "ERROR" {
		t.Errorf("Level = %q, want ERROR", e.Level)
	}
	if e.Msg != "db connection acquire failed" {
		t.Errorf("Msg = %q, want db connection acquire failed", e.Msg)
	}
	if e.Service != "checkout-api" {
		t.Errorf("Service = %q, want checkout-api", e.Service)
	}
	if e.Query != "insert order" {
		t.Errorf("Query = %q, want insert order", e.Query)
	}
	if e.WaitMS != 500.2 {
		t.Errorf("WaitMS = %v, want 500.2", e.WaitMS)
	}
	if e.PoolInUse != 5 {
		t.Errorf("PoolInUse = %d, want 5", e.PoolInUse)
	}
	if e.PoolMax != 5 {
		t.Errorf("PoolMax = %d, want 5", e.PoolMax)
	}
	if e.Error != "db: connection pool exhausted" {
		t.Errorf("Error = %q, want db: connection pool exhausted", e.Error)
	}

	want := time.Date(
		2026, time.October, 9,
		14, 32, 1, 123456789,
		time.UTC,
	)
	if !e.Time.Equal(want) {
		t.Errorf("Time = %v, want %v", e.Time, want)
	}
}
