package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestChangeParsesJSONLine(t *testing.T) {
	line := `{
		"id": "chg-001",
		"time": "2026-10-09T14:30:00Z",
		"service": "checkout-api",
		"kind": "config_change",
		"target": "db.pool.max_conns",
		"old_value": "50",
		"new_value": "5",
		"author": "alice",
		"description": "reduce pool size"
	}`

	var c Change
	if err := json.Unmarshal([]byte(line), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.ID != "chg-001" || c.Kind != ChangeKindConfig || c.Target != "db.pool.max_conns" {
		t.Errorf("identity fields wrong: %+v", c)
	}
	if c.OldValue != "50" || c.NewValue != "5" || c.Author != "alice" {
		t.Errorf("value fields wrong: %+v", c)
	}
	want := time.Date(2026, 10, 9, 14, 30, 0, 0, time.UTC)
	if !c.Time.Equal(want) {
		t.Errorf("time = %v, want %v", c.Time, want)
	}
}

func TestChangeValidate(t *testing.T) {
	valid := Change{Service: "checkout-api", Kind: ChangeKindDeploy, Target: "checkout-api", NewValue: "v1.4.2"}

	tests := []struct {
		name    string
		mutate  func(c *Change)
		wantErr bool
	}{
		{"valid deploy", func(c *Change) {}, false},
		{"valid config change", func(c *Change) { c.Kind = ChangeKindConfig }, false},
		{"missing service", func(c *Change) { c.Service = "" }, true},
		{"missing target", func(c *Change) { c.Target = "" }, true},
		{"missing kind", func(c *Change) { c.Kind = "" }, true},
		{"unknown kind", func(c *Change) { c.Kind = "reboot" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.mutate(&c)
			if err := c.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
