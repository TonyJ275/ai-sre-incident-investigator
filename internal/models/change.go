package models

import (
	"fmt"
	"time"
)

// The kinds of change the demo service records.
const (
	ChangeKindConfig = "config_change"
	ChangeKindDeploy = "deploy"
)

// Change is one recorded modification to a service. It states what changed,
// never why or what it caused: judging whether a change is related to an
// incident is the investigator's job, not the data's.
type Change struct {
	ID          string    `json:"id"` // assigned by the change log, e.g. "chg-001"
	Time        time.Time `json:"time"`
	Service     string    `json:"service"`
	Kind        string    `json:"kind"`
	Target      string    `json:"target"` // what was changed, e.g. "db.pool.max_conns"
	OldValue    string    `json:"old_value,omitempty"`
	NewValue    string    `json:"new_value,omitempty"`
	Author      string    `json:"author,omitempty"`
	Description string    `json:"description,omitempty"`
}

// Validate reports whether the change has the fields every record needs.
// ID and Time are not checked: the change log fills them in.
func (c Change) Validate() error {
	switch {
	case c.Service == "":
		return fmt.Errorf("change: service is required")
	case c.Target == "":
		return fmt.Errorf("change: target is required")
	case c.Kind != ChangeKindConfig && c.Kind != ChangeKindDeploy:
		return fmt.Errorf("change: unknown kind %q", c.Kind)
	}
	return nil
}
