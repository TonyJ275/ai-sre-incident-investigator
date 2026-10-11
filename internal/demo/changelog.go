package demo

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

// Appender is anything that can persist one record as a JSON line.
// A file writer satisfies it in production; tests use an in-memory fake.
type Appender interface {
	Append(v any) error
}

// ChangeLog validates changes, stamps them with an ID and a UTC time, and
// appends them to its output. It is safe for concurrent use.
type ChangeLog struct {
	out Appender
	now func() time.Time
	seq atomic.Int64
}

// NewChangeLog uses time.Now when now is nil. The clock is injected so tests
// (and later, replayed scenarios) control time.
func NewChangeLog(out Appender, now func() time.Time) *ChangeLog {
	if now == nil {
		now = time.Now
	}
	return &ChangeLog{out: out, now: now}
}

// Record stores a change and returns it as written. The ID is always assigned
// here. The time is filled in only if the caller left it zero, which lets
// scenarios record changes at chosen moments.
func (l *ChangeLog) Record(c models.Change) (models.Change, error) {
	if err := c.Validate(); err != nil {
		return models.Change{}, err
	}
	if c.Time.IsZero() {
		c.Time = l.now()
	}
	c.Time = c.Time.UTC()
	c.ID = fmt.Sprintf("chg-%03d", l.seq.Add(1))

	if err := l.out.Append(c); err != nil {
		return models.Change{}, fmt.Errorf("record change %s: %w", c.ID, err)
	}
	return c, nil
}
