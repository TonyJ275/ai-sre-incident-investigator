package demo

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/TonyJ275/ai-sre-incident-investigator/internal/models"
)

// fakeAppender records what was appended instead of writing a file.
type fakeAppender struct {
	mu  sync.Mutex
	got []models.Change
	err error
}

func (f *fakeAppender) Append(v any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, v.(models.Change))
	return nil
}

func validChange() models.Change {
	return models.Change{
		Service: "checkout-api", Kind: models.ChangeKindConfig,
		Target: "db.pool.max_conns", OldValue: "50", NewValue: "5",
	}
}

func TestRecordAssignsIDAndUTCTime(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	now := time.Date(2026, 10, 9, 20, 0, 0, 0, ist) // 14:30 UTC
	out := &fakeAppender{}
	log := NewChangeLog(out, func() time.Time { return now })

	got, err := log.Record(validChange())
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if got.ID != "chg-001" {
		t.Errorf("id = %q, want chg-001", got.ID)
	}
	if want := time.Date(2026, 10, 9, 14, 30, 0, 0, time.UTC); !got.Time.Equal(want) || got.Time.Location() != time.UTC {
		t.Errorf("time = %v, want %v in UTC", got.Time, want)
	}
	if len(out.got) != 1 || out.got[0].ID != "chg-001" {
		t.Errorf("appended = %+v, want the same change that was returned", out.got)
	}
}

func TestRecordKeepsExplicitTimeAndIncrementsIDs(t *testing.T) {
	out := &fakeAppender{}
	log := NewChangeLog(out, func() time.Time { return time.Now() })

	explicit := time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)
	c := validChange()
	c.Time = explicit
	first, _ := log.Record(c)
	second, _ := log.Record(validChange())

	if !first.Time.Equal(explicit) {
		t.Errorf("explicit time was overwritten: %v", first.Time)
	}
	if first.ID != "chg-001" || second.ID != "chg-002" {
		t.Errorf("ids = %q, %q; want chg-001, chg-002", first.ID, second.ID)
	}
}

func TestRecordRejectsInvalidChangeWithoutWriting(t *testing.T) {
	out := &fakeAppender{}
	log := NewChangeLog(out, nil)

	c := validChange()
	c.Target = ""
	if _, err := log.Record(c); err == nil {
		t.Fatal("want a validation error, got nil")
	}
	if len(out.got) != 0 {
		t.Errorf("invalid change was written: %+v", out.got)
	}
}

func TestRecordPropagatesWriteError(t *testing.T) {
	boom := errors.New("disk full")
	log := NewChangeLog(&fakeAppender{err: boom}, nil)

	if _, err := log.Record(validChange()); !errors.Is(err, boom) {
		t.Errorf("err = %v, want it to wrap %v", err, boom)
	}
}

func TestRecordIDsAreUniqueUnderConcurrency(t *testing.T) {
	out := &fakeAppender{}
	log := NewChangeLog(out, nil)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := log.Record(validChange()); err != nil {
				t.Errorf("Record: %v", err)
			}
		}()
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, c := range out.got {
		if seen[c.ID] {
			t.Fatalf("duplicate id %s", c.ID)
		}
		seen[c.ID] = true
	}
	if len(seen) != 50 {
		t.Errorf("got %d unique ids, want 50", len(seen))
	}
}
