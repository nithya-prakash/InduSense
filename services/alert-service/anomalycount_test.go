package main

import (
	"testing"
	"time"
)

func TestAnomalyCountTrackerCountsWithinWindow(t *testing.T) {
	tr := newAnomalyCountTracker()
	base := time.Now()

	if n := tr.record("k", base, 5*time.Minute); n != 1 {
		t.Fatalf("expected count 1, got %d", n)
	}
	if n := tr.record("k", base.Add(1*time.Minute), 5*time.Minute); n != 2 {
		t.Fatalf("expected count 2, got %d", n)
	}
	if n := tr.record("k", base.Add(2*time.Minute), 5*time.Minute); n != 3 {
		t.Fatalf("expected count 3, got %d", n)
	}
}

func TestAnomalyCountTrackerExpiresOldEntries(t *testing.T) {
	tr := newAnomalyCountTracker()
	base := time.Now()

	tr.record("k", base, 5*time.Minute)
	tr.record("k", base.Add(1*time.Minute), 5*time.Minute)
	// This occurrence is 10 minutes after the first two, well past the
	// 5-minute window, so they should no longer count.
	n := tr.record("k", base.Add(10*time.Minute), 5*time.Minute)
	if n != 1 {
		t.Fatalf("expected old entries to have expired, count = %d, want 1", n)
	}
}

func TestAnomalyCountTrackerIsolatesKeys(t *testing.T) {
	tr := newAnomalyCountTracker()
	now := time.Now()
	tr.record("a", now, time.Minute)
	tr.record("a", now, time.Minute)
	n := tr.record("b", now, time.Minute)
	if n != 1 {
		t.Fatalf("expected key %q to be tracked independently, got count %d", "b", n)
	}
}

func TestNextSeverityLadder(t *testing.T) {
	cases := []struct{ from, want string }{
		{"WARNING", "HIGH"},
		{"HIGH", "CRITICAL"},
		{"CRITICAL", "CRITICAL"}, // already at the top
		{"UNKNOWN", "UNKNOWN"},   // not on the ladder at all
	}
	for _, c := range cases {
		if got := nextSeverity(c.from); got != c.want {
			t.Errorf("nextSeverity(%q) = %q, want %q", c.from, got, c.want)
		}
	}
}
