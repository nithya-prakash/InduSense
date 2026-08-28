package main

import (
	"math"
	"testing"
	"time"
)

func TestSeriesBufferStatsForEmptyReturnsNotOK(t *testing.T) {
	buf := newSeriesBuffer(time.Minute)
	_, ok := buf.statsFor(time.Now(), 10*time.Second)
	if ok {
		t.Fatal("expected ok=false for an empty buffer")
	}
}

func TestSeriesBufferComputesBasicStats(t *testing.T) {
	buf := newSeriesBuffer(time.Minute)
	base := time.Now()
	buf.add(base, 10)
	buf.add(base.Add(1*time.Second), 20)
	buf.add(base.Add(2*time.Second), 30)

	stats, ok := buf.statsFor(base.Add(2*time.Second), 10*time.Second)
	if !ok {
		t.Fatal("expected stats to be available")
	}
	if stats.Count != 3 {
		t.Errorf("Count = %d, want 3", stats.Count)
	}
	if stats.MovingAvg != 20 {
		t.Errorf("MovingAvg = %v, want 20", stats.MovingAvg)
	}
	if stats.Min != 10 || stats.Max != 30 {
		t.Errorf("Min/Max = %v/%v, want 10/30", stats.Min, stats.Max)
	}
	// rate of change: (30-10)/2s = 10/s
	if math.Abs(stats.RateOfChange-10) > 1e-9 {
		t.Errorf("RateOfChange = %v, want 10", stats.RateOfChange)
	}
	wantStdDev := math.Sqrt(((10.0*10.0)*2 + 0) / 3.0) // values deviate -10,0,+10 from mean 20
	if math.Abs(stats.MovingStdDev-wantStdDev) > 1e-9 {
		t.Errorf("MovingStdDev = %v, want %v", stats.MovingStdDev, wantStdDev)
	}
}

func TestSeriesBufferExcludesSamplesOutsideWindow(t *testing.T) {
	buf := newSeriesBuffer(time.Minute)
	base := time.Now()
	buf.add(base, 100) // outside the 5s window we'll query
	buf.add(base.Add(20*time.Second), 5)

	stats, ok := buf.statsFor(base.Add(20*time.Second), 5*time.Second)
	if !ok {
		t.Fatal("expected stats to be available")
	}
	if stats.Count != 1 {
		t.Fatalf("expected only the in-window sample to count, got Count=%d", stats.Count)
	}
	if stats.MovingAvg != 5 {
		t.Errorf("MovingAvg = %v, want 5 (the only in-window sample)", stats.MovingAvg)
	}
}

func TestSeriesBufferTrimsOldSamplesBeyondMaxWindow(t *testing.T) {
	buf := newSeriesBuffer(10 * time.Second)
	base := time.Now()
	buf.add(base, 1)
	buf.add(base.Add(20*time.Second), 2) // triggers trim of the first sample

	buf.mu.Lock()
	n := len(buf.samples)
	buf.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected old sample to be trimmed, buffer has %d samples", n)
	}
}

// TestSeriesBufferOutOfOrderArrival_RateOfChangeUsesEventTimeNotArrivalOrder
// reproduces the pre-GitHub audit finding directly: three readings whose
// event timestamps are strictly increasing (0s, 1s, 2s — the same as
// TestSeriesBufferComputesBasicStats, which asserts rate of change =
// (30-10)/2s = 10/s) are added out of arrival order — the middle reading
// arrives last, as network delay or Kafka redelivery could cause. Before
// the fix, add() simply appended, so samples ended up ordered [t=0, t=2,
// t=1] and computeStats' samples[0]/samples[n-1] read t=0 and t=1 instead
// of t=0 and t=2 — silently computing (20-10)/1s = 10/s here by
// coincidence, but genuinely wrong (and, for other orderings, capable of
// making the elapsed denominator negative). The fix keeps s.samples sorted
// by event time regardless of insertion order, so the result must match
// the in-order case exactly.
func TestSeriesBufferOutOfOrderArrival_RateOfChangeUsesEventTimeNotArrivalOrder(t *testing.T) {
	buf := newSeriesBuffer(time.Minute)
	base := time.Now()

	buf.add(base, 10)                    // t=0s, arrives 1st
	buf.add(base.Add(2*time.Second), 30) // t=2s, arrives 2nd (out of order: skips ahead of t=1s)
	buf.add(base.Add(1*time.Second), 20) // t=1s, arrives 3rd (late — this is the out-of-order one)

	stats, ok := buf.statsFor(base.Add(2*time.Second), 10*time.Second)
	if !ok {
		t.Fatal("expected stats to be available")
	}
	if stats.Count != 3 {
		t.Fatalf("Count = %d, want 3", stats.Count)
	}
	// Must match TestSeriesBufferComputesBasicStats exactly: (30-10)/2s = 10/s,
	// computed from the chronologically first (t=0, value=10) and last
	// (t=2s, value=30) samples — not whichever arrived first/last.
	if math.Abs(stats.RateOfChange-10) > 1e-9 {
		t.Errorf("RateOfChange = %v, want 10 (same as if the samples had arrived in order)", stats.RateOfChange)
	}
	if stats.Min != 10 || stats.Max != 30 {
		t.Errorf("Min/Max = %v/%v, want 10/30 (order-independent, sanity check)", stats.Min, stats.Max)
	}

	buf.mu.Lock()
	for i := 1; i < len(buf.samples); i++ {
		if buf.samples[i].at.Before(buf.samples[i-1].at) {
			t.Errorf("samples not sorted by event time after an out-of-order insert: [%d]=%v before [%d]=%v",
				i, buf.samples[i].at, i-1, buf.samples[i-1].at)
		}
	}
	buf.mu.Unlock()
}

// TestSeriesBufferOutOfOrderArrival_TrimUsesLatestEventTimeNotArrivalTime
// reproduces the second half of the same bug: before the fix, the trim
// cutoff in add() was computed from the just-inserted sample's own
// timestamp (`at.Add(-s.maxWindow)`). A late-arriving, old-timestamped
// sample would compute a stale (too-old) cutoff from its own timestamp —
// e.g. inserting t=1s would compute cutoff=1s-10s=-9s, so the trim loop
// (which only scans from the front) would stop immediately and leave
// everything in place, including samples that are genuinely expired
// relative to the buffer's actual newest data. The fix bases the cutoff on
// the newest event timestamp actually in the buffer after insertion, so a
// late, old sample can never suppress trimming of real, expired data.
func TestSeriesBufferOutOfOrderArrival_TrimUsesLatestEventTimeNotArrivalTime(t *testing.T) {
	buf := newSeriesBuffer(10 * time.Second)
	base := time.Now()

	buf.add(base, 1)                     // t=0s
	buf.add(base.Add(20*time.Second), 2) // t=20s: 20s newer, trims t=0s (outside the 10s maxWindow) — buffer is now just [t=20s]
	buf.add(base.Add(1*time.Second), 3)  // t=1s, arriving last (out of order, and itself already outside the window relative to t=20s)

	buf.mu.Lock()
	n := len(buf.samples)
	buf.mu.Unlock()
	// The late t=1s sample is itself more than maxWindow behind the
	// buffer's newest known time (t=20s), so it must be trimmed away on
	// the very insert that adds it — leaving only t=20s. The buggy
	// version would compute its cutoff from t=1s itself (cutoff=-9s) and
	// trim nothing, leaving 2 samples.
	if n != 1 {
		t.Fatalf("expected the stale t=1s sample to be trimmed immediately (its cutoff must be based on the buffer's newest known time t=20s, not its own old timestamp), got %d samples", n)
	}
}

func TestSeriesRegistryTracksSeparateSeriesIndependently(t *testing.T) {
	reg := newSeriesRegistry(time.Minute)
	now := time.Now()

	keyA := seriesKey{DeviceID: "device-a", Metric: "temperature"}
	keyB := seriesKey{DeviceID: "device-b", Metric: "temperature"}

	reg.record(keyA, now, 50)
	reg.record(keyB, now, 999)

	snap := reg.snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 tracked series, got %d", len(snap))
	}

	found := map[string]float64{}
	for _, s := range snap {
		stats, ok := s.Buf.statsFor(now, time.Minute)
		if !ok {
			t.Fatalf("expected stats for series %s", s.Key.id())
		}
		found[s.Key.DeviceID] = stats.MovingAvg
	}
	if found["device-a"] != 50 || found["device-b"] != 999 {
		t.Errorf("series values got mixed up: %+v", found)
	}
}
