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
