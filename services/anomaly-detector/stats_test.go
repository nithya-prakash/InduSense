package main

import (
	"math/rand"
	"testing"
)

func TestEWMATrackerDoesNotFireOnStableSeries(t *testing.T) {
	tracker := newEWMATracker(0.1)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		z, n := tracker.update(50 + rng.NormFloat64()*0.5)
		if fired, _, _ := statCheck(z, n, 30, 3.0); fired {
			t.Fatalf("stable series should not fire at sample %d (z=%v)", i, z)
		}
	}
}

func TestEWMATrackerFiresOnSpike(t *testing.T) {
	tracker := newEWMATracker(0.1)
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 60; i++ {
		tracker.update(50 + rng.NormFloat64()*0.5)
	}
	// A single large spike, judged against the now-stable baseline.
	z, n := tracker.update(200)
	fired, severity, score := statCheck(z, n, 30, 3.0)
	if !fired {
		t.Fatalf("expected a 150-unit spike against a tight baseline to fire, z=%v", z)
	}
	if severity == "" || score <= 0 {
		t.Errorf("expected non-empty severity and positive score, got severity=%q score=%v", severity, score)
	}
}

func TestStatCheckSuppressedBeforeMinSamples(t *testing.T) {
	fired, _, _ := statCheck(10.0, 5, 30, 3.0)
	if fired {
		t.Fatal("expected statCheck to suppress firing before minSamples is reached, regardless of z-score")
	}
}

func TestStatisticalTrackersIsolatesSeriesByDeviceAndMetric(t *testing.T) {
	trackers := newStatisticalTrackers(0.1)
	for i := 0; i < 60; i++ {
		trackers.update("device-a", "temperature", 50)
		trackers.update("device-b", "temperature", 5000) // wildly different baseline
	}
	zA, _ := trackers.update("device-a", "temperature", 51)
	zB, _ := trackers.update("device-b", "temperature", 5001)

	if zA > 3 || zB > 3 {
		t.Errorf("small in-baseline moves should not produce large z-scores: zA=%v zB=%v", zA, zB)
	}
}
