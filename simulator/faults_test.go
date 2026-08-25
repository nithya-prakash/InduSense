package main

import (
	"math/rand"
	"testing"
)

func TestDecideFaultsRatesApproximateConfiguredProbability(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cfg := Config{
		DuplicateRate:    0.10,
		NetworkDelayRate: 0.10,
		OutOfOrderRate:   0.10,
	}

	const trials = 20000
	var duplicates, delayed, outOfOrder int
	for i := 0; i < trials; i++ {
		d := decideFaults(rng, cfg)
		if d.Duplicate {
			duplicates++
		}
		if d.Delayed {
			delayed++
		}
		if d.OutOfOrder {
			outOfOrder++
		}
	}

	assertApprox(t, "duplicate", float64(duplicates)/trials, cfg.DuplicateRate, 0.02)
	assertApprox(t, "out_of_order", float64(outOfOrder)/trials, cfg.OutOfOrderRate, 0.02)
	// delayed is the union of NetworkDelayRate and OutOfOrderRate triggers, so
	// it should be at least the network delay rate and no more than the sum.
	got := float64(delayed) / trials
	if got < cfg.NetworkDelayRate-0.02 || got > cfg.NetworkDelayRate+cfg.OutOfOrderRate+0.02 {
		t.Errorf("delayed rate %.4f outside expected band [%.4f, %.4f]",
			got, cfg.NetworkDelayRate-0.02, cfg.NetworkDelayRate+cfg.OutOfOrderRate+0.02)
	}
}

func TestOutOfOrderAlwaysImpliesDelayed(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	cfg := Config{OutOfOrderRate: 1.0}
	for i := 0; i < 100; i++ {
		d := decideFaults(rng, cfg)
		if d.OutOfOrder && !d.Delayed {
			t.Fatal("out-of-order sample must also be marked delayed")
		}
		if d.OutOfOrder && d.DelayFor <= 0 {
			t.Fatal("out-of-order sample must have a positive delay")
		}
	}
}

func TestSensorShouldFailRecoversFromFailure(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	recovered := false
	for i := 0; i < 200; i++ {
		if !sensorShouldFail(rng, true, 0) {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Fatal("a failed sensor should eventually recover within 200 ticks (~10% recovery chance/tick)")
	}
}

func assertApprox(t *testing.T, label string, got, want, tolerance float64) {
	t.Helper()
	if got < want-tolerance || got > want+tolerance {
		t.Errorf("%s rate = %.4f, want ~%.4f (+/- %.4f)", label, got, want, tolerance)
	}
}
