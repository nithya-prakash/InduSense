package main

import (
	"math/rand"
	"testing"
)

func TestSensorGeneratorStaysWithinRangeWithoutAnomalies(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	gen := newSensorGenerator(rng, 20, 90, 0 /* anomalyRate */)

	for i := 0; i < 10000; i++ {
		value, isAnomaly := gen.next()
		if isAnomaly {
			t.Fatalf("anomalyRate=0 but sample %d was flagged anomalous", i)
		}
		// Noise can push slightly outside [min,max]; only the baseline itself
		// is clamped. Assert against a generous envelope instead.
		if value < 0 || value > 110 {
			t.Fatalf("sample %d = %f is wildly outside operating range [20,90]", i, value)
		}
	}
}

func TestSensorGeneratorAnomalyRateProducesSpikes(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	gen := newSensorGenerator(rng, 20, 90, 1.0 /* always anomalous */)

	_, isAnomaly := gen.next()
	if !isAnomaly {
		t.Fatal("anomalyRate=1.0 should always flag the sample as anomalous")
	}
}

func TestClamp(t *testing.T) {
	cases := []struct{ v, min, max, want float64 }{
		{5, 0, 10, 5},
		{-5, 0, 10, 0},
		{15, 0, 10, 10},
	}
	for _, c := range cases {
		if got := clamp(c.v, c.min, c.max); got != c.want {
			t.Errorf("clamp(%v, %v, %v) = %v, want %v", c.v, c.min, c.max, got, c.want)
		}
	}
}
