package main

import (
	"math/rand"
	"time"
)

// faultDecision captures which fault-injection behaviors apply to a single
// generated sample, decided independently so they can combine (e.g. a
// delayed duplicate).
type faultDecision struct {
	Duplicate  bool
	OutOfOrder bool
	Delayed    bool
	DelayFor   time.Duration
}

func decideFaults(rng *rand.Rand, cfg Config) faultDecision {
	d := faultDecision{}
	if rng.Float64() < cfg.DuplicateRate {
		d.Duplicate = true
	}
	if rng.Float64() < cfg.NetworkDelayRate {
		d.Delayed = true
		// 100ms-5s jitter, representative of a congested network link.
		d.DelayFor = 100*time.Millisecond + time.Duration(rng.Int63n(int64(4900*time.Millisecond)))
	}
	if rng.Float64() < cfg.OutOfOrderRate {
		d.OutOfOrder = true
		if !d.Delayed {
			// Guarantee the delay is long enough that the *next* tick for
			// this sensor is published first, producing a genuine
			// out-of-order arrival rather than just jitter.
			d.Delayed = true
			d.DelayFor = 500 * time.Millisecond
		}
	}
	return d
}

// sensorShouldFail decides, once per tick, whether a healthy sensor
// transitions into a failed (non-reporting) state, and whether a failed
// sensor recovers this tick.
func sensorShouldFail(rng *rand.Rand, currentlyFailed bool, failureRate float64) bool {
	if currentlyFailed {
		// ~10% chance per tick to recover, i.e. average outage spans ~10 ticks.
		return rng.Float64() >= 0.10
	}
	return rng.Float64() < failureRate
}
