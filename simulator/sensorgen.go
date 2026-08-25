package main

import (
	"math"
	"math/rand"
)

// sensorGenerator produces successive readings for one sensor: a baseline
// that drifts slowly within the sensor's operating range (a bounded random
// walk that reverts toward the midpoint), plus per-sample gaussian noise and
// occasional anomalous spikes.
type sensorGenerator struct {
	min, max    float64
	mid         float64
	baseline    float64
	driftStep   float64
	rng         *rand.Rand
	anomalyRate float64
}

func newSensorGenerator(rng *rand.Rand, min, max, anomalyRate float64) *sensorGenerator {
	mid := (min + max) / 2
	return &sensorGenerator{
		min:         min,
		max:         max,
		mid:         mid,
		baseline:    mid,
		rng:         rng,
		anomalyRate: anomalyRate,
	}
}

const (
	driftMaxStep  = 0.01 // fraction of range the baseline may move per tick
	reversionPull = 0.05 // fraction of distance-to-midpoint pulled back per tick
	noiseFraction = 0.01 // gaussian noise stddev as a fraction of range
	spikeMinFrac  = 0.3  // anomaly spike magnitude, as a fraction of range
	spikeMaxFrac  = 0.9
)

// next advances the internal drift state and returns the next reading along
// with whether this sample was injected as an anomalous spike.
func (g *sensorGenerator) next() (value float64, isAnomaly bool) {
	rangeSpan := g.max - g.min

	// Gradual drift: small random step, pulled back toward the midpoint so
	// the baseline doesn't wander out of the operating range over time.
	g.driftStep = g.driftStep + (g.rng.Float64()*2-1)*driftMaxStep*rangeSpan
	pull := (g.mid - g.baseline) * reversionPull
	g.baseline += g.driftStep*0.1 + pull
	g.baseline = clamp(g.baseline, g.min, g.max)

	noise := g.rng.NormFloat64() * noiseFraction * rangeSpan
	value = g.baseline + noise

	if g.rng.Float64() < g.anomalyRate {
		magnitude := (spikeMinFrac + g.rng.Float64()*(spikeMaxFrac-spikeMinFrac)) * rangeSpan
		sign := 1.0
		if g.rng.Float64() < 0.5 {
			sign = -1.0
		}
		value += sign * magnitude
		isAnomaly = true
	}

	return math.Round(value*100) / 100, isAnomaly
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
