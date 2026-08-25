package main

import (
	"math"
	"sync"

	"github.com/nithya-prakash/indusense/pkg/events"
)

// ewmaTracker maintains an exponentially-weighted moving mean and variance
// for one (device, metric) series, updated online in O(1) per sample. EWMA
// (rather than a plain cumulative average) is used deliberately so the
// baseline adapts to a genuine regime change (e.g. a machine settling into a
// new normal after maintenance) instead of being permanently anchored to
// whatever the series looked like at startup.
type ewmaTracker struct {
	alpha       float64
	mean        float64
	variance    float64
	sampleCount int
	initialized bool
}

func newEWMATracker(alpha float64) *ewmaTracker {
	return &ewmaTracker{alpha: alpha}
}

// update feeds one new sample and returns the z-score of that sample
// against the mean/stddev *before* this sample was folded in (so a single
// huge spike is judged against the prior baseline, not a baseline it just
// dragged toward itself).
func (t *ewmaTracker) update(value float64) (zScore float64, sampleCount int) {
	if !t.initialized {
		t.mean = value
		t.variance = 0
		t.initialized = true
		t.sampleCount = 1
		return 0, t.sampleCount
	}

	stddev := math.Sqrt(t.variance)
	if stddev > 0 {
		zScore = (value - t.mean) / stddev
	}

	delta := value - t.mean
	t.mean += t.alpha * delta
	t.variance = (1 - t.alpha) * (t.variance + t.alpha*delta*delta)

	t.sampleCount++
	return zScore, t.sampleCount
}

// statisticalTrackers holds one ewmaTracker per (device_id, metric) series.
type statisticalTrackers struct {
	mu       sync.Mutex
	trackers map[string]*ewmaTracker
	alpha    float64
}

func newStatisticalTrackers(alpha float64) *statisticalTrackers {
	return &statisticalTrackers{trackers: make(map[string]*ewmaTracker), alpha: alpha}
}

func (s *statisticalTrackers) update(deviceID, metric string, value float64) (zScore float64, sampleCount int) {
	key := deviceID + "|" + metric
	s.mu.Lock()
	t, ok := s.trackers[key]
	if !ok {
		t = newEWMATracker(s.alpha)
		s.trackers[key] = t
	}
	s.mu.Unlock()
	return t.update(value)
}

// statCheck flags a sample whose z-score against its series' EWMA baseline
// exceeds threshold, but only once enough samples have accumulated that the
// baseline itself is meaningful — otherwise every series' first few dozen
// readings would trivially "deviate" from an unstable baseline.
func statCheck(zScore float64, sampleCount int, minSamples int, threshold float64) (fired bool, severity string, score float64) {
	if sampleCount < minSamples {
		return false, "", 0
	}
	absZ := math.Abs(zScore)
	if absZ < threshold {
		return false, "", 0
	}

	score = clamp01(absZ / (threshold * 2))
	switch {
	case absZ >= threshold*1.6:
		severity = events.SeverityCritical
	case absZ >= threshold*1.3:
		severity = events.SeverityHigh
	default:
		severity = events.SeverityWarning
	}
	return true, severity, score
}
