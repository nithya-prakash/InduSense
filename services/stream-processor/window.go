package main

import (
	"math"
	"sync"
	"time"
)

type sample struct {
	at    time.Time
	value float64
}

// windowStats summarizes a single (device, metric) series over one window
// duration: moving average, moving standard deviation, min, max, and rate of
// change (last-first value over the window's elapsed time) — the same shape
// covers "vibration trend" or "energy consumption rate" from the spec; those
// are just rate_of_change applied to the vibration/power metric respectively,
// not separately named computations.
type windowStats struct {
	Count        int
	MovingAvg    float64
	MovingStdDev float64
	Min          float64
	Max          float64
	RateOfChange float64
}

// seriesBuffer is a per-(device_id,metric) ring of recent samples, trimmed to
// the longest configured window on every insert so memory stays bounded
// regardless of how long the process runs.
type seriesBuffer struct {
	mu        sync.Mutex
	samples   []sample
	maxWindow time.Duration
}

func newSeriesBuffer(maxWindow time.Duration) *seriesBuffer {
	return &seriesBuffer{maxWindow: maxWindow}
}

func (s *seriesBuffer) add(at time.Time, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, sample{at: at, value: value})
	cutoff := at.Add(-s.maxWindow)
	i := 0
	for i < len(s.samples) && s.samples[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		s.samples = s.samples[i:]
	}
}

// statsFor computes windowStats over the trailing `window` duration, as of
// `now`. Returns ok=false if there are no samples in that window.
func (s *seriesBuffer) statsFor(now time.Time, window time.Duration) (windowStats, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := now.Add(-window)
	var inWindow []sample
	for _, sm := range s.samples {
		if !sm.at.Before(cutoff) {
			inWindow = append(inWindow, sm)
		}
	}
	if len(inWindow) == 0 {
		return windowStats{}, false
	}
	return computeStats(inWindow), true
}

func computeStats(samples []sample) windowStats {
	n := len(samples)
	sum, min, max := 0.0, samples[0].value, samples[0].value
	for _, sm := range samples {
		sum += sm.value
		if sm.value < min {
			min = sm.value
		}
		if sm.value > max {
			max = sm.value
		}
	}
	avg := sum / float64(n)

	variance := 0.0
	for _, sm := range samples {
		d := sm.value - avg
		variance += d * d
	}
	variance /= float64(n)
	stddev := math.Sqrt(variance)

	rateOfChange := 0.0
	if n > 1 {
		elapsed := samples[n-1].at.Sub(samples[0].at).Seconds()
		if elapsed > 0 {
			rateOfChange = (samples[n-1].value - samples[0].value) / elapsed
		}
	}

	return windowStats{
		Count:        n,
		MovingAvg:    avg,
		MovingStdDev: stddev,
		Min:          min,
		Max:          max,
		RateOfChange: rateOfChange,
	}
}
