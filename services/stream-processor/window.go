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

// add inserts by event timestamp, not arrival order. Telemetry can arrive
// out of order (network delay, MQTT/Kafka redelivery — the simulator's
// OUT_OF_ORDER_RATE exercises this deliberately), and a pre-GitHub audit
// found this method simply appending regardless: with an out-of-order
// arrival, s.samples stops being sorted by .at, which corrupts two things
// that assume it is — the trim loop below (it only scans from the front,
// so a stale sample stuck behind a later-timestamped-but-earlier-arrived
// one never gets dropped) and computeStats's rate-of-change, which reads
// samples[0]/samples[n-1] as "chronologically first/last in the window"
// (an out-of-order append could make the "elapsed" denominator negative,
// or simply compute a rate between the wrong two readings).
func (s *seriesBuffer) add(at time.Time, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	i := len(s.samples)
	for i > 0 && s.samples[i-1].at.After(at) {
		i--
	}
	s.samples = append(s.samples, sample{})
	copy(s.samples[i+1:], s.samples[i:])
	s.samples[i] = sample{at: at, value: value}

	// Trim against the newest timestamp actually in the buffer (not this
	// call's `at`, which — for an out-of-order arrival — could be the
	// oldest thing in it and would under-trim).
	cutoff := s.samples[len(s.samples)-1].at.Add(-s.maxWindow)
	j := 0
	for j < len(s.samples) && s.samples[j].at.Before(cutoff) {
		j++
	}
	if j > 0 {
		s.samples = s.samples[j:]
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
