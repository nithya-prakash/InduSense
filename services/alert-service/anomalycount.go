package main

import (
	"sync"
	"time"
)

// anomalyCountTracker maintains, per (rule_id, scope), a trimmed list of
// recent anomaly timestamps so ANOMALY_COUNT rules ("three anomalies within
// five minutes") can be evaluated without querying Postgres on every event.
type anomalyCountTracker struct {
	mu   sync.Mutex
	seen map[string][]time.Time
}

func newAnomalyCountTracker() *anomalyCountTracker {
	return &anomalyCountTracker{seen: make(map[string][]time.Time)}
}

// record adds one occurrence at `at` for the given key and returns how many
// occurrences remain within `window` of `at` after trimming older ones.
func (t *anomalyCountTracker) record(key string, at time.Time, window time.Duration) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	times := append(t.seen[key], at)
	cutoff := at.Add(-window)
	i := 0
	for i < len(times) && times[i].Before(cutoff) {
		i++
	}
	times = times[i:]
	t.seen[key] = times
	return len(times)
}
