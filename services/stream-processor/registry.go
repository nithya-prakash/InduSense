package main

import (
	"sync"
	"time"
)

// seriesKey identifies one (device, metric) time series and carries the tag
// metadata InfluxDB needs — captured once when the series is first seen,
// since it doesn't change across samples for the same device+metric.
type seriesKey struct {
	FactoryID        string
	ProductionLineID string
	MachineID        string
	DeviceID         string
	SensorID         string
	Metric           string
}

func (k seriesKey) id() string { return k.DeviceID + "|" + k.Metric }

// seriesRegistry tracks one seriesBuffer per (device, metric) pair seen so
// far. Bounded by the number of distinct sensors in the system (1000 in this
// deployment) — it does not grow with message volume.
type seriesRegistry struct {
	mu        sync.Mutex
	buffers   map[string]*seriesBuffer
	keys      map[string]seriesKey
	maxWindow time.Duration
}

func newSeriesRegistry(maxWindow time.Duration) *seriesRegistry {
	return &seriesRegistry{
		buffers:   make(map[string]*seriesBuffer),
		keys:      make(map[string]seriesKey),
		maxWindow: maxWindow,
	}
}

func (r *seriesRegistry) record(key seriesKey, at time.Time, value float64) {
	id := key.id()

	r.mu.Lock()
	buf, ok := r.buffers[id]
	if !ok {
		buf = newSeriesBuffer(r.maxWindow)
		r.buffers[id] = buf
		r.keys[id] = key
	}
	r.mu.Unlock()

	buf.add(at, value)
}

// snapshot returns every tracked series' key and buffer, for a periodic
// flush to iterate without holding the registry lock during I/O.
func (r *seriesRegistry) snapshot() []struct {
	Key seriesKey
	Buf *seriesBuffer
} {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]struct {
		Key seriesKey
		Buf *seriesBuffer
	}, 0, len(r.buffers))
	for id, buf := range r.buffers {
		out = append(out, struct {
			Key seriesKey
			Buf *seriesBuffer
		}{Key: r.keys[id], Buf: buf})
	}
	return out
}
