package main

import "sync"

// featureStore tracks the most recently seen value of each metric per
// device (for assembling a device's multivariate feature vector from
// asynchronous single-metric telemetry events — "last known value" per
// metric, updated as readings arrive) and a rolling per-machine-type buffer
// of complete feature vectors used to (re)train that machine type's
// isolation forest.
type featureStore struct {
	mu           sync.Mutex
	latest       map[string]map[string]float64 // device_id -> metric -> value
	trainingData map[string][][]float64        // machine_type -> feature vectors
	bufferSize   int
}

func newFeatureStore(bufferSize int) *featureStore {
	return &featureStore{
		latest:       make(map[string]map[string]float64),
		trainingData: make(map[string][][]float64),
		bufferSize:   bufferSize,
	}
}

// observe records a new reading and, if the device now has a value for
// every metric in its machine type's feature order, returns the assembled
// feature vector (ok=true) and appends it to that machine type's training
// buffer.
func (fs *featureStore) observe(deviceID, machineType, metric string, value float64, featureOrder []string) (vector []float64, ok bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	values, exists := fs.latest[deviceID]
	if !exists {
		values = make(map[string]float64)
		fs.latest[deviceID] = values
	}
	values[metric] = value

	if len(featureOrder) == 0 {
		return nil, false
	}
	vec := make([]float64, len(featureOrder))
	for i, m := range featureOrder {
		v, seen := values[m]
		if !seen {
			return nil, false // still waiting on at least one metric for this device
		}
		vec[i] = v
	}

	buf := fs.trainingData[machineType]
	buf = append(buf, vec)
	if len(buf) > fs.bufferSize {
		buf = buf[len(buf)-fs.bufferSize:]
	}
	fs.trainingData[machineType] = buf

	return vec, true
}

// trainingSnapshot returns a copy of the current training buffer for a
// machine type, safe to hand to FitIsolationForest without holding the lock
// during training.
func (fs *featureStore) trainingSnapshot(machineType string) [][]float64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	buf := fs.trainingData[machineType]
	out := make([][]float64, len(buf))
	copy(out, buf)
	return out
}

// machineTypesWithData returns every machine type that currently has at
// least one training sample, for the periodic retrain loop to iterate over.
func (fs *featureStore) machineTypesWithData() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]string, 0, len(fs.trainingData))
	for mt := range fs.trainingData {
		out = append(out, mt)
	}
	return out
}
