package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// forestRegistry holds the current isolation forest for each machine type
// and periodically retrains them from the featureStore's rolling buffers. A
// machine type has no forest at all until its buffer has accumulated a
// reasonable minimum of samples — before that, level 3 detection simply
// doesn't fire for that machine type yet (levels 1 and 2 still do).
type forestRegistry struct {
	mu      sync.RWMutex
	forests map[string]*IsolationForest
}

func newForestRegistry() *forestRegistry {
	return &forestRegistry{forests: make(map[string]*IsolationForest)}
}

func (r *forestRegistry) get(machineType string) *IsolationForest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.forests[machineType]
}

func (r *forestRegistry) set(machineType string, f *IsolationForest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forests[machineType] = f
}

const minTrainingSamples = 60

func runForestTrainer(ctx context.Context, cfg Config, fs *featureStore, registry *forestRegistry) {
	ticker := time.NewTicker(cfg.ForestRetrainEvery)
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, machineType := range fs.machineTypesWithData() {
				data := fs.trainingSnapshot(machineType)
				if len(data) < minTrainingSamples {
					continue
				}
				forest := FitIsolationForest(data, cfg.ForestNumTrees, cfg.ForestSubsampleSize, rng)
				registry.set(machineType, forest)
				metricForestsTrained.Inc()
				log.Printf("anomaly-detector: retrained isolation forest for machine_type=%s on %d samples", machineType, len(data))
			}
		}
	}
}
