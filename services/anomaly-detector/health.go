package main

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startHealthServer(port string, registry *forestRegistry) {
	mux := http.NewServeMux()

	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": true})
	})

	mux.HandleFunc("/forests", func(w http.ResponseWriter, r *http.Request) {
		registry.mu.RLock()
		defer registry.mu.RUnlock()
		types := make([]string, 0, len(registry.forests))
		for mt := range registry.forests {
			types = append(types, mt)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"trained_machine_types": types})
	})

	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		_ = http.ListenAndServe(":"+port, mux) //nolint:gosec // internal-only health/metrics endpoint
	}()
}
