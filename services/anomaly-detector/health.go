package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// startHealthServer exposes /live (process is up — never fails on a
// dependency), /ready (this instance can currently do useful work — the
// device/sensor catalog it needs for detection is Postgres-backed, so a
// Postgres outage means it genuinely isn't ready, and an open Kafka
// breaker means it can detect but not actually publish results), /forests,
// and /metrics.
func startHealthServer(port string, cat *catalog, kio *kafkaIO, registry *forestRegistry) {
	mux := http.NewServeMux()

	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		postgresOK := cat.ping(ctx) == nil
		breakerState := kio.breakerState()
		ready := postgresOK && breakerState != "OPEN"

		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready":                 ready,
			"postgres_connected":    postgresOK,
			"kafka_circuit_breaker": breakerState,
		})
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
		// ListenAndServe only returns once the listener stops; nothing in
		// this service ever calls Shutdown/Close on it, so any return here
		// is unexpected (most likely a bind failure, e.g. the port already
		// in use) and worth surfacing loudly rather than leaving /live and
		// /ready silently unreachable for the rest of the process's life.
		if err := http.ListenAndServe(":"+port, mux); err != nil { //nolint:gosec // internal-only health/metrics endpoint
			log.Printf("anomaly-detector: health/metrics server on :%s stopped: %v", port, err)
		}
	}()
}
