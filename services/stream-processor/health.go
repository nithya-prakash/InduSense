package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startHealthServer(port string, dedup *deduplicator, influx *influxSink) {
	mux := http.NewServeMux()

	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		redisOK := dedup.ping(ctx) == nil
		influxState := influx.breakerState()
		ready := redisOK && influxState != "OPEN"

		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready":                    ready,
			"redis_connected":          redisOK,
			"influxdb_circuit_breaker": influxState,
		})
	})

	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		_ = http.ListenAndServe(":"+port, mux) //nolint:gosec // internal-only health/metrics endpoint
	}()
}
