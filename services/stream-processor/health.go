package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startHealthServer(port string, dedup *deduplicator, influx *influxSink, kio *kafkaIO) {
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
		kafkaState := kio.breakerState()
		ready := redisOK && influxState != "OPEN" && kafkaState != "OPEN"

		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready":                    ready,
			"redis_connected":          redisOK,
			"influxdb_circuit_breaker": influxState,
			"kafka_circuit_breaker":    kafkaState,
		})
	})

	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		// ListenAndServe only returns once the listener stops; nothing in
		// this service ever calls Shutdown/Close on it, so any return here
		// is unexpected (most likely a bind failure, e.g. the port already
		// in use) and worth surfacing loudly rather than leaving /live and
		// /ready silently unreachable for the rest of the process's life.
		if err := http.ListenAndServe(":"+port, mux); err != nil { //nolint:gosec // internal-only health/metrics endpoint
			log.Printf("stream-processor: health/metrics server on :%s stopped: %v", port, err)
		}
	}()
}
