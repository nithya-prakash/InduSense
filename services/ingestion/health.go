package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// startHealthServer exposes /live (process is up — never fails on a
// dependency), /ready (this instance can currently do useful work), and
// /metrics (Prometheus scrape endpoint). Liveness deliberately does not
// depend on MQTT/Kafka health: a transient broker outage should not cause
// an orchestrator to kill and restart a perfectly healthy process.
func startHealthServer(port string, mqttConnected *atomic.Bool, sink *kafkaSink) {
	mux := http.NewServeMux()

	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		state := sink.breakerState()
		ready := mqttConnected.Load() && state != "OPEN"

		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready":                 ready,
			"mqtt_connected":        mqttConnected.Load(),
			"kafka_circuit_breaker": state,
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mqtt_connected":        mqttConnected.Load(),
			"kafka_circuit_breaker": sink.breakerState(),
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
			log.Printf("ingestion: health/metrics server on :%s stopped: %v", port, err)
		}
	}()
}
