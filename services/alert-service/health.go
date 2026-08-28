package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// startHealthServer exposes /live (process is up — never fails on a
// dependency), /ready (this instance can currently do useful work — every
// alert this service creates is a Postgres write, so a Postgres outage
// means it genuinely isn't ready), and /metrics.
func startHealthServer(port string, pool *pgxpool.Pool) {
	mux := http.NewServeMux()

	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		postgresOK := pool.Ping(ctx) == nil

		w.Header().Set("Content-Type", "application/json")
		if !postgresOK {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ready":              postgresOK,
			"postgres_connected": postgresOK,
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
			log.Printf("alert-service: health/metrics server on :%s stopped: %v", port, err)
		}
	}()
}
