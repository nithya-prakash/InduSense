package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

// handleLive always returns 200 while the process is up — liveness must
// never fail just because a downstream dependency (Postgres, Kafka, ...) is
// temporarily unavailable, or an orchestrator would kill and restart a
// perfectly healthy process for a problem restarting it can't fix.
func handleLive() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

type depStatus struct {
	Postgres string `json:"postgres"`
	Redis    string `json:"redis"`
	Kafka    string `json:"kafka"`
	MQTT     string `json:"mqtt"`
	InfluxDB string `json:"influxdb"`
}

func checkDependencies(cfg Config, pool *pgxpool.Pool, redisClient *redis.Client) depStatus {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	status := depStatus{Postgres: "ok", Redis: "ok", Kafka: "ok", MQTT: "ok", InfluxDB: "ok"}

	if err := pool.Ping(ctx); err != nil {
		status.Postgres = "unreachable: " + err.Error()
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		status.Redis = "unreachable: " + err.Error()
	}

	conn, err := kafka.DialContext(ctx, "tcp", cfg.KafkaBrokers[0])
	if err != nil {
		status.Kafka = "unreachable: " + err.Error()
	} else {
		_ = conn.Close()
	}

	opts := mqtt.NewClientOptions().AddBroker(cfg.MQTTBrokerURL).SetConnectTimeout(2 * time.Second)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(2*time.Second) || token.Error() != nil {
		status.MQTT = "unreachable"
	} else {
		client.Disconnect(100)
	}

	influxClient := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	defer influxClient.Close()
	if _, err := influxClient.Ping(ctx); err != nil {
		status.InfluxDB = "unreachable: " + err.Error()
	}

	return status
}

// handleReady fails (503) if any dependency this instance actually needs
// to serve requests is unreachable — distinct from liveness, per spec.
func handleReady(cfg Config, pool *pgxpool.Pool, redisClient *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := checkDependencies(cfg, pool, redisClient)
		ready := status.Postgres == "ok" && status.Redis == "ok" && status.Kafka == "ok" && status.InfluxDB == "ok"
		// MQTT is deliberately excluded from readiness: the API doesn't
		// itself consume/publish MQTT, so its outage shouldn't take API
		// readiness down with it — it's still reported in /health though.

		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": ready, "dependencies": status})
	}
}

func handleHealth(cfg Config, pool *pgxpool.Pool, redisClient *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := checkDependencies(cfg, pool, redisClient)
		writeJSON(w, http.StatusOK, status)
	}
}
