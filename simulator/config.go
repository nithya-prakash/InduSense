package main

import (
	"os"
	"strconv"
)

type Config struct {
	PostgresDSN       string
	PostgresMaxConns  int
	MQTTBrokerURL     string
	MQTTClientID      string
	MQTTQoS           byte
	SensorCount       int
	MessagesPerSec    int
	AnomalyRate       float64
	DuplicateRate     float64
	OutOfOrderRate    float64
	NetworkDelayRate  float64
	SensorFailureRate float64
	PublisherWorkers  int
	QueueCapacity     int
}

func loadConfig() Config {
	return Config{
		PostgresDSN:       envStr("SIM_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),
		PostgresMaxConns:  envInt("SIM_POSTGRES_MAX_CONNS", 4),
		MQTTBrokerURL:     envStr("SIM_MQTT_BROKER_URL", "tcp://localhost:1883"),
		MQTTClientID:      envStr("SIM_MQTT_CLIENT_ID", "indusense-simulator"),
		MQTTQoS:           byte(envInt("MQTT_QOS", 1)),
		SensorCount:       envInt("SENSOR_COUNT", 1000),
		MessagesPerSec:    envInt("MESSAGES_PER_SECOND", 1000),
		AnomalyRate:       envFloat("ANOMALY_RATE", 0.02),
		DuplicateRate:     envFloat("DUPLICATE_RATE", 0.01),
		OutOfOrderRate:    envFloat("OUT_OF_ORDER_RATE", 0.02),
		NetworkDelayRate:  envFloat("NETWORK_DELAY_RATE", 0.03),
		SensorFailureRate: envFloat("SENSOR_FAILURE_RATE", 0.005),
		PublisherWorkers:  envInt("SIM_PUBLISHER_WORKERS", 32),
		QueueCapacity:     envInt("SIM_QUEUE_CAPACITY", 20000),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
