package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers    []string
	ConsumerGroupID string
	TopicProcessed  string
	TopicAnomalies  string
	TopicDeadLetter string

	PostgresDSN         string
	CatalogRefreshEvery time.Duration

	// Level 2 — statistical (EWMA z-score)
	EWMAAlpha           float64
	ZScoreThreshold     float64
	MinSamplesForZScore int

	// Level 3 — Isolation Forest
	ForestTrainingBufferSize int
	ForestRetrainEvery       time.Duration
	ForestNumTrees           int
	ForestSubsampleSize      int
	ForestScoreThreshold     float64

	HTTPPort string
}

func loadConfig() Config {
	return Config{
		KafkaBrokers:    strings.Split(envStr("KAFKA_BROKERS", "localhost:9094"), ","),
		ConsumerGroupID: envStr("KAFKA_CONSUMER_GROUP_PREFIX", "indusense") + "-anomaly-detector",
		TopicProcessed:  envStr("KAFKA_TOPIC_TELEMETRY_PROCESSED", "telemetry.processed"),
		TopicAnomalies:  envStr("KAFKA_TOPIC_ANOMALIES_DETECTED", "anomalies.detected"),
		TopicDeadLetter: envStr("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),

		PostgresDSN:         envStr("ANOMALY_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),
		CatalogRefreshEvery: time.Duration(envInt("ANOMALY_CATALOG_REFRESH_SECONDS", 300)) * time.Second,

		EWMAAlpha:           envFloat("ANOMALY_EWMA_ALPHA", 0.1),
		ZScoreThreshold:     envFloat("ANOMALY_ZSCORE_THRESHOLD", 3.0),
		MinSamplesForZScore: envInt("ANOMALY_MIN_SAMPLES", 30),

		ForestTrainingBufferSize: envInt("ANOMALY_FOREST_BUFFER_SIZE", 512),
		ForestRetrainEvery:       time.Duration(envInt("ANOMALY_FOREST_RETRAIN_SECONDS", 120)) * time.Second,
		ForestNumTrees:           envInt("ANOMALY_FOREST_NUM_TREES", 100),
		ForestSubsampleSize:      envInt("ANOMALY_FOREST_SUBSAMPLE_SIZE", 256),
		ForestScoreThreshold:     envFloat("ANOMALY_FOREST_SCORE_THRESHOLD", 0.62),

		HTTPPort: envStr("ANOMALY_DETECTOR_PORT", "8083"),
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
