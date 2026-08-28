package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers      []string
	ConsumerGroupID   string
	TopicTelemetryRaw string
	TopicProcessed    string
	TopicDeadLetter   string

	RedisAddr     string
	RedisPassword string
	RedisDB       int
	DedupTTL      time.Duration

	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string

	WindowFlushInterval time.Duration
	Windows             []time.Duration

	InfluxMaxRetries        int
	InfluxRetryBaseDelay    time.Duration
	KafkaMaxRetries         int
	KafkaRetryBaseDelay     time.Duration
	BreakerFailureThreshold int
	BreakerCooldown         time.Duration

	HTTPPort string
}

func loadConfig() Config {
	return Config{
		KafkaBrokers:      strings.Split(envStr("KAFKA_BROKERS", "localhost:9094"), ","),
		ConsumerGroupID:   envStr("KAFKA_CONSUMER_GROUP_PREFIX", "indusense") + "-stream-processor",
		TopicTelemetryRaw: envStr("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw"),
		TopicProcessed:    envStr("KAFKA_TOPIC_TELEMETRY_PROCESSED", "telemetry.processed"),
		TopicDeadLetter:   envStr("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),

		RedisAddr:     envStr("REDIS_HOST", "localhost") + ":" + envStr("REDIS_PORT", "6379"),
		RedisPassword: envStr("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_DB", 0),
		DedupTTL:      time.Duration(envInt("STREAM_DEDUP_TTL_SECONDS", 3600)) * time.Second,

		InfluxURL:    envStr("INFLUXDB_URL", "http://localhost:8086"),
		InfluxToken:  envStr("INFLUXDB_TOKEN", "dev-only-influx-admin-token-change-me"),
		InfluxOrg:    envStr("INFLUXDB_ORG", "indusense"),
		InfluxBucket: envStr("INFLUXDB_BUCKET", "telemetry"),

		WindowFlushInterval: time.Duration(envInt("STREAM_WINDOW_FLUSH_SECONDS", 10)) * time.Second,
		Windows: []time.Duration{
			10 * time.Second,
			30 * time.Second,
			time.Minute,
			5 * time.Minute,
			15 * time.Minute,
		},

		InfluxMaxRetries:        envInt("STREAM_INFLUX_MAX_RETRIES", 5),
		InfluxRetryBaseDelay:    time.Duration(envInt("STREAM_INFLUX_RETRY_BASE_MS", 1000)) * time.Millisecond,
		KafkaMaxRetries:         envInt("STREAM_KAFKA_MAX_RETRIES", 5),
		KafkaRetryBaseDelay:     time.Duration(envInt("STREAM_KAFKA_RETRY_BASE_MS", 1000)) * time.Millisecond,
		BreakerFailureThreshold: envInt("STREAM_BREAKER_THRESHOLD", 5),
		BreakerCooldown:         time.Duration(envInt("STREAM_BREAKER_COOLDOWN_S", 15)) * time.Second,

		HTTPPort: envStr("STREAM_PROCESSOR_PORT", "8082"),
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
