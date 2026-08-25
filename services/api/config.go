package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port string

	PostgresDSN string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string

	KafkaBrokers  []string
	TopicAlerts   string
	MQTTBrokerURL string

	JWTAccessSecret  string
	JWTRefreshSecret string
	JWTAccessTTL     time.Duration
	JWTRefreshTTL    time.Duration

	CORSAllowedOrigin string

	RateLimitAuthPerMinute int
	RateLimitDefaultPerMin int
}

func loadConfig() Config {
	return Config{
		Port: envStr("API_PORT", "8080"),

		PostgresDSN: envStr("API_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),

		RedisAddr:     envStr("REDIS_HOST", "localhost") + ":" + envStr("REDIS_PORT", "6379"),
		RedisPassword: envStr("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_DB", 0),

		InfluxURL:    envStr("INFLUXDB_URL", "http://localhost:8086"),
		InfluxToken:  envStr("INFLUXDB_TOKEN", "dev-only-influx-admin-token-change-me"),
		InfluxOrg:    envStr("INFLUXDB_ORG", "indusense"),
		InfluxBucket: envStr("INFLUXDB_BUCKET", "telemetry"),

		KafkaBrokers:  strings.Split(envStr("KAFKA_BROKERS", "localhost:9094"), ","),
		TopicAlerts:   envStr("KAFKA_TOPIC_ALERTS", "alerts"),
		MQTTBrokerURL: envStr("MQTT_BROKER_URL", "tcp://localhost:1883"),

		JWTAccessSecret:  envStr("JWT_ACCESS_SECRET", "change-me-dev-only-access-secret"),
		JWTRefreshSecret: envStr("JWT_REFRESH_SECRET", "change-me-dev-only-refresh-secret"),
		JWTAccessTTL:     time.Duration(envInt("JWT_ACCESS_TTL_MINUTES", 15)) * time.Minute,
		JWTRefreshTTL:    time.Duration(envInt("JWT_REFRESH_TTL_HOURS", 168)) * time.Hour,

		CORSAllowedOrigin: envStr("API_CORS_ALLOWED_ORIGINS", "http://localhost:3000"),

		RateLimitAuthPerMinute: envInt("API_RATE_LIMIT_AUTH_PER_MIN", 10),
		RateLimitDefaultPerMin: envInt("API_RATE_LIMIT_DEFAULT_PER_MIN", 120),
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
