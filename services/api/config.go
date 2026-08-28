package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port string

	PostgresDSN      string
	PostgresMaxConns int

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

	// TrustProxyHeaders gates whether X-Forwarded-For is ever consulted for
	// the "real" client IP (used for rate limiting and auth audit logging).
	// Defaults to false: an X-Forwarded-For header is trivially spoofable by
	// any direct client, so trusting it unconditionally lets an attacker
	// reset their own rate-limit bucket on every request just by varying
	// the header. Only set this true when the API sits behind a reverse
	// proxy that itself sets/overwrites X-Forwarded-For — and even then,
	// TrustedProxyCIDRs must list that proxy's address so a client that
	// connects directly (bypassing the proxy) can't still spoof it.
	TrustProxyHeaders bool
	TrustedProxyCIDRs []string
}

func loadConfig() Config {
	return Config{
		Port: envStr("API_PORT", "8080"),

		PostgresDSN:      envStr("API_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),
		PostgresMaxConns: envInt("API_POSTGRES_MAX_CONNS", 10),

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

		TrustProxyHeaders: envBool("API_TRUST_PROXY_HEADERS", false),
		TrustedProxyCIDRs: envStringList("API_TRUSTED_PROXY_CIDRS", nil),
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

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// envStringList reads a comma-separated env var into a slice, trimming
// whitespace around each entry and dropping empty ones. Returns def
// (typically nil) if the var is unset.
func envStringList(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
