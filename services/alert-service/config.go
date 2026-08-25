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
	TopicAnomalies    string
	TopicDeviceEvents string
	TopicAlerts       string
	TopicDeadLetter   string

	PostgresDSN      string
	RuleRefreshEvery time.Duration

	EscalationCheckEvery   time.Duration
	EscalationAfterSeconds int // an OPEN alert unacknowledged this long gets escalated one rung

	NotificationConsoleEnabled bool
	NotificationWebhookURL     string

	HTTPPort string
}

func loadConfig() Config {
	return Config{
		KafkaBrokers:      strings.Split(envStr("KAFKA_BROKERS", "localhost:9094"), ","),
		ConsumerGroupID:   envStr("KAFKA_CONSUMER_GROUP_PREFIX", "indusense") + "-alert-service",
		TopicAnomalies:    envStr("KAFKA_TOPIC_ANOMALIES_DETECTED", "anomalies.detected"),
		TopicDeviceEvents: envStr("KAFKA_TOPIC_DEVICE_EVENTS", "device.events"),
		TopicAlerts:       envStr("KAFKA_TOPIC_ALERTS", "alerts"),
		TopicDeadLetter:   envStr("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),

		PostgresDSN:      envStr("ALERT_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),
		RuleRefreshEvery: time.Duration(envInt("ALERT_RULE_REFRESH_SECONDS", 60)) * time.Second,

		EscalationCheckEvery:   time.Duration(envInt("ALERT_ESCALATION_CHECK_SECONDS", 60)) * time.Second,
		EscalationAfterSeconds: envInt("ALERT_ESCALATION_AFTER_SECONDS", 900),

		NotificationConsoleEnabled: envStr("ALERT_NOTIFY_CONSOLE", "true") == "true",
		NotificationWebhookURL:     envStr("ALERT_NOTIFY_WEBHOOK_URL", ""),

		HTTPPort: envStr("ALERT_SERVICE_PORT", "8084"),
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
