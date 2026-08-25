package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	MQTTBrokerURL string
	MQTTClientID  string
	MQTTQoS       byte

	KafkaBrokers      []string
	TopicTelemetryRaw string
	TopicDeviceEvents string
	TopicDeadLetter   string

	WorkerPoolSize int
	QueueCapacity  int

	KafkaMaxRetries     int
	KafkaRetryBaseDelay time.Duration

	BreakerFailureThreshold int
	BreakerCooldown         time.Duration

	HTTPPort string
}

func loadConfig() Config {
	mqttHost := envStr("MQTT_BROKER_HOST", "localhost")
	mqttPort := envStr("MQTT_BROKER_PORT", "1883")

	return Config{
		MQTTBrokerURL: envStr("MQTT_BROKER_URL", "tcp://"+mqttHost+":"+mqttPort),
		MQTTClientID:  envStr("MQTT_CLIENT_ID_PREFIX", "indusense") + "-ingestion",
		MQTTQoS:       byte(envInt("MQTT_QOS", 1)),

		KafkaBrokers:      strings.Split(envStr("KAFKA_BROKERS", "localhost:9094"), ","),
		TopicTelemetryRaw: envStr("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw"),
		TopicDeviceEvents: envStr("KAFKA_TOPIC_DEVICE_EVENTS", "device.events"),
		TopicDeadLetter:   envStr("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),

		WorkerPoolSize: envInt("INGESTION_WORKER_POOL_SIZE", 50),
		QueueCapacity:  envInt("INGESTION_QUEUE_CAPACITY", 10000),

		KafkaMaxRetries:     envInt("INGESTION_KAFKA_MAX_RETRIES", 5),
		KafkaRetryBaseDelay: time.Duration(envInt("INGESTION_KAFKA_RETRY_BASE_MS", 1000)) * time.Millisecond,

		BreakerFailureThreshold: envInt("INGESTION_BREAKER_THRESHOLD", 5),
		BreakerCooldown:         time.Duration(envInt("INGESTION_BREAKER_COOLDOWN_S", 15)) * time.Second,

		HTTPPort: envStr("INGESTION_PORT", "8081"),
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
