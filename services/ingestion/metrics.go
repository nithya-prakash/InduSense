package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricMessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_received_total",
		Help: "MQTT messages received by the ingestion service, by topic kind.",
	}, []string{"topic_kind"})

	metricMessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "Messages successfully published downstream, by result.",
	}, []string{"result"})

	metricMessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_failed_total",
		Help: "Messages that failed processing, by reason.",
	}, []string{"reason"})

	metricDLQMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dlq_messages_total",
		Help: "Messages routed to the dead-letter topic.",
	})

	metricProcessingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "processing_latency_seconds",
		Help:    "End-to-end time from MQTT receipt to Kafka publish (or dead-letter) completion.",
		Buckets: prometheus.DefBuckets,
	})

	metricMQTTConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_connections",
		Help: "Whether the ingestion service currently holds a live MQTT connection (0 or 1).",
	})
)
