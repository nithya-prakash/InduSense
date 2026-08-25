package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricMessagesConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_consumed_total",
		Help: "Telemetry messages consumed from telemetry.processed.",
	})

	metricAnomaliesDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anomalies_detected_total",
		Help: "Anomalies detected, by method.",
	}, []string{"method"})

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
		Help:    "Time from Kafka fetch to commit.",
		Buckets: prometheus.DefBuckets,
	})

	metricKafkaConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Consumer lag on telemetry.processed.",
	})

	metricForestsTrained = promauto.NewCounter(prometheus.CounterOpts{
		Name: "isolation_forests_trained_total",
		Help: "Isolation forest (re)training runs completed, across all machine types.",
	})
)
