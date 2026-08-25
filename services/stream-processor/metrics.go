package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricMessagesConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_consumed_total",
		Help: "Telemetry messages consumed from telemetry.raw.",
	})

	metricDuplicateEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duplicate_events_total",
		Help: "Telemetry events recognized as duplicates via Redis dedup and skipped.",
	})

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
		Help:    "Time from Kafka fetch to commit (dedup + InfluxDB write + republish).",
		Buckets: prometheus.DefBuckets,
	})

	metricKafkaConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Consumer lag on telemetry.raw, as reported by the Kafka client.",
	})

	metricAggregatesWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "windowed_aggregates_written_total",
		Help: "Windowed aggregate points written to InfluxDB.",
	})
)
