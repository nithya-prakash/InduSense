"""Prometheus metrics, the Python port of metrics.go. Same metric names and
labels, so existing Grafana dashboards/alerts keep working unchanged."""

from prometheus_client import Counter, Gauge, Histogram

messages_consumed_total = Counter(
    "messages_consumed_total",
    "Telemetry messages consumed from telemetry.processed.",
)

anomalies_detected_total = Counter(
    "anomalies_detected_total",
    "Anomalies detected, by method.",
    ["method"],
)

messages_failed_total = Counter(
    "messages_failed_total",
    "Messages that failed processing, by reason.",
    ["reason"],
)

dlq_messages_total = Counter(
    "dlq_messages_total",
    "Messages routed to the dead-letter topic.",
)

processing_latency_seconds = Histogram(
    "processing_latency_seconds",
    "Time from Kafka fetch to commit.",
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Consumer lag on telemetry.processed.",
)

isolation_forests_trained_total = Counter(
    "isolation_forests_trained_total",
    "Isolation forest (re)training runs completed, across all machine types.",
)
