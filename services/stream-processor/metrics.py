"""Prometheus metrics, the Python port of metrics.go. Same metric names and
labels, so existing Grafana dashboards/alerts keep working unchanged."""

from prometheus_client import Counter, Gauge, Histogram

messages_consumed_total = Counter(
    "messages_consumed_total",
    "Telemetry messages consumed from telemetry.raw.",
)

duplicate_events_total = Counter(
    "duplicate_events_total",
    "Telemetry events recognized as duplicates via Redis dedup and skipped.",
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
    "Time from Kafka fetch to commit (dedup + InfluxDB write + republish).",
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Consumer lag on telemetry.raw, as reported by the Kafka client.",
)

windowed_aggregates_written_total = Counter(
    "windowed_aggregates_written_total",
    "Windowed aggregate points written to InfluxDB.",
)
