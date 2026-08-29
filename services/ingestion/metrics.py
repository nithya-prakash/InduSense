"""Prometheus metrics, the Python port of metrics.go. Same metric names and
labels, so existing Grafana dashboards/alerts keep working unchanged."""

from prometheus_client import Counter, Gauge, Histogram

messages_received_total = Counter(
    "messages_received_total",
    "MQTT messages received by the ingestion service, by topic kind.",
    ["topic_kind"],
)

messages_processed_total = Counter(
    "messages_processed_total",
    "Messages successfully published downstream, by result.",
    ["result"],
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
    "End-to-end time from MQTT receipt to Kafka publish (or dead-letter) completion.",
)

mqtt_connections = Gauge(
    "mqtt_connections",
    "Whether the ingestion service currently holds a live MQTT connection (0 or 1).",
)
