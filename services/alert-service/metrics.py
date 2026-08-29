"""Prometheus metrics, the Python port of metrics.go. Same metric names and
labels, so existing Grafana dashboards/alerts keep working unchanged."""

from prometheus_client import Counter, Gauge

anomalies_consumed_total = Counter(
    "anomalies_consumed_total",
    "Anomaly events consumed from anomalies.detected.",
)

alerts_generated_total = Counter(
    "alerts_generated_total",
    "Alerts created, by severity.",
    ["severity"],
)

alerts_suppressed_total = Counter(
    "alerts_suppressed_total",
    "Alert-worthy events suppressed by dedup or cooldown, by reason.",
    ["reason"],
)

alerts_escalated_total = Counter(
    "alerts_escalated_total",
    "Alerts escalated to a higher severity after remaining unacknowledged.",
)

incidents_open_total = Gauge(
    "incidents_open_total",
    "Currently open (non-terminal) incidents.",
)

incidents_open_by_severity = Gauge(
    "incidents_open_by_severity",
    "Currently open (non-terminal) incidents, by severity -- what the Grafana 'critical incidents' panel actually needs.",
    ["severity"],
)

incidents_created_total = Counter(
    "incidents_created_total",
    "New incidents opened from an alert.",
)

alerts_attached_to_incident_total = Counter(
    "alerts_attached_to_incident_total",
    "Alerts attached to an already-open incident instead of creating a new one.",
)

notifications_sent_total = Counter(
    "notifications_sent_total",
    "Notifications successfully sent, by provider.",
    ["provider"],
)

notifications_failed_total = Counter(
    "notifications_failed_total",
    "Notification attempts that failed, by provider.",
    ["provider"],
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
