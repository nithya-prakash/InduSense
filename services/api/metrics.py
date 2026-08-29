"""Prometheus metrics, the Python port of metrics.go. Same metric names and
labels, so existing Grafana dashboards/alerts keep working unchanged."""

from prometheus_client import Counter, Gauge, Histogram

api_requests_total = Counter(
    "api_requests_total",
    "HTTP requests handled, by method, path, and status.",
    ["method", "path", "status"],
)

api_request_duration_seconds = Histogram(
    "api_request_duration_seconds",
    "HTTP request handling duration, by method and path.",
    ["method", "path"],
)

websocket_connections = Gauge(
    "websocket_connections",
    "Currently connected WebSocket clients.",
)

devices_by_status = Gauge(
    "devices_by_status",
    "Devices grouped by lifecycle status, across all organizations -- backs the IoT dashboard's active/offline device panels.",
    ["status"],
)
