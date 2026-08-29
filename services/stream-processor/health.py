"""HTTP health/metrics server, the Python port of health.go. Same three
endpoints, same semantics: /live never depends on a dependency, /ready
reflects real Redis+InfluxDB+Kafka state, /metrics is the Prometheus scrape
endpoint.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from dedup import Deduplicator
from influx import InfluxSink
from kafka_io import KafkaIO

_logger = logging.getLogger("stream-processor")


def _make_handler(dedup: Deduplicator, influx: InfluxSink, kio: KafkaIO) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:  # noqa: A002 - silence stdlib's default access log
            pass

        def do_GET(self) -> None:
            if self.path == "/live":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
                return

            if self.path == "/ready":
                redis_ok = dedup.ping()
                influx_state = influx.breaker_state()
                kafka_state = kio.breaker_state()
                ready = redis_ok and influx_state != "OPEN" and kafka_state != "OPEN"
                body = json.dumps(
                    {
                        "ready": ready,
                        "redis_connected": redis_ok,
                        "influxdb_circuit_breaker": influx_state,
                        "kafka_circuit_breaker": kafka_state,
                    }
                ).encode("utf-8")
                self.send_response(200 if ready else 503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/metrics":
                body = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(404)
            self.end_headers()

    return Handler


def start_health_server(port: str, dedup: Deduplicator, influx: InfluxSink, kio: KafkaIO) -> ThreadingHTTPServer | None:
    """Starts the health/metrics server in a background thread. A bind
    failure (e.g. the port already in use) is logged loudly rather than
    silently leaving /live and /ready unreachable for the process's whole
    lifetime — the exact fix a pre-GitHub audit made to health.go, ported
    here rather than reintroducing the original bug."""
    try:
        server = ThreadingHTTPServer(("", int(port)), _make_handler(dedup, influx, kio))
    except OSError as exc:
        _logger.error("stream-processor: health/metrics server on :%s stopped: %s", port, exc)
        return None
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
