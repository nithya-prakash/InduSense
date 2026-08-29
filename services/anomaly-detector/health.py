"""HTTP health/metrics server, the Python port of health.go. Exposes /live
(process is up — never fails on a dependency), /ready (this instance can
currently do useful work — the device/sensor catalog it needs for
detection is Postgres-backed, so a Postgres outage means it genuinely
isn't ready, and an open Kafka breaker means it can detect but not
actually publish results), /forests, and /metrics.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from catalog import Catalog
from forestregistry import ForestRegistry
from kafka_io import KafkaIO

_logger = logging.getLogger("anomaly-detector")


def _make_handler(cat: Catalog, kio: KafkaIO, registry: ForestRegistry) -> type[BaseHTTPRequestHandler]:
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
                postgres_ok = cat.ping()
                breaker_state = kio.breaker_state()
                ready = postgres_ok and breaker_state != "OPEN"
                body = json.dumps(
                    {"ready": ready, "postgres_connected": postgres_ok, "kafka_circuit_breaker": breaker_state}
                ).encode("utf-8")
                self.send_response(200 if ready else 503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/forests":
                body = json.dumps({"trained_machine_types": registry.trained_machine_types()}).encode("utf-8")
                self.send_response(200)
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


def start_health_server(port: str, cat: Catalog, kio: KafkaIO, registry: ForestRegistry) -> ThreadingHTTPServer | None:
    """Starts the health/metrics server in a background thread. A bind
    failure (e.g. the port already in use) is logged loudly rather than
    silently leaving /live and /ready unreachable for the process's whole
    lifetime — the exact fix a pre-GitHub audit made to health.go, ported
    here rather than reintroducing the original bug."""
    try:
        server = ThreadingHTTPServer(("", int(port)), _make_handler(cat, kio, registry))
    except OSError as exc:
        _logger.error("anomaly-detector: health/metrics server on :%s stopped: %s", port, exc)
        return None
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
