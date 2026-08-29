"""HTTP health/metrics server, the Python port of health.go. Same three
endpoints, same semantics: /live never depends on a dependency (a
transient broker outage should not cause an orchestrator to kill and
restart a perfectly healthy process), /ready reflects real MQTT+Kafka
state, /metrics is the Prometheus scrape endpoint.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from kafka_producer import KafkaSink
from mqtt_client import AtomicBool

_logger = logging.getLogger("ingestion")


def _make_handler(mqtt_connected: AtomicBool, sink: KafkaSink) -> type[BaseHTTPRequestHandler]:
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
                state = sink.breaker_state()
                connected = mqtt_connected.get()
                ready = connected and state != "OPEN"
                body = json.dumps(
                    {"ready": ready, "mqtt_connected": connected, "kafka_circuit_breaker": state}
                ).encode("utf-8")
                self.send_response(200 if ready else 503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/health":
                body = json.dumps(
                    {"mqtt_connected": mqtt_connected.get(), "kafka_circuit_breaker": sink.breaker_state()}
                ).encode("utf-8")
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


def start_health_server(port: str, mqtt_connected: AtomicBool, sink: KafkaSink) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("", int(port)), _make_handler(mqtt_connected, sink))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
