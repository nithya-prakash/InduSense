"""HTTP health/metrics server, the Python port of health.go. Exposes /live
(process is up — never fails on a dependency), /ready (this instance can
currently do useful work — every alert this service creates is a Postgres
write, so a Postgres outage means it genuinely isn't ready), and /metrics.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import ConnectionPool

_logger = logging.getLogger("alert-service")


def _make_handler(pool: ConnectionPool | None) -> type[BaseHTTPRequestHandler]:
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
                postgres_ok = _ping(pool)
                body = json.dumps({"ready": postgres_ok, "postgres_connected": postgres_ok}).encode("utf-8")
                self.send_response(200 if postgres_ok else 503)
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


def _ping(pool: ConnectionPool | None) -> bool:
    if pool is None:
        return False
    try:
        with pool.connection(timeout=2.0) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:  # noqa: BLE001
        return False


def start_health_server(port: str, pool: ConnectionPool | None) -> ThreadingHTTPServer | None:
    """Starts the health/metrics server in a background thread. A bind
    failure (e.g. the port already in use) is logged loudly rather than
    silently leaving /live and /ready unreachable for the process's whole
    lifetime — the exact fix a pre-GitHub audit made to health.go, ported
    here rather than reintroducing the original bug."""
    try:
        server = ThreadingHTTPServer(("", int(port)), _make_handler(pool))
    except OSError as exc:
        _logger.error("alert-service: health/metrics server on :%s stopped: %s", port, exc)
        return None
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
