import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request

import pytest
from psycopg_pool import ConnectionPool

from health import start_health_server


def _free_port() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return str(s.getsockname()[1])


def _wait_for_health_server(port: str) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"http://localhost:{port}/live", timeout=0.5).close()
            return
        except Exception:  # noqa: BLE001
            time.sleep(0.02)
    pytest.fail(f"health server on port {port} never became reachable")


def test_start_health_server_bind_failure_is_logged(caplog):
    """Verifies the fix for a pre-GitHub audit finding: start_health_server
    used to silently swallow a bind failure, leaving /live and /ready
    unreachable for the process's whole lifetime with no log line
    explaining why. Passes a None pool deliberately -- the /ready handler
    that would use it never runs, since the server never successfully
    starts serving."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ln:
        ln.bind(("", 0))
        ln.listen(1)
        port = str(ln.getsockname()[1])

        with caplog.at_level(logging.ERROR, logger="alert-service"):
            result = start_health_server(port, None)

        assert result is None
        assert any("health/metrics server" in r.message for r in caplog.records)


def test_ready_endpoint_reflects_real_postgres_state():
    """Verifies the fix for a pre-GitHub audit finding: /ready used to
    unconditionally return {"ready": true} regardless of whether
    alert-service could actually reach Postgres -- its one
    genuinely-required dependency, since every alert this service creates
    is a Postgres write. Checks both directions: ready against a real,
    reachable database, and not-ready (503) against a pool that can never
    connect."""
    dsn = os.environ.get(
        "ALERT_POSTGRES_DSN",
        "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable",
    )
    try:
        real_pool = ConnectionPool(dsn, min_size=1, max_size=2, open=True, timeout=5.0)
        with real_pool.connection(timeout=5.0) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")

    try:
        port = _free_port()
        start_health_server(port, real_pool)
        _wait_for_health_server(port)

        with urllib.request.urlopen(f"http://localhost:{port}/ready", timeout=2.0) as resp:
            assert resp.status == 200
            body = json.loads(resp.read())
        assert body["ready"] is True
        assert body["postgres_connected"] is True

        # Not-ready case: a pool pointed at a port nothing is listening on
        # can never connect, so ping must fail and /ready must report it.
        dead_pool = ConnectionPool(
            "postgres://indusense:wrong@localhost:1/indusense?sslmode=disable&connect_timeout=1",
            min_size=0,
            max_size=1,
            open=True,
        )
        try:
            dead_port = _free_port()
            start_health_server(dead_port, dead_pool)
            _wait_for_health_server(dead_port)

            try:
                urllib.request.urlopen(f"http://localhost:{dead_port}/ready", timeout=10.0)
                pytest.fail("expected HTTPError with status 503")
            except urllib.error.HTTPError as exc:
                assert exc.code == 503
                body2 = json.loads(exc.read())
            assert body2["ready"] is False
            assert body2["postgres_connected"] is False
        finally:
            dead_pool.close()
    finally:
        real_pool.close()
