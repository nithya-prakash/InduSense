import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest
from psycopg_pool import ConnectionPool

from catalog import Catalog
from forestregistry import ForestRegistry
from health import start_health_server
from kafka_io import KafkaIO
from shared.reliability import CircuitBreaker


def _test_kafka_io() -> KafkaIO:
    """Builds a KafkaIO with just enough set for breaker_state() — the only
    thing the /ready handler under test actually calls on it — so this test
    doesn't need a real Kafka broker. Mirrors health_test.go's testKafkaIO."""
    kio = KafkaIO.__new__(KafkaIO)
    kio._breaker = CircuitBreaker(5, 15.0)
    return kio


def _free_port() -> str:
    """Asks the OS for a currently-unused port by briefly binding to :0 and
    closing again — a fresh port per call sidesteps the leaked-listener
    collision a hardcoded port would hit across repeated test runs, same
    as health_test.go's freePort."""
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
    used to silently swallow a bind failure (e.g. the port already in use),
    leaving /live and /ready silently unreachable for the process's whole
    lifetime with no log line explaining why. Occupies a real port first,
    then asserts the resulting bind failure is actually logged. Passes None
    dependencies deliberately — the handlers that would use them never run,
    since the server never successfully starts serving."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ln:
        ln.bind(("", 0))
        ln.listen(1)
        port = str(ln.getsockname()[1])

        with caplog.at_level(logging.ERROR, logger="anomaly-detector"):
            result = start_health_server(port, None, None, None)

        assert result is None
        assert any("health/metrics server" in r.message for r in caplog.records)


def test_ready_endpoint_reflects_real_postgres_state():
    """Verifies the fix for a pre-GitHub audit finding: /ready used to
    unconditionally return {"ready": true} regardless of whether
    anomaly-detector could actually reach Postgres — its one
    genuinely-required dependency, since detection depends on the
    Postgres-backed device/sensor catalog. Checks both directions: ready
    against a real, reachable database, and not-ready (503) against a
    catalog whose pool can never connect."""
    dsn = os.environ.get(
        "ANOMALY_POSTGRES_DSN",
        "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable",
    )
    try:
        real_cat = Catalog(dsn, 5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")

    try:
        registry = ForestRegistry()
        port = _free_port()
        start_health_server(port, real_cat, _test_kafka_io(), registry)
        _wait_for_health_server(port)

        with urllib.request.urlopen(f"http://localhost:{port}/ready", timeout=2.0) as resp:
            assert resp.status == 200
            body = json.loads(resp.read())
        assert body["ready"] is True
        assert body["postgres_connected"] is True

        # Not-ready case: a catalog whose pool points at a port nothing is
        # listening on can never connect, so ping must fail and /ready must
        # report it. Built directly (bypassing Catalog.__init__, which
        # requires an initial successful refresh) since the point here is
        # only to exercise the /ready handler's ping check, not catalog
        # loading itself.
        dead_pool = ConnectionPool(
            "postgres://indusense:wrong@localhost:1/indusense?sslmode=disable&connect_timeout=1",
            min_size=0,
            max_size=1,
            open=True,
        )
        try:
            dead_cat = Catalog.__new__(Catalog)
            dead_cat._lock = threading.Lock()
            dead_cat._devices = {}
            dead_cat._feature_order = {}
            dead_cat._pool = dead_pool

            dead_port = _free_port()
            start_health_server(dead_port, dead_cat, _test_kafka_io(), registry)
            _wait_for_health_server(dead_port)

            try:
                # Generous client-side timeout: the handler's own ping()
                # bounds itself to 2s internally, so the client must allow
                # comfortably more than that or the two timeouts race.
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
        real_cat.close()
