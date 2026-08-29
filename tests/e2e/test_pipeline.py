"""Drives the real pipeline end to end: publishes a genuine MQTT message
to the same broker ingestion subscribes to, and polls the real API until
the effect that message should have -- a stored telemetry point, or a
generated alert -- becomes visible. Nothing here is mocked or
short-circuited: a passing test means an MQTT message actually rode
through ingestion, Kafka, stream-processor/anomaly-detector,
InfluxDB/Postgres, alert-service, and back out through the HTTP API.

Runs against the docker-compose stack (`make up`), using real seeded
device/sensor rows looked up from Postgres rather than hardcoded IDs,
since those rows get fresh UUIDs on every seed. Skips (not fails) if the
stack isn't reachable.

Python port of tests/e2e/pipeline_test.go.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
import requests
from paho.mqtt.client import CallbackAPIVersion, Client

DEMO_PASSWORD = "ChangeMe123!"


def api_base_url() -> str:
    return os.environ.get("API_BASE_URL", "http://localhost:8080")


def mqtt_broker_url() -> str:
    return os.environ.get("SIM_MQTT_BROKER_URL", "tcp://localhost:1883")


def postgres_dsn() -> str:
    return os.environ.get("ALERT_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable")


def _parse_broker_url(url: str) -> tuple[str, int]:
    without_scheme = url.split("://", 1)[-1]
    host, _, port = without_scheme.partition(":")
    return host, int(port) if port else 1883


def flush_rate_limit_buckets(redis_client) -> None:
    """See the identical helper in tests/integration/test_api.py for the
    full reasoning."""
    if redis_client is None:
        return
    for key in redis_client.scan_iter("ratelimit:*"):
        redis_client.delete(key)


@dataclass
class SensorFixture:
    """One real, currently-seeded sensor and the full hierarchy chain
    above it, looked up by organization slug so the test never hardcodes
    a UUID that only existed in some past seed run."""

    organization_id: str
    factory_id: str
    production_line_id: str
    machine_id: str
    device_id: str
    sensor_id: str
    metric: str
    unit: str
    min_operating: float
    max_operating: float


_FIXTURE_COLUMNS = """
    o.id, fac.id, pl.id, m.id, d.id, s.id, s.metric, s.unit,
    COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
"""
_FIXTURE_JOINS = """
    FROM sensors s
    JOIN devices d ON d.id = s.device_id
    JOIN machines m ON m.id = d.machine_id
    JOIN production_lines pl ON pl.id = m.production_line_id
    JOIN factories fac ON fac.id = pl.factory_id
    JOIN organizations o ON o.id = fac.organization_id
"""


def _row_to_fixture(row) -> SensorFixture:
    return SensorFixture(
        organization_id=str(row[0]), factory_id=str(row[1]), production_line_id=str(row[2]),
        machine_id=str(row[3]), device_id=str(row[4]), sensor_id=str(row[5]),
        metric=row[6], unit=row[7], min_operating=float(row[8]), max_operating=float(row[9]),
    )


def lookup_sensor_fixture(conn, org_slug: str, metric: str) -> SensorFixture:
    row = conn.execute(
        f"SELECT {_FIXTURE_COLUMNS} {_FIXTURE_JOINS} WHERE o.slug = %s AND s.metric = %s LIMIT 1",
        (org_slug, metric),
    ).fetchone()
    assert row is not None, f"look up a seeded {metric!r} sensor for organization {org_slug!r} (has `make seed` been run?)"
    return _row_to_fixture(row)


def lookup_isolated_sensor_fixture(conn, org_slug: str, metric: str) -> SensorFixture:
    """Picks a sensor whose device has never produced *any* CRITICAL
    alert -- not just one from the given rule title.

    This used to only exclude the specific rule title being tested, which
    was a real, observed source of flakiness: the live simulator generates
    background telemetry and other alert types independently of this test
    (e.g. "Unexpected machine shutdown" from a simulated status change),
    so a device could already carry an unrelated CRITICAL alert.
    test_e2e_anomaly_triggers_alert polls /api/v1/alerts?severity=CRITICAL
    and matches by device_id -- with the old, narrower exclusion, that
    unrelated alert would be the first (and only) match found for the
    device, and the test failed on a title mismatch that had nothing to
    do with the telemetry it had just published. Excluding every existing
    CRITICAL alert for the device (any title, any status) means the
    chosen device is provably clean before the test starts.

    alert-service also dedupes/cooldowns by (rule, device+metric), so
    this continues to double as protection against that: a device with
    zero existing alerts of any kind always takes the alert-creation
    path, never the cooldown-suppression path."""
    row = conn.execute(
        f"""
        SELECT {_FIXTURE_COLUMNS} {_FIXTURE_JOINS}
        WHERE o.slug = %s AND s.metric = %s
          AND NOT EXISTS (SELECT 1 FROM alerts a WHERE a.device_id = d.id AND a.severity = 'CRITICAL')
        ORDER BY d.id
        LIMIT 1
        """,
        (org_slug, metric),
    ).fetchone()
    assert row is not None, f"find a {metric!r} device with no existing CRITICAL alert in organization {org_slug!r}"
    return _row_to_fixture(row)


@pytest.fixture()
def live_stack(redis_client):
    """Skips (not fails) if any dependency of the stack isn't reachable --
    mirrors Go's requireLiveStack helper. Yields (api_url, pg_conn,
    mqtt_client)."""
    api_url = api_base_url()
    try:
        resp = requests.get(f"{api_url}/live", timeout=3.0)
        assert resp.status_code == 200
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"api not reachable at {api_url}, skipping e2e test: {exc}")

    try:
        conn = psycopg.connect(postgres_dsn(), autocommit=True, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres not reachable, skipping e2e test: {exc}")

    mqtt_client = Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=f"indusense-e2e-test-{uuid.uuid4()}")
    host, port = _parse_broker_url(mqtt_broker_url())
    try:
        mqtt_client.connect(host, port, keepalive=30)
        mqtt_client.loop_start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not mqtt_client.is_connected():
            time.sleep(0.05)
        if not mqtt_client.is_connected():
            raise ConnectionError("mqtt connect timed out")
    except Exception as exc:  # noqa: BLE001
        conn.close()
        pytest.skip(f"mqtt broker not reachable at {mqtt_broker_url()}, skipping e2e test: {exc}")

    try:
        yield api_url, conn, mqtt_client
    finally:
        mqtt_client.disconnect()
        mqtt_client.loop_stop()
        conn.close()


def login(redis_client, api_url: str, email: str) -> str:
    """See tests/integration/test_api.py's login() for why this no longer
    sets a synthetic X-Forwarded-For to dodge the auth endpoint's rate
    limit -- every login in this file simply shares the real-IP "auth"
    bucket like any other client, comfortably within the configured
    limit alongside tests/integration's own handful of logins."""
    flush_rate_limit_buckets(redis_client)
    resp = requests.post(f"{api_url}/api/v1/auth/login", json={"email": email, "password": DEMO_PASSWORD}, timeout=5.0)
    assert resp.status_code == 200, f"login as {email}: expected 200, got {resp.status_code}"
    return resp.json().get("access_token", "")


def publish_telemetry(mqtt_client: Client, f: SensorFixture, value: float) -> str:
    event_id = str(uuid.uuid4())
    evt = {
        "event_id": event_id,
        "organization_id": f.organization_id,
        "factory_id": f.factory_id,
        "production_line_id": f.production_line_id,
        "machine_id": f.machine_id,
        "device_id": f.device_id,
        "sensor_id": f.sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sequence_number": 1,
        "metric": f.metric,
        "value": value,
        "unit": f.unit,
    }
    payload = json.dumps(evt).encode("utf-8")

    topic = f"factory/{f.factory_id}/machine/{f.machine_id}/sensor/{f.sensor_id}/telemetry"
    info = mqtt_client.publish(topic, payload, qos=1)
    info.wait_for_publish(timeout=5.0)
    assert info.is_published(), f"publish telemetry to {topic} timed out"
    return event_id


def test_e2e_telemetry_round_trip(live_stack, redis_client):
    """Publishes one real telemetry reading -- an in-range value, so it's
    just normal data, not an anomaly -- and polls the API until it's
    visible, proving MQTT -> ingestion -> Kafka -> stream-processor ->
    InfluxDB -> API works end to end."""
    api_url, conn, mqtt_client = live_stack
    fixture = lookup_sensor_fixture(conn, "musterfabrik-gmbh", "temperature")

    # Squarely inside the sensor's own operating range, and distinctive
    # enough (three decimal places) that it can't be confused with a
    # coincidentally similar reading from unrelated traffic on the shared
    # stack (the simulator, other tests) touching the same device.
    value = fixture.min_operating + (fixture.max_operating - fixture.min_operating) * 0.5 + 0.137

    token = login(redis_client, api_url, "admin@musterfabrik-gmbh.de")
    publish_telemetry(mqtt_client, fixture, value)

    deadline = time.monotonic() + 15.0
    last_status, last_body = None, None
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{api_url}/api/v1/telemetry/latest",
            params={"device_id": fixture.device_id, "metric": fixture.metric},
            headers={"Authorization": f"Bearer {token}"},
            timeout=5.0,
        )
        last_status, last_body = resp.status_code, resp.text
        if resp.status_code == 200 and resp.json().get("value") == value:
            return  # found it -- round trip confirmed
        time.sleep(0.5)
    pytest.fail(f"telemetry value {value} never appeared via the API within 15s (last response: {last_status} {last_body})")


def test_e2e_anomaly_triggers_alert(live_stack, redis_client):
    """Publishes a reading far outside the operating range for a
    musterfabrik-gmbh device that has never produced any CRITICAL alert
    before (picked dynamically -- see lookup_isolated_sensor_fixture --
    so this test can't collide with alert cooldown state, or an unrelated
    alert type, left behind by the simulator or earlier test runs), and
    polls for the CRITICAL "High temperature" alert (rule GREATER_THAN
    90) that reading should produce. This proves MQTT -> ingestion ->
    Kafka -> anomaly-detector's rule check -> alert-service's rule match
    -> Postgres -> API, the platform's actual reason for existing.

    The match requires all three of device_id, title, AND triggered_at
    being after this test's own start time (with a small clock-skew
    allowance) -- not device_id alone. A device is excluded from
    selection if it already has *any* CRITICAL alert, so in normal
    operation nothing should ever need the title/time checks to
    disambiguate -- but they're real assertions on the outcome this test
    claims to prove, not just a defensive fallback.

    zweite-firma-gmbh isn't used here: it was seeded with only a factory
    and a machine (for the tenant-isolation test in tests/integration)
    and has no devices or sensors of its own."""
    api_url, conn, mqtt_client = live_stack
    fixture = lookup_isolated_sensor_fixture(conn, "musterfabrik-gmbh", "temperature")

    anomalous_value = 150.0  # "High temperature" rule fires above 90
    clock_skew_allowance = 5.0

    token = login(redis_client, api_url, "admin@musterfabrik-gmbh.de")
    test_start = datetime.now(timezone.utc) - timedelta(seconds=clock_skew_allowance)
    publish_telemetry(mqtt_client, fixture, anomalous_value)

    # 45s, not 20s: `make seed` restarts anomaly-detector/alert-service so
    # their Postgres-derived caches are immediately fresh, but neither
    # service's /ready endpoint checks Kafka consumer-group state, so
    # there's no signal for "the group has finished rebalancing" after
    # that restart.
    deadline = time.monotonic() + 45.0
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{api_url}/api/v1/alerts", params={"severity": "CRITICAL"},
            headers={"Authorization": f"Bearer {token}"}, timeout=5.0,
        )
        items = resp.json().get("items", []) if resp.status_code == 200 else []

        for a in items:
            if a.get("device_id") != fixture.device_id:
                continue
            triggered_at = datetime.fromisoformat(a["triggered_at"].replace("Z", "+00:00"))
            if triggered_at < test_start:
                continue  # not this test's alert -- a pre-existing or unrelated one, keep polling
            if a.get("title") != "High temperature":
                # lookup_isolated_sensor_fixture guarantees this device had
                # no CRITICAL alert before test_start, so a *different*,
                # post-test_start CRITICAL alert appearing for it means
                # something concurrent (most plausibly the live simulator)
                # produced it -- not our own out-of-range reading.
                continue
            return  # found it -- anomaly-to-alert pipeline confirmed, and it's this test's own alert
        time.sleep(0.5)
    pytest.fail(
        f"no CRITICAL \"High temperature\" alert for device {fixture.device_id} "
        f"(triggered after this test started) appeared via the API within 45s of publishing temperature={anomalous_value:.0f}"
    )


def test_lookup_isolated_sensor_fixture_excludes_device_with_any_existing_critical_alert():
    """Regression test for the exact bug that made
    test_e2e_anomaly_triggers_alert flaky: the old fixture lookup only
    excluded devices with an existing alert matching the specific rule
    title under test ("High temperature"), so a device already carrying
    an unrelated CRITICAL alert (e.g., from a simulated machine shutdown)
    could still be selected -- and would then be the first, wrong match
    the test's poll loop found. This inserts a throwaway CRITICAL alert
    with a deliberately different title against a real, currently-
    selectable device, then asserts the lookup never selects that device
    again while the alert exists -- proving the exclusion is
    title-independent, not just re-testing the original narrower
    behavior."""
    try:
        conn = psycopg.connect(postgres_dsn(), autocommit=True, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")

    try:
        fixture = lookup_isolated_sensor_fixture(conn, "musterfabrik-gmbh", "temperature")

        alert_id = conn.execute(
            """
            INSERT INTO alerts (organization_id, machine_id, device_id, severity, status, title, description, dedupe_key)
            VALUES (%s, %s, %s, 'CRITICAL', 'OPEN', 'Unexpected machine shutdown', 'regression test alert -- unrelated to High temperature', %s)
            RETURNING id
            """,
            (fixture.organization_id, fixture.machine_id, fixture.device_id, f"regression-test-dedupe-{uuid.uuid4()}"),
        ).fetchone()[0]
        try:
            again = lookup_isolated_sensor_fixture(conn, "musterfabrik-gmbh", "temperature")
            assert again.device_id != fixture.device_id, (
                f"lookup_isolated_sensor_fixture re-selected device {fixture.device_id} after it was given an unrelated "
                f'CRITICAL alert ("Unexpected machine shutdown") -- the exclusion must cover any existing CRITICAL alert, '
                f"not just one matching the rule title under test"
            )
        finally:
            conn.execute("DELETE FROM alerts WHERE id = %s", (alert_id,))
    finally:
        conn.close()
