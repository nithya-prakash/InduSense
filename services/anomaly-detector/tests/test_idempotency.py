import os
import uuid
from datetime import datetime, timezone

import pytest
from psycopg_pool import ConnectionPool

from featurestore import FeatureStore
from forestregistry import ForestRegistry
from idempotency import ANOMALY_DEDUP_SCOPE, claim_telemetry_event_once
from stats import StatisticalTrackers
from config import Config
from main import _process_message
from shared.events import NormalizedTelemetryEvent


def _real_pool():
    dsn = os.environ.get(
        "ANOMALY_POSTGRES_DSN",
        "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable",
    )
    try:
        pool = ConnectionPool(dsn, min_size=1, max_size=2, open=True, timeout=5.0)
        with pool.connection(timeout=5.0) as conn:
            conn.execute("SELECT 1")
        return pool
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")


def test_claim_telemetry_event_once_dedupes_against_real_postgres():
    """Verifies the fix for a pre-GitHub audit finding: anomaly-detector
    generated a fresh anomaly_id and republished on every call to
    process_message, so a Kafka redelivery of the same telemetry.processed
    message (e.g. after a crash between publish and offset commit)
    produced a second, distinct anomaly — and, downstream, a second
    alert/incident — for one physical reading. Exercises the real
    idempotency_keys table (not a mock): the first claim for an event ID
    must succeed, a second claim for the same event ID must report it as
    already claimed, and a different event ID must be unaffected by
    either."""
    pool = _real_pool()
    try:
        event_id = str(uuid.uuid4())
        try:
            claimed = claim_telemetry_event_once(pool, event_id)
            assert claimed, "expected the first claim for a never-seen event ID to succeed"

            claimed_again = claim_telemetry_event_once(pool, event_id)
            assert not claimed_again, "expected a second claim for the same event ID (simulating Kafka redelivery) to report already-claimed"

            other_event_id = str(uuid.uuid4())
            try:
                claimed_other = claim_telemetry_event_once(pool, other_event_id)
                assert claimed_other, "expected a claim for a genuinely different event ID to succeed"
            finally:
                with pool.connection() as conn:
                    conn.execute("DELETE FROM idempotency_keys WHERE scope = %s AND key = %s", (ANOMALY_DEDUP_SCOPE, other_event_id))
                    conn.commit()
        finally:
            with pool.connection() as conn:
                conn.execute("DELETE FROM idempotency_keys WHERE scope = %s AND key = %s", (ANOMALY_DEDUP_SCOPE, event_id))
                conn.commit()
    finally:
        pool.close()


class _FakeMessage:
    """Stands in for confluent_kafka.Message at the one boundary
    _process_message actually touches (.value()/.headers()) -- the same
    kind of scope-narrowing the kafka_io tests already use to test logic
    without a live broker, not a mock of the code under test itself."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def value(self) -> bytes:
        return self._payload

    def headers(self):
        return None


class _FakeKafkaIO:
    """No-op publish/dead-letter -- this test is about detector state
    (trackers/feature store), not about Kafka delivery, which is already
    covered by test_kafka_io.py."""

    def publish_anomaly(self, key, anomaly):
        pass

    def dead_letter(self, raw_payload, cause, stage, event_id):
        raise AssertionError(f"unexpected dead-letter: stage={stage} cause={cause}")


class _FakeCatalog:
    """cat.lookup() always misses (info=None) -- this test targets the
    idempotency-vs-state-mutation ordering, not catalog-dependent
    rule/forest behavior, so no seeded device data is needed. cat.pool()
    is the one real thing: the actual idempotency_keys claim must hit
    real Postgres, not a mock, or this test would prove nothing."""

    def __init__(self, pool):
        self._pool = pool

    def pool(self):
        return self._pool

    def lookup(self, device_id):
        return None

    def features_for(self, machine_type):
        return []


def _counting_trackers(alpha: float):
    """Wraps the real StatisticalTrackers so the test can assert exactly
    how many times its state-mutating .update() ran, without faking the
    EWMA logic itself."""
    real = StatisticalTrackers(alpha)
    calls = {"n": 0}

    original_update = real.update

    def counting_update(*args, **kwargs):
        calls["n"] += 1
        return original_update(*args, **kwargs)

    real.update = counting_update  # type: ignore[method-assign]
    return real, calls


def test_process_message_does_not_double_count_a_redelivered_event_into_detector_state():
    """Regression test for a real gap found during a rigorous evaluation
    of this project's delivery-semantics claims (see the README's
    "Delivery semantics" section): the idempotency claim used to guard
    only the anomaly *publish*, so trackers.update (the EWMA baseline)
    and fs.observe (the Isolation Forest training buffer) ran on every
    call regardless of whether this exact event_id had already been
    processed. A Kafka redelivery of telemetry.processed (e.g. after a
    crash between detection and offset commit -- an ordinary occurrence
    under this system's at-least-once delivery model, not a rare edge
    case) silently folded the same physical reading into both baselines
    a second time, even though the resulting AnomalyDetected publish was
    correctly suppressed.

    Fixed by claiming idempotency before any detector state is touched,
    not just before publish_anomaly. This test calls _process_message
    twice with the identical event_id (simulating exactly that
    redelivery) against a real idempotency_keys table, and asserts the
    EWMA tracker's .update() ran exactly once, not twice."""
    pool = _real_pool()
    event_id = str(uuid.uuid4())
    trackers, calls = _counting_trackers(alpha=0.5)
    fs = FeatureStore(buffer_size=64)
    forests = ForestRegistry()
    cat = _FakeCatalog(pool)
    kio = _FakeKafkaIO()

    evt = NormalizedTelemetryEvent(
        event_id=event_id,
        organization_id="org-1",
        factory_id="factory-1",
        production_line_id="line-1",
        machine_id="machine-1",
        device_id="device-1",
        sensor_id="sensor-1",
        metric="temperature",
        value=42.0,
        unit="C",
        correlation_id=event_id,
        ingested_at=datetime.now(timezone.utc),
    )
    msg = _FakeMessage(evt.model_dump_json().encode("utf-8"))
    cfg = Config(
        kafka_brokers=[], consumer_group_id="", topic_processed="", topic_anomalies="", topic_dead_letter="",
        postgres_dsn="", postgres_max_conns=1, catalog_refresh_every_seconds=1,
        kafka_max_retries=1, kafka_retry_base_delay_seconds=0.001,
        breaker_failure_threshold=1, breaker_cooldown_seconds=1,
        ewma_alpha=0.5, zscore_threshold=3.0, min_samples_for_zscore=30,
        forest_training_buffer_size=1, forest_retrain_every_seconds=1,
        forest_num_trees=1, forest_subsample_size=1, forest_score_threshold=0.62,
        http_port="0",
    )

    try:
        ok_first = _process_message(cfg, kio, cat, trackers, fs, forests, msg)
        ok_second = _process_message(cfg, kio, cat, trackers, fs, forests, msg)

        assert ok_first is True
        assert ok_second is True
        assert calls["n"] == 1, (
            f"expected trackers.update to run exactly once across two calls with the "
            f"same event_id (the second is a simulated redelivery), got {calls['n']} — "
            "the idempotency claim must gate detector state, not just the publish"
        )
    finally:
        with pool.connection() as conn:
            conn.execute("DELETE FROM idempotency_keys WHERE scope = %s AND key = %s", (ANOMALY_DEDUP_SCOPE, event_id))
            conn.commit()
        pool.close()
