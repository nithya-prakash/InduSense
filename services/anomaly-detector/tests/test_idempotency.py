import os
import uuid

import pytest
from psycopg_pool import ConnectionPool

from idempotency import ANOMALY_DEDUP_SCOPE, claim_telemetry_event_once


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
