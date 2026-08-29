"""Exactly-once anomaly publication, the Python port of idempotency.go.

Atomically claims a source telemetry event's event_id for anomaly-detection
processing, using the same INSERT...ON CONFLICT DO NOTHING RETURNING
pattern already used by alert-service for race-safe, at-least-once
-delivery-tolerant dedup. Kafka redelivering telemetry.processed after a
crash between detection and offset commit would otherwise re-run detection
and publish a second AnomalyDetected (with a new anomaly_id) for the same
underlying reading — this claim makes that a no-op instead.
"""

from __future__ import annotations

from psycopg_pool import ConnectionPool

# Namespaces this service's claims in the shared idempotency_keys table
# (see migrations/000008_idempotency_keys.up.sql) — used by any
# consumer/service that must guarantee "process this event_id/request
# exactly once".
ANOMALY_DEDUP_SCOPE = "anomaly_detection"


def claim_telemetry_event_once(pool: ConnectionPool, event_id: str) -> bool:
    """Returns True only the first time a given event_id is claimed."""
    with pool.connection() as conn:
        cur = conn.execute(
            "INSERT INTO idempotency_keys (key, scope) VALUES (%s, %s) "
            "ON CONFLICT (scope, key) DO NOTHING RETURNING id",
            (event_id, ANOMALY_DEDUP_SCOPE),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None
