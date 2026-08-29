"""Alert persistence, deduplication, cooldown, and escalation, the Python
port of store.go."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg_pool import ConnectionPool

CREATED = "created"
SUPPRESSED_OPEN = "suppressed_open"
SUPPRESSED_COOLDOWN = "suppressed_cooldown"

_SEVERITY_LADDER = ["WARNING", "HIGH", "CRITICAL"]


@dataclass
class Alert:
    organization_id: str
    severity: str
    factory_id: str = ""
    machine_id: str = ""
    device_id: str = ""
    sensor_id: str = ""
    title: str = ""
    description: str = ""
    id: str = ""
    status: str = ""
    triggered_at: datetime | None = None
    escalation_level: int = 0


class AlertStore:
    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    def create_if_due(self, rule_id: str, cooldown_seconds: int, alert: Alert, dedupe_key: str) -> tuple[str, Alert | None]:
        """Implements alert deduplication and cooldown: refuses to create a
        new alert if one for the same (rule, dedupe scope) is still
        open/unresolved, or if one resolved too recently (within
        cooldown_seconds) — the latter is what stops a flapping condition
        from re-paging someone every time it crosses the threshold."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT status, triggered_at FROM alerts WHERE alert_rule_id = %s AND dedupe_key = %s ORDER BY triggered_at DESC LIMIT 1",
                (rule_id, dedupe_key),
            ).fetchone()

            if row is not None:
                last_status, last_triggered_at = row
                if last_status != "RESOLVED":
                    return SUPPRESSED_OPEN, None
                if datetime.now(timezone.utc) - last_triggered_at < timedelta(seconds=cooldown_seconds):
                    return SUPPRESSED_COOLDOWN, None

            inserted = conn.execute(
                """
                INSERT INTO alerts (organization_id, alert_rule_id, factory_id, machine_id, device_id, sensor_id, severity, status, title, description, dedupe_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN', %s, %s, %s)
                ON CONFLICT (alert_rule_id, dedupe_key) WHERE status = 'OPEN' DO NOTHING
                RETURNING id, triggered_at
                """,
                (
                    alert.organization_id, rule_id, _null_if_empty(alert.factory_id), _null_if_empty(alert.machine_id),
                    _null_if_empty(alert.device_id), _null_if_empty(alert.sensor_id),
                    alert.severity, alert.title, alert.description, dedupe_key,
                ),
            ).fetchone()

        if inserted is None:
            # Lost a race with a concurrent insert for the same open alert.
            return SUPPRESSED_OPEN, None

        # id comes back as a uuid.UUID (not str) since the RETURNING clause
        # doesn't cast it -- same psycopg3 gotcha as elsewhere in this port.
        alert.id, alert.triggered_at = str(inserted[0]), inserted[1]
        alert.status = "OPEN"
        return CREATED, alert

    def due_for_escalation(self, after_seconds: int) -> list[Alert]:
        """Returns OPEN alerts that have gone unacknowledged past
        after_seconds since their last escalation (or since triggering, if
        never escalated), for the periodic escalation sweep."""
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, organization_id, severity, title, description,
                       COALESCE(machine_id::text, ''), COALESCE(device_id::text, ''),
                       triggered_at, escalation_level
                FROM alerts
                WHERE status = 'OPEN'
                  AND escalation_level < 2
                  AND COALESCE(last_escalated_at, triggered_at) < now() - make_interval(secs => %s)
                """,
                (after_seconds,),
            ).fetchall()

        return [
            Alert(
                id=str(r[0]), organization_id=str(r[1]), severity=r[2], title=r[3], description=r[4],
                machine_id=r[5], device_id=r[6], triggered_at=r[7], escalation_level=r[8],
            )
            for r in rows
        ]

    def escalate(self, alert_id: str, new_severity: str) -> None:
        """Bumps an alert's severity and escalation_level, for the caller
        to use in its re-notification."""
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE alerts SET severity = %s, escalation_level = escalation_level + 1, last_escalated_at = now(), updated_at = now() WHERE id = %s",
                (new_severity, alert_id),
            )


def next_severity(current: str) -> str:
    for i, s in enumerate(_SEVERITY_LADDER):
        if s == current and i + 1 < len(_SEVERITY_LADDER):
            return _SEVERITY_LADDER[i + 1]
    return current


def _null_if_empty(s: str) -> str | None:
    return s if s else None
