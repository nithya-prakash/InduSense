"""Alert rule cache, the Python port of rules.go."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from psycopg_pool import ConnectionPool


@dataclass
class AlertRule:
    id: str = ""
    organization_id: str = ""
    name: str = ""
    metric: str = ""
    condition: str = ""  # GREATER_THAN | LESS_THAN | OUTSIDE_RANGE | ANOMALY_COUNT
    threshold_value: float | None = None
    threshold_min: float | None = None
    threshold_max: float | None = None
    severity: str = ""
    cooldown_seconds: int = 0
    window_seconds: int = 0
    machine_id: str | None = None
    device_id: str | None = None
    sensor_id: str | None = None

    def scope_matches(self, machine_id: str, device_id: str, sensor_id: str) -> bool:
        """Reports whether this rule's optional machine/device/sensor
        scoping (None = wildcard) matches the given anomaly's
        identifiers."""
        if self.machine_id is not None and self.machine_id != machine_id:
            return False
        if self.device_id is not None and self.device_id != device_id:
            return False
        if self.sensor_id is not None and self.sensor_id != sensor_id:
            return False
        return True


class RuleCache:
    """Holds a periodically-refreshed snapshot of active alert rules,
    grouped by (organization_id, metric) for fast lookup per incoming
    anomaly — rules are re-read on a timer rather than per-event since
    they change far less often than telemetry arrives.

    Reuses the caller's pool rather than opening its own — alert-service's
    Go version used to open a second, independently-sized pool here purely
    to run this cache's periodic refresh query, doubling its Postgres
    connection footprint for no reason.
    """

    def __init__(self, pool: ConnectionPool):
        self._lock = threading.Lock()
        self._by_key: dict[str, list[AlertRule]] = {}
        self._pool = pool
        self.refresh()

    def refresh(self) -> None:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, organization_id, name, metric, condition,
                       threshold_value, threshold_min, threshold_max,
                       severity, cooldown_seconds, window_seconds,
                       machine_id, device_id, sensor_id
                FROM alert_rules
                WHERE is_active = true
                """
            ).fetchall()

        by_key: dict[str, list[AlertRule]] = {}
        for row in rows:
            rule = AlertRule(
                id=str(row[0]), organization_id=str(row[1]), name=row[2], metric=row[3], condition=row[4],
                # threshold_* columns are Postgres `numeric`, which psycopg3
                # returns as decimal.Decimal, not float -- cast explicitly
                # (same lesson as anomaly-detector's catalog.py UUID gotcha).
                threshold_value=float(row[5]) if row[5] is not None else None,
                threshold_min=float(row[6]) if row[6] is not None else None,
                threshold_max=float(row[7]) if row[7] is not None else None,
                severity=row[8], cooldown_seconds=row[9], window_seconds=row[10],
                machine_id=str(row[11]) if row[11] is not None else None,
                device_id=str(row[12]) if row[12] is not None else None,
                sensor_id=str(row[13]) if row[13] is not None else None,
            )
            key = f"{rule.organization_id}|{rule.metric}"
            by_key.setdefault(key, []).append(rule)

        with self._lock:
            self._by_key = by_key

    def rules_for(self, org_id: str, metric: str) -> list[AlertRule]:
        with self._lock:
            return self._by_key.get(f"{org_id}|{metric}", [])
