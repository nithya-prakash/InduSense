"""Device/sensor metadata catalog, the Python port of catalog.go.

Holds a periodically-refreshed snapshot of device metadata needed for
detection: each device's machine type (for grouping isolation forests) and
each sensor's operating range (for rule-based thresholds). It's kept in
memory and refreshed on a timer rather than queried per-event, since this
metadata changes rarely relative to telemetry volume.
"""

from __future__ import annotations

import threading

from psycopg_pool import ConnectionPool

from rules import MetricRange


class DeviceInfo:
    def __init__(self, machine_type: str):
        self.machine_type = machine_type
        self.ranges: dict[str, MetricRange] = {}


_CATALOG_QUERY = """
    SELECT d.id, m.machine_type, s.metric,
           COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
    FROM devices d
    JOIN machines m ON m.id = d.machine_id
    JOIN sensors s ON s.device_id = d.id
"""


class Catalog:
    """Opens this service's only Postgres connection pool, sized explicitly
    via max_conns rather than a library default (see
    ANOMALY_POSTGRES_MAX_CONNS in .env.example). Raises if the initial
    refresh fails, matching Go's newCatalog fail-fast startup behavior —
    the caller is expected to treat that as fatal."""

    def __init__(self, dsn: str, max_conns: int):
        self._lock = threading.Lock()
        self._devices: dict[str, DeviceInfo] = {}
        self._feature_order: dict[str, list[str]] = {}
        self._pool = ConnectionPool(dsn, min_size=1, max_size=max_conns, open=True)
        self.refresh()

    def close(self) -> None:
        self._pool.close()

    def pool(self) -> ConnectionPool:
        """Exposes the underlying pool for idempotency.py's claim query,
        which needs a raw connection outside the catalog's own read/refresh
        paths — mirrors Go's cat.pool field access from main.go (same
        package there; a small accessor here, since Python module
        boundaries make a bare attribute reach-through less idiomatic)."""
        return self._pool

    def ping(self) -> bool:
        try:
            with self._pool.connection(timeout=2.0) as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:  # noqa: BLE001
            return False

    def refresh(self) -> None:
        with self._pool.connection() as conn:
            rows = conn.execute(_CATALOG_QUERY).fetchall()

        devices: dict[str, DeviceInfo] = {}
        metrics_by_type: dict[str, set[str]] = {}

        for device_id, machine_type, metric, lo, hi in rows:
            # psycopg3 returns Postgres `uuid` columns as uuid.UUID objects,
            # not str — cast explicitly so lookup() (keyed by the plain
            # device_id string every event carries) actually matches.
            device_id = str(device_id)
            info = devices.get(device_id)
            if info is None:
                info = DeviceInfo(machine_type)
                devices[device_id] = info
            info.ranges[metric] = MetricRange(min=float(lo), max=float(hi))
            metrics_by_type.setdefault(machine_type, set()).add(metric)

        feature_order = {mt: sorted(metrics) for mt, metrics in metrics_by_type.items()}

        with self._lock:
            self._devices = devices
            self._feature_order = feature_order

    def lookup(self, device_id: str) -> DeviceInfo | None:
        with self._lock:
            return self._devices.get(device_id)

    def features_for(self, machine_type: str) -> list[str]:
        with self._lock:
            return self._feature_order.get(machine_type, [])
