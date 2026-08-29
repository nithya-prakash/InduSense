"""Sensor catalog loading, the Python port of loader.go."""

from __future__ import annotations

from psycopg_pool import ConnectionPool

from model import SensorCatalogEntry


def load_sensor_catalog(dsn: str, limit: int, max_conns: int) -> list[SensorCatalogEntry]:
    """Reads every sensor along with the full organization -> factory ->
    machine -> device path needed to build MQTT topics and telemetry
    events. max_conns is explicit rather than a library default, since
    this pool exists only for the one startup query below, not for
    sustained load."""
    pool = ConnectionPool(dsn, min_size=1, max_size=max_conns, open=True, timeout=30.0)
    try:
        with pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT o.id, f.id, pl.id, m.id, d.id, s.id, s.metric, s.unit,
                       COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
                FROM sensors s
                JOIN devices d ON d.id = s.device_id
                JOIN machines m ON m.id = d.machine_id
                JOIN production_lines pl ON pl.id = m.production_line_id
                JOIN factories f ON f.id = pl.factory_id
                JOIN organizations o ON o.id = f.organization_id
                ORDER BY s.id
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
    finally:
        pool.close()

    return [
        SensorCatalogEntry(
            organization_id=str(r[0]), factory_id=str(r[1]), production_line_id=str(r[2]),
            machine_id=str(r[3]), device_id=str(r[4]), sensor_id=str(r[5]),
            metric=r[6], unit=r[7], min_value=float(r[8]), max_value=float(r[9]),
        )
        for r in rows
    ]
