"""SensorCatalogEntry, the Python port of model.go."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SensorCatalogEntry:
    """Describes one sensor and the full hierarchy path needed to build
    MQTT topics and telemetry events, as loaded from Postgres."""

    organization_id: str
    factory_id: str
    production_line_id: str
    machine_id: str
    device_id: str
    sensor_id: str
    metric: str
    unit: str
    min_value: float
    max_value: float
