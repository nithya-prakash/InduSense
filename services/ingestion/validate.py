"""Validation rules for inbound telemetry and machine events, the Python
port of validate.go. Same checks, same order, so the first violation named
in an error is identical between the two implementations.
"""

from __future__ import annotations

import math
import uuid

from shared.events import VALID_METRICS, MachineEvent, TelemetryEvent


def require_uuid(field: str, value: str) -> None:
    if not value:
        raise ValueError(f"{field} must not be empty")
    try:
        uuid.UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field} {value!r} is not a valid UUID: {exc}") from exc


def validate_telemetry(evt: TelemetryEvent) -> None:
    """Checks a raw telemetry event against the schema the ingestion
    boundary trusts downstream services to rely on. Raises ValueError
    naming the first violation found."""
    require_uuid("event_id", evt.event_id)
    require_uuid("organization_id", evt.organization_id)
    require_uuid("factory_id", evt.factory_id)
    require_uuid("production_line_id", evt.production_line_id)
    require_uuid("machine_id", evt.machine_id)
    require_uuid("device_id", evt.device_id)
    require_uuid("sensor_id", evt.sensor_id)
    if evt.timestamp is None:
        raise ValueError("timestamp must not be empty")
    if evt.sequence_number == 0:
        raise ValueError("sequence_number must be present and greater than zero")
    if evt.metric not in VALID_METRICS:
        raise ValueError(f"metric {evt.metric!r} is not a recognized sensor metric")
    if math.isnan(evt.value) or math.isinf(evt.value):
        raise ValueError(f"value must be a finite number, got {evt.value}")
    if not evt.unit:
        raise ValueError("unit must not be empty")


def validate_machine_event(evt: MachineEvent) -> None:
    """Checks a raw status/event message from the
    factory/{f}/machine/{m}/status or /events topics."""
    require_uuid("organization_id", evt.organization_id)
    require_uuid("factory_id", evt.factory_id)
    require_uuid("machine_id", evt.machine_id)
    if evt.timestamp is None:
        raise ValueError("timestamp must not be empty")
    if not evt.status and not evt.event_type:
        raise ValueError("machine event must set either status or event_type")
