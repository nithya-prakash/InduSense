import math
import uuid
from datetime import datetime, timezone

import pytest

from shared.events import MachineEvent, TelemetryEvent
from validate import validate_machine_event, validate_telemetry


def valid_telemetry() -> TelemetryEvent:
    return TelemetryEvent(
        event_id=str(uuid.uuid4()),
        organization_id=str(uuid.uuid4()),
        factory_id=str(uuid.uuid4()),
        production_line_id=str(uuid.uuid4()),
        machine_id=str(uuid.uuid4()),
        device_id=str(uuid.uuid4()),
        sensor_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc),
        sequence_number=1,
        metric="temperature",
        value=42.5,
        unit="celsius",
    )


def test_validate_telemetry_accepts_well_formed_event():
    validate_telemetry(valid_telemetry())


def test_validate_telemetry_rejects_bad_uuid():
    evt = valid_telemetry()
    evt.device_id = "not-a-uuid"
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_telemetry_rejects_missing_uuid():
    evt = valid_telemetry()
    evt.sensor_id = ""
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_telemetry_rejects_missing_timestamp():
    evt = valid_telemetry()
    evt.timestamp = None
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_telemetry_rejects_zero_sequence_number():
    evt = valid_telemetry()
    evt.sequence_number = 0
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_telemetry_rejects_unknown_metric():
    evt = valid_telemetry()
    evt.metric = "banana_ripeness"
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_telemetry_rejects_non_finite_value():
    evt = valid_telemetry()
    evt.value = math.inf
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_telemetry_rejects_empty_unit():
    evt = valid_telemetry()
    evt.unit = ""
    with pytest.raises(ValueError):
        validate_telemetry(evt)


def test_validate_machine_event_requires_status_or_event_type():
    evt = MachineEvent(
        organization_id=str(uuid.uuid4()),
        factory_id=str(uuid.uuid4()),
        machine_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc),
    )
    with pytest.raises(ValueError):
        validate_machine_event(evt)

    evt.status = "RUNNING"
    validate_machine_event(evt)
