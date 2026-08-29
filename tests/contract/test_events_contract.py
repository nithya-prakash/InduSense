"""Locks the JSON wire format of every event type in shared.events against
a fixed expected payload. These events cross a serialization boundary
between independently-deployable services with no schema registry
enforcing compatibility -- a renamed or retyped field would work fine in
isolation and only surface as a runtime validation failure or silent
default value in another service. These tests turn that into an
immediate, local test failure instead.

Python port of tests/contract/events_contract_test.go. The expected JSON
strings here are NOT copy-pasted from the Go version: Pydantic's own
serializer has two real, permanent differences from Go's encoding/json --
a whole-number float always renders with a decimal point (150.0, not 150),
and MachineEvent's omitempty-equivalent fields are handled by a hand-written
override (see shared/events.py) rather than a struct tag. Both are
verified, deliberate properties of the actual Python wire format now that
Python is the only producer, not something to contort into matching Go's
old output.

Each test round-trips in both directions: model -> JSON is compared against
a fixed expected string (catches an accidental field rename, removal, or
added field), and that same fixed string -> model -> JSON is compared
against the original (catches a type change that would still serialize to
similar-looking JSON but silently fail to parse an older message shape).
"""

from __future__ import annotations

from datetime import datetime, timezone

from shared.events import (
    ALERT_EVENT_CREATED,
    ALERT_STATUS_OPEN,
    ERROR_TYPE_VALIDATION,
    SEVERITY_CRITICAL,
    AlertEvent,
    AnomalyDetected,
    DeadLetterRecord,
    MachineEvent,
    NormalizedMachineEvent,
    NormalizedTelemetryEvent,
)


def fixed_time() -> datetime:
    return datetime(2026, 3, 15, 10, 30, 0, tzinfo=timezone.utc)


def round_trip(name: str, model, want: str) -> None:
    got = model.model_dump_json()
    assert got == want, f"{name}: JSON wire format changed.\n got:  {got}\n want: {want}"

    decoded = type(model).model_validate_json(want)
    redecoded = decoded.model_dump_json()
    assert redecoded == want, f"{name}: decoding the contract payload and re-encoding it lost information.\n got:  {redecoded}\n want: {want}"


def test_normalized_telemetry_event_contract():
    evt = NormalizedTelemetryEvent(
        event_id="11111111-1111-1111-1111-111111111111",
        organization_id="22222222-2222-2222-2222-222222222222",
        factory_id="33333333-3333-3333-3333-333333333333",
        production_line_id="44444444-4444-4444-4444-444444444444",
        machine_id="55555555-5555-5555-5555-555555555555",
        device_id="66666666-6666-6666-6666-666666666666",
        sensor_id="77777777-7777-7777-7777-777777777777",
        timestamp=fixed_time(),
        sequence_number=42,
        metric="temperature",
        value=73.5,
        unit="celsius",
        correlation_id="11111111-1111-1111-1111-111111111111",
        ingested_at=fixed_time(),
        schema_version=1,
    )

    want = (
        '{"event_id":"11111111-1111-1111-1111-111111111111","organization_id":"22222222-2222-2222-2222-222222222222",'
        '"factory_id":"33333333-3333-3333-3333-333333333333","production_line_id":"44444444-4444-4444-4444-444444444444",'
        '"machine_id":"55555555-5555-5555-5555-555555555555","device_id":"66666666-6666-6666-6666-666666666666",'
        '"sensor_id":"77777777-7777-7777-7777-777777777777","timestamp":"2026-03-15T10:30:00Z","sequence_number":42,'
        '"metric":"temperature","value":73.5,"unit":"celsius","correlation_id":"11111111-1111-1111-1111-111111111111",'
        '"ingested_at":"2026-03-15T10:30:00Z","schema_version":1}'
    )

    round_trip("NormalizedTelemetryEvent", evt, want)


def test_normalized_machine_event_contract():
    evt = NormalizedMachineEvent(
        organization_id="22222222-2222-2222-2222-222222222222",
        factory_id="33333333-3333-3333-3333-333333333333",
        machine_id="55555555-5555-5555-5555-555555555555",
        device_id="66666666-6666-6666-6666-666666666666",
        event_type="MACHINE_STOPPED",
        timestamp=fixed_time(),
        correlation_id="88888888-8888-8888-8888-888888888888",
        ingested_at=fixed_time(),
        schema_version=1,
    )

    # sensor_id/status are omitted entirely (empty, and MachineEvent's
    # omitempty-equivalent override drops them) -- unlike Go's fixed
    # string, which also omits them because Go's struct simply never
    # unmarshals them as present either way. Same observable contract,
    # confirmed by the round trip below re-encoding to this exact string.
    want = (
        '{"organization_id":"22222222-2222-2222-2222-222222222222","factory_id":"33333333-3333-3333-3333-333333333333",'
        '"machine_id":"55555555-5555-5555-5555-555555555555","device_id":"66666666-6666-6666-6666-666666666666",'
        '"event_type":"MACHINE_STOPPED","timestamp":"2026-03-15T10:30:00Z",'
        '"correlation_id":"88888888-8888-8888-8888-888888888888","ingested_at":"2026-03-15T10:30:00Z","schema_version":1}'
    )

    round_trip("NormalizedMachineEvent", evt, want)


def test_machine_event_omits_empty_optional_fields():
    """Direct regression test for the real bug this contract-test port
    caught: MachineEvent's device_id/sensor_id/status/event_type match
    Go's `omitempty` JSON tags, so an empty one must be left out of the
    wire format entirely, not sent as "" -- ingestion and simulator both
    publish this shape, so a lapse here would make every message larger
    (and, if a naive consumer ever distinguished "empty" from "absent",
    wrong) than the deployed Go version ever produced."""
    evt = MachineEvent(
        organization_id="org", factory_id="fac", machine_id="mac",
        status="RUNNING", timestamp=fixed_time(),
    )
    got = evt.model_dump_json()
    assert '"device_id"' not in got
    assert '"sensor_id"' not in got
    assert '"event_type"' not in got
    assert '"status":"RUNNING"' in got


def test_anomaly_detected_contract():
    evt = AnomalyDetected(
        anomaly_id="99999999-9999-9999-9999-999999999999",
        event_id="11111111-1111-1111-1111-111111111111",
        organization_id="22222222-2222-2222-2222-222222222222",
        factory_id="33333333-3333-3333-3333-333333333333",
        production_line_id="44444444-4444-4444-4444-444444444444",
        machine_id="55555555-5555-5555-5555-555555555555",
        device_id="66666666-6666-6666-6666-666666666666",
        sensor_id="77777777-7777-7777-7777-777777777777",
        metric="temperature",
        value=150,
        severity=SEVERITY_CRITICAL,
        score=0.91,
        methods=["RULE", "ISOLATION_FOREST"],
        reason="value exceeds operating range",
        detected_at=fixed_time(),
    )

    # value is 150.0, not 150 -- Pydantic always renders a float field with
    # a decimal point, unlike Go's encoding/json for a float64 holding a
    # whole number. A real, permanent, harmless difference: every consumer
    # here is Python too, and json.loads("150.0") == json.loads("150").
    want = (
        '{"anomaly_id":"99999999-9999-9999-9999-999999999999","event_id":"11111111-1111-1111-1111-111111111111",'
        '"organization_id":"22222222-2222-2222-2222-222222222222","factory_id":"33333333-3333-3333-3333-333333333333",'
        '"production_line_id":"44444444-4444-4444-4444-444444444444","machine_id":"55555555-5555-5555-5555-555555555555",'
        '"device_id":"66666666-6666-6666-6666-666666666666","sensor_id":"77777777-7777-7777-7777-777777777777",'
        '"metric":"temperature","value":150.0,"severity":"CRITICAL","score":0.91,"methods":["RULE","ISOLATION_FOREST"],'
        '"reason":"value exceeds operating range","detected_at":"2026-03-15T10:30:00Z"}'
    )

    round_trip("AnomalyDetected", evt, want)


def test_alert_event_contract():
    evt = AlertEvent(
        alert_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        event_type=ALERT_EVENT_CREATED,
        organization_id="22222222-2222-2222-2222-222222222222",
        alert_rule_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        factory_id="33333333-3333-3333-3333-333333333333",
        machine_id="55555555-5555-5555-5555-555555555555",
        device_id="66666666-6666-6666-6666-666666666666",
        sensor_id="77777777-7777-7777-7777-777777777777",
        severity=SEVERITY_CRITICAL,
        status=ALERT_STATUS_OPEN,
        title="High temperature",
        description="temperature 150.00 exceeds threshold 90.00",
        escalation_level=0,
        triggered_at=fixed_time(),
        timestamp=fixed_time(),
    )

    want = (
        '{"alert_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","event_type":"CREATED",'
        '"organization_id":"22222222-2222-2222-2222-222222222222","alert_rule_id":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",'
        '"factory_id":"33333333-3333-3333-3333-333333333333","machine_id":"55555555-5555-5555-5555-555555555555",'
        '"device_id":"66666666-6666-6666-6666-666666666666","sensor_id":"77777777-7777-7777-7777-777777777777",'
        '"severity":"CRITICAL","status":"OPEN","title":"High temperature",'
        '"description":"temperature 150.00 exceeds threshold 90.00","escalation_level":0,'
        '"triggered_at":"2026-03-15T10:30:00Z","timestamp":"2026-03-15T10:30:00Z"}'
    )

    round_trip("AlertEvent", evt, want)


def test_dead_letter_record_contract():
    rec = DeadLetterRecord(
        original_payload='{"malformed": true',
        error="unexpected end of JSON input",
        error_type=ERROR_TYPE_VALIDATION,
        service="ingestion",
        processing_stage="validation",
        retry_count=0,
        timestamp=fixed_time(),
        correlation_id="11111111-1111-1111-1111-111111111111",
        source_topic="factory/1/machine/1/sensor/1/telemetry",
    )

    want = (
        '{"original_payload":"{\\"malformed\\": true","error":"unexpected end of JSON input","error_type":"VALIDATION_ERROR",'
        '"service":"ingestion","processing_stage":"validation","retry_count":0,"timestamp":"2026-03-15T10:30:00Z",'
        '"correlation_id":"11111111-1111-1111-1111-111111111111","source_topic":"factory/1/machine/1/sensor/1/telemetry"}'
    )

    round_trip("DeadLetterRecord", rec, want)
