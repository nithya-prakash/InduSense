"""Wire formats shared by every service that produces or consumes them over
MQTT/Kafka. This is the Python port of pkg/events (see the Go source for
history) — keeping one definition here, instead of each service declaring
its own copy, is what keeps schema drift between producer and consumer from
becoming a runtime surprise.

Models are added here as each service is ported (ingestion's TelemetryEvent/
MachineEvent, stream-processor's use of NormalizedTelemetryEvent,
anomaly-detector's AnomalyDetected, alert-service's AlertEvent) — see the
migration plan for the full list still to come (api).
"""

import json
from datetime import datetime, timezone

from pydantic import BaseModel, Field

# Metrics a sensor is allowed to report. Matches pkg/events.ValidMetrics
# exactly — ingestion's validation rejects anything not in this set.
VALID_METRICS = {
    "temperature",
    "vibration",
    "pressure",
    "rpm",
    "current",
    "voltage",
    "power",
    "humidity",
    "acoustic_level",
}

SCHEMA_VERSION = 1

ERROR_TYPE_VALIDATION = "VALIDATION_ERROR"
ERROR_TYPE_TRANSIENT = "TRANSIENT_ERROR"

SEVERITY_INFO = "INFO"
SEVERITY_WARNING = "WARNING"
SEVERITY_HIGH = "HIGH"
SEVERITY_CRITICAL = "CRITICAL"

ALERT_STATUS_OPEN = "OPEN"
ALERT_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
ALERT_STATUS_SUPPRESSED = "SUPPRESSED"
ALERT_STATUS_RESOLVED = "RESOLVED"

ALERT_EVENT_CREATED = "CREATED"
ALERT_EVENT_ESCALATED = "ESCALATED"
ALERT_EVENT_RESOLVED = "RESOLVED"


class TelemetryEvent(BaseModel):
    """Wire format published by the simulator on
    factory/{factory_id}/machine/{machine_id}/sensor/{sensor_id}/telemetry.
    """

    event_id: str = ""
    organization_id: str = ""
    factory_id: str = ""
    production_line_id: str = ""
    machine_id: str = ""
    device_id: str = ""
    sensor_id: str = ""
    timestamp: datetime | None = None
    sequence_number: int = 0
    metric: str = ""
    value: float = 0.0
    unit: str = ""


class NormalizedTelemetryEvent(TelemetryEvent):
    """What ingestion publishes to telemetry.raw: the original event plus
    metadata attached at the ingestion boundary."""

    correlation_id: str
    ingested_at: datetime
    schema_version: int = SCHEMA_VERSION


class MachineStatusEvent(BaseModel):
    """Published on factory/{factory_id}/machine/{machine_id}/status
    whenever a machine transitions between operating states. Deliberately
    narrower than MachineEvent (no device_id/event_type) -- the simulator
    is the only producer of this shape."""

    organization_id: str = ""
    factory_id: str = ""
    machine_id: str = ""
    status: str = ""
    timestamp: datetime | None = None


class MachineEvent(BaseModel):
    """Raw status/event message from the factory/{f}/machine/{m}/status or
    /events topics. Status and EventType share this shape (a status-topic
    message sets status, an events-topic message sets event_type) so
    ingestion can validate both uniformly.

    device_id/sensor_id/status/event_type match Go's `omitempty` JSON tags
    (pkg/events.MachineEvent) -- omitted from the wire format entirely when
    empty, not sent as ""  -- via the model_dump_json override below.
    Pydantic has no native per-field "omit if default" for JSON dumping, so
    this is done by hand rather than via a blanket exclude_defaults=True,
    which would also wrongly hide NormalizedMachineEvent's schema_version
    whenever it equals its own default of 1 (organization_id/factory_id/
    machine_id/timestamp have no omitempty in Go and must always appear,
    matching how they're never actually empty in practice either)."""

    organization_id: str = ""
    factory_id: str = ""
    machine_id: str = ""
    device_id: str = ""
    sensor_id: str = ""
    status: str = ""
    event_type: str = Field(default="", alias="event_type")
    timestamp: datetime | None = None

    model_config = {"populate_by_name": True}

    def model_dump_json(self, **kwargs) -> str:
        d = self.model_dump(mode="json")
        for optional_field in ("device_id", "sensor_id", "status", "event_type"):
            if not d.get(optional_field):
                d.pop(optional_field, None)
        return json.dumps(d, separators=(",", ":"))


class NormalizedMachineEvent(MachineEvent):
    """What ingestion publishes to device.events."""

    correlation_id: str
    ingested_at: datetime
    schema_version: int = SCHEMA_VERSION


class DeadLetterRecord(BaseModel):
    """Shared shape every service uses when routing a message to the
    dead-letter topic. There is no admin API for this topic — it's
    write-only from every service's perspective; inspect it with Kafka UI
    (http://localhost:8089) or kafka-console-consumer (see README.md,
    "Dead-letter queue")."""

    original_payload: str
    error: str
    error_type: str
    service: str
    processing_stage: str
    retry_count: int
    timestamp: datetime
    correlation_id: str
    source_topic: str


class AnomalyDetected(BaseModel):
    """Published on anomalies.detected by the anomaly detector.
    Severity/Score are the worst across whichever detection method(s)
    fired for this event; Methods lists all of them so downstream
    consumers (and humans) can see the full picture, not just the
    headline."""

    anomaly_id: str
    event_id: str
    organization_id: str
    factory_id: str
    production_line_id: str
    machine_id: str
    device_id: str
    sensor_id: str
    metric: str
    value: float
    severity: str
    score: float
    methods: list[str]
    reason: str
    detected_at: datetime


class AlertEvent(BaseModel):
    """Published to the `alerts` Kafka topic whenever the alert engine
    creates, escalates, or resolves an alert. event_type distinguishes
    which; the alert's current state (status/severity/escalation_level) is
    always the full current snapshot, not a diff."""

    alert_id: str
    event_type: str  # CREATED | ESCALATED | RESOLVED
    organization_id: str
    alert_rule_id: str = ""
    factory_id: str = ""
    machine_id: str = ""
    device_id: str = ""
    sensor_id: str = ""
    severity: str
    status: str
    title: str
    description: str
    escalation_level: int
    triggered_at: datetime
    timestamp: datetime


def utc_now() -> datetime:
    """time.Now().UTC() equivalent, used at every IngestedAt/Timestamp
    call site instead of a bare datetime.now() so the timezone is never
    ambiguous."""
    return datetime.now(timezone.utc)
