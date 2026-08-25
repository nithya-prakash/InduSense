package main

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nithya-prakash/indusense/pkg/events"
)

func validTelemetry() events.TelemetryEvent {
	return events.TelemetryEvent{
		EventID:          uuid.NewString(),
		OrganizationID:   uuid.NewString(),
		FactoryID:        uuid.NewString(),
		ProductionLineID: uuid.NewString(),
		MachineID:        uuid.NewString(),
		DeviceID:         uuid.NewString(),
		SensorID:         uuid.NewString(),
		Timestamp:        time.Now().UTC(),
		SequenceNumber:   1,
		Metric:           "temperature",
		Value:            42.5,
		Unit:             "celsius",
	}
}

func TestValidateTelemetryAcceptsWellFormedEvent(t *testing.T) {
	if err := validateTelemetry(validTelemetry()); err != nil {
		t.Fatalf("expected valid event to pass, got: %v", err)
	}
}

func TestValidateTelemetryRejectsBadUUID(t *testing.T) {
	e := validTelemetry()
	e.DeviceID = "not-a-uuid"
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for invalid device_id UUID")
	}
}

func TestValidateTelemetryRejectsMissingUUID(t *testing.T) {
	e := validTelemetry()
	e.SensorID = ""
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for empty sensor_id")
	}
}

func TestValidateTelemetryRejectsZeroTimestamp(t *testing.T) {
	e := validTelemetry()
	e.Timestamp = time.Time{}
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for zero-value timestamp")
	}
}

func TestValidateTelemetryRejectsZeroSequenceNumber(t *testing.T) {
	e := validTelemetry()
	e.SequenceNumber = 0
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for zero sequence_number")
	}
}

func TestValidateTelemetryRejectsUnknownMetric(t *testing.T) {
	e := validTelemetry()
	e.Metric = "banana_ripeness"
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for unknown metric")
	}
}

func TestValidateTelemetryRejectsNonFiniteValue(t *testing.T) {
	e := validTelemetry()
	e.Value = math.Inf(1)
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for +Inf value")
	}
}

func TestValidateTelemetryRejectsEmptyUnit(t *testing.T) {
	e := validTelemetry()
	e.Unit = ""
	if err := validateTelemetry(e); err == nil {
		t.Fatal("expected error for empty unit")
	}
}

func TestValidateMachineEventRequiresStatusOrEventType(t *testing.T) {
	e := events.MachineEvent{
		OrganizationID: uuid.NewString(),
		FactoryID:      uuid.NewString(),
		MachineID:      uuid.NewString(),
		Timestamp:      time.Now().UTC(),
	}
	if err := validateMachineEvent(e); err == nil {
		t.Fatal("expected error when both status and event_type are empty")
	}
	e.Status = "RUNNING"
	if err := validateMachineEvent(e); err != nil {
		t.Fatalf("expected valid event with status set, got: %v", err)
	}
}
