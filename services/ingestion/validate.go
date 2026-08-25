package main

import (
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

var validMetrics = map[string]bool{
	"temperature":    true,
	"vibration":      true,
	"pressure":       true,
	"rpm":            true,
	"current":        true,
	"voltage":        true,
	"power":          true,
	"humidity":       true,
	"acoustic_level": true,
}

// validateTelemetry checks a raw telemetry event against the schema the
// ingestion boundary trusts downstream services to rely on. It returns a
// descriptive error naming the first violation found.
func validateTelemetry(e RawTelemetryEvent) error {
	if err := requireUUID("event_id", e.EventID); err != nil {
		return err
	}
	if err := requireUUID("organization_id", e.OrganizationID); err != nil {
		return err
	}
	if err := requireUUID("factory_id", e.FactoryID); err != nil {
		return err
	}
	if err := requireUUID("machine_id", e.MachineID); err != nil {
		return err
	}
	if err := requireUUID("device_id", e.DeviceID); err != nil {
		return err
	}
	if err := requireUUID("sensor_id", e.SensorID); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Timestamp); err != nil {
		return fmt.Errorf("timestamp %q is not a valid RFC3339 timestamp: %w", e.Timestamp, err)
	}
	if e.SequenceNumber == 0 {
		return fmt.Errorf("sequence_number must be present and greater than zero")
	}
	if !validMetrics[e.Metric] {
		return fmt.Errorf("metric %q is not a recognized sensor metric", e.Metric)
	}
	if math.IsNaN(e.Value) || math.IsInf(e.Value, 0) {
		return fmt.Errorf("value must be a finite number, got %v", e.Value)
	}
	if e.Unit == "" {
		return fmt.Errorf("unit must not be empty")
	}
	return nil
}

// validateMachineEvent checks a raw status/event message from the
// factory/{f}/machine/{m}/status or /events topics.
func validateMachineEvent(e RawMachineEvent) error {
	if err := requireUUID("factory_id", e.FactoryID); err != nil {
		return err
	}
	if err := requireUUID("machine_id", e.MachineID); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Timestamp); err != nil {
		return fmt.Errorf("timestamp %q is not a valid RFC3339 timestamp: %w", e.Timestamp, err)
	}
	if e.Status == "" && e.EventType == "" {
		return fmt.Errorf("machine event must set either status or event_type")
	}
	return nil
}

func requireUUID(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("%s %q is not a valid UUID: %w", field, value, err)
	}
	return nil
}
