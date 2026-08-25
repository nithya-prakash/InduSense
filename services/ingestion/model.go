package main

import "time"

// RawTelemetryEvent mirrors the wire format published by the simulator on
// factory/{factory_id}/machine/{machine_id}/sensor/{sensor_id}/telemetry.
type RawTelemetryEvent struct {
	EventID        string  `json:"event_id"`
	OrganizationID string  `json:"organization_id"`
	FactoryID      string  `json:"factory_id"`
	MachineID      string  `json:"machine_id"`
	DeviceID       string  `json:"device_id"`
	SensorID       string  `json:"sensor_id"`
	Timestamp      string  `json:"timestamp"`
	SequenceNumber uint64  `json:"sequence_number"`
	Metric         string  `json:"metric"`
	Value          float64 `json:"value"`
	Unit           string  `json:"unit"`
}

// NormalizedTelemetryEvent is what ingestion publishes to telemetry.raw: the
// original event plus metadata attached at the ingestion boundary.
type NormalizedTelemetryEvent struct {
	RawTelemetryEvent
	CorrelationID string    `json:"correlation_id"`
	IngestedAt    time.Time `json:"ingested_at"`
	SchemaVersion int       `json:"schema_version"`
}

// RawMachineEvent mirrors both MachineStatusEvent and MachineEvent from the
// simulator (status/events topics share enough shape to validate uniformly;
// EventType is empty for status messages and Status is empty for event
// messages).
type RawMachineEvent struct {
	FactoryID string `json:"factory_id"`
	MachineID string `json:"machine_id"`
	DeviceID  string `json:"device_id,omitempty"`
	SensorID  string `json:"sensor_id,omitempty"`
	Status    string `json:"status,omitempty"`
	EventType string `json:"event_type,omitempty"`
	Timestamp string `json:"timestamp"`
}

type NormalizedMachineEvent struct {
	RawMachineEvent
	CorrelationID string    `json:"correlation_id"`
	IngestedAt    time.Time `json:"ingested_at"`
	SchemaVersion int       `json:"schema_version"`
}

// DeadLetterRecord is what's published to the dead-letter topic for any
// message that fails validation or exhausts Kafka publish retries.
type DeadLetterRecord struct {
	OriginalPayload string    `json:"original_payload"`
	Error           string    `json:"error"`
	ErrorType       string    `json:"error_type"`
	Service         string    `json:"service"`
	ProcessingStage string    `json:"processing_stage"`
	RetryCount      int       `json:"retry_count"`
	Timestamp       time.Time `json:"timestamp"`
	CorrelationID   string    `json:"correlation_id"`
	SourceTopic     string    `json:"source_topic"`
}

const schemaVersion = 1

const (
	errorTypeValidation = "VALIDATION_ERROR"
	errorTypeTransient  = "TRANSIENT_ERROR"
)
