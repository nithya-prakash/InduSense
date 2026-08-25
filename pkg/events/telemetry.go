// Package events defines the wire formats shared by every service that
// produces or consumes them over MQTT/Kafka: the simulator, ingestion, the
// stream processor, and beyond. Keeping one definition here (instead of each
// service re-declaring its own copy) is what keeps schema drift between
// producer and consumer from becoming a runtime surprise.
package events

import "time"

// TelemetryEvent is the wire format published by the simulator on
// factory/{factory_id}/machine/{machine_id}/sensor/{sensor_id}/telemetry and
// forwarded (as NormalizedTelemetryEvent) to the telemetry.raw Kafka topic.
type TelemetryEvent struct {
	EventID          string    `json:"event_id"`
	OrganizationID   string    `json:"organization_id"`
	FactoryID        string    `json:"factory_id"`
	ProductionLineID string    `json:"production_line_id"`
	MachineID        string    `json:"machine_id"`
	DeviceID         string    `json:"device_id"`
	SensorID         string    `json:"sensor_id"`
	Timestamp        time.Time `json:"timestamp"`
	SequenceNumber   uint64    `json:"sequence_number"`
	Metric           string    `json:"metric"`
	Value            float64   `json:"value"`
	Unit             string    `json:"unit"`
}

// NormalizedTelemetryEvent is what ingestion publishes to telemetry.raw: the
// original event plus metadata attached at the ingestion boundary.
type NormalizedTelemetryEvent struct {
	TelemetryEvent
	CorrelationID string    `json:"correlation_id"`
	IngestedAt    time.Time `json:"ingested_at"`
	SchemaVersion int       `json:"schema_version"`
}

// MachineStatusEvent is published on factory/{factory_id}/machine/{machine_id}/status
// whenever a machine transitions between operating states.
type MachineStatusEvent struct {
	OrganizationID string    `json:"organization_id"`
	FactoryID      string    `json:"factory_id"`
	MachineID      string    `json:"machine_id"`
	Status         string    `json:"status"`
	Timestamp      time.Time `json:"timestamp"`
}

// MachineEvent is published on factory/{factory_id}/machine/{machine_id}/events
// for out-of-band occurrences such as sensor failure. Status and EventType
// share this shape (a status-topic message sets Status, an events-topic
// message sets EventType) so ingestion can validate both uniformly.
type MachineEvent struct {
	OrganizationID string    `json:"organization_id"`
	FactoryID      string    `json:"factory_id"`
	MachineID      string    `json:"machine_id"`
	DeviceID       string    `json:"device_id,omitempty"`
	SensorID       string    `json:"sensor_id,omitempty"`
	Status         string    `json:"status,omitempty"`
	EventType      string    `json:"event_type,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// NormalizedMachineEvent is what ingestion publishes to device.events.
type NormalizedMachineEvent struct {
	MachineEvent
	CorrelationID string    `json:"correlation_id"`
	IngestedAt    time.Time `json:"ingested_at"`
	SchemaVersion int       `json:"schema_version"`
}

// DeadLetterRecord is the shared shape every service uses when routing a
// message to the dead-letter topic (see docs on the DLQ admin API, Phase 17).
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

// AnomalyDetected is published on anomalies.detected by the anomaly
// detector. Severity/Score are the worst across whichever detection
// method(s) fired for this event; Methods lists all of them so downstream
// consumers (and humans) can see the full picture, not just the headline.
type AnomalyDetected struct {
	AnomalyID        string    `json:"anomaly_id"`
	EventID          string    `json:"event_id"`
	OrganizationID   string    `json:"organization_id"`
	FactoryID        string    `json:"factory_id"`
	ProductionLineID string    `json:"production_line_id"`
	MachineID        string    `json:"machine_id"`
	DeviceID         string    `json:"device_id"`
	SensorID         string    `json:"sensor_id"`
	Metric           string    `json:"metric"`
	Value            float64   `json:"value"`
	Severity         string    `json:"severity"`
	Score            float64   `json:"score"`
	Methods          []string  `json:"methods"`
	Reason           string    `json:"reason"`
	DetectedAt       time.Time `json:"detected_at"`
}

const (
	SeverityInfo     = "INFO"
	SeverityWarning  = "WARNING"
	SeverityHigh     = "HIGH"
	SeverityCritical = "CRITICAL"
)

const SchemaVersion = 1

const (
	ErrorTypeValidation = "VALIDATION_ERROR"
	ErrorTypeTransient  = "TRANSIENT_ERROR"
)

var ValidMetrics = map[string]bool{
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
