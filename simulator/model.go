package main

import "time"

// TelemetryEvent is the wire format published on
// factory/{factory_id}/machine/{machine_id}/sensor/{sensor_id}/telemetry.
type TelemetryEvent struct {
	EventID        string    `json:"event_id"`
	OrganizationID string    `json:"organization_id"`
	FactoryID      string    `json:"factory_id"`
	MachineID      string    `json:"machine_id"`
	DeviceID       string    `json:"device_id"`
	SensorID       string    `json:"sensor_id"`
	Timestamp      time.Time `json:"timestamp"`
	SequenceNumber uint64    `json:"sequence_number"`
	Metric         string    `json:"metric"`
	Value          float64   `json:"value"`
	Unit           string    `json:"unit"`
}

// MachineStatusEvent is published on factory/{factory_id}/machine/{machine_id}/status
// whenever a machine transitions between operating states.
type MachineStatusEvent struct {
	FactoryID string    `json:"factory_id"`
	MachineID string    `json:"machine_id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// MachineEvent is published on factory/{factory_id}/machine/{machine_id}/events
// for out-of-band occurrences such as sensor failure.
type MachineEvent struct {
	FactoryID string    `json:"factory_id"`
	MachineID string    `json:"machine_id"`
	DeviceID  string    `json:"device_id"`
	SensorID  string    `json:"sensor_id,omitempty"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
}

// SensorCatalogEntry describes one sensor and the full hierarchy path needed
// to build MQTT topics and telemetry events, as loaded from Postgres.
type SensorCatalogEntry struct {
	OrganizationID string
	FactoryID      string
	MachineID      string
	DeviceID       string
	SensorID       string
	Metric         string
	Unit           string
	MinValue       float64
	MaxValue       float64
}
