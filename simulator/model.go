package main

// SensorCatalogEntry describes one sensor and the full hierarchy path needed
// to build MQTT topics and telemetry events, as loaded from Postgres.
type SensorCatalogEntry struct {
	OrganizationID   string
	FactoryID        string
	ProductionLineID string
	MachineID        string
	DeviceID         string
	SensorID         string
	Metric           string
	Unit             string
	MinValue         float64
	MaxValue         float64
}
