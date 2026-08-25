package events

import "time"

// AlertEvent is published to the `alerts` Kafka topic whenever the alert
// engine creates, escalates, or resolves an alert. EventType distinguishes
// which; the alert's current state (status/severity/escalation_level) is
// always the full current snapshot, not a diff.
type AlertEvent struct {
	AlertID         string    `json:"alert_id"`
	EventType       string    `json:"event_type"` // CREATED | ESCALATED | RESOLVED
	OrganizationID  string    `json:"organization_id"`
	AlertRuleID     string    `json:"alert_rule_id,omitempty"`
	FactoryID       string    `json:"factory_id,omitempty"`
	MachineID       string    `json:"machine_id,omitempty"`
	DeviceID        string    `json:"device_id,omitempty"`
	SensorID        string    `json:"sensor_id,omitempty"`
	Severity        string    `json:"severity"`
	Status          string    `json:"status"`
	Title           string    `json:"title"`
	Description     string    `json:"description"`
	EscalationLevel int       `json:"escalation_level"`
	TriggeredAt     time.Time `json:"triggered_at"`
	Timestamp       time.Time `json:"timestamp"`
}

const (
	AlertStatusOpen         = "OPEN"
	AlertStatusAcknowledged = "ACKNOWLEDGED"
	AlertStatusSuppressed   = "SUPPRESSED"
	AlertStatusResolved     = "RESOLVED"
)

const (
	AlertEventCreated   = "CREATED"
	AlertEventEscalated = "ESCALATED"
	AlertEventResolved  = "RESOLVED"
)
