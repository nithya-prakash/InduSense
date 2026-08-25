// Package contract locks the JSON wire format of every event type in
// pkg/events against a fixed expected payload. These events cross a
// serialization boundary between independently-deployable services
// (ingestion publishes what stream-processor/anomaly-detector/alert-service
// consume) with no schema registry enforcing compatibility — a renamed or
// retyped field would compile fine in isolation and only surface as a
// runtime unmarshal failure or silent zero-value in another service. These
// tests turn that into an immediate, local test failure instead.
//
// Each test round-trips in both directions: struct -> JSON is compared
// against a fixed expected string (catches an accidental field rename,
// removal, or added field), and that same fixed string -> struct is
// compared against the original struct (catches a type change that would
// still marshal to similar-looking JSON but silently fail to decode an
// older message shape).
package contract

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nithya-prakash/indusense/pkg/events"
)

func fixedTime() time.Time {
	return time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
}

func roundTrip[T any](t *testing.T, name string, value T, want string) {
	t.Helper()

	got, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("%s: marshal: %v", name, err)
	}
	if string(got) != want {
		t.Fatalf("%s: JSON wire format changed.\n got:  %s\n want: %s", name, got, want)
	}

	var decoded T
	if err := json.Unmarshal([]byte(want), &decoded); err != nil {
		t.Fatalf("%s: unmarshal fixed contract payload: %v", name, err)
	}
	redecoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("%s: re-marshal decoded value: %v", name, err)
	}
	if string(redecoded) != want {
		t.Fatalf("%s: decoding the contract payload and re-encoding it lost information.\n got:  %s\n want: %s", name, redecoded, want)
	}
}

func TestNormalizedTelemetryEventContract(t *testing.T) {
	evt := events.NormalizedTelemetryEvent{
		TelemetryEvent: events.TelemetryEvent{
			EventID:          "11111111-1111-1111-1111-111111111111",
			OrganizationID:   "22222222-2222-2222-2222-222222222222",
			FactoryID:        "33333333-3333-3333-3333-333333333333",
			ProductionLineID: "44444444-4444-4444-4444-444444444444",
			MachineID:        "55555555-5555-5555-5555-555555555555",
			DeviceID:         "66666666-6666-6666-6666-666666666666",
			SensorID:         "77777777-7777-7777-7777-777777777777",
			Timestamp:        fixedTime(),
			SequenceNumber:   42,
			Metric:           "temperature",
			Value:            73.5,
			Unit:             "celsius",
		},
		CorrelationID: "11111111-1111-1111-1111-111111111111",
		IngestedAt:    fixedTime(),
		SchemaVersion: 1,
	}

	want := `{"event_id":"11111111-1111-1111-1111-111111111111","organization_id":"22222222-2222-2222-2222-222222222222","factory_id":"33333333-3333-3333-3333-333333333333","production_line_id":"44444444-4444-4444-4444-444444444444","machine_id":"55555555-5555-5555-5555-555555555555","device_id":"66666666-6666-6666-6666-666666666666","sensor_id":"77777777-7777-7777-7777-777777777777","timestamp":"2026-03-15T10:30:00Z","sequence_number":42,"metric":"temperature","value":73.5,"unit":"celsius","correlation_id":"11111111-1111-1111-1111-111111111111","ingested_at":"2026-03-15T10:30:00Z","schema_version":1}`

	roundTrip(t, "NormalizedTelemetryEvent", evt, want)
}

func TestNormalizedMachineEventContract(t *testing.T) {
	evt := events.NormalizedMachineEvent{
		MachineEvent: events.MachineEvent{
			OrganizationID: "22222222-2222-2222-2222-222222222222",
			FactoryID:      "33333333-3333-3333-3333-333333333333",
			MachineID:      "55555555-5555-5555-5555-555555555555",
			DeviceID:       "66666666-6666-6666-6666-666666666666",
			EventType:      "MACHINE_STOPPED",
			Timestamp:      fixedTime(),
		},
		CorrelationID: "88888888-8888-8888-8888-888888888888",
		IngestedAt:    fixedTime(),
		SchemaVersion: 1,
	}

	want := `{"organization_id":"22222222-2222-2222-2222-222222222222","factory_id":"33333333-3333-3333-3333-333333333333","machine_id":"55555555-5555-5555-5555-555555555555","device_id":"66666666-6666-6666-6666-666666666666","event_type":"MACHINE_STOPPED","timestamp":"2026-03-15T10:30:00Z","correlation_id":"88888888-8888-8888-8888-888888888888","ingested_at":"2026-03-15T10:30:00Z","schema_version":1}`

	roundTrip(t, "NormalizedMachineEvent", evt, want)
}

func TestAnomalyDetectedContract(t *testing.T) {
	evt := events.AnomalyDetected{
		AnomalyID:        "99999999-9999-9999-9999-999999999999",
		EventID:          "11111111-1111-1111-1111-111111111111",
		OrganizationID:   "22222222-2222-2222-2222-222222222222",
		FactoryID:        "33333333-3333-3333-3333-333333333333",
		ProductionLineID: "44444444-4444-4444-4444-444444444444",
		MachineID:        "55555555-5555-5555-5555-555555555555",
		DeviceID:         "66666666-6666-6666-6666-666666666666",
		SensorID:         "77777777-7777-7777-7777-777777777777",
		Metric:           "temperature",
		Value:            150,
		Severity:         events.SeverityCritical,
		Score:            0.91,
		Methods:          []string{"RULE", "ISOLATION_FOREST"},
		Reason:           "value exceeds operating range",
		DetectedAt:       fixedTime(),
	}

	want := `{"anomaly_id":"99999999-9999-9999-9999-999999999999","event_id":"11111111-1111-1111-1111-111111111111","organization_id":"22222222-2222-2222-2222-222222222222","factory_id":"33333333-3333-3333-3333-333333333333","production_line_id":"44444444-4444-4444-4444-444444444444","machine_id":"55555555-5555-5555-5555-555555555555","device_id":"66666666-6666-6666-6666-666666666666","sensor_id":"77777777-7777-7777-7777-777777777777","metric":"temperature","value":150,"severity":"CRITICAL","score":0.91,"methods":["RULE","ISOLATION_FOREST"],"reason":"value exceeds operating range","detected_at":"2026-03-15T10:30:00Z"}`

	roundTrip(t, "AnomalyDetected", evt, want)
}

func TestAlertEventContract(t *testing.T) {
	evt := events.AlertEvent{
		AlertID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		EventType:       events.AlertEventCreated,
		OrganizationID:  "22222222-2222-2222-2222-222222222222",
		AlertRuleID:     "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		FactoryID:       "33333333-3333-3333-3333-333333333333",
		MachineID:       "55555555-5555-5555-5555-555555555555",
		DeviceID:        "66666666-6666-6666-6666-666666666666",
		SensorID:        "77777777-7777-7777-7777-777777777777",
		Severity:        events.SeverityCritical,
		Status:          events.AlertStatusOpen,
		Title:           "High temperature",
		Description:     "temperature 150.00 exceeds threshold 90.00",
		EscalationLevel: 0,
		TriggeredAt:     fixedTime(),
		Timestamp:       fixedTime(),
	}

	want := `{"alert_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","event_type":"CREATED","organization_id":"22222222-2222-2222-2222-222222222222","alert_rule_id":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb","factory_id":"33333333-3333-3333-3333-333333333333","machine_id":"55555555-5555-5555-5555-555555555555","device_id":"66666666-6666-6666-6666-666666666666","sensor_id":"77777777-7777-7777-7777-777777777777","severity":"CRITICAL","status":"OPEN","title":"High temperature","description":"temperature 150.00 exceeds threshold 90.00","escalation_level":0,"triggered_at":"2026-03-15T10:30:00Z","timestamp":"2026-03-15T10:30:00Z"}`

	roundTrip(t, "AlertEvent", evt, want)
}

func TestDeadLetterRecordContract(t *testing.T) {
	rec := events.DeadLetterRecord{
		OriginalPayload: `{"malformed": true`,
		Error:           "unexpected end of JSON input",
		ErrorType:       events.ErrorTypeValidation,
		Service:         "ingestion",
		ProcessingStage: "validation",
		RetryCount:      0,
		Timestamp:       fixedTime(),
		CorrelationID:   "11111111-1111-1111-1111-111111111111",
		SourceTopic:     "factory/1/machine/1/sensor/1/telemetry",
	}

	want := `{"original_payload":"{\"malformed\": true","error":"unexpected end of JSON input","error_type":"VALIDATION_ERROR","service":"ingestion","processing_stage":"validation","retry_count":0,"timestamp":"2026-03-15T10:30:00Z","correlation_id":"11111111-1111-1111-1111-111111111111","source_topic":"factory/1/machine/1/sensor/1/telemetry"}`

	roundTrip(t, "DeadLetterRecord", rec, want)
}
