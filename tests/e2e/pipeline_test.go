// Package e2e drives the real pipeline end to end: it publishes a genuine
// MQTT message to the same broker ingestion subscribes to, and polls the
// real API until the effect that message should have — a stored telemetry
// point, or a generated alert — becomes visible. Nothing here is mocked or
// short-circuited: a passing test means an MQTT message actually rode
// through ingestion, Kafka, stream-processor/anomaly-detector,
// InfluxDB/Postgres, alert-service, and back out through the HTTP API.
//
// It runs against the docker-compose stack (`make up`), using real seeded
// device/sensor rows looked up from Postgres rather than hardcoded IDs,
// since those rows get fresh UUIDs on every seed. It skips (not fails) if
// the stack isn't reachable.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/events"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const demoPassword = "ChangeMe123!"

func apiBaseURL() string {
	if v := os.Getenv("API_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func mqttBrokerURL() string {
	if v := os.Getenv("SIM_MQTT_BROKER_URL"); v != "" {
		return v
	}
	return "tcp://localhost:1883"
}

func postgresDSN() string {
	if v := os.Getenv("ALERT_POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"
}

// sensorFixture is one real, currently-seeded sensor and the full
// hierarchy chain above it, looked up by organization slug so the test
// never hardcodes a UUID that only existed in some past seed run.
type sensorFixture struct {
	OrganizationID   string
	FactoryID        string
	ProductionLineID string
	MachineID        string
	DeviceID         string
	SensorID         string
	Metric           string
	Unit             string
	MinOperating     float64
	MaxOperating     float64
}

func lookupSensorFixture(t *testing.T, pool *pgxpool.Pool, orgSlug, metric string) sensorFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var f sensorFixture
	err := pool.QueryRow(ctx, `
		SELECT o.id, fac.id, pl.id, m.id, d.id, s.id, s.metric, s.unit,
		       COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
		FROM sensors s
		JOIN devices d ON d.id = s.device_id
		JOIN machines m ON m.id = d.machine_id
		JOIN production_lines pl ON pl.id = m.production_line_id
		JOIN factories fac ON fac.id = pl.factory_id
		JOIN organizations o ON o.id = fac.organization_id
		WHERE o.slug = $1 AND s.metric = $2
		LIMIT 1
	`, orgSlug, metric).Scan(
		&f.OrganizationID, &f.FactoryID, &f.ProductionLineID, &f.MachineID, &f.DeviceID, &f.SensorID,
		&f.Metric, &f.Unit, &f.MinOperating, &f.MaxOperating,
	)
	if err != nil {
		t.Fatalf("look up a seeded %q sensor for organization %q: %v (has `make seed` been run?)", metric, orgSlug, err)
	}
	return f
}

// lookupNeverAlertedSensorFixture picks a sensor whose device has never
// produced an alert with the given rule title. alert-service dedupes and
// cooldowns by (rule, device+metric), so reusing an already-alerted device
// would make the anomaly test flaky depending on whatever traffic (the
// simulator, other test runs) happened to touch it before. A device with
// zero rows for this rule title always takes the "create" path — no
// cooldown state to race against. It's naturally self-renewing across
// repeated test runs: once a device is used, it has an alert on record and
// the next run picks a different one.
func lookupNeverAlertedSensorFixture(t *testing.T, pool *pgxpool.Pool, orgSlug, metric, ruleTitle string) sensorFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var f sensorFixture
	err := pool.QueryRow(ctx, `
		SELECT o.id, fac.id, pl.id, m.id, d.id, s.id, s.metric, s.unit,
		       COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
		FROM sensors s
		JOIN devices d ON d.id = s.device_id
		JOIN machines m ON m.id = d.machine_id
		JOIN production_lines pl ON pl.id = m.production_line_id
		JOIN factories fac ON fac.id = pl.factory_id
		JOIN organizations o ON o.id = fac.organization_id
		WHERE o.slug = $1 AND s.metric = $2
		  AND NOT EXISTS (SELECT 1 FROM alerts a WHERE a.device_id = d.id AND a.title = $3)
		ORDER BY d.id
		LIMIT 1
	`, orgSlug, metric, ruleTitle).Scan(
		&f.OrganizationID, &f.FactoryID, &f.ProductionLineID, &f.MachineID, &f.DeviceID, &f.SensorID,
		&f.Metric, &f.Unit, &f.MinOperating, &f.MaxOperating,
	)
	if err != nil {
		t.Fatalf("find a %q device never alerted by rule %q in organization %q: %v", metric, ruleTitle, orgSlug, err)
	}
	return f
}

func requireLiveStack(t *testing.T) (apiURL string, pool *pgxpool.Pool, mqttClient mqtt.Client) {
	t.Helper()

	apiURL = apiBaseURL()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(apiURL + "/live")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Skipf("api not reachable at %s, skipping e2e test", apiURL)
	}
	resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err = pgxpool.New(ctx, postgresDSN())
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("postgres not reachable, skipping e2e test: %v", err)
	}
	t.Cleanup(pool.Close)

	opts := mqtt.NewClientOptions().
		AddBroker(mqttBrokerURL()).
		SetClientID("indusense-e2e-test-" + uuid.NewString()).
		SetConnectTimeout(5 * time.Second)
	mqttClient = mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Skipf("mqtt broker not reachable at %s, skipping e2e test", mqttBrokerURL())
	}
	t.Cleanup(func() { mqttClient.Disconnect(250) })

	return apiURL, pool, mqttClient
}

var loginIPCounter atomic.Int64

// login uses its own synthetic X-Forwarded-For per call (RFC 5737
// TEST-NET-3) so this package's logins never share the auth endpoint's
// per-IP rate-limit bucket with tests/integration or with each other —
// both packages otherwise hit the API from the same real test-runner IP,
// which made two suites (or two runs) within the same minute interfere.
func login(t *testing.T, apiURL, email string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email, "password": demoPassword})
	req, _ := http.NewRequest(http.MethodPost, apiURL+"/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", loginIPCounter.Add(1)%250+1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("login as %s: %v", email, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login as %s: expected 200, got %d", email, resp.StatusCode)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	json.NewDecoder(resp.Body).Decode(&tok)
	return tok.AccessToken
}

func publishTelemetry(t *testing.T, client mqtt.Client, f sensorFixture, value float64) string {
	t.Helper()
	eventID := uuid.NewString()
	evt := events.TelemetryEvent{
		EventID:          eventID,
		OrganizationID:   f.OrganizationID,
		FactoryID:        f.FactoryID,
		ProductionLineID: f.ProductionLineID,
		MachineID:        f.MachineID,
		DeviceID:         f.DeviceID,
		SensorID:         f.SensorID,
		Timestamp:        time.Now().UTC(),
		SequenceNumber:   1,
		Metric:           f.Metric,
		Value:            value,
		Unit:             f.Unit,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal telemetry event: %v", err)
	}

	topic := fmt.Sprintf("factory/%s/machine/%s/sensor/%s/telemetry", f.FactoryID, f.MachineID, f.SensorID)
	token := client.Publish(topic, 1, false, payload)
	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("publish telemetry to %s: %v", topic, token.Error())
	}
	return eventID
}

// TestE2E_TelemetryRoundTrip publishes one real telemetry reading — an
// in-range value, so it's just normal data, not an anomaly — and polls the
// API until it's visible, proving MQTT -> ingestion -> Kafka ->
// stream-processor -> InfluxDB -> API works end to end.
func TestE2E_TelemetryRoundTrip(t *testing.T) {
	apiURL, pool, mqttClient := requireLiveStack(t)
	fixture := lookupSensorFixture(t, pool, "musterfabrik-gmbh", "temperature")

	// Squarely inside the sensor's own operating range, and distinctive
	// enough (three decimal places) that it can't be confused with a
	// coincidentally similar reading from unrelated traffic on the shared
	// stack (the simulator, other tests) touching the same device.
	value := fixture.MinOperating + (fixture.MaxOperating-fixture.MinOperating)*0.5 + 0.137

	token := login(t, apiURL, "admin@musterfabrik-gmbh.de")
	publishTelemetry(t, mqttClient, fixture, value)

	deadline := time.Now().Add(15 * time.Second)
	var lastStatus int
	var lastBody string
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/api/v1/telemetry/latest?device_id=%s&metric=%s", apiURL, fixture.DeviceID, fixture.Metric), nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /api/v1/telemetry/latest: %v", err)
		}
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		resp.Body.Close()
		lastStatus, lastBody = resp.StatusCode, buf.String()

		if resp.StatusCode == http.StatusOK {
			var point struct {
				Value float64 `json:"value"`
			}
			if err := json.Unmarshal(buf.Bytes(), &point); err == nil && point.Value == value {
				return // found it — round trip confirmed
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("telemetry value %v never appeared via the API within 15s (last response: %d %s)", value, lastStatus, lastBody)
}

// TestE2E_AnomalyTriggersAlert publishes a reading far outside the operating
// range for a musterfabrik-gmbh device that has never triggered "High
// temperature" before (picked dynamically — see
// lookupNeverAlertedSensorFixture — so this test can't collide with alert
// cooldown state left behind by the simulator or earlier test runs), and
// polls for the CRITICAL alert that rule (GREATER_THAN 90) should produce.
// This proves MQTT -> ingestion -> Kafka -> anomaly-detector's rule check
// -> alert-service's rule match -> Postgres -> API, the platform's actual
// reason for existing.
//
// zweite-firma-gmbh isn't used here: it was seeded with only a factory and
// a machine (for the tenant-isolation test in tests/integration) and has no
// devices or sensors of its own.
func TestE2E_AnomalyTriggersAlert(t *testing.T) {
	apiURL, pool, mqttClient := requireLiveStack(t)
	fixture := lookupNeverAlertedSensorFixture(t, pool, "musterfabrik-gmbh", "temperature", "High temperature")

	const anomalousValue = 150.0 // "High temperature" rule fires above 90

	token := login(t, apiURL, "admin@musterfabrik-gmbh.de")
	publishTelemetry(t, mqttClient, fixture, anomalousValue)

	type alert struct {
		Severity string `json:"severity"`
		DeviceID string `json:"device_id"`
		Title    string `json:"title"`
	}
	type page struct {
		Items []alert `json:"items"`
	}

	// 45s, not 20s: `make seed` restarts anomaly-detector/alert-service so
	// their Postgres-derived caches are immediately fresh (see the seed
	// target's comment in Makefile), but neither service's /ready endpoint
	// checks Kafka consumer-group state, so there's no signal for "the
	// group has finished rebalancing" after that restart. A CI run once
	// hit this directly: it restarted both services, published test
	// traffic almost immediately after, and timed out at 20s even though
	// the pipeline was working correctly — the alert just landed a few
	// seconds later than that budget allowed for.
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, apiURL+"/api/v1/alerts?severity=CRITICAL", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /api/v1/alerts: %v", err)
		}
		var p page
		json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()

		for _, a := range p.Items {
			if a.DeviceID == fixture.DeviceID {
				if a.Title != "High temperature" {
					t.Fatalf("expected the triggered alert to be from the \"High temperature\" rule, got %q", a.Title)
				}
				return // found it — anomaly-to-alert pipeline confirmed
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no CRITICAL alert for device %s appeared via the API within 45s of publishing temperature=%.0f", fixture.DeviceID, anomalousValue)
}
