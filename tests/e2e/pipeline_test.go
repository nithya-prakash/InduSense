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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/events"
	"github.com/redis/go-redis/v9"

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

func redisAddr() string {
	host := "localhost"
	if v := os.Getenv("REDIS_HOST"); v != "" {
		host = v
	}
	port := "6379"
	if v := os.Getenv("REDIS_PORT"); v != "" {
		port = v
	}
	return host + ":" + port
}

// TestMain flushes this API's rate-limit buckets before any test in this
// package runs — see the identical TestMain in tests/integration/api_test.go
// for the full reasoning. This package logs in at least once per test
// (login, publishTelemetry's caller) against the same real-IP "auth"
// bucket that package shares, so both need the same flush to avoid
// inheriting stale counters across consecutive `go test ./...` runs.
// testRedisClient is a package-level client (set up in TestMain, closed
// after m.Run()) so flushRateLimitBuckets can be called cheaply from every
// login() call without reconnecting each time. Left nil if Redis isn't
// reachable at startup, in which case flushRateLimitBuckets is a no-op and
// individual tests' own live-stack checks (e.g. requireLiveStack) still
// handle an unreachable environment gracefully.
var testRedisClient *redis.Client

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	client := redis.NewClient(&redis.Options{Addr: redisAddr()})
	if client.Ping(ctx).Err() == nil {
		testRedisClient = client
	} else {
		client.Close()
	}
	cancel()

	code := m.Run()
	if testRedisClient != nil {
		testRedisClient.Close()
	}
	os.Exit(code)
}

// flushRateLimitBuckets clears every rate-limit counter so the call that
// follows always starts from a clean budget — see the identical helper and
// its full reasoning in tests/integration/api_test.go. A single flush at
// process start (the old TestMain here) isn't enough on its own: it
// doesn't cover repetitions within one `-count=N` run, or interference
// from tests/integration's own deliberate rate-limit exhaustion test
// sharing the same real-IP bucket.
func flushRateLimitBuckets(t *testing.T) {
	t.Helper()
	if testRedisClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	iter := testRedisClient.Scan(ctx, 0, "ratelimit:*", 0).Iterator()
	for iter.Next(ctx) {
		testRedisClient.Del(ctx, iter.Val())
	}
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

// lookupIsolatedSensorFixture picks a sensor whose device has never
// produced *any* CRITICAL alert — not just one from the given rule title.
//
// This used to only exclude the specific rule title being tested
// (`lookupNeverAlertedSensorFixture`), which was a real, observed source of
// flakiness: the live simulator generates background telemetry and other
// alert types independently of this test (e.g. "Unexpected machine
// shutdown" from a simulated status change), so a device could already
// carry an unrelated CRITICAL alert. TestE2E_AnomalyTriggersAlert polls
// `/api/v1/alerts?severity=CRITICAL` and matches by device_id — with the
// old, narrower exclusion, that unrelated alert would be the first (and
// only) match found for the device, and the test failed on a title
// mismatch that had nothing to do with the telemetry it had just
// published. Excluding every existing CRITICAL alert for the device (any
// title, any status — matching exactly what that query can return) means
// the chosen device is provably clean before the test starts: any CRITICAL
// alert that later appears for it can only be caused by this test's own
// telemetry, not by pre-existing state.
//
// alert-service also dedupes/cooldowns by (rule, device+metric), so this
// continues to double as protection against that: a device with zero
// existing alerts of any kind always takes the alert-creation path, never
// the cooldown-suppression path. Naturally self-renewing across repeated
// runs, same as before: once a device is used it has an alert on record,
// so the next run picks a different one.
func lookupIsolatedSensorFixture(t *testing.T, pool *pgxpool.Pool, orgSlug, metric string) sensorFixture {
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
		  AND NOT EXISTS (SELECT 1 FROM alerts a WHERE a.device_id = d.id AND a.severity = 'CRITICAL')
		ORDER BY d.id
		LIMIT 1
	`, orgSlug, metric).Scan(
		&f.OrganizationID, &f.FactoryID, &f.ProductionLineID, &f.MachineID, &f.DeviceID, &f.SensorID,
		&f.Metric, &f.Unit, &f.MinOperating, &f.MaxOperating,
	)
	if err != nil {
		t.Fatalf("find a %q device with no existing CRITICAL alert in organization %q: %v", metric, orgSlug, err)
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

// login no longer sets a synthetic X-Forwarded-For to dodge the auth
// endpoint's rate limit — that relied on the API trusting a
// client-supplied header for its own rate-limit key, exactly the spoofing
// vulnerability the pre-GitHub audit found and closed (see
// services/api/middleware.go's clientIPResolver). The header is now
// correctly ignored by default, so this package's 2 logins simply share
// the real-IP "auth" bucket like any other client — comfortably within
// the configured limit (default 30/min in this deployment, see
// docker-compose.yml) alongside tests/integration's own handful of logins.
func login(t *testing.T, apiURL, email string) string {
	t.Helper()
	flushRateLimitBuckets(t)
	body, _ := json.Marshal(map[string]string{"email": email, "password": demoPassword})
	resp, err := http.Post(apiURL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
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
// range for a musterfabrik-gmbh device that has never produced any CRITICAL
// alert before (picked dynamically — see lookupIsolatedSensorFixture — so
// this test can't collide with alert cooldown state, or an unrelated alert
// type, left behind by the simulator or earlier test runs), and polls for
// the CRITICAL "High temperature" alert (rule GREATER_THAN 90) that reading
// should produce. This proves MQTT -> ingestion -> Kafka ->
// anomaly-detector's rule check -> alert-service's rule match -> Postgres
// -> API, the platform's actual reason for existing.
//
// The match requires all three of device_id, title, AND triggered_at being
// after this test's own start time (with a small clock-skew allowance for
// the test binary and the Docker containers not sharing a clock with
// perfect precision) — not device_id alone. A device is excluded from
// selection if it already has *any* CRITICAL alert (see
// lookupIsolatedSensorFixture), so in normal operation nothing should ever
// need the title/time checks to disambiguate — but they're real assertions
// on the outcome this test claims to prove, not just a defensive fallback,
// so an alert that happens to share the device_id without actually being
// the one this test's telemetry produced still correctly fails to match.
//
// zweite-firma-gmbh isn't used here: it was seeded with only a factory and
// a machine (for the tenant-isolation test in tests/integration) and has no
// devices or sensors of its own.
func TestE2E_AnomalyTriggersAlert(t *testing.T) {
	apiURL, pool, mqttClient := requireLiveStack(t)
	fixture := lookupIsolatedSensorFixture(t, pool, "musterfabrik-gmbh", "temperature")

	const anomalousValue = 150.0 // "High temperature" rule fires above 90
	const clockSkewAllowance = 5 * time.Second

	token := login(t, apiURL, "admin@musterfabrik-gmbh.de")
	testStart := time.Now().Add(-clockSkewAllowance)
	publishTelemetry(t, mqttClient, fixture, anomalousValue)

	type alert struct {
		Severity    string    `json:"severity"`
		DeviceID    string    `json:"device_id"`
		Title       string    `json:"title"`
		TriggeredAt time.Time `json:"triggered_at"`
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
			if a.DeviceID != fixture.DeviceID || a.TriggeredAt.Before(testStart) {
				continue // not this test's alert — a pre-existing or unrelated one, keep polling
			}
			if a.Title != "High temperature" {
				// lookupIsolatedSensorFixture guarantees this device had no
				// CRITICAL alert before testStart, so a *different*,
				// post-testStart CRITICAL alert appearing for it means
				// something concurrent (most plausibly the live simulator)
				// produced it — not our own out-of-range reading. That's not
				// this test's assertion to make: keep polling for the one
				// this test's own telemetry should still produce.
				continue
			}
			return // found it — anomaly-to-alert pipeline confirmed, and it's this test's own alert
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no CRITICAL \"High temperature\" alert for device %s (triggered after this test started) appeared via the API within 45s of publishing temperature=%.0f", fixture.DeviceID, anomalousValue)
}

// TestLookupIsolatedSensorFixture_ExcludesDeviceWithAnyExistingCriticalAlert
// is a regression test for the exact bug that made TestE2E_AnomalyTriggersAlert
// flaky: the old fixture lookup only excluded devices with an existing
// alert matching the specific rule title under test ("High temperature"),
// so a device already carrying an unrelated CRITICAL alert (e.g., from a
// simulated machine shutdown) could still be selected — and would then be
// the first, wrong match the test's poll loop found. This inserts a
// throwaway CRITICAL alert with a deliberately different title against a
// real, currently-selectable device, then asserts the lookup never selects
// that device again while the alert exists — proving the exclusion is
// title-independent, not just re-testing the original narrower behavior.
func TestLookupIsolatedSensorFixture_ExcludesDeviceWithAnyExistingCriticalAlert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, postgresDSN())
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("no live Postgres reachable, skipping: %v", err)
	}
	defer pool.Close()

	fixture := lookupIsolatedSensorFixture(t, pool, "musterfabrik-gmbh", "temperature")

	var alertID string
	err = pool.QueryRow(ctx, `
		INSERT INTO alerts (organization_id, machine_id, device_id, severity, status, title, description, dedupe_key)
		VALUES ($1, $2, $3, 'CRITICAL', 'OPEN', 'Unexpected machine shutdown', 'regression test alert — unrelated to High temperature', $4)
		RETURNING id
	`, fixture.OrganizationID, fixture.MachineID, fixture.DeviceID, "regression-test-dedupe-"+uuid.NewString()).Scan(&alertID)
	if err != nil {
		t.Fatalf("insert throwaway CRITICAL alert with an unrelated title: %v", err)
	}
	defer pool.Exec(context.Background(), `DELETE FROM alerts WHERE id = $1`, alertID) //nolint:errcheck

	again := lookupIsolatedSensorFixture(t, pool, "musterfabrik-gmbh", "temperature")
	if again.DeviceID == fixture.DeviceID {
		t.Fatalf("lookupIsolatedSensorFixture re-selected device %s after it was given an unrelated CRITICAL alert (\"Unexpected machine shutdown\") — the exclusion must cover any existing CRITICAL alert, not just one matching the rule title under test", fixture.DeviceID)
	}
}
