// Command seed populates the factory hierarchy (organization, factories,
// production lines, machines, devices, sensors) used by the sensor simulator
// and the rest of the platform. It is idempotent: re-running it against an
// already-seeded database is a no-op (it checks for an existing organization
// slug before inserting anything).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type metricSpec struct {
	unit     string
	minValue float64
	maxValue float64
}

var metricSpecs = map[string]metricSpec{
	"temperature":    {unit: "celsius", minValue: 20, maxValue: 90},
	"vibration":      {unit: "mm_s", minValue: 0, maxValue: 10},
	"pressure":       {unit: "bar", minValue: 0, maxValue: 250},
	"rpm":            {unit: "rpm", minValue: 0, maxValue: 3000},
	"current":        {unit: "ampere", minValue: 0, maxValue: 100},
	"voltage":        {unit: "volt", minValue: 200, maxValue: 400},
	"power":          {unit: "kilowatt", minValue: 0, maxValue: 50},
	"humidity":       {unit: "percent", minValue: 10, maxValue: 80},
	"acoustic_level": {unit: "decibel", minValue: 40, maxValue: 110},
}

// machineProfile maps a realistic German-manufacturing machine type to the
// five sensor metrics it is instrumented with.
type machineProfile struct {
	machineType string
	metrics     [5]string
}

var machineProfiles = []machineProfile{
	{"CNC_MILLING_MACHINE", [5]string{"temperature", "vibration", "rpm", "current", "power"}},
	{"HYDRAULIC_PRESS", [5]string{"temperature", "pressure", "vibration", "current", "power"}},
	{"CONVEYOR_BELT", [5]string{"temperature", "current", "vibration", "power", "rpm"}},
	{"WELDING_ROBOT", [5]string{"temperature", "current", "voltage", "power", "vibration"}},
	{"AIR_COMPRESSOR", [5]string{"temperature", "pressure", "vibration", "current", "power"}},
}

var germanFactories = []struct {
	name string
	city string
}{
	{"Berlin Plant", "Berlin"},
	{"Dresden Plant", "Dresden"},
	{"Munich Plant", "Munich"},
	{"Hamburg Plant", "Hamburg"},
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func randomHexSecret(nBytes int) (string, error) {
	buf := make([]byte, nBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func randomStatus(weights map[string]int) string {
	total := 0
	for _, w := range weights {
		total += w
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(total)))
	roll := int(n.Int64())
	for status, w := range weights {
		if roll < w {
			return status
		}
		roll -= w
	}
	for status := range weights {
		return status
	}
	return ""
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("seed failed: %v", err)
	}
}

func run() error {
	linesPerFactory := envInt("SEED_LINES_PER_FACTORY", 5)
	machinesPerLine := envInt("SEED_MACHINES_PER_LINE", 10)

	dsn := os.Getenv("SEED_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	const orgSlug = "musterfabrik-gmbh"

	var existingOrgID string
	err = pool.QueryRow(ctx, `SELECT id FROM organizations WHERE slug = $1`, orgSlug).Scan(&existingOrgID)
	if err == nil {
		log.Printf("organization %q already seeded (id=%s) — skipping hierarchy, seed is idempotent", orgSlug, existingOrgID)
		return seedAlertRulesIfMissing(ctx, pool, existingOrgID)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op if already committed

	var orgID string
	err = tx.QueryRow(ctx,
		`INSERT INTO organizations (name, slug) VALUES ($1, $2) RETURNING id`,
		"Musterfabrik GmbH", orgSlug,
	).Scan(&orgID)
	if err != nil {
		return fmt.Errorf("insert organization: %w", err)
	}
	log.Printf("created organization Musterfabrik GmbH (id=%s)", orgID)

	factoryCount, lineCount, machineCount, deviceCount, sensorCount, credCount := 0, 0, 0, 0, 0, 0

	for _, f := range germanFactories {
		var factoryID string
		err = tx.QueryRow(ctx,
			`INSERT INTO factories (organization_id, name, city, country) VALUES ($1, $2, $3, 'DE') RETURNING id`,
			orgID, f.name, f.city,
		).Scan(&factoryID)
		if err != nil {
			return fmt.Errorf("insert factory %s: %w", f.name, err)
		}
		factoryCount++

		for lineIdx := 1; lineIdx <= linesPerFactory; lineIdx++ {
			lineName := fmt.Sprintf("Line %02d", lineIdx)
			var lineID string
			err = tx.QueryRow(ctx,
				`INSERT INTO production_lines (factory_id, name) VALUES ($1, $2) RETURNING id`,
				factoryID, lineName,
			).Scan(&lineID)
			if err != nil {
				return fmt.Errorf("insert production line %s/%s: %w", f.name, lineName, err)
			}
			lineCount++

			for machineIdx := 1; machineIdx <= machinesPerLine; machineIdx++ {
				profile := machineProfiles[(machineIdx-1)%len(machineProfiles)]
				machineName := fmt.Sprintf("%s-%s-M%03d", f.city, lineName, machineIdx)
				machineStatus := randomStatus(map[string]int{
					"RUNNING":     70,
					"IDLE":        15,
					"MAINTENANCE": 8,
					"FAULT":       5,
					"STOPPED":     2,
				})

				var machineID string
				err = tx.QueryRow(ctx,
					`INSERT INTO machines (production_line_id, name, machine_type, status)
					 VALUES ($1, $2, $3, $4) RETURNING id`,
					lineID, machineName, profile.machineType, machineStatus,
				).Scan(&machineID)
				if err != nil {
					return fmt.Errorf("insert machine %s: %w", machineName, err)
				}
				machineCount++

				serial := fmt.Sprintf("SN-%s-%s-%02d-%03d", f.city, profile.machineType, lineIdx, machineIdx)
				deviceStatus := randomStatus(map[string]int{
					"ACTIVE":      85,
					"OFFLINE":     8,
					"MAINTENANCE": 5,
					"PROVISIONED": 2,
				})

				var activatedAt any
				if deviceStatus == "ACTIVE" || deviceStatus == "OFFLINE" || deviceStatus == "MAINTENANCE" {
					activatedAt = time.Now().Add(-time.Duration(machineIdx) * time.Hour)
				}

				var deviceID string
				err = tx.QueryRow(ctx,
					`INSERT INTO devices (machine_id, organization_id, serial_number, status, firmware_version, activated_at)
					 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
					machineID, orgID, serial, deviceStatus, "fw-1.4.2", activatedAt,
				).Scan(&deviceID)
				if err != nil {
					return fmt.Errorf("insert device %s: %w", serial, err)
				}
				deviceCount++

				secret, err := randomHexSecret(24)
				if err != nil {
					return fmt.Errorf("generate device secret: %w", err)
				}
				hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
				if err != nil {
					return fmt.Errorf("hash device secret: %w", err)
				}
				_, err = tx.Exec(ctx,
					`INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active)
					 VALUES ($1, 'shared_secret', $2, true)`,
					deviceID, string(hash),
				)
				if err != nil {
					return fmt.Errorf("insert device credential for %s: %w", serial, err)
				}
				credCount++

				for _, metric := range profile.metrics {
					spec := metricSpecs[metric]
					_, err = tx.Exec(ctx,
						`INSERT INTO sensors (device_id, metric, unit, min_operating_value, max_operating_value)
						 VALUES ($1, $2, $3, $4, $5)`,
						deviceID, metric, spec.unit, spec.minValue, spec.maxValue,
					)
					if err != nil {
						return fmt.Errorf("insert sensor %s for device %s: %w", metric, serial, err)
					}
					sensorCount++
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("seed complete: %d factories, %d production lines, %d machines, %d devices, %d device credentials, %d sensors",
		factoryCount, lineCount, machineCount, deviceCount, credCount, sensorCount)

	return seedAlertRulesIfMissing(ctx, pool, orgID)
}

// seedAlertRulesIfMissing inserts a handful of representative,
// organization-wide alert rules (scoped by metric only, not to a specific
// machine/device/sensor) matching the examples from the spec: a hard
// temperature threshold, a vibration threshold, a power-spike threshold, and
// an anomaly-count rule ("three anomalies within five minutes"). Idempotent:
// skipped entirely if this organization already has any alert rules.
func seedAlertRulesIfMissing(ctx context.Context, pool *pgxpool.Pool, orgID string) error {
	var existing int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_rules WHERE organization_id = $1`, orgID).Scan(&existing); err != nil {
		return fmt.Errorf("check existing alert rules: %w", err)
	}
	if existing > 0 {
		log.Printf("organization already has %d alert rule(s) — skipping, seed is idempotent", existing)
		return nil
	}

	rules := []struct {
		name         string
		metric       string
		condition    string
		thresholdVal *float64
		thresholdMin *float64
		thresholdMax *float64
		severity     string
		cooldownSecs int
		windowSecs   int
	}{
		{name: "High temperature", metric: "temperature", condition: "GREATER_THAN", thresholdVal: ptr(90.0), severity: "CRITICAL", cooldownSecs: 300, windowSecs: 300},
		{name: "Excessive vibration", metric: "vibration", condition: "GREATER_THAN", thresholdVal: ptr(8.0), severity: "HIGH", cooldownSecs: 180, windowSecs: 300},
		{name: "Power spike", metric: "power", condition: "GREATER_THAN", thresholdVal: ptr(45.0), severity: "WARNING", cooldownSecs: 180, windowSecs: 300},
		{name: "Repeated temperature anomalies", metric: "temperature", condition: "ANOMALY_COUNT", thresholdVal: ptr(3.0), severity: "HIGH", cooldownSecs: 300, windowSecs: 300},
		// Sentinel rule for the alert-service's direct machine-shutdown handler
		// (services/alert-service/main.go): "machine_status" isn't a real
		// telemetry metric, so this rule is never matched against sensor
		// readings — it exists only to give shutdown alerts a stable
		// alert_rule_id to dedupe/cooldown against.
		{name: "Unexpected machine shutdown", metric: "machine_status", condition: "ANOMALY_COUNT", thresholdVal: ptr(1.0), severity: "WARNING", cooldownSecs: 300, windowSecs: 300},
	}

	for _, r := range rules {
		_, err := pool.Exec(ctx,
			`INSERT INTO alert_rules (organization_id, name, metric, condition, threshold_value, threshold_min, threshold_max, severity, cooldown_seconds, window_seconds)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			orgID, r.name, r.metric, r.condition, r.thresholdVal, r.thresholdMin, r.thresholdMax, r.severity, r.cooldownSecs, r.windowSecs,
		)
		if err != nil {
			return fmt.Errorf("insert alert rule %q: %w", r.name, err)
		}
	}
	log.Printf("seeded %d alert rules", len(rules))
	return nil
}

func ptr(f float64) *float64 { return &f }
