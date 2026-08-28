package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AlertRule struct {
	ID              string
	OrganizationID  string
	Name            string
	Metric          string
	Condition       string // GREATER_THAN | LESS_THAN | OUTSIDE_RANGE | ANOMALY_COUNT
	ThresholdValue  *float64
	ThresholdMin    *float64
	ThresholdMax    *float64
	Severity        string
	CooldownSeconds int
	WindowSeconds   int
	MachineID       *string
	DeviceID        *string
	SensorID        *string
}

// scopeMatches reports whether this rule's optional machine/device/sensor
// scoping (NULL = wildcard) matches the given anomaly's identifiers.
func (r AlertRule) scopeMatches(machineID, deviceID, sensorID string) bool {
	if r.MachineID != nil && *r.MachineID != machineID {
		return false
	}
	if r.DeviceID != nil && *r.DeviceID != deviceID {
		return false
	}
	if r.SensorID != nil && *r.SensorID != sensorID {
		return false
	}
	return true
}

// ruleCache holds a periodically-refreshed snapshot of active alert rules,
// grouped by (organization_id, metric) for fast lookup per incoming
// anomaly — rules are re-read on a timer rather than per-event since they
// change far less often than telemetry arrives.
type ruleCache struct {
	mu    sync.RWMutex
	byKey map[string][]AlertRule // "orgID|metric" -> matching rules
	pool  *pgxpool.Pool
}

// newRuleCache reuses the caller's pool rather than opening its own —
// alert-service used to open a second, independently-sized pool here
// purely to run this cache's periodic refresh query, doubling its Postgres
// connection footprint for no reason (see the pool passed in from main.go).
func newRuleCache(ctx context.Context, pool *pgxpool.Pool) (*ruleCache, error) {
	rc := &ruleCache{byKey: make(map[string][]AlertRule), pool: pool}
	if err := rc.refresh(ctx); err != nil {
		return nil, err
	}
	return rc, nil
}

func (rc *ruleCache) refresh(ctx context.Context) error {
	rows, err := rc.pool.Query(ctx, `
		SELECT id, organization_id, name, metric, condition,
		       threshold_value, threshold_min, threshold_max,
		       severity, cooldown_seconds, window_seconds,
		       machine_id, device_id, sensor_id
		FROM alert_rules
		WHERE is_active = true
	`)
	if err != nil {
		return fmt.Errorf("query alert_rules: %w", err)
	}
	defer rows.Close()

	byKey := make(map[string][]AlertRule)
	for rows.Next() {
		var r AlertRule
		if err := rows.Scan(&r.ID, &r.OrganizationID, &r.Name, &r.Metric, &r.Condition,
			&r.ThresholdValue, &r.ThresholdMin, &r.ThresholdMax,
			&r.Severity, &r.CooldownSeconds, &r.WindowSeconds,
			&r.MachineID, &r.DeviceID, &r.SensorID); err != nil {
			return fmt.Errorf("scan alert_rule row: %w", err)
		}
		key := r.OrganizationID + "|" + r.Metric
		byKey[key] = append(byKey[key], r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate alert_rule rows: %w", err)
	}

	rc.mu.Lock()
	rc.byKey = byKey
	rc.mu.Unlock()
	return nil
}

func (rc *ruleCache) rulesFor(orgID, metric string) []AlertRule {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.byKey[orgID+"|"+metric]
}
