package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Alert struct {
	ID              string
	OrganizationID  string
	Severity        string
	Status          string
	Title           string
	Description     string
	MachineID       string
	DeviceID        string
	SensorID        string
	FactoryID       string
	TriggeredAt     time.Time
	EscalationLevel int
}

type alertStore struct {
	pool *pgxpool.Pool
}

func newAlertStore(pool *pgxpool.Pool) *alertStore {
	return &alertStore{pool: pool}
}

type createResult string

const (
	resultCreated            createResult = "created"
	resultSuppressedOpen     createResult = "suppressed_open"
	resultSuppressedCooldown createResult = "suppressed_cooldown"
)

// createIfDue implements alert deduplication and cooldown: it refuses to
// create a new alert if one for the same (rule, dedupe scope) is still
// open/unresolved, or if one resolved too recently (within cooldownSeconds)
// — the latter is what stops a flapping condition from re-paging someone
// every time it crosses the threshold.
func (s *alertStore) createIfDue(ctx context.Context, ruleID string, cooldownSeconds int, a Alert, dedupeKey string) (createResult, Alert, error) {
	var lastStatus string
	var lastTriggeredAt time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT status, triggered_at FROM alerts WHERE alert_rule_id = $1 AND dedupe_key = $2 ORDER BY triggered_at DESC LIMIT 1`,
		ruleID, dedupeKey,
	).Scan(&lastStatus, &lastTriggeredAt)

	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", Alert{}, fmt.Errorf("check existing alert: %w", err)
	}
	if err == nil {
		if lastStatus != "RESOLVED" {
			return resultSuppressedOpen, Alert{}, nil
		}
		if time.Since(lastTriggeredAt) < time.Duration(cooldownSeconds)*time.Second {
			return resultSuppressedCooldown, Alert{}, nil
		}
	}

	var id string
	var triggeredAt time.Time
	err = s.pool.QueryRow(ctx,
		`INSERT INTO alerts (organization_id, alert_rule_id, factory_id, machine_id, device_id, sensor_id, severity, status, title, description, dedupe_key)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, 'OPEN', $8, $9, $10)
		 ON CONFLICT (alert_rule_id, dedupe_key) WHERE status = 'OPEN' DO NOTHING
		 RETURNING id, triggered_at`,
		a.OrganizationID, ruleID, nullIfEmpty(a.FactoryID), nullIfEmpty(a.MachineID), nullIfEmpty(a.DeviceID), nullIfEmpty(a.SensorID),
		a.Severity, a.Title, a.Description, dedupeKey,
	).Scan(&id, &triggeredAt)

	if errors.Is(err, pgx.ErrNoRows) {
		// Lost a race with a concurrent insert for the same open alert.
		return resultSuppressedOpen, Alert{}, nil
	}
	if err != nil {
		return "", Alert{}, fmt.Errorf("insert alert: %w", err)
	}

	a.ID = id
	a.Status = "OPEN"
	a.TriggeredAt = triggeredAt
	return resultCreated, a, nil
}

// dueForEscalation returns OPEN alerts that have gone unacknowledged past
// afterSeconds since their last escalation (or since triggering, if never
// escalated), for the periodic escalation sweep.
func (s *alertStore) dueForEscalation(ctx context.Context, afterSeconds int) ([]Alert, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, organization_id, severity, title, description,
		       COALESCE(machine_id::text, ''), COALESCE(device_id::text, ''),
		       triggered_at, escalation_level
		FROM alerts
		WHERE status = 'OPEN'
		  AND escalation_level < 2
		  AND COALESCE(last_escalated_at, triggered_at) < now() - make_interval(secs => $1)
	`, afterSeconds)
	if err != nil {
		return nil, fmt.Errorf("query alerts due for escalation: %w", err)
	}
	defer rows.Close()

	var alerts []Alert
	for rows.Next() {
		var a Alert
		if err := rows.Scan(&a.ID, &a.OrganizationID, &a.Severity, &a.Title, &a.Description,
			&a.MachineID, &a.DeviceID, &a.TriggeredAt, &a.EscalationLevel); err != nil {
			return nil, fmt.Errorf("scan escalation candidate: %w", err)
		}
		alerts = append(alerts, a)
	}
	return alerts, rows.Err()
}

var severityLadder = []string{"WARNING", "HIGH", "CRITICAL"}

func nextSeverity(current string) string {
	for i, s := range severityLadder {
		if s == current && i+1 < len(severityLadder) {
			return severityLadder[i+1]
		}
	}
	return current
}

// escalate bumps an alert's severity and escalation_level, returning the
// new severity for the re-notification.
func (s *alertStore) escalate(ctx context.Context, alertID, newSeverity string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE alerts SET severity = $1, escalation_level = escalation_level + 1, last_escalated_at = now(), updated_at = now() WHERE id = $2`,
		newSeverity, alertID,
	)
	if err != nil {
		return fmt.Errorf("escalate alert %s: %w", alertID, err)
	}
	return nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
