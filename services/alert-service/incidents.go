package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// incidentStore owns incident lifecycle management. Incidents are
// deliberately kept in the same service that already processes alerts
// (rather than a separate "incident-service") since incident creation is
// tightly coupled to alert creation and there's no independent workflow for
// it yet — the manual lifecycle transitions below exist so Phase 10's REST
// API can call them directly once there's a human operator to invoke them.
type incidentStore struct {
	pool *pgxpool.Pool
}

func newIncidentStore(pool *pgxpool.Pool) *incidentStore {
	return &incidentStore{pool: pool}
}

// validTransitions defines the incident lifecycle state machine. RESOLVED
// can move back to INVESTIGATING (a "resolved" issue recurring) but CLOSED
// is terminal — once closed, a recurrence opens a new incident instead of
// reanimating an old one, which keeps the audit trail honest about when the
// underlying problem was actually first seen.
var validTransitions = map[string][]string{
	"OPEN":          {"ACKNOWLEDGED", "INVESTIGATING", "RESOLVED"},
	"ACKNOWLEDGED":  {"INVESTIGATING", "RESOLVED"},
	"INVESTIGATING": {"RESOLVED"},
	"RESOLVED":      {"CLOSED", "INVESTIGATING"},
	"CLOSED":        {},
}

func isValidTransition(from, to string) bool {
	for _, allowed := range validTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// openOrAttach implements "alerts can result in incidents, but don't create
// unlimited incidents from repeated alerts": it reuses any incident already
// active for this machine (OPEN/ACKNOWLEDGED/INVESTIGATING — enforced by the
// partial unique index from Phase 2, so this is race-safe, not just an
// application-level check) rather than opening a second one, logging the
// new alert's arrival as an ALERT_ATTACHED audit event instead.
func (s *incidentStore) openOrAttach(ctx context.Context, alert Alert) (incidentID string, created bool, err error) {
	var existingID string
	err = s.pool.QueryRow(ctx,
		`SELECT id FROM incidents WHERE machine_id = $1 AND status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING')`,
		alert.MachineID,
	).Scan(&existingID)

	if err == nil {
		if attachErr := s.logEvent(ctx, existingID, "ALERT_ATTACHED", nil, "", "", fmt.Sprintf("alert %s (%s) attached: %s", alert.ID, alert.Severity, alert.Title)); attachErr != nil {
			return "", false, attachErr
		}
		return existingID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("check existing incident: %w", err)
	}

	var id string
	err = s.pool.QueryRow(ctx,
		`INSERT INTO incidents (organization_id, alert_id, machine_id, device_id, sensor_id, severity, status, title, description)
		 VALUES ($1, $2, $3, $4, $5, $6, 'OPEN', $7, $8)
		 ON CONFLICT (machine_id) WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING') DO NOTHING
		 RETURNING id`,
		alert.OrganizationID, alert.ID, nullIfEmpty(alert.MachineID), nullIfEmpty(alert.DeviceID), nullIfEmpty(alert.SensorID),
		alert.Severity, alert.Title, alert.Description,
	).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
		// Lost a race with a concurrent incident creation for this machine —
		// fetch the one that won and attach to it instead.
		if selErr := s.pool.QueryRow(ctx,
			`SELECT id FROM incidents WHERE machine_id = $1 AND status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING')`,
			alert.MachineID,
		).Scan(&existingID); selErr != nil {
			return "", false, fmt.Errorf("resolve incident creation race: %w", selErr)
		}
		return existingID, false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("insert incident: %w", err)
	}

	if logErr := s.logEvent(ctx, id, "STATUS_CHANGE", nil, "", "OPEN", "incident opened from alert "+alert.ID); logErr != nil {
		return "", false, logErr
	}
	return id, true, nil
}

// transition moves an incident to a new status if the move is valid,
// recording the change in incident_events for the audit trail. actorUserID
// is nil until Phase 9 (auth) exists to identify who performed the action.
func (s *incidentStore) transition(ctx context.Context, incidentID, newStatus string, actorUserID *string, note string) error {
	var currentStatus string
	if err := s.pool.QueryRow(ctx, `SELECT status FROM incidents WHERE id = $1`, incidentID).Scan(&currentStatus); err != nil {
		return fmt.Errorf("load incident %s: %w", incidentID, err)
	}
	if !isValidTransition(currentStatus, newStatus) {
		return fmt.Errorf("invalid incident transition %s -> %s", currentStatus, newStatus)
	}

	var extra string
	switch newStatus {
	case "RESOLVED":
		extra = `, resolved_at = now()`
	case "CLOSED":
		extra = `, closed_at = now()`
	}

	_, err := s.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE incidents SET status = $1, updated_at = now()%s WHERE id = $2`, extra),
		newStatus, incidentID,
	)
	if err != nil {
		return fmt.Errorf("update incident %s status: %w", incidentID, err)
	}

	return s.logEvent(ctx, incidentID, "STATUS_CHANGE", actorUserID, currentStatus, newStatus, note)
}

func (s *incidentStore) assign(ctx context.Context, incidentID, userID string, actorUserID *string) error {
	_, err := s.pool.Exec(ctx, `UPDATE incidents SET assigned_to = $1, updated_at = now() WHERE id = $2`, userID, incidentID)
	if err != nil {
		return fmt.Errorf("assign incident %s: %w", incidentID, err)
	}
	return s.logEvent(ctx, incidentID, "ASSIGNMENT", actorUserID, "", userID, "assigned to technician "+userID)
}

func (s *incidentStore) resolve(ctx context.Context, incidentID, resolutionNotes string, actorUserID *string) error {
	if _, err := s.pool.Exec(ctx, `UPDATE incidents SET resolution_notes = $1 WHERE id = $2`, resolutionNotes, incidentID); err != nil {
		return fmt.Errorf("set resolution notes for incident %s: %w", incidentID, err)
	}
	return s.transition(ctx, incidentID, "RESOLVED", actorUserID, resolutionNotes)
}

func (s *incidentStore) logEvent(ctx context.Context, incidentID, eventType string, actorUserID *string, oldValue, newValue, note string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO incident_events (incident_id, event_type, actor_user_id, old_value, new_value, note)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		incidentID, eventType, actorUserID, nullIfEmpty(oldValue), nullIfEmpty(newValue), note,
	)
	if err != nil {
		return fmt.Errorf("log incident event for %s: %w", incidentID, err)
	}
	return nil
}
