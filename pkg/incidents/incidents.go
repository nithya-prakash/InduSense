// Package incidents implements incident lifecycle management — shared by
// alert-service (which opens incidents automatically from new alerts) and
// the api service (which exposes manual transitions/assignment to a human
// operator). Extracted from alert-service once a second caller needed the
// exact same state machine and persistence, rather than duplicating it.
package incidents

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AlertRef is the minimal alert data needed to open or attach an incident —
// deliberately independent of alert-service's own Alert type so this
// package has no dependency on it.
type AlertRef struct {
	ID             string
	OrganizationID string
	Severity       string
	FactoryID      string
	MachineID      string
	DeviceID       string
	SensorID       string
	Title          string
	Description    string
}

type Incident struct {
	ID              string
	OrganizationID  string
	AlertID         string
	FactoryID       string
	MachineID       string
	DeviceID        string
	SensorID        string
	Severity        string
	Status          string
	Title           string
	Description     string
	AssignedTo      string
	ResolutionNotes string
	OpenedAt        time.Time
	ResolvedAt      *time.Time
	ClosedAt        *time.Time
}

type Event struct {
	ID          string
	IncidentID  string
	EventType   string
	ActorUserID *string
	OldValue    string
	NewValue    string
	Note        string
	CreatedAt   time.Time
}

// Publisher lets Store announce lifecycle changes on the `incidents` Kafka
// topic. Optional — a nil Publisher (or NewStore called without one) simply
// skips publishing, since not every caller needs it.
type Publisher interface {
	PublishIncidentEvent(ctx context.Context, eventType string, inc Incident) error
}

// validTransitions defines the incident lifecycle state machine. RESOLVED
// can move back to INVESTIGATING (a recurrence) but CLOSED is terminal —
// once closed, a recurrence opens a new incident instead of reanimating an
// old one, keeping the audit trail honest about when the problem was first
// actually seen.
var validTransitions = map[string][]string{
	"OPEN":          {"ACKNOWLEDGED", "INVESTIGATING", "RESOLVED"},
	"ACKNOWLEDGED":  {"INVESTIGATING", "RESOLVED"},
	"INVESTIGATING": {"RESOLVED"},
	"RESOLVED":      {"CLOSED", "INVESTIGATING"},
	"CLOSED":        {},
}

func IsValidTransition(from, to string) bool {
	for _, allowed := range validTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

type Store struct {
	pool      *pgxpool.Pool
	publisher Publisher
}

func NewStore(pool *pgxpool.Pool, publisher Publisher) *Store {
	return &Store{pool: pool, publisher: publisher}
}

// OpenOrAttach implements "alerts can result in incidents, but don't create
// unlimited incidents from repeated alerts": it reuses any incident already
// active for this machine (enforced by the partial unique index from
// Phase 2, so this is race-safe, not just an application-level check)
// rather than opening a second one, logging the new alert's arrival as an
// ALERT_ATTACHED audit event instead.
func (s *Store) OpenOrAttach(ctx context.Context, alert AlertRef) (incidentID string, created bool, err error) {
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
		`INSERT INTO incidents (organization_id, alert_id, factory_id, machine_id, device_id, sensor_id, severity, status, title, description)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, 'OPEN', $8, $9)
		 ON CONFLICT (machine_id) WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING') DO NOTHING
		 RETURNING id`,
		alert.OrganizationID, alert.ID, nullIfEmpty(alert.FactoryID), nullIfEmpty(alert.MachineID), nullIfEmpty(alert.DeviceID), nullIfEmpty(alert.SensorID),
		alert.Severity, alert.Title, alert.Description,
	).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
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
	s.publish(ctx, "CREATED", id)
	return id, true, nil
}

// Transition moves an incident to a new status if the move is valid,
// recording the change in incident_events. actorUserID is nil for
// system-initiated transitions (e.g. none currently) and set for
// human-initiated ones once the caller has a JWT to identify who acted.
func (s *Store) Transition(ctx context.Context, incidentID, newStatus string, actorUserID *string, note string) error {
	var currentStatus string
	if err := s.pool.QueryRow(ctx, `SELECT status FROM incidents WHERE id = $1`, incidentID).Scan(&currentStatus); err != nil {
		return fmt.Errorf("load incident %s: %w", incidentID, err)
	}
	if !IsValidTransition(currentStatus, newStatus) {
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

	if err := s.logEvent(ctx, incidentID, "STATUS_CHANGE", actorUserID, currentStatus, newStatus, note); err != nil {
		return err
	}
	s.publish(ctx, "STATUS_CHANGED", incidentID)
	return nil
}

func (s *Store) Assign(ctx context.Context, incidentID, userID string, actorUserID *string) error {
	_, err := s.pool.Exec(ctx, `UPDATE incidents SET assigned_to = $1, updated_at = now() WHERE id = $2`, userID, incidentID)
	if err != nil {
		return fmt.Errorf("assign incident %s: %w", incidentID, err)
	}
	if err := s.logEvent(ctx, incidentID, "ASSIGNMENT", actorUserID, "", userID, "assigned to technician "+userID); err != nil {
		return err
	}
	s.publish(ctx, "ASSIGNED", incidentID)
	return nil
}

func (s *Store) Resolve(ctx context.Context, incidentID, resolutionNotes string, actorUserID *string) error {
	if _, err := s.pool.Exec(ctx, `UPDATE incidents SET resolution_notes = $1 WHERE id = $2`, resolutionNotes, incidentID); err != nil {
		return fmt.Errorf("set resolution notes for incident %s: %w", incidentID, err)
	}
	return s.Transition(ctx, incidentID, "RESOLVED", actorUserID, resolutionNotes)
}

// Get fetches an incident scoped to orgID — a resource that doesn't belong
// to that organization returns pgx.ErrNoRows, indistinguishable from "does
// not exist," which is the correct tenant-isolation behavior (never reveal
// that a resource exists in another organization).
func (s *Store) Get(ctx context.Context, orgID, incidentID string) (*Incident, error) {
	var inc Incident
	err := s.pool.QueryRow(ctx, `
		SELECT id, organization_id, COALESCE(alert_id::text,''), COALESCE(factory_id::text,''),
		       COALESCE(machine_id::text,''), COALESCE(device_id::text,''), COALESCE(sensor_id::text,''),
		       severity, status, title, description, COALESCE(assigned_to::text,''), COALESCE(resolution_notes,''),
		       opened_at, resolved_at, closed_at
		FROM incidents WHERE id = $1 AND organization_id = $2
	`, incidentID, orgID).Scan(
		&inc.ID, &inc.OrganizationID, &inc.AlertID, &inc.FactoryID, &inc.MachineID, &inc.DeviceID, &inc.SensorID,
		&inc.Severity, &inc.Status, &inc.Title, &inc.Description, &inc.AssignedTo, &inc.ResolutionNotes,
		&inc.OpenedAt, &inc.ResolvedAt, &inc.ClosedAt,
	)
	if err != nil {
		return nil, err
	}
	return &inc, nil
}

// List returns incidents for orgID, optionally filtered by status, newest
// first, with simple offset pagination.
func (s *Store) List(ctx context.Context, orgID, statusFilter string, limit, offset int) ([]Incident, error) {
	query := `
		SELECT id, organization_id, COALESCE(alert_id::text,''), COALESCE(factory_id::text,''),
		       COALESCE(machine_id::text,''), COALESCE(device_id::text,''), COALESCE(sensor_id::text,''),
		       severity, status, title, description, COALESCE(assigned_to::text,''), COALESCE(resolution_notes,''),
		       opened_at, resolved_at, closed_at
		FROM incidents WHERE organization_id = $1`
	args := []any{orgID}
	if statusFilter != "" {
		query += " AND status = $2"
		args = append(args, statusFilter)
	}
	query += fmt.Sprintf(" ORDER BY opened_at DESC LIMIT %d OFFSET %d", limit, offset)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()

	var out []Incident
	for rows.Next() {
		var inc Incident
		if err := rows.Scan(
			&inc.ID, &inc.OrganizationID, &inc.AlertID, &inc.FactoryID, &inc.MachineID, &inc.DeviceID, &inc.SensorID,
			&inc.Severity, &inc.Status, &inc.Title, &inc.Description, &inc.AssignedTo, &inc.ResolutionNotes,
			&inc.OpenedAt, &inc.ResolvedAt, &inc.ClosedAt,
		); err != nil {
			return nil, fmt.Errorf("scan incident row: %w", err)
		}
		out = append(out, inc)
	}
	return out, rows.Err()
}

func (s *Store) ListEvents(ctx context.Context, incidentID string) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, incident_id, event_type, actor_user_id, COALESCE(old_value,''), COALESCE(new_value,''), COALESCE(note,''), created_at
		FROM incident_events WHERE incident_id = $1 ORDER BY created_at ASC
	`, incidentID)
	if err != nil {
		return nil, fmt.Errorf("list incident events: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.IncidentID, &e.EventType, &e.ActorUserID, &e.OldValue, &e.NewValue, &e.Note, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan incident event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) logEvent(ctx context.Context, incidentID, eventType string, actorUserID *string, oldValue, newValue, note string) error {
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

// publish is best-effort: a Kafka outage must never fail the underlying
// Postgres mutation that already succeeded, so errors here are swallowed.
func (s *Store) publish(ctx context.Context, eventType, incidentID string) {
	if s.publisher == nil {
		return
	}
	full, err := s.fetchByID(ctx, incidentID)
	if err != nil {
		return
	}
	_ = s.publisher.PublishIncidentEvent(ctx, eventType, *full)
}

func (s *Store) fetchByID(ctx context.Context, incidentID string) (*Incident, error) {
	var inc Incident
	err := s.pool.QueryRow(ctx, `
		SELECT id, organization_id, COALESCE(alert_id::text,''), COALESCE(factory_id::text,''),
		       COALESCE(machine_id::text,''), COALESCE(device_id::text,''), COALESCE(sensor_id::text,''),
		       severity, status, title, description, COALESCE(assigned_to::text,''), COALESCE(resolution_notes,''),
		       opened_at, resolved_at, closed_at
		FROM incidents WHERE id = $1
	`, incidentID).Scan(
		&inc.ID, &inc.OrganizationID, &inc.AlertID, &inc.FactoryID, &inc.MachineID, &inc.DeviceID, &inc.SensorID,
		&inc.Severity, &inc.Status, &inc.Title, &inc.Description, &inc.AssignedTo, &inc.ResolutionNotes,
		&inc.OpenedAt, &inc.ResolvedAt, &inc.ClosedAt,
	)
	if err != nil {
		return nil, err
	}
	return &inc, nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
