package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestIncidentLifecycleAgainstRealPostgres exercises the full incident
// state machine — open, attach, acknowledge, investigate, resolve, reopen,
// close, plus assignment and invalid-transition rejection — against a real
// Postgres instance, not a mock. It creates its own throwaway
// organization/factory/machine row so it doesn't collide with the seeded
// demo data's "one active incident per machine" constraint, and cleans up
// after itself. Skipped if no live Postgres is reachable (e.g. in an
// environment without Docker Compose running).
func TestIncidentLifecycleAgainstRealPostgres(t *testing.T) {
	dsn := os.Getenv("ALERT_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no live Postgres reachable, skipping: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("no live Postgres reachable, skipping: %v", err)
	}

	var orgID, factoryID, lineID, machineID string
	err = pool.QueryRow(ctx, `INSERT INTO organizations (name, slug) VALUES ('Test Org', 'test-org-'||gen_random_uuid()) RETURNING id`).Scan(&orgID)
	if err != nil {
		t.Fatalf("insert test org: %v", err)
	}
	defer pool.Exec(context.Background(), `DELETE FROM organizations WHERE id = $1`, orgID) //nolint:errcheck

	err = pool.QueryRow(ctx, `INSERT INTO factories (organization_id, name, city) VALUES ($1, 'Test Factory', 'Testville') RETURNING id`, orgID).Scan(&factoryID)
	if err != nil {
		t.Fatalf("insert test factory: %v", err)
	}
	err = pool.QueryRow(ctx, `INSERT INTO production_lines (factory_id, name) VALUES ($1, 'Test Line') RETURNING id`, factoryID).Scan(&lineID)
	if err != nil {
		t.Fatalf("insert test line: %v", err)
	}
	err = pool.QueryRow(ctx, `INSERT INTO machines (production_line_id, name, machine_type) VALUES ($1, 'Test Machine', 'TEST_TYPE') RETURNING id`, lineID).Scan(&machineID)
	if err != nil {
		t.Fatalf("insert test machine: %v", err)
	}

	var technicianID string
	err = pool.QueryRow(ctx,
		`INSERT INTO users (organization_id, email, password_hash, full_name) VALUES ($1, 'tech-'||gen_random_uuid()||'@test.local', 'x', 'Test Technician') RETURNING id`,
		orgID,
	).Scan(&technicianID)
	if err != nil {
		t.Fatalf("insert test technician: %v", err)
	}

	incidents := newIncidentStore(pool)

	// incidents.alert_id is a real foreign key into alerts, so — matching
	// how the real flow works (store.createIfDue always persists the alert
	// row before openOrAttachIncident is called) — the test inserts genuine
	// alert rows rather than fabricating IDs.
	insertAlert := func(severity, title, description string) Alert {
		var id string
		err := pool.QueryRow(ctx,
			`INSERT INTO alerts (organization_id, machine_id, severity, title, description, dedupe_key)
			 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			orgID, machineID, severity, title, description, "test-dedupe-"+uuid.NewString(),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert test alert %q: %v", title, err)
		}
		return Alert{ID: id, OrganizationID: orgID, Severity: severity, MachineID: machineID, Title: title, Description: description}
	}

	alert := insertAlert("HIGH", "Test alert", "test description")
	incidentID, created, err := incidents.openOrAttach(ctx, alert)
	if err != nil {
		t.Fatalf("openOrAttach (first): %v", err)
	}
	if !created {
		t.Fatal("expected the first alert for this machine to create a new incident")
	}

	// A second alert for the SAME machine must attach, not create another
	// incident — this is the "don't create unlimited incidents from
	// repeated alerts" requirement, enforced by the DB constraint, not just
	// application logic.
	alert2 := insertAlert("CRITICAL", "Second alert", "another reading")
	incidentID2, created2, err := incidents.openOrAttach(ctx, alert2)
	if err != nil {
		t.Fatalf("openOrAttach (second): %v", err)
	}
	if created2 {
		t.Fatal("expected the second alert for the same machine to attach to the existing incident, not create a new one")
	}
	if incidentID2 != incidentID {
		t.Fatalf("expected attach to return the same incident id, got %s vs %s", incidentID2, incidentID)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM incidents WHERE id = $1`, incidentID).Scan(&status); err != nil {
		t.Fatalf("query incident status: %v", err)
	}
	if status != "OPEN" {
		t.Fatalf("expected new incident status OPEN, got %s", status)
	}

	// Invalid transition should be rejected without mutating anything.
	if err := incidents.transition(ctx, incidentID, "CLOSED", nil, "skip straight to closed"); err == nil {
		t.Fatal("expected OPEN -> CLOSED to be rejected as an invalid transition")
	}

	if err := incidents.transition(ctx, incidentID, "ACKNOWLEDGED", nil, "ack'd by test"); err != nil {
		t.Fatalf("transition to ACKNOWLEDGED: %v", err)
	}
	if err := incidents.assign(ctx, incidentID, technicianID, nil); err != nil {
		t.Fatalf("assign: %v", err)
	}
	if err := incidents.transition(ctx, incidentID, "INVESTIGATING", nil, "looking into it"); err != nil {
		t.Fatalf("transition to INVESTIGATING: %v", err)
	}
	if err := incidents.resolve(ctx, incidentID, "root cause found and fixed", nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	var resolvedAt *time.Time
	var assignedTo, resolutionNotes string
	if err := pool.QueryRow(ctx, `SELECT resolved_at, assigned_to, resolution_notes FROM incidents WHERE id = $1`, incidentID).
		Scan(&resolvedAt, &assignedTo, &resolutionNotes); err != nil {
		t.Fatalf("query resolved incident: %v", err)
	}
	if resolvedAt == nil {
		t.Error("expected resolved_at to be set after resolve()")
	}
	if assignedTo != technicianID {
		t.Errorf("assigned_to = %q, want %q", assignedTo, technicianID)
	}
	if resolutionNotes != "root cause found and fixed" {
		t.Errorf("resolution_notes = %q, want the resolve() note", resolutionNotes)
	}

	// A recurrence can reopen a resolved incident back to INVESTIGATING...
	if err := incidents.transition(ctx, incidentID, "INVESTIGATING", nil, "recurred"); err != nil {
		t.Fatalf("reopen to INVESTIGATING: %v", err)
	}
	if err := incidents.transition(ctx, incidentID, "RESOLVED", nil, "fixed for real this time"); err != nil {
		t.Fatalf("re-resolve: %v", err)
	}
	// ...and once CLOSED, it's terminal.
	if err := incidents.transition(ctx, incidentID, "CLOSED", nil, "closing out"); err != nil {
		t.Fatalf("transition to CLOSED: %v", err)
	}
	if err := incidents.transition(ctx, incidentID, "INVESTIGATING", nil, "should fail"); err == nil {
		t.Fatal("expected CLOSED to be terminal — no transition should succeed from it")
	}

	// The full audit trail should be present in incident_events.
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM incident_events WHERE incident_id = $1`, incidentID).Scan(&eventCount); err != nil {
		t.Fatalf("count incident_events: %v", err)
	}
	// open, attach, ack, assign, investigate, resolve, reopen, re-resolve, close = 9
	if eventCount != 9 {
		t.Errorf("expected 9 audit events for the full lifecycle, got %d", eventCount)
	}

	// A NEW alert after closure should open a fresh incident, not reuse the
	// closed one — closed incidents are terminal, per the state machine.
	alert3 := insertAlert("WARNING", "Third alert", "recurrence after closure")
	incidentID3, created3, err := incidents.openOrAttach(ctx, alert3)
	if err != nil {
		t.Fatalf("openOrAttach (after closure): %v", err)
	}
	if !created3 {
		t.Fatal("expected a new incident after the previous one closed")
	}
	if incidentID3 == incidentID {
		t.Fatal("expected a genuinely new incident id, not the closed one")
	}
}
