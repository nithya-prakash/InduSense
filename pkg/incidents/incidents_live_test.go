package incidents

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
// after itself. Skipped if no live Postgres is reachable.
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

	store := NewStore(pool, nil)

	insertAlert := func(severity, title, description string) AlertRef {
		var id string
		err := pool.QueryRow(ctx,
			`INSERT INTO alerts (organization_id, machine_id, severity, title, description, dedupe_key)
			 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			orgID, machineID, severity, title, description, "test-dedupe-"+uuid.NewString(),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert test alert %q: %v", title, err)
		}
		return AlertRef{ID: id, OrganizationID: orgID, Severity: severity, MachineID: machineID, Title: title, Description: description}
	}

	alert := insertAlert("HIGH", "Test alert", "test description")
	incidentID, created, err := store.OpenOrAttach(ctx, alert)
	if err != nil {
		t.Fatalf("OpenOrAttach (first): %v", err)
	}
	if !created {
		t.Fatal("expected the first alert for this machine to create a new incident")
	}

	alert2 := insertAlert("CRITICAL", "Second alert", "another reading")
	incidentID2, created2, err := store.OpenOrAttach(ctx, alert2)
	if err != nil {
		t.Fatalf("OpenOrAttach (second): %v", err)
	}
	if created2 {
		t.Fatal("expected the second alert for the same machine to attach to the existing incident, not create a new one")
	}
	if incidentID2 != incidentID {
		t.Fatalf("expected attach to return the same incident id, got %s vs %s", incidentID2, incidentID)
	}

	inc, err := store.Get(ctx, orgID, incidentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inc.Status != "OPEN" {
		t.Fatalf("expected new incident status OPEN, got %s", inc.Status)
	}

	// Cross-tenant Get must behave as not-found.
	if _, err := store.Get(ctx, "00000000-0000-0000-0000-000000000000", incidentID); err == nil {
		t.Fatal("expected Get with the wrong organization_id to fail as not-found")
	}

	if err := store.Transition(ctx, incidentID, "CLOSED", nil, "skip straight to closed"); err == nil {
		t.Fatal("expected OPEN -> CLOSED to be rejected as an invalid transition")
	}

	if err := store.Transition(ctx, incidentID, "ACKNOWLEDGED", nil, "ack'd by test"); err != nil {
		t.Fatalf("transition to ACKNOWLEDGED: %v", err)
	}
	if err := store.Assign(ctx, incidentID, technicianID, nil); err != nil {
		t.Fatalf("assign: %v", err)
	}
	if err := store.Transition(ctx, incidentID, "INVESTIGATING", nil, "looking into it"); err != nil {
		t.Fatalf("transition to INVESTIGATING: %v", err)
	}
	if err := store.Resolve(ctx, incidentID, "root cause found and fixed", nil); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	resolved, err := store.Get(ctx, orgID, incidentID)
	if err != nil {
		t.Fatalf("Get after resolve: %v", err)
	}
	if resolved.ResolvedAt == nil {
		t.Error("expected resolved_at to be set after Resolve()")
	}
	if resolved.AssignedTo != technicianID {
		t.Errorf("AssignedTo = %q, want %q", resolved.AssignedTo, technicianID)
	}
	if resolved.ResolutionNotes != "root cause found and fixed" {
		t.Errorf("ResolutionNotes = %q, want the resolve() note", resolved.ResolutionNotes)
	}

	if err := store.Transition(ctx, incidentID, "INVESTIGATING", nil, "recurred"); err != nil {
		t.Fatalf("reopen to INVESTIGATING: %v", err)
	}
	if err := store.Transition(ctx, incidentID, "RESOLVED", nil, "fixed for real this time"); err != nil {
		t.Fatalf("re-resolve: %v", err)
	}
	if err := store.Transition(ctx, incidentID, "CLOSED", nil, "closing out"); err != nil {
		t.Fatalf("transition to CLOSED: %v", err)
	}
	if err := store.Transition(ctx, incidentID, "INVESTIGATING", nil, "should fail"); err == nil {
		t.Fatal("expected CLOSED to be terminal — no transition should succeed from it")
	}

	events, err := store.ListEvents(ctx, incidentID)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	// open, attach, ack, assign, investigate, resolve, reopen, re-resolve, close = 9
	if len(events) != 9 {
		t.Errorf("expected 9 audit events for the full lifecycle, got %d", len(events))
	}

	alert3 := insertAlert("WARNING", "Third alert", "recurrence after closure")
	incidentID3, created3, err := store.OpenOrAttach(ctx, alert3)
	if err != nil {
		t.Fatalf("OpenOrAttach (after closure): %v", err)
	}
	if !created3 {
		t.Fatal("expected a new incident after the previous one closed")
	}
	if incidentID3 == incidentID {
		t.Fatal("expected a genuinely new incident id, not the closed one")
	}

	list, err := store.List(ctx, orgID, "", 10, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 { // the closed one + the new one from alert3
		t.Errorf("expected 2 incidents in List, got %d", len(list))
	}
}
