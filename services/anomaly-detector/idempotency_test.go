package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestClaimTelemetryEventOnce_DedupesAgainstRealPostgres verifies the fix
// for a pre-GitHub audit finding: anomaly-detector generated a fresh
// AnomalyID and republished on every call to processMessage, so a Kafka
// redelivery of the same telemetry.processed message (e.g. after a crash
// between publish and offset commit) produced a second, distinct anomaly —
// and, downstream, a second alert/incident — for one physical reading.
// Exercises the real idempotency_keys table (not a mock): the first claim
// for an event ID must succeed, a second claim for the same event ID must
// report it as already claimed, and a different event ID must be
// unaffected by either. Skipped if no live Postgres is reachable.
func TestClaimTelemetryEventOnce_DedupesAgainstRealPostgres(t *testing.T) {
	dsn := os.Getenv("ANOMALY_POSTGRES_DSN")
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

	eventID := uuid.NewString()
	defer pool.Exec(context.Background(), //nolint:errcheck
		`DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2`, anomalyDedupScope, eventID)

	claimed, err := claimTelemetryEventOnce(ctx, pool, eventID)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed {
		t.Fatal("expected the first claim for a never-seen event ID to succeed")
	}

	claimedAgain, err := claimTelemetryEventOnce(ctx, pool, eventID)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimedAgain {
		t.Fatal("expected a second claim for the same event ID (simulating Kafka redelivery) to report already-claimed, not succeed again")
	}

	otherEventID := uuid.NewString()
	defer pool.Exec(context.Background(), //nolint:errcheck
		`DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2`, anomalyDedupScope, otherEventID)

	claimedOther, err := claimTelemetryEventOnce(ctx, pool, otherEventID)
	if err != nil {
		t.Fatalf("claim for a different event ID: %v", err)
	}
	if !claimedOther {
		t.Fatal("expected a claim for a genuinely different event ID to succeed even though another event ID was already claimed")
	}
}
