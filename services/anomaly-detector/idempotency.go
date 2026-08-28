package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// anomalyDedupScope namespaces this service's claims in the shared
// idempotency_keys table (see migrations/000008_idempotency_keys.up.sql —
// "Used by any consumer/service that must guarantee 'process this
// event_id/request exactly once'", a table that existed but had no actual
// caller until this fix).
const anomalyDedupScope = "anomaly_detection"

// claimTelemetryEventOnce atomically claims a source telemetry event's
// EventID for anomaly-detection processing, using the same
// INSERT...ON CONFLICT DO NOTHING RETURNING pattern already used by
// alert-service (see createIfDue in store.go) for race-safe, at-least-once
// -delivery-tolerant dedup. Kafka redelivering telemetry.processed after a
// crash between detection and offset commit would otherwise re-run
// detection and publish a second AnomalyDetected (with a new AnomalyID) for
// the same underlying reading — this claim makes that a no-op instead.
// Returns true only the first time a given eventID is claimed.
func claimTelemetryEventOnce(ctx context.Context, pool *pgxpool.Pool, eventID string) (bool, error) {
	var id string
	err := pool.QueryRow(ctx,
		`INSERT INTO idempotency_keys (key, scope) VALUES ($1, $2) ON CONFLICT (scope, key) DO NOTHING RETURNING id`,
		eventID, anomalyDedupScope,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // already claimed by an earlier delivery of this event
	}
	if err != nil {
		return false, fmt.Errorf("claim event %q for anomaly detection: %w", eventID, err)
	}
	return true, nil
}
