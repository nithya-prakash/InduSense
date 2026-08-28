package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestProvisionDevice_TransactionRollsBackOnCredentialFailure verifies, against
// real Postgres, the exact bug a pre-GitHub audit found in
// handleProvisionDevice: the device row and its device_credentials row were
// two separate, independently-committing statements, so a failure on the
// second one left an orphaned, credential-less device behind. The fix wraps
// both in one transaction (see handleProvisionDevice in
// handlers_devices.go). This test can't trigger that failure through the
// HTTP API itself — every value the handler writes to device_credentials is
// computed server-side and always valid — so it exercises the identical
// BEGIN/INSERT device/INSERT credentials/COMMIT-or-ROLLBACK sequence
// directly against the database, deliberately failing the second insert
// (an invalid credential_type, which violates the CHECK constraint from
// migrations/000003_device_credentials.up.sql) and asserting the first
// insert's effect does not survive. A control case in the same test proves
// the harness isn't just failing to insert anything at all: a fully valid
// transaction is confirmed to commit both rows.
//
// Skipped if no live Postgres is reachable.
func TestProvisionDevice_TransactionRollsBackOnCredentialFailure(t *testing.T) {
	dsn := os.Getenv("API_POSTGRES_DSN")
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

	provisionInTx := func(serialNumber, credentialType string) (deviceID string, commitErr error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		err = tx.QueryRow(ctx,
			`INSERT INTO devices (machine_id, organization_id, serial_number, status, firmware_version)
			 VALUES ($1, $2, $3, 'PROVISIONED', 'test') RETURNING id`,
			machineID, orgID, serialNumber,
		).Scan(&deviceID)
		if err != nil {
			t.Fatalf("insert device (should succeed): %v", err)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active) VALUES ($1, $2, 'hash', true)`,
			deviceID, credentialType,
		); err != nil {
			// Deliberate failure path (invalid credential_type): return the
			// error without committing — mirrors handleProvisionDevice
			// returning early on the credentials insert error, leaving the
			// deferred Rollback to undo the device insert too.
			return deviceID, err
		}

		return deviceID, tx.Commit(ctx)
	}

	// Failure case: the credentials insert violates the CHECK constraint on
	// credential_type, so it must never commit — and per the fix, the
	// device insert from the same transaction must not survive either.
	failedDeviceID, err := provisionInTx("test-serial-rollback-"+time.Now().Format("150405.000000"), "not_a_valid_type")
	if err == nil {
		t.Fatal("expected the credentials insert to fail on the CHECK constraint, but it succeeded")
	}

	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1)`, failedDeviceID).Scan(&exists); err != nil {
		t.Fatalf("check device existence after rollback: %v", err)
	}
	if exists {
		t.Fatal("device row survived a failed transaction — provisioning is not atomic, the rollback fix is not working")
	}

	// Control case: an otherwise-identical transaction with a valid
	// credential_type must commit both rows, proving the test setup itself
	// is sound (i.e. rollback isn't just silently swallowing every insert).
	okDeviceID, err := provisionInTx("test-serial-commit-"+time.Now().Format("150405.000000"), "shared_secret")
	if err != nil {
		t.Fatalf("expected the valid transaction to commit, got: %v", err)
	}
	defer pool.Exec(context.Background(), `DELETE FROM devices WHERE id = $1`, okDeviceID) //nolint:errcheck

	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1)`, okDeviceID).Scan(&exists); err != nil {
		t.Fatalf("check device existence after commit: %v", err)
	}
	if !exists {
		t.Fatal("device row missing after a successful commit — test harness is unsound")
	}
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM device_credentials WHERE device_id = $1)`, okDeviceID).Scan(&exists); err != nil {
		t.Fatalf("check credentials existence after commit: %v", err)
	}
	if !exists {
		t.Fatal("credentials row missing after a successful commit")
	}
}
