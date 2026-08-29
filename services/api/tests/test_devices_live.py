import os
import time
import uuid

import psycopg.errors
import pytest
from psycopg_pool import ConnectionPool


def _real_pool() -> ConnectionPool:
    dsn = os.environ.get("API_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable")
    try:
        pool = ConnectionPool(dsn, min_size=1, max_size=2, open=True, kwargs={"autocommit": True}, timeout=5.0)
        with pool.connection(timeout=5.0) as conn:
            conn.execute("SELECT 1")
        return pool
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")


def test_provision_device_transaction_rolls_back_on_credential_failure():
    """Verifies, against real Postgres, the exact bug a pre-GitHub audit
    found in handleProvisionDevice: the device row and its
    device_credentials row were two separate, independently-committing
    statements, so a failure on the second one left an orphaned,
    credential-less device behind. The fix wraps both in one transaction
    (see provision_device in handlers_devices.py, which uses the exact
    same conn.transaction() context manager exercised here directly).
    This test can't trigger that failure through the HTTP API itself --
    every value the handler writes to device_credentials is computed
    server-side and always valid -- so it exercises the identical
    transaction/INSERT device/INSERT credentials/commit-or-rollback
    sequence directly against the database, deliberately failing the
    second insert (an invalid credential_type, which violates the CHECK
    constraint) and asserting the first insert's effect does not survive.
    A control case proves the harness isn't just failing to insert
    anything at all: a fully valid transaction is confirmed to commit
    both rows."""
    pool = _real_pool()
    try:
        with pool.connection() as conn:
            org_id = str(conn.execute(
                "INSERT INTO organizations (name, slug) VALUES ('Test Org', 'test-org-' || gen_random_uuid()) RETURNING id"
            ).fetchone()[0])
        try:
            with pool.connection() as conn:
                factory_id = str(conn.execute(
                    "INSERT INTO factories (organization_id, name, city) VALUES (%s, 'Test Factory', 'Testville') RETURNING id", (org_id,)
                ).fetchone()[0])
                line_id = str(conn.execute(
                    "INSERT INTO production_lines (factory_id, name) VALUES (%s, 'Test Line') RETURNING id", (factory_id,)
                ).fetchone()[0])
                machine_id = str(conn.execute(
                    "INSERT INTO machines (production_line_id, name, machine_type) VALUES (%s, 'Test Machine', 'TEST_TYPE') RETURNING id", (line_id,)
                ).fetchone()[0])

            def provision_in_tx(serial_number: str, credential_type: str) -> tuple[str, Exception | None]:
                with pool.connection() as conn:
                    try:
                        with conn.transaction():
                            device_id = str(conn.execute(
                                """
                                INSERT INTO devices (machine_id, organization_id, serial_number, status, firmware_version)
                                VALUES (%s, %s, %s, 'PROVISIONED', 'test') RETURNING id
                                """,
                                (machine_id, org_id, serial_number),
                            ).fetchone()[0])
                            conn.execute(
                                "INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active) VALUES (%s, %s, 'hash', true)",
                                (device_id, credential_type),
                            )
                    except Exception as exc:  # noqa: BLE001
                        return device_id, exc
                return device_id, None

            # Failure case: the credentials insert violates the CHECK
            # constraint on credential_type, so it must never commit --
            # and per the fix, the device insert from the same
            # transaction must not survive either.
            failed_device_id, exc = provision_in_tx(f"test-serial-rollback-{time.time()}", "not_a_valid_type")
            assert isinstance(exc, psycopg.errors.CheckViolation), f"expected a CHECK constraint violation, got {exc!r}"

            with pool.connection() as conn:
                exists = conn.execute("SELECT EXISTS(SELECT 1 FROM devices WHERE id = %s)", (failed_device_id,)).fetchone()[0]
            assert not exists, "device row survived a failed transaction -- provisioning is not atomic"

            # Control case: an otherwise-identical transaction with a
            # valid credential_type must commit both rows, proving the
            # test setup itself is sound.
            ok_device_id, exc = provision_in_tx(f"test-serial-commit-{time.time()}", "shared_secret")
            try:
                assert exc is None, f"expected the valid transaction to commit, got: {exc}"
                with pool.connection() as conn:
                    device_exists = conn.execute("SELECT EXISTS(SELECT 1 FROM devices WHERE id = %s)", (ok_device_id,)).fetchone()[0]
                    creds_exist = conn.execute("SELECT EXISTS(SELECT 1 FROM device_credentials WHERE device_id = %s)", (ok_device_id,)).fetchone()[0]
                assert device_exists, "device row missing after a successful commit -- test harness is unsound"
                assert creds_exist, "credentials row missing after a successful commit"
            finally:
                with pool.connection() as conn:
                    conn.execute("DELETE FROM devices WHERE id = %s", (ok_device_id,))
        finally:
            with pool.connection() as conn:
                conn.execute("DELETE FROM organizations WHERE id = %s", (org_id,))
    finally:
        pool.close()
