import os
import uuid

import pytest
from psycopg_pool import ConnectionPool

from shared.incidents import AlertRef, Store, is_valid_transition


def test_is_valid_transition_allows_documented_paths():
    cases = [
        ("OPEN", "ACKNOWLEDGED"),
        ("OPEN", "INVESTIGATING"),
        ("OPEN", "RESOLVED"),
        ("ACKNOWLEDGED", "INVESTIGATING"),
        ("ACKNOWLEDGED", "RESOLVED"),
        ("INVESTIGATING", "RESOLVED"),
        ("RESOLVED", "CLOSED"),
        ("RESOLVED", "INVESTIGATING"),  # reopen a recurrence
    ]
    for frm, to in cases:
        assert is_valid_transition(frm, to), f"expected {frm} -> {to} to be valid"


def test_is_valid_transition_rejects_invalid_paths():
    cases = [
        ("CLOSED", "OPEN"),
        ("CLOSED", "RESOLVED"),
        ("OPEN", "CLOSED"),
        ("RESOLVED", "ACKNOWLEDGED"),
        ("INVESTIGATING", "OPEN"),
    ]
    for frm, to in cases:
        assert not is_valid_transition(frm, to), f"expected {frm} -> {to} to be rejected"


def test_is_valid_transition_closed_is_terminal():
    from shared.incidents import _VALID_TRANSITIONS

    assert _VALID_TRANSITIONS["CLOSED"] == []


def _real_pool() -> ConnectionPool:
    dsn = os.environ.get(
        "ALERT_POSTGRES_DSN",
        "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable",
    )
    try:
        pool = ConnectionPool(dsn, min_size=1, max_size=2, open=True, kwargs={"autocommit": True}, timeout=5.0)
        with pool.connection(timeout=5.0) as conn:
            conn.execute("SELECT 1")
        return pool
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")


def test_incident_lifecycle_against_real_postgres():
    """Exercises the full incident state machine -- open, attach,
    acknowledge, investigate, resolve, reopen, close, plus assignment and
    invalid-transition rejection -- against a real Postgres instance, not
    a mock. Creates its own throwaway organization/factory/machine row so
    it doesn't collide with the seeded demo data's "one active incident
    per machine" constraint, and cleans up after itself."""
    pool = _real_pool()
    try:
        with pool.connection() as conn:
            org_id = str(conn.execute(
                "INSERT INTO organizations (name, slug) VALUES ('Test Org', 'test-org-' || gen_random_uuid()) RETURNING id"
            ).fetchone()[0])
        try:
            with pool.connection() as conn:
                factory_id = str(conn.execute(
                    "INSERT INTO factories (organization_id, name, city) VALUES (%s, 'Test Factory', 'Testville') RETURNING id",
                    (org_id,),
                ).fetchone()[0])
                line_id = str(conn.execute(
                    "INSERT INTO production_lines (factory_id, name) VALUES (%s, 'Test Line') RETURNING id",
                    (factory_id,),
                ).fetchone()[0])
                machine_id = str(conn.execute(
                    "INSERT INTO machines (production_line_id, name, machine_type) VALUES (%s, 'Test Machine', 'TEST_TYPE') RETURNING id",
                    (line_id,),
                ).fetchone()[0])
                technician_id = str(conn.execute(
                    "INSERT INTO users (organization_id, email, password_hash, full_name) VALUES (%s, %s, 'x', 'Test Technician') RETURNING id",
                    (org_id, f"tech-{uuid.uuid4()}@test.local"),
                ).fetchone()[0])

            store = Store(pool, None)

            def insert_alert(severity: str, title: str, description: str) -> AlertRef:
                with pool.connection() as c:
                    alert_id = str(c.execute(
                        """
                        INSERT INTO alerts (organization_id, machine_id, severity, title, description, dedupe_key)
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                        """,
                        (org_id, machine_id, severity, title, description, f"test-dedupe-{uuid.uuid4()}"),
                    ).fetchone()[0])
                return AlertRef(id=alert_id, organization_id=org_id, severity=severity, machine_id=machine_id, title=title, description=description)

            alert = insert_alert("HIGH", "Test alert", "test description")
            incident_id, created = store.open_or_attach(alert)
            assert created, "expected the first alert for this machine to create a new incident"

            alert2 = insert_alert("CRITICAL", "Second alert", "another reading")
            incident_id2, created2 = store.open_or_attach(alert2)
            assert not created2, "expected the second alert for the same machine to attach to the existing incident"
            assert incident_id2 == incident_id

            inc = store.get(org_id, incident_id)
            assert inc is not None
            assert inc.status == "OPEN"

            # Cross-tenant get must behave as not-found.
            assert store.get("00000000-0000-0000-0000-000000000000", incident_id) is None

            with pytest.raises(ValueError):
                store.transition(incident_id, "CLOSED", None, "skip straight to closed")

            store.transition(incident_id, "ACKNOWLEDGED", None, "ack'd by test")
            store.assign(incident_id, technician_id, None)
            store.transition(incident_id, "INVESTIGATING", None, "looking into it")
            store.resolve(incident_id, "root cause found and fixed", None)

            resolved = store.get(org_id, incident_id)
            assert resolved.resolved_at is not None
            assert resolved.assigned_to == technician_id
            assert resolved.resolution_notes == "root cause found and fixed"

            store.transition(incident_id, "INVESTIGATING", None, "recurred")
            store.transition(incident_id, "RESOLVED", None, "fixed for real this time")
            store.transition(incident_id, "CLOSED", None, "closing out")
            with pytest.raises(ValueError):
                store.transition(incident_id, "INVESTIGATING", None, "should fail")

            events = store.list_events(incident_id)
            # open, attach, ack, assign, investigate, resolve, reopen, re-resolve, close = 9
            assert len(events) == 9

            alert3 = insert_alert("WARNING", "Third alert", "recurrence after closure")
            incident_id3, created3 = store.open_or_attach(alert3)
            assert created3, "expected a new incident after the previous one closed"
            assert incident_id3 != incident_id

            listing = store.list(org_id, "", 10, 0)
            assert len(listing) == 2  # the closed one + the new one from alert3
        finally:
            with pool.connection() as conn:
                conn.execute("DELETE FROM organizations WHERE id = %s", (org_id,))
    finally:
        pool.close()
