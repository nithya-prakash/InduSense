"""Incident lifecycle management, the Python port of pkg/incidents.

Shared by alert-service (which opens incidents automatically from new
alerts) and the api service (which will expose manual transitions/
assignment to a human operator once it's ported). Extracted into shared/
for the same reason the Go version was pulled out of alert-service into
its own package once a second caller needed the exact same state machine
and persistence, rather than duplicating it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from psycopg_pool import ConnectionPool


@dataclass
class AlertRef:
    """The minimal alert data needed to open or attach an incident —
    deliberately independent of alert-service's own Alert type so this
    module has no dependency on it."""

    id: str
    organization_id: str
    severity: str
    factory_id: str = ""
    machine_id: str = ""
    device_id: str = ""
    sensor_id: str = ""
    title: str = ""
    description: str = ""


@dataclass
class Incident:
    id: str
    organization_id: str
    alert_id: str
    factory_id: str
    machine_id: str
    device_id: str
    sensor_id: str
    severity: str
    status: str
    title: str
    description: str
    assigned_to: str
    resolution_notes: str
    opened_at: datetime
    resolved_at: datetime | None
    closed_at: datetime | None


@dataclass
class Event:
    id: str
    incident_id: str
    event_type: str
    actor_user_id: str | None
    old_value: str
    new_value: str
    note: str
    created_at: datetime


class Publisher(Protocol):
    """Lets Store announce lifecycle changes on the `incidents` Kafka
    topic. Optional — a Store built without one simply skips publishing,
    since not every caller needs it."""

    def publish_incident_event(self, event_type: str, inc: Incident) -> None: ...


# The incident lifecycle state machine. RESOLVED can move back to
# INVESTIGATING (a recurrence) but CLOSED is terminal — once closed, a
# recurrence opens a new incident instead of reanimating an old one,
# keeping the audit trail honest about when the problem was first actually
# seen.
_VALID_TRANSITIONS: dict[str, list[str]] = {
    "OPEN": ["ACKNOWLEDGED", "INVESTIGATING", "RESOLVED"],
    "ACKNOWLEDGED": ["INVESTIGATING", "RESOLVED"],
    "INVESTIGATING": ["RESOLVED"],
    "RESOLVED": ["CLOSED", "INVESTIGATING"],
    "CLOSED": [],
}


def is_valid_transition(from_status: str, to_status: str) -> bool:
    return to_status in _VALID_TRANSITIONS.get(from_status, [])


_INCIDENT_COLUMNS = """
    id, organization_id, COALESCE(alert_id::text,''), COALESCE(factory_id::text,''),
    COALESCE(machine_id::text,''), COALESCE(device_id::text,''), COALESCE(sensor_id::text,''),
    severity, status, title, description, COALESCE(assigned_to::text,''), COALESCE(resolution_notes,''),
    opened_at, resolved_at, closed_at
"""


def _row_to_incident(row) -> Incident:
    return Incident(
        # id/organization_id are plain uuid columns in this query (unlike
        # the others below, which are ::text-cast in SQL) -- cast here too.
        id=str(row[0]), organization_id=str(row[1]), alert_id=row[2], factory_id=row[3],
        machine_id=row[4], device_id=row[5], sensor_id=row[6],
        severity=row[7], status=row[8], title=row[9], description=row[10],
        assigned_to=row[11], resolution_notes=row[12],
        opened_at=row[13], resolved_at=row[14], closed_at=row[15],
    )


class Store:
    def __init__(self, pool: ConnectionPool, publisher: Publisher | None = None):
        self._pool = pool
        self._publisher = publisher

    def open_or_attach(self, alert: AlertRef) -> tuple[str, bool]:
        """Implements "alerts can result in incidents, but don't create
        unlimited incidents from repeated alerts": reuses any incident
        already active for this machine (enforced by a partial unique
        index, so this is race-safe, not just an application-level check)
        rather than opening a second one, logging the new alert's arrival
        as an ALERT_ATTACHED audit event instead. Returns (incident_id,
        created)."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT id FROM incidents WHERE machine_id = %s AND status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING')",
                (alert.machine_id,),
            ).fetchone()

            if row is not None:
                # id/uuid columns come back as uuid.UUID, not str -- cast
                # explicitly everywhere one is returned from a query.
                existing_id = str(row[0])
                self._log_event(
                    conn, existing_id, "ALERT_ATTACHED", None, "", "",
                    f"alert {alert.id} ({alert.severity}) attached: {alert.title}",
                )
                return existing_id, False

            row = conn.execute(
                """
                INSERT INTO incidents (organization_id, alert_id, factory_id, machine_id, device_id, sensor_id, severity, status, title, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN', %s, %s)
                ON CONFLICT (machine_id) WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING') DO NOTHING
                RETURNING id
                """,
                (
                    alert.organization_id, alert.id, _null_if_empty(alert.factory_id), _null_if_empty(alert.machine_id),
                    _null_if_empty(alert.device_id), _null_if_empty(alert.sensor_id), alert.severity, alert.title, alert.description,
                ),
            ).fetchone()

            if row is None:
                # Lost a race with a concurrent insert for the same open incident.
                existing = conn.execute(
                    "SELECT id FROM incidents WHERE machine_id = %s AND status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING')",
                    (alert.machine_id,),
                ).fetchone()
                if existing is None:
                    raise RuntimeError(f"resolve incident creation race for machine {alert.machine_id}: no incident found")
                return str(existing[0]), False

            incident_id = str(row[0])
            self._log_event(conn, incident_id, "STATUS_CHANGE", None, "", "OPEN", f"incident opened from alert {alert.id}")
        self._publish("CREATED", incident_id)
        return incident_id, True

    def transition(self, incident_id: str, new_status: str, actor_user_id: str | None, note: str) -> None:
        """Moves an incident to a new status if the move is valid,
        recording the change in incident_events. actor_user_id is None for
        system-initiated transitions and set for human-initiated ones once
        the caller has identified who acted."""
        with self._pool.connection() as conn:
            row = conn.execute("SELECT status FROM incidents WHERE id = %s", (incident_id,)).fetchone()
            if row is None:
                raise RuntimeError(f"load incident {incident_id}: not found")
            current_status = row[0]
            if not is_valid_transition(current_status, new_status):
                raise ValueError(f"invalid incident transition {current_status} -> {new_status}")

            extra = ""
            if new_status == "RESOLVED":
                extra = ", resolved_at = now()"
            elif new_status == "CLOSED":
                extra = ", closed_at = now()"

            conn.execute(f"UPDATE incidents SET status = %s, updated_at = now(){extra} WHERE id = %s", (new_status, incident_id))
            self._log_event(conn, incident_id, "STATUS_CHANGE", actor_user_id, current_status, new_status, note)
        self._publish("STATUS_CHANGED", incident_id)

    def assign(self, incident_id: str, user_id: str, actor_user_id: str | None) -> None:
        with self._pool.connection() as conn:
            conn.execute("UPDATE incidents SET assigned_to = %s, updated_at = now() WHERE id = %s", (user_id, incident_id))
            self._log_event(conn, incident_id, "ASSIGNMENT", actor_user_id, "", user_id, f"assigned to technician {user_id}")
        self._publish("ASSIGNED", incident_id)

    def resolve(self, incident_id: str, resolution_notes: str, actor_user_id: str | None) -> None:
        with self._pool.connection() as conn:
            conn.execute("UPDATE incidents SET resolution_notes = %s WHERE id = %s", (resolution_notes, incident_id))
        self.transition(incident_id, "RESOLVED", actor_user_id, resolution_notes)

    def get(self, org_id: str, incident_id: str) -> Incident | None:
        """Fetches an incident scoped to org_id — a resource that doesn't
        belong to that organization returns None, indistinguishable from
        "does not exist," which is the correct tenant-isolation behavior
        (never reveal that a resource exists in another organization)."""
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT {_INCIDENT_COLUMNS} FROM incidents WHERE id = %s AND organization_id = %s",
                (incident_id, org_id),
            ).fetchone()
        return _row_to_incident(row) if row is not None else None

    def list(self, org_id: str, status_filter: str, limit: int, offset: int) -> list[Incident]:
        """Returns incidents for org_id, optionally filtered by status,
        newest first, with simple offset pagination."""
        query = f"SELECT {_INCIDENT_COLUMNS} FROM incidents WHERE organization_id = %s"
        params: list = [org_id]
        if status_filter:
            query += " AND status = %s"
            params.append(status_filter)
        query += " ORDER BY opened_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        with self._pool.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_row_to_incident(row) for row in rows]

    def list_events(self, incident_id: str) -> list[Event]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, incident_id, event_type, actor_user_id, COALESCE(old_value,''), COALESCE(new_value,''), COALESCE(note,''), created_at
                FROM incident_events WHERE incident_id = %s ORDER BY created_at ASC
                """,
                (incident_id,),
            ).fetchall()
        return [
            Event(
                id=str(r[0]), incident_id=str(r[1]), event_type=r[2],
                actor_user_id=str(r[3]) if r[3] is not None else None,
                old_value=r[4], new_value=r[5], note=r[6], created_at=r[7],
            )
            for r in rows
        ]

    def _log_event(self, conn, incident_id: str, event_type: str, actor_user_id: str | None, old_value: str, new_value: str, note: str) -> None:
        conn.execute(
            """
            INSERT INTO incident_events (incident_id, event_type, actor_user_id, old_value, new_value, note)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (incident_id, event_type, actor_user_id, _null_if_empty(old_value), _null_if_empty(new_value), note),
        )

    def _publish(self, event_type: str, incident_id: str) -> None:
        """Best-effort: a Kafka outage must never fail the underlying
        Postgres mutation that already succeeded, so errors here are
        swallowed."""
        if self._publisher is None:
            return
        full = self._fetch_by_id(incident_id)
        if full is None:
            return
        try:
            self._publisher.publish_incident_event(event_type, full)
        except Exception:  # noqa: BLE001
            pass

    def _fetch_by_id(self, incident_id: str) -> Incident | None:
        with self._pool.connection() as conn:
            row = conn.execute(f"SELECT {_INCIDENT_COLUMNS} FROM incidents WHERE id = %s", (incident_id,)).fetchone()
        return _row_to_incident(row) if row is not None else None


def _null_if_empty(s: str) -> str | None:
    return s if s else None
