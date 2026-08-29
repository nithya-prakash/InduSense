"""Alert listing/acknowledgment endpoints, the Python port of
handlers_alerts.go."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from psycopg_pool import ConnectionPool

from pagination import parse_limit_offset
from response import APIError, internal_error, paginated_response
from shared import auth


def _format_time(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _alert_dto(row) -> dict:
    """Builds the DTO the same way Go's `omitempty` JSON tags do: an
    empty machine_id/device_id or a nil resolved_at is left out of the
    payload entirely, not sent as null."""
    dto = {
        "id": str(row[0]), "severity": row[1], "status": row[2], "title": row[3], "description": row[4],
        "triggered_at": _format_time(row[7]),
    }
    if row[5]:
        dto["machine_id"] = row[5]
    if row[6]:
        dto["device_id"] = row[6]
    if row[8] is not None:
        dto["resolved_at"] = _format_time(row[8])
    return dto


def register_alert_routes(pool: ConnectionPool, permission_dep: Callable[[str], Callable], default_limit) -> APIRouter:
    router = APIRouter()

    @router.get("/api/v1/alerts", dependencies=[Depends(default_limit)])
    def list_alerts(request: Request, claims: auth.Claims = Depends(permission_dep(auth.PERM_ALERTS_READ))) -> dict:
        limit, offset = parse_limit_offset(request)

        query = """
            SELECT id, severity, status, title, description, COALESCE(machine_id::text,''), COALESCE(device_id::text,''), triggered_at, resolved_at
            FROM alerts WHERE organization_id = %s
        """
        params: list = [claims.organization_id]

        status_filter = request.query_params.get("status", "")
        if status_filter:
            query += " AND status = %s"
            params.append(status_filter)
        severity_filter = request.query_params.get("severity", "")
        if severity_filter:
            query += " AND severity = %s"
            params.append(severity_filter)
        query += " ORDER BY triggered_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        try:
            with pool.connection() as conn:
                rows = conn.execute(query, params).fetchall()
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        return paginated_response([_alert_dto(r) for r in rows], limit, offset)

    @router.get("/api/v1/alerts/{id}")
    def get_alert(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_ALERTS_READ))) -> dict:
        with pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, severity, status, title, description, COALESCE(machine_id::text,''), COALESCE(device_id::text,''), triggered_at, resolved_at
                FROM alerts WHERE id = %s AND organization_id = %s
                """,
                (id, claims.organization_id),
            ).fetchone()
        if row is None:
            raise APIError(404, "NOT_FOUND", "alert does not exist")
        return _alert_dto(row)

    @router.post("/api/v1/alerts/{id}/acknowledge", status_code=204, response_model=None, dependencies=[Depends(default_limit)])
    def acknowledge_alert(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_ALERTS_MANAGE))) -> None:
        """Moves an alert from OPEN to ACKNOWLEDGED -- the same "someone
        is looking at this" signal as an incident acknowledgment, but for
        the alert record itself, before or independent of any incident."""
        with pool.connection() as conn:
            cur = conn.execute(
                "UPDATE alerts SET status = 'ACKNOWLEDGED', updated_at = now() WHERE id = %s AND organization_id = %s AND status = 'OPEN'",
                (id, claims.organization_id),
            )
            if cur.rowcount == 0:
                raise APIError(409, "CONFLICT", "alert does not exist, belongs to another organization, or is not OPEN")

    return router
