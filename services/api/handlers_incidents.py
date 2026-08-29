"""Incident listing/transition/assignment/resolution endpoints, the Python
port of handlers_incidents.go. Wraps shared.incidents.Store (already
ported for alert-service) rather than reimplementing incident lifecycle
logic."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from pagination import parse_limit_offset
from response import APIError, internal_error, paginated_response
from shared import auth, incidents


def _format_time(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _incident_dto(inc: incidents.Incident) -> dict:
    dto = {
        "id": inc.id, "severity": inc.severity, "status": inc.status, "title": inc.title,
        "description": inc.description, "opened_at": _format_time(inc.opened_at),
    }
    if inc.alert_id:
        dto["alert_id"] = inc.alert_id
    if inc.factory_id:
        dto["factory_id"] = inc.factory_id
    if inc.machine_id:
        dto["machine_id"] = inc.machine_id
    if inc.device_id:
        dto["device_id"] = inc.device_id
    if inc.sensor_id:
        dto["sensor_id"] = inc.sensor_id
    if inc.assigned_to:
        dto["assigned_to"] = inc.assigned_to
    if inc.resolution_notes:
        dto["resolution_notes"] = inc.resolution_notes
    if inc.resolved_at is not None:
        dto["resolved_at"] = _format_time(inc.resolved_at)
    if inc.closed_at is not None:
        dto["closed_at"] = _format_time(inc.closed_at)
    return dto


def _incident_event_dto(e: incidents.Event) -> dict:
    dto = {"event_type": e.event_type, "created_at": _format_time(e.created_at)}
    if e.old_value:
        dto["old_value"] = e.old_value
    if e.new_value:
        dto["new_value"] = e.new_value
    if e.note:
        dto["note"] = e.note
    return dto


class TransitionRequest(BaseModel):
    status: str = ""
    note: str = ""


class AssignRequest(BaseModel):
    user_id: str = ""


class ResolveRequest(BaseModel):
    resolution_notes: str = ""


def register_incident_routes(store: incidents.Store, permission_dep: Callable[[str], Callable]) -> APIRouter:
    router = APIRouter()

    @router.get("/api/v1/incidents")
    def list_incidents(request: Request, claims: auth.Claims = Depends(permission_dep(auth.PERM_INCIDENTS_READ))) -> dict:
        limit, offset = parse_limit_offset(request)
        status_filter = request.query_params.get("status", "")
        try:
            listing = store.list(claims.organization_id, status_filter, limit, offset)
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        return paginated_response([_incident_dto(inc) for inc in listing], limit, offset)

    @router.get("/api/v1/incidents/{id}")
    def get_incident(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_INCIDENTS_READ))) -> dict:
        try:
            inc = store.get(claims.organization_id, id)
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        if inc is None:
            raise APIError(404, "NOT_FOUND", "incident does not exist")

        try:
            events = store.list_events(id)
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)

        return {"incident": _incident_dto(inc), "history": [_incident_event_dto(e) for e in events]}

    @router.post("/api/v1/incidents/{id}/transition", status_code=204, response_model=None)
    def transition_incident(id: str, req: TransitionRequest, claims: auth.Claims = Depends(permission_dep(auth.PERM_INCIDENTS_MANAGE))) -> None:
        if not req.status:
            raise APIError(400, "INVALID_BODY", "status is required")
        if store.get(claims.organization_id, id) is None:
            raise APIError(404, "NOT_FOUND", "incident does not exist")
        try:
            store.transition(id, req.status, claims.user_id, req.note)
        except (ValueError, RuntimeError) as exc:
            raise APIError(409, "INVALID_TRANSITION", str(exc))

    @router.post("/api/v1/incidents/{id}/assign", status_code=204, response_model=None)
    def assign_incident(id: str, req: AssignRequest, claims: auth.Claims = Depends(permission_dep(auth.PERM_INCIDENTS_MANAGE))) -> None:
        if not req.user_id:
            raise APIError(400, "INVALID_BODY", "user_id is required")
        if store.get(claims.organization_id, id) is None:
            raise APIError(404, "NOT_FOUND", "incident does not exist")
        try:
            store.assign(id, req.user_id, claims.user_id)
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)

    @router.post("/api/v1/incidents/{id}/resolve", status_code=204, response_model=None)
    def resolve_incident(id: str, req: ResolveRequest, claims: auth.Claims = Depends(permission_dep(auth.PERM_INCIDENTS_MANAGE))) -> None:
        if store.get(claims.organization_id, id) is None:
            raise APIError(404, "NOT_FOUND", "incident does not exist")
        try:
            store.resolve(id, req.resolution_notes, claims.user_id)
        except (ValueError, RuntimeError) as exc:
            raise APIError(409, "INVALID_TRANSITION", str(exc))

    return router
