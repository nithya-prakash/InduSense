"""InfluxDB-backed telemetry query endpoints, the Python port of
handlers_telemetry.go."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from influxdb_client.client.query_api import QueryApi
from psycopg_pool import ConnectionPool

from response import APIError, internal_error, paginated_response
from shared import auth
from shared.events import VALID_METRICS


def _format_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_device_and_metric(request: Request, claims: auth.Claims, pool: ConnectionPool) -> tuple[str, str]:
    """Validates the query params and confirms the device belongs to the
    caller's organization, raising APIError if not."""
    device_id = request.query_params.get("device_id", "")
    metric = request.query_params.get("metric", "")
    if not device_id or not metric:
        raise APIError(400, "VALIDATION_ERROR", "device_id and metric query parameters are required")
    # Validated before use in the Postgres lookup (avoids a raw Postgres
    # type-cast error surfacing as a 500) and before interpolation into
    # the Flux query string below (closes off any Flux-injection surface
    # -- only a well-formed UUID ever reaches that string).
    try:
        uuid.UUID(device_id)
    except ValueError:
        raise APIError(400, "VALIDATION_ERROR", "device_id must be a valid UUID")
    if metric not in VALID_METRICS:
        raise APIError(400, "VALIDATION_ERROR", "metric is not a recognized sensor metric")

    with pool.connection() as conn:
        exists = conn.execute(
            "SELECT EXISTS(SELECT 1 FROM devices WHERE id = %s AND organization_id = %s)", (device_id, claims.organization_id)
        ).fetchone()[0]
    if not exists:
        raise APIError(404, "NOT_FOUND", "device does not exist")
    return device_id, metric


def _build_range_clause(request: Request) -> str:
    start = request.query_params.get("start", "")
    if start:
        end = request.query_params.get("end", "")
        if not _is_rfc3339(start):
            raise ValueError("start must be RFC3339")
        if not end:
            return f"start: {start}"
        if not _is_rfc3339(end):
            raise ValueError("end must be RFC3339")
        return f"start: {start}, stop: {end}"

    range_param = request.query_params.get("range", "")
    if range_param in ("5m", ""):
        return "start: -5m"
    if range_param == "1h":
        return "start: -1h"
    if range_param == "24h":
        return "start: -24h"
    raise ValueError("range must be one of 5m, 1h, 24h, or provide start (and optionally end) as RFC3339 timestamps")


def _is_rfc3339(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def register_telemetry_routes(pool: ConnectionPool, query_api: QueryApi, bucket: str, permission_dep: Callable[[str], Callable], default_limit) -> APIRouter:
    router = APIRouter()

    @router.get("/api/v1/telemetry/latest", dependencies=[Depends(default_limit)])
    def telemetry_latest(request: Request, claims: auth.Claims = Depends(permission_dep(auth.PERM_TELEMETRY_READ))) -> dict:
        """Returns the most recent reading for a sensor. The device_id is
        verified against the caller's organization in Postgres before the
        InfluxDB query even runs -- InfluxDB itself has no concept of this
        system's tenants, so the tenant check has to happen at this
        layer."""
        device_id, metric = _require_device_and_metric(request, claims, pool)

        flux = f'''
            from(bucket: "{bucket}")
              |> range(start: -24h)
              |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r.device_id == "{device_id}" and r.metric == "{metric}" and r._field == "value")
              |> last()
        '''

        try:
            tables = query_api.query(flux)
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)

        for table in tables:
            for record in table.records:
                return {"time": _format_time(record.get_time()), "value": float(record.get_value())}
        raise APIError(404, "NOT_FOUND", "no telemetry found for this device/metric in the last 24h")

    @router.get("/api/v1/telemetry/range", dependencies=[Depends(default_limit)])
    def telemetry_range(request: Request, claims: auth.Claims = Depends(permission_dep(auth.PERM_TELEMETRY_READ))) -> dict:
        """Returns readings over a caller-selected window -- one of the
        fixed presets (5m/1h/24h) or a custom start/end pair."""
        device_id, metric = _require_device_and_metric(request, claims, pool)

        try:
            range_clause = _build_range_clause(request)
        except ValueError as exc:
            raise APIError(400, "VALIDATION_ERROR", str(exc))

        flux = f'''
            from(bucket: "{bucket}")
              |> range({range_clause})
              |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r.device_id == "{device_id}" and r.metric == "{metric}" and r._field == "value")
              |> sort(columns: ["_time"])
        '''

        try:
            tables = query_api.query(flux)
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)

        out = [{"time": _format_time(record.get_time()), "value": float(record.get_value())} for table in tables for record in table.records]
        return paginated_response(out, len(out), 0)

    return router
