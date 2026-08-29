"""Device listing/provisioning/credential-rotation/decommission endpoints,
the Python port of handlers_devices.go."""

from __future__ import annotations

import secrets
from collections.abc import Callable

import psycopg.errors
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from psycopg_pool import ConnectionPool

from pagination import parse_limit_offset
from response import APIError, internal_error, paginated_response
from shared import auth


def _device_dto(row) -> dict:
    return {"id": str(row[0]), "serial_number": row[1], "status": row[2], "firmware_version": row[3]}


class ProvisionDeviceRequest(BaseModel):
    machine_id: str = ""
    serial_number: str = ""
    firmware_version: str = ""


def register_device_routes(pool: ConnectionPool, permission_dep: Callable[[str], Callable], default_limit) -> APIRouter:
    router = APIRouter()

    @router.get("/api/v1/devices", dependencies=[Depends(default_limit)])
    def list_devices(request: Request, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_READ))) -> dict:
        limit, offset = parse_limit_offset(request)
        status_filter = request.query_params.get("status", "")

        query = "SELECT id, serial_number, status, firmware_version FROM devices WHERE organization_id = %s"
        params: list = [claims.organization_id]
        if status_filter:
            query += " AND status = %s"
            params.append(status_filter)
        query += " ORDER BY serial_number LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        try:
            with pool.connection() as conn:
                rows = conn.execute(query, params).fetchall()
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        return paginated_response([_device_dto(r) for r in rows], limit, offset)

    @router.get("/api/v1/devices/{id}")
    def get_device(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_READ))) -> dict:
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT id, serial_number, status, firmware_version FROM devices WHERE id = %s AND organization_id = %s",
                (id, claims.organization_id),
            ).fetchone()
        if row is None:
            raise APIError(404, "NOT_FOUND", "device does not exist")
        return _device_dto(row)

    @router.get("/api/v1/devices/{id}/sensors")
    def list_device_sensors(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_READ))) -> dict:
        with pool.connection() as conn:
            exists = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM devices WHERE id = %s AND organization_id = %s)", (id, claims.organization_id)
            ).fetchone()[0]
            if not exists:
                raise APIError(404, "NOT_FOUND", "device does not exist")
            rows = conn.execute(
                "SELECT id, metric, unit, COALESCE(min_operating_value,0), COALESCE(max_operating_value,0) FROM sensors WHERE device_id = %s ORDER BY metric",
                (id,),
            ).fetchall()
        out = [{"id": str(r[0]), "metric": r[1], "unit": r[2], "min_operating_value": float(r[3]), "max_operating_value": float(r[4])} for r in rows]
        return paginated_response(out, len(out), 0)

    @router.post("/api/v1/devices", status_code=201, dependencies=[Depends(default_limit)])
    def provision_device(req: ProvisionDeviceRequest, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_WRITE))) -> dict:
        """Registers a new device and generates its shared secret,
        returning the plaintext exactly once -- mirroring how the seed
        script provisions devices, but through the API and with
        device:write enforced instead of a trusted local script."""
        if not req.machine_id or not req.serial_number:
            raise APIError(400, "VALIDATION_ERROR", "machine_id and serial_number are required")

        with pool.connection() as conn:
            machine_exists = conn.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM machines m
                    JOIN production_lines pl ON pl.id = m.production_line_id
                    JOIN factories f ON f.id = pl.factory_id
                    WHERE m.id = %s AND f.organization_id = %s
                )
                """,
                (req.machine_id, claims.organization_id),
            ).fetchone()[0]
            if not machine_exists:
                raise APIError(400, "VALIDATION_ERROR", "machine_id does not belong to your organization")

            secret = secrets.token_hex(24)
            password_hash = auth.hash_password(secret)
            firmware = req.firmware_version or "unknown"

            try:
                with conn.transaction():
                    row = conn.execute(
                        """
                        INSERT INTO devices (machine_id, organization_id, serial_number, status, firmware_version)
                        VALUES (%s, %s, %s, 'PROVISIONED', %s) RETURNING id, serial_number, status, firmware_version
                        """,
                        (req.machine_id, claims.organization_id, req.serial_number, firmware),
                    ).fetchone()
                    device_id = str(row[0])
                    conn.execute(
                        "INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active) VALUES (%s, 'shared_secret', %s, true)",
                        (device_id, password_hash),
                    )
            except psycopg.errors.UniqueViolation:
                raise APIError(409, "CONFLICT", "a device with this serial_number already exists")
            except Exception as exc:  # noqa: BLE001
                raise internal_error(exc)

        device = {"id": device_id, "serial_number": row[1], "status": row[2], "firmware_version": row[3]}
        return {"device": device, "secret": secret}

    @router.post("/api/v1/devices/{id}/rotate-credentials", dependencies=[Depends(default_limit)])
    def rotate_credentials(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_WRITE))) -> dict:
        """Deactivates the device's current credential and issues a new
        one, returning the new plaintext secret once."""
        with pool.connection() as conn:
            exists = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM devices WHERE id = %s AND organization_id = %s)", (id, claims.organization_id)
            ).fetchone()[0]
            if not exists:
                raise APIError(404, "NOT_FOUND", "device does not exist")

            secret = secrets.token_hex(24)
            password_hash = auth.hash_password(secret)

            try:
                with conn.transaction():
                    conn.execute("UPDATE device_credentials SET is_active = false WHERE device_id = %s AND is_active = true", (id,))
                    conn.execute(
                        "INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active, rotated_at) VALUES (%s, 'shared_secret', %s, true, now())",
                        (id, password_hash),
                    )
            except Exception as exc:  # noqa: BLE001
                raise internal_error(exc)

        return {"secret": secret}

    @router.post("/api/v1/devices/{id}/decommission", status_code=204, response_model=None, dependencies=[Depends(default_limit)])
    def decommission_device(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_WRITE))) -> None:
        with pool.connection() as conn:
            cur = conn.execute(
                "UPDATE devices SET status = 'DECOMMISSIONED', decommissioned_at = now(), updated_at = now() WHERE id = %s AND organization_id = %s",
                (id, claims.organization_id),
            )
            if cur.rowcount == 0:
                raise APIError(404, "NOT_FOUND", "device does not exist")

    return router
