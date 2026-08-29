"""Factory/production-line/machine/device-listing endpoints, the Python
port of handlers_factories.go.

Every query in this file scopes by claims.organization_id from the
validated JWT -- never by anything the client passes in -- which is what
actually enforces multi-tenancy at the backend rather than trusting the
frontend not to ask for someone else's data.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, Request
from psycopg_pool import ConnectionPool

from pagination import parse_limit_offset
from response import APIError, internal_error, paginated_response
from shared import auth


def register_factory_routes(pool: ConnectionPool, permission_dep: Callable[[str], Callable], default_limit) -> APIRouter:
    router = APIRouter()

    @router.get("/api/v1/factories", dependencies=[Depends(default_limit)])
    def list_factories(request: Request, claims: auth.Claims = Depends(permission_dep(auth.PERM_FACTORIES_READ))) -> dict:
        limit, offset = parse_limit_offset(request)
        try:
            with pool.connection() as conn:
                rows = conn.execute(
                    "SELECT id, name, city, country FROM factories WHERE organization_id = %s ORDER BY name LIMIT %s OFFSET %s",
                    (claims.organization_id, limit, offset),
                ).fetchall()
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        out = [{"id": str(r[0]), "name": r[1], "city": r[2], "country": r[3]} for r in rows]
        return paginated_response(out, limit, offset)

    @router.get("/api/v1/factories/{id}")
    def get_factory(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_FACTORIES_READ))) -> dict:
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT id, name, city, country FROM factories WHERE id = %s AND organization_id = %s", (id, claims.organization_id)
            ).fetchone()
        if row is None:
            # A factory belonging to another organization is reported as
            # not-found, not forbidden -- never confirm that a cross-tenant
            # resource even exists.
            raise APIError(404, "NOT_FOUND", "factory does not exist")
        return {"id": str(row[0]), "name": row[1], "city": row[2], "country": row[3]}

    @router.get("/api/v1/factories/{id}/production-lines")
    def list_production_lines(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_FACTORIES_READ))) -> dict:
        with pool.connection() as conn:
            # Confirm the factory belongs to this tenant before listing its
            # children -- otherwise a caller probing IDs could learn that a
            # real factory ID exists versus a wrong one if a JOIN silently
            # returned zero rows either way. Explicit is safer than implicit.
            exists = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM factories WHERE id = %s AND organization_id = %s)", (id, claims.organization_id)
            ).fetchone()[0]
            if not exists:
                raise APIError(404, "NOT_FOUND", "factory does not exist")
            rows = conn.execute("SELECT id, name FROM production_lines WHERE factory_id = %s ORDER BY name", (id,)).fetchall()
        out = [{"id": str(r[0]), "name": r[1]} for r in rows]
        return paginated_response(out, len(out), 0)

    @router.get("/api/v1/machines/{id}")
    def get_machine(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_FACTORIES_READ))) -> dict:
        with pool.connection() as conn:
            row = conn.execute(
                """
                SELECT m.id, m.name, m.machine_type, m.status
                FROM machines m
                JOIN production_lines pl ON pl.id = m.production_line_id
                JOIN factories f ON f.id = pl.factory_id
                WHERE m.id = %s AND f.organization_id = %s
                """,
                (id, claims.organization_id),
            ).fetchone()
        if row is None:
            raise APIError(404, "NOT_FOUND", "machine does not exist")
        return {"id": str(row[0]), "name": row[1], "machine_type": row[2], "status": row[3]}

    @router.get("/api/v1/production-lines/{id}/machines")
    def list_line_machines(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_FACTORIES_READ))) -> dict:
        with pool.connection() as conn:
            exists = conn.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM production_lines pl
                    JOIN factories f ON f.id = pl.factory_id
                    WHERE pl.id = %s AND f.organization_id = %s
                )
                """,
                (id, claims.organization_id),
            ).fetchone()[0]
            if not exists:
                raise APIError(404, "NOT_FOUND", "production line does not exist")
            rows = conn.execute(
                "SELECT id, name, machine_type, status FROM machines WHERE production_line_id = %s ORDER BY name", (id,)
            ).fetchall()
        out = [{"id": str(r[0]), "name": r[1], "machine_type": r[2], "status": r[3]} for r in rows]
        return paginated_response(out, len(out), 0)

    @router.get("/api/v1/machines/{id}/devices")
    def list_machine_devices(id: str, claims: auth.Claims = Depends(permission_dep(auth.PERM_DEVICES_READ))) -> dict:
        with pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT d.id, d.serial_number, d.status, d.firmware_version
                FROM devices d
                JOIN machines m ON m.id = d.machine_id
                JOIN production_lines pl ON pl.id = m.production_line_id
                JOIN factories f ON f.id = pl.factory_id
                WHERE m.id = %s AND f.organization_id = %s
                ORDER BY d.serial_number
                """,
                (id, claims.organization_id),
            ).fetchall()
        out = [{"id": str(r[0]), "serial_number": r[1], "status": r[2], "firmware_version": r[3]} for r in rows]
        return paginated_response(out, len(out), 0)

    return router
