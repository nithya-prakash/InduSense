"""Security-sensitive action logging, the Python port of pkg/audit.

Currently used only by shared.auth, for exactly three actions: user.login
(success and failure, with a reason), user.refresh_token_reuse (a reused
refresh token JTI -- a signal of a possibly-stolen token), and user.logout.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from psycopg_pool import ConnectionPool

RESULT_SUCCESS = "SUCCESS"
RESULT_FAILURE = "FAILURE"


@dataclass
class Entry:
    """Mirrors the audit_logs schema. organization_id/user_id/resource_id/
    ip_address/request_id may be None because several audit-worthy actions
    (a failed login with an unknown email, a system-initiated action) have
    no value for one or more of these."""

    action: str
    resource_type: str
    result: str
    organization_id: str | None = None
    user_id: str | None = None
    resource_id: str | None = None
    ip_address: str | None = None
    request_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class Logger:
    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    def log(self, e: Entry) -> None:
        metadata_json = json.dumps(e.metadata or {})
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO audit_logs (organization_id, user_id, action, resource_type, resource_id, ip_address, request_id, result, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (e.organization_id, e.user_id, e.action, e.resource_type, e.resource_id, e.ip_address, e.request_id, e.result, metadata_json),
            )
