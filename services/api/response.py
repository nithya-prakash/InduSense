"""Error envelope and pagination response shape, the Python port of
response.go.

Matches the spec's consistent error envelope exactly:
{"error": {"code", "message", "request_id"}}. Internal error details
(stack traces, raw DB errors) are never included in the response -- they
are logged server-side with the request ID for correlation.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException

_logger = logging.getLogger("api")


class APIError(HTTPException):
    """Raised by handlers for any non-2xx response with the standard
    {code, message} envelope shape. A registered exception handler (see
    main.py) converts this into the actual JSON body, adding request_id
    from request state."""

    def __init__(self, status_code: int, code: str, message: str, headers: dict[str, str] | None = None):
        super().__init__(status_code=status_code, detail={"code": code, "message": message}, headers=headers)


def internal_error(exc: Exception) -> APIError:
    """Logs the real error (server-side only) and returns a generic
    message to the client -- never leaking internals, per spec."""
    _logger.error("api: internal error: %s", exc)
    return APIError(500, "INTERNAL_ERROR", "an unexpected error occurred")


def paginated_response(items: list[Any], limit: int, offset: int) -> dict[str, Any]:
    items = items or []
    return {"items": items, "limit": limit, "offset": offset, "returned_count": len(items)}
