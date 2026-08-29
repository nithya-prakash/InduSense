"""Limit/offset query-param parsing, the Python port of pagination.go."""

from __future__ import annotations

from fastapi import Request

DEFAULT_LIMIT = 20
MAX_LIMIT = 100


def parse_limit_offset(request: Request) -> tuple[int, int]:
    limit = DEFAULT_LIMIT
    offset = 0

    v = request.query_params.get("limit")
    if v is not None:
        try:
            n = int(v)
            if n > 0:
                limit = n
        except ValueError:
            pass
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    v = request.query_params.get("offset")
    if v is not None:
        try:
            n = int(v)
            if n >= 0:
                offset = n
        except ValueError:
            pass

    return limit, offset
