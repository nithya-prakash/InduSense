"""Structured JSON logging, the Python port of pkg/logging. Every record
carries the fields the observability spec requires — timestamp, service,
level, message — plus whatever contextual fields a call site passes (in Go,
via slog's key/value pairs; here, via the `extra=` dict on the stdlib
logging calls) and a trace_id when one is in scope.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

_RESERVED_LOG_RECORD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
}


class _JSONFormatter(logging.Formatter):
    def __init__(self, service_name: str):
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "service": self._service_name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # Contextual fields passed via `extra={...}` land as arbitrary
        # attributes on the record — this is stdlib logging's equivalent of
        # slog's `logger.With("key", value)` / call-site key/value pairs.
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_RECORD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def init(service_name: str) -> logging.Logger:
    """Builds the service-wide structured logger: JSON to stdout, matching
    pkg/logging.Init's shape exactly (timestamp/service/level/message)."""
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JSONFormatter(service_name))
    logger.handlers = [handler]
    logger.propagate = False
    return logger


def with_context(logger: logging.Logger, **fields: Any) -> logging.LoggerAdapter:
    """Returns a logger that attaches `fields` (e.g. trace_id, event_id,
    device_id) to every record it emits — the Python equivalent of
    logging.WithContext(ctx, logger) enriching with the active span's trace
    ID, except the caller supplies the fields directly (Python's stdlib
    logging has no notion of "the active context" the way Go's context.Context
    does, so trace_id is passed in explicitly by call sites that have a
    current span — see shared/tracing.py's current_trace_id())."""
    return logging.LoggerAdapter(logger, fields)
