"""Redis-backed exactly-once processing, the Python port of dedup.go.

Claims event_ids in Redis so a duplicate delivery (e.g. an MQTT-redelivery
duplicate produced during a Kafka outage, or any at-least-once retry
anywhere upstream) is processed exactly once here.

The claim happens via SETNX *before* the event is processed, so a known
duplicate is skipped without doing the InfluxDB write or windowed-stats
work again. This is a deliberate tradeoff: if the process crashes between a
successful claim and finishing the rest of processing, that one event will
be treated as "already handled" on redelivery and under-counted in the
windowed aggregates. That's judged acceptable here because windowed stats
are informational, not a business-critical record — the InfluxDB raw point
itself stays correct regardless (same measurement+tags+timestamp overwrites
idempotently), and Postgres records (alerts/incidents) get their own
uniqueness constraints downstream where duplicates truly cannot be
tolerated.
"""

from __future__ import annotations

import redis

from config import Config


class Deduplicator:
    def __init__(self, cfg: Config):
        self._client = redis.Redis(
            host=cfg.redis_addr.rsplit(":", 1)[0],
            port=int(cfg.redis_addr.rsplit(":", 1)[1]),
            password=cfg.redis_password or None,
            db=cfg.redis_db,
        )
        self._ttl_seconds = cfg.dedup_ttl_seconds

    def claim(self, event_id: str) -> bool:
        """Returns True if this event_id has not been seen before (and is
        now marked as seen), or False if it's a duplicate."""
        key = f"dedup:telemetry:{event_id}"
        return bool(self._client.set(key, 1, ex=int(self._ttl_seconds), nx=True))

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.RedisError:
            return False

    def close(self) -> None:
        self._client.close()
