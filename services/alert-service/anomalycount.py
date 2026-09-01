"""Sliding-window anomaly counting for ANOMALY_COUNT rules, the Python port
of anomalycount.go."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta


class AnomalyCountTracker:
    """Maintains, per (rule_id, scope), a trimmed list of recent anomaly
    timestamps so ANOMALY_COUNT rules ("three anomalies within five
    minutes") can be evaluated without querying Postgres on every event."""

    def __init__(self):
        self._lock = threading.Lock()
        self._seen: dict[str, list[datetime]] = {}

    def record(self, key: str, at: datetime, window_seconds: float) -> int:
        """Adds one occurrence at `at` for the given key and returns how
        many occurrences remain within `window_seconds` of `at` after
        trimming older ones.

        Trims by filtering every stored timestamp against the cutoff,
        rather than scanning/dropping only a leading prefix: `at` values
        aren't guaranteed to arrive in non-decreasing order (Kafka
        redelivery, backfill, or ordinary clock skew can all deliver an
        earlier `at` after a later one), and a prefix-only trim leaves a
        stale entry permanently stuck ahead of a newer one that never gets
        old enough to be re-scanned past it -- inflating the count for
        that key forever. A full filter costs O(n) instead of O(trimmed)
        per call, which is fine given these lists are bounded to one
        rule's window (typically a few minutes of anomalies, not an
        unbounded history)."""
        with self._lock:
            cutoff = at - timedelta(seconds=window_seconds)
            times = [t for t in self._seen.get(key, []) if t >= cutoff]
            times.append(at)
            self._seen[key] = times
            return len(times)
