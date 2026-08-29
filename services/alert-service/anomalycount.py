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
        trimming older ones."""
        with self._lock:
            times = self._seen.get(key, [])
            times.append(at)
            cutoff = at - timedelta(seconds=window_seconds)
            i = 0
            while i < len(times) and times[i] < cutoff:
                i += 1
            times = times[i:]
            self._seen[key] = times
            return len(times)
