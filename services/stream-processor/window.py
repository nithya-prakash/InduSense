"""Rolling per-series windowed statistics, the Python port of window.go.

Includes the same event-time-ordered insert this file's Go counterpart was
fixed to do after a pre-GitHub audit: telemetry can arrive out of order
(network delay, MQTT/Kafka redelivery — the simulator's OUT_OF_ORDER_RATE
exercises this deliberately), and a naive `samples.append()` would silently
break both the trim loop (which only scans from the front, so a stale
sample stuck behind a later-timestamped-but-earlier-arrived one never gets
dropped) and rate-of-change (which reads samples[0]/samples[-1] as
"chronologically first/last" — an out-of-order append could compute a
negative elapsed time, or simply the wrong two readings). See
tests/test_window.py for the regression tests ported 1:1 from window_test.go.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class Sample:
    at: datetime
    value: float


@dataclass
class WindowStats:
    """Summarizes a single (device, metric) series over one window
    duration: moving average, moving standard deviation, min, max, and rate
    of change (last-first value over the window's elapsed time) — the same
    shape covers "vibration trend" or "energy consumption rate" from the
    spec; those are just rate_of_change applied to the vibration/power
    metric respectively, not separately named computations."""

    count: int = 0
    moving_avg: float = 0.0
    moving_stddev: float = 0.0
    min: float = 0.0
    max: float = 0.0
    rate_of_change: float = 0.0


class SeriesBuffer:
    """A per-(device_id,metric) ordered list of recent samples, trimmed to
    the longest configured window on every insert so memory stays bounded
    regardless of how long the process runs."""

    def __init__(self, max_window_seconds: float):
        self._lock = threading.Lock()
        self._samples: list[Sample] = []
        self._max_window = timedelta(seconds=max_window_seconds)

    def add(self, at: datetime, value: float) -> None:
        """Inserts by event timestamp, not arrival order — see this
        module's docstring for why that matters."""
        with self._lock:
            i = len(self._samples)
            while i > 0 and self._samples[i - 1].at > at:
                i -= 1
            self._samples.insert(i, Sample(at=at, value=value))

            # Trim against the newest timestamp actually in the buffer (not
            # this call's `at`, which — for an out-of-order arrival — could
            # be the oldest thing in it and would under-trim).
            cutoff = self._samples[-1].at - self._max_window
            j = 0
            while j < len(self._samples) and self._samples[j].at < cutoff:
                j += 1
            if j > 0:
                self._samples = self._samples[j:]

    def stats_for(self, now: datetime, window_seconds: float) -> tuple[WindowStats, bool]:
        """Computes WindowStats over the trailing `window_seconds`, as of
        `now`. Returns ok=False if there are no samples in that window."""
        with self._lock:
            cutoff = now - timedelta(seconds=window_seconds)
            in_window = [s for s in self._samples if s.at >= cutoff]
        if not in_window:
            return WindowStats(), False
        return _compute_stats(in_window), True


def _compute_stats(samples: list[Sample]) -> WindowStats:
    n = len(samples)
    values = [s.value for s in samples]
    total = sum(values)
    lo, hi = min(values), max(values)
    avg = total / n

    variance = sum((v - avg) ** 2 for v in values) / n
    stddev = math.sqrt(variance)

    rate_of_change = 0.0
    if n > 1:
        elapsed = (samples[-1].at - samples[0].at).total_seconds()
        if elapsed > 0:
            rate_of_change = (samples[-1].value - samples[0].value) / elapsed

    return WindowStats(
        count=n,
        moving_avg=avg,
        moving_stddev=stddev,
        min=lo,
        max=hi,
        rate_of_change=rate_of_change,
    )
