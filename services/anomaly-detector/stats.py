"""Online statistical (EWMA z-score) detection, the Python port of stats.go."""

from __future__ import annotations

import math
import threading

from rules import clamp01
from shared.events import SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_WARNING


class EWMATracker:
    """Maintains an exponentially-weighted moving mean and variance for one
    (device, metric) series, updated online in O(1) per sample. EWMA
    (rather than a plain cumulative average) is used deliberately so the
    baseline adapts to a genuine regime change (e.g. a machine settling
    into a new normal after maintenance) instead of being permanently
    anchored to whatever the series looked like at startup."""

    def __init__(self, alpha: float):
        self._alpha = alpha
        self._mean = 0.0
        self._variance = 0.0
        self._sample_count = 0
        self._initialized = False

    def update(self, value: float) -> tuple[float, int]:
        """Feeds one new sample and returns the z-score of that sample
        against the mean/stddev *before* this sample was folded in (so a
        single huge spike is judged against the prior baseline, not a
        baseline it just dragged toward itself)."""
        if not self._initialized:
            self._mean = value
            self._variance = 0.0
            self._initialized = True
            self._sample_count = 1
            return 0.0, self._sample_count

        stddev = math.sqrt(self._variance)
        z_score = (value - self._mean) / stddev if stddev > 0 else 0.0

        delta = value - self._mean
        self._mean += self._alpha * delta
        self._variance = (1 - self._alpha) * (self._variance + self._alpha * delta * delta)

        self._sample_count += 1
        return z_score, self._sample_count


class StatisticalTrackers:
    """Holds one EWMATracker per (device_id, metric) series."""

    def __init__(self, alpha: float):
        self._lock = threading.Lock()
        self._trackers: dict[str, EWMATracker] = {}
        self._alpha = alpha

    def update(self, device_id: str, metric: str, value: float) -> tuple[float, int]:
        key = f"{device_id}|{metric}"
        with self._lock:
            tracker = self._trackers.get(key)
            if tracker is None:
                tracker = EWMATracker(self._alpha)
                self._trackers[key] = tracker
        return tracker.update(value)


def stat_check(z_score: float, sample_count: int, min_samples: int, threshold: float) -> tuple[bool, str, float]:
    """Flags a sample whose z-score against its series' EWMA baseline
    exceeds threshold, but only once enough samples have accumulated that
    the baseline itself is meaningful — otherwise every series' first few
    dozen readings would trivially "deviate" from an unstable baseline."""
    if sample_count < min_samples:
        return False, "", 0.0
    abs_z = abs(z_score)
    if abs_z < threshold:
        return False, "", 0.0

    score = clamp01(abs_z / (threshold * 2))
    if abs_z >= threshold * 1.6:
        severity = SEVERITY_CRITICAL
    elif abs_z >= threshold * 1.3:
        severity = SEVERITY_HIGH
    else:
        severity = SEVERITY_WARNING
    return True, severity, score
