"""Per-sensor synthetic reading generator, the Python port of sensorgen.go."""

from __future__ import annotations

import random

DRIFT_MAX_STEP = 0.01  # fraction of range the baseline may move per tick
REVERSION_PULL = 0.05  # fraction of distance-to-midpoint pulled back per tick
NOISE_FRACTION = 0.01  # gaussian noise stddev as a fraction of range
SPIKE_MIN_FRAC = 0.3  # anomaly spike magnitude, as a fraction of range
SPIKE_MAX_FRAC = 0.9


def clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


class SensorGenerator:
    """Produces successive readings for one sensor: a baseline that
    drifts slowly within the sensor's operating range (a bounded random
    walk that reverts toward the midpoint), plus per-sample gaussian noise
    and occasional anomalous spikes."""

    def __init__(self, rng: random.Random, lo: float, hi: float, anomaly_rate: float):
        self._min = lo
        self._max = hi
        self._mid = (lo + hi) / 2
        self._baseline = self._mid
        self._drift_step = 0.0
        self._rng = rng
        self._anomaly_rate = anomaly_rate

    def next(self) -> tuple[float, bool]:
        """Advances the internal drift state and returns the next reading
        along with whether this sample was injected as an anomalous
        spike."""
        range_span = self._max - self._min

        # Gradual drift: small random step, pulled back toward the
        # midpoint so the baseline doesn't wander out of the operating
        # range over time.
        self._drift_step = self._drift_step + (self._rng.random() * 2 - 1) * DRIFT_MAX_STEP * range_span
        pull = (self._mid - self._baseline) * REVERSION_PULL
        self._baseline += self._drift_step * 0.1 + pull
        self._baseline = clamp(self._baseline, self._min, self._max)

        noise = self._rng.gauss(0, 1) * NOISE_FRACTION * range_span
        value = self._baseline + noise

        is_anomaly = False
        if self._rng.random() < self._anomaly_rate:
            magnitude = (SPIKE_MIN_FRAC + self._rng.random() * (SPIKE_MAX_FRAC - SPIKE_MIN_FRAC)) * range_span
            sign = 1.0 if self._rng.random() >= 0.5 else -1.0
            value += sign * magnitude
            is_anomaly = True

        return round(value * 100) / 100, is_anomaly
