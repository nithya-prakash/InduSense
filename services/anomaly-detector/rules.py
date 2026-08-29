"""Rule-based operating-range detection, the Python port of rules.go."""

from __future__ import annotations

from dataclasses import dataclass

from shared.events import SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_WARNING


@dataclass
class MetricRange:
    min: float = 0.0
    max: float = 0.0


def clamp01(v: float) -> float:
    if v < 0:
        return 0.0
    if v > 1:
        return 1.0
    return v


def rule_check(value: float, r: MetricRange) -> tuple[bool, str, float]:
    """Flags a reading that falls outside its sensor's known safe operating
    range (temperature > threshold, pressure outside safe range, etc. —
    all expressed the same way: value outside [min, max]). Severity scales
    with how far outside the range the value falls, as a fraction of the
    range's own span, so a wildly-out-of-range spike reads as more severe
    than a reading just past the boundary."""
    span = r.max - r.min
    if span <= 0:
        return False, "", 0.0

    if value > r.max:
        overshoot = (value - r.max) / span
    elif value < r.min:
        overshoot = (r.min - value) / span
    else:
        return False, "", 0.0

    score = clamp01(overshoot)
    if overshoot >= 0.5:
        severity = SEVERITY_CRITICAL
    elif overshoot >= 0.2:
        severity = SEVERITY_HIGH
    else:
        severity = SEVERITY_WARNING
    return True, severity, score
