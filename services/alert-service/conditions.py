"""Alert rule condition evaluation, the Python port of conditions.go."""

from __future__ import annotations

from rules import AlertRule


def condition_matches(rule: AlertRule, value: float, count_in_window: int) -> bool:
    """Evaluates one alert rule's condition against an incoming anomaly.
    For GREATER_THAN/LESS_THAN/OUTSIDE_RANGE, value is the anomaly's raw
    reading. For ANOMALY_COUNT, count_in_window is how many qualifying
    anomalies have occurred for this rule's scope within its window, and
    value is ignored."""
    if rule.condition == "GREATER_THAN":
        return rule.threshold_value is not None and value > rule.threshold_value
    if rule.condition == "LESS_THAN":
        return rule.threshold_value is not None and value < rule.threshold_value
    if rule.condition == "OUTSIDE_RANGE":
        return (
            rule.threshold_min is not None
            and rule.threshold_max is not None
            and (value < rule.threshold_min or value > rule.threshold_max)
        )
    if rule.condition == "ANOMALY_COUNT":
        return rule.threshold_value is not None and count_in_window >= rule.threshold_value
    return False
