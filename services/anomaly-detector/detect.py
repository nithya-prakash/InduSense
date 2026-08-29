"""Combines all three detection levels, the Python port of detect.go."""

from __future__ import annotations

from dataclasses import dataclass

from config import Config
from rules import MetricRange, rule_check
from shared.events import SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_INFO, SEVERITY_WARNING
from stats import stat_check


@dataclass
class Detection:
    method: str
    severity: str
    score: float
    reason: str


_SEVERITY_RANK = {
    SEVERITY_INFO: 0,
    SEVERITY_WARNING: 1,
    SEVERITY_HIGH: 2,
    SEVERITY_CRITICAL: 3,
}


def run_detectors(
    value: float,
    rng: MetricRange,
    has_range: bool,
    z_score: float,
    sample_count: int,
    cfg: Config,
    forest_score: float,
    has_forest: bool,
) -> list[Detection]:
    """Applies all three detection levels to one telemetry sample and
    returns every method that fired. Running all three independently
    (rather than short-circuiting on the first hit) is deliberate: the
    combined anomaly record should report every corroborating signal,
    since a reading flagged by both the rule engine and the isolation
    forest is more actionable than one flagged by either alone."""
    results: list[Detection] = []

    if has_range:
        fired, severity, score = rule_check(value, rng)
        if fired:
            results.append(
                Detection(
                    method="RULE",
                    severity=severity,
                    score=score,
                    reason=f"value {value:.2f} outside safe operating range [{rng.min:.2f}, {rng.max:.2f}]",
                )
            )

    fired, severity, score = stat_check(z_score, sample_count, cfg.min_samples_for_zscore, cfg.zscore_threshold)
    if fired:
        results.append(
            Detection(
                method="STATISTICAL",
                severity=severity,
                score=score,
                reason=f"z-score {z_score:.2f} exceeds threshold {cfg.zscore_threshold:.2f} against rolling baseline",
            )
        )

    if has_forest:
        fired, severity, score = isolation_check(forest_score, cfg.forest_score_threshold)
        if fired:
            results.append(
                Detection(
                    method="ISOLATION_FOREST",
                    severity=severity,
                    score=score,
                    reason=f"isolation forest anomaly score {forest_score:.3f} exceeds threshold {cfg.forest_score_threshold:.3f}",
                )
            )

    return results


def combine_detections(results: list[Detection]) -> tuple[str, float, list[str], str]:
    """Folds multiple firing detectors into one anomaly record: worst
    severity wins, score is the max across methods, and the reason lists
    every contributing method so a human reading the alert can see the
    full picture."""
    severity = SEVERITY_INFO
    score = 0.0
    methods: list[str] = []
    reason = ""
    for r in results:
        methods.append(r.method)
        if r.score > score:
            score = r.score
        if _SEVERITY_RANK[r.severity] > _SEVERITY_RANK[severity]:
            severity = r.severity
        reason = r.reason if not reason else f"{reason}; {r.reason}"
    return severity, score, methods, reason


def isolation_check(score: float, threshold: float) -> tuple[bool, str, float]:
    if score < threshold:
        return False, "", 0.0
    if score >= 0.75:
        severity = SEVERITY_CRITICAL
    elif score >= threshold + 0.06:
        severity = SEVERITY_HIGH
    else:
        severity = SEVERITY_WARNING
    return True, severity, score
