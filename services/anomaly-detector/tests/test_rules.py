from rules import MetricRange, rule_check
from shared.events import SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_WARNING


def test_rule_check_passes_within_range():
    fired, _, _ = rule_check(50, MetricRange(min=20, max=90))
    assert not fired


def test_rule_check_fires_above_max():
    fired, severity, score = rule_check(100, MetricRange(min=20, max=90))
    assert fired
    assert score > 0
    assert severity


def test_rule_check_severity_scales_with_overshoot():
    r = MetricRange(min=0, max=100)
    _, sev_small, _ = rule_check(105, r)  # 5% overshoot
    _, sev_big, _ = rule_check(300, r)  # 200% overshoot

    rank = {SEVERITY_WARNING: 1, SEVERITY_HIGH: 2, SEVERITY_CRITICAL: 3}
    assert rank[sev_big] > rank[sev_small]


def test_rule_check_degenerate_range_never_fires():
    fired, _, _ = rule_check(999, MetricRange(min=5, max=5))
    assert not fired
