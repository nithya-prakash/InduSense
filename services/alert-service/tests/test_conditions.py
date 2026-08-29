from conditions import condition_matches
from rules import AlertRule


def test_condition_matches_greater_than():
    rule = AlertRule(condition="GREATER_THAN", threshold_value=90)
    assert condition_matches(rule, 91, 0)
    assert not condition_matches(rule, 90, 0)  # strict


def test_condition_matches_less_than():
    rule = AlertRule(condition="LESS_THAN", threshold_value=10)
    assert condition_matches(rule, 5, 0)


def test_condition_matches_outside_range():
    rule = AlertRule(condition="OUTSIDE_RANGE", threshold_min=20, threshold_max=90)
    assert not condition_matches(rule, 50, 0)  # inside [20,90]
    assert condition_matches(rule, 100, 0)  # outside
    assert condition_matches(rule, 5, 0)  # outside


def test_condition_matches_anomaly_count():
    rule = AlertRule(condition="ANOMALY_COUNT", threshold_value=3)
    assert not condition_matches(rule, 0, 2)
    assert condition_matches(rule, 0, 3)


def test_condition_matches_nil_threshold_never_matches():
    rule = AlertRule(condition="GREATER_THAN")
    assert not condition_matches(rule, 1000, 0)


def test_condition_matches_unknown_condition_never_matches():
    rule = AlertRule(condition="SOMETHING_ELSE", threshold_value=0)
    assert not condition_matches(rule, 1000, 0)
