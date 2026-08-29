from datetime import datetime, timedelta, timezone

from anomalycount import AnomalyCountTracker
from store import next_severity


def now():
    return datetime.now(timezone.utc)


def test_anomaly_count_tracker_counts_within_window():
    tr = AnomalyCountTracker()
    base = now()

    assert tr.record("k", base, 300) == 1
    assert tr.record("k", base + timedelta(minutes=1), 300) == 2
    assert tr.record("k", base + timedelta(minutes=2), 300) == 3


def test_anomaly_count_tracker_expires_old_entries():
    tr = AnomalyCountTracker()
    base = now()

    tr.record("k", base, 300)
    tr.record("k", base + timedelta(minutes=1), 300)
    # This occurrence is 10 minutes after the first two, well past the
    # 5-minute window, so they should no longer count.
    n = tr.record("k", base + timedelta(minutes=10), 300)
    assert n == 1


def test_anomaly_count_tracker_isolates_keys():
    tr = AnomalyCountTracker()
    n = now()
    tr.record("a", n, 60)
    tr.record("a", n, 60)
    assert tr.record("b", n, 60) == 1


def test_next_severity_ladder():
    cases = [
        ("WARNING", "HIGH"),
        ("HIGH", "CRITICAL"),
        ("CRITICAL", "CRITICAL"),  # already at the top
        ("UNKNOWN", "UNKNOWN"),  # not on the ladder at all
    ]
    for frm, want in cases:
        assert next_severity(frm) == want
