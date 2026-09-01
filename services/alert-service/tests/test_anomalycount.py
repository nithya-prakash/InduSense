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


def test_anomaly_count_tracker_evicts_stale_entry_hidden_behind_an_out_of_order_one():
    # Regression test for a naive prefix-only trim: it scans from index 0
    # and stops at the first entry that isn't stale yet, so a genuinely
    # stale entry sitting *behind* a newer, out-of-order one in arrival
    # order is never reached and never evicted -- permanently inflating
    # the count. The fix filters every stored timestamp against the
    # cutoff on every call instead of scanning a prefix, so no entry can
    # hide behind another regardless of arrival order.
    tr = AnomalyCountTracker()
    base = now()

    # Call 1 arrives first with the *latest* timestamp of the three.
    assert tr.record("k", base + timedelta(seconds=1000), 300) == 1
    # Call 2 arrives second (Kafka redelivery/backfill/clock skew) but its
    # own timestamp is far *earlier* -- appended after call 1 in arrival
    # order, so a prefix scan would see [T+1000, T] and never look past
    # index 0 (T+1000 isn't stale relative to T's own cutoff).
    assert tr.record("k", base, 300) == 2
    # Call 3 continues in normal, rising order. By this point `base` (call
    # 2's timestamp) IS more than 300s old relative to base+1001, and must
    # be evicted -- a prefix scan gets stuck behind the still-fresh
    # base+1000 entry at index 0 and never reaches it.
    n3 = tr.record("k", base + timedelta(seconds=1001), 300)
    assert n3 == 2  # base+1000 and base+1001 -- the stale `base` entry is gone


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
