import math
from datetime import datetime, timedelta, timezone

from registry import SeriesKey, SeriesRegistry
from window import SeriesBuffer


def now():
    return datetime.now(timezone.utc)


def test_series_buffer_stats_for_empty_returns_not_ok():
    buf = SeriesBuffer(60)
    _, ok = buf.stats_for(now(), 10)
    assert not ok


def test_series_buffer_computes_basic_stats():
    buf = SeriesBuffer(60)
    base = now()
    buf.add(base, 10)
    buf.add(base + timedelta(seconds=1), 20)
    buf.add(base + timedelta(seconds=2), 30)

    stats, ok = buf.stats_for(base + timedelta(seconds=2), 10)
    assert ok
    assert stats.count == 3
    assert stats.moving_avg == 20
    assert stats.min == 10
    assert stats.max == 30
    assert math.isclose(stats.rate_of_change, 10, abs_tol=1e-9)
    want_stddev = math.sqrt(((10.0 * 10.0) * 2 + 0) / 3.0)
    assert math.isclose(stats.moving_stddev, want_stddev, abs_tol=1e-9)


def test_series_buffer_excludes_samples_outside_window():
    buf = SeriesBuffer(60)
    base = now()
    buf.add(base, 100)  # outside the 5s window we'll query
    buf.add(base + timedelta(seconds=20), 5)

    stats, ok = buf.stats_for(base + timedelta(seconds=20), 5)
    assert ok
    assert stats.count == 1
    assert stats.moving_avg == 5


def test_series_buffer_trims_old_samples_beyond_max_window():
    buf = SeriesBuffer(10)
    base = now()
    buf.add(base, 1)
    buf.add(base + timedelta(seconds=20), 2)  # triggers trim of the first sample

    assert len(buf._samples) == 1


def test_series_buffer_out_of_order_arrival_rate_of_change_uses_event_time_not_arrival_order():
    """Reproduces the pre-GitHub audit finding directly: three readings whose
    event timestamps are strictly increasing (0s, 1s, 2s — same as
    test_series_buffer_computes_basic_stats, which asserts rate of change =
    (30-10)/2s = 10/s) are added out of arrival order — the middle reading
    arrives last, as network delay or Kafka redelivery could cause. The fix
    keeps samples sorted by event time regardless of insertion order, so the
    result must match the in-order case exactly."""
    buf = SeriesBuffer(60)
    base = now()

    buf.add(base, 10)                            # t=0s, arrives 1st
    buf.add(base + timedelta(seconds=2), 30)     # t=2s, arrives 2nd (skips ahead of t=1s)
    buf.add(base + timedelta(seconds=1), 20)     # t=1s, arrives 3rd (late, out of order)

    stats, ok = buf.stats_for(base + timedelta(seconds=2), 10)
    assert ok
    assert stats.count == 3
    assert math.isclose(stats.rate_of_change, 10, abs_tol=1e-9)
    assert stats.min == 10 and stats.max == 30

    for i in range(1, len(buf._samples)):
        assert buf._samples[i].at >= buf._samples[i - 1].at


def test_series_buffer_out_of_order_arrival_trim_uses_latest_event_time_not_arrival_time():
    """Reproduces the second half of the same bug: the trim cutoff must be
    based on the newest event timestamp actually in the buffer, not the
    just-inserted sample's own (possibly old, out-of-order) timestamp."""
    buf = SeriesBuffer(10)
    base = now()

    buf.add(base, 1)                             # t=0s
    buf.add(base + timedelta(seconds=20), 2)     # t=20s: trims t=0s -> buffer is now just [t=20s]
    buf.add(base + timedelta(seconds=1), 3)      # t=1s, arriving last (already outside the window vs t=20s)

    assert len(buf._samples) == 1


def test_series_registry_tracks_separate_series_independently():
    reg = SeriesRegistry(60)
    n = now()

    key_a = SeriesKey(device_id="device-a", metric="temperature")
    key_b = SeriesKey(device_id="device-b", metric="temperature")

    reg.record(key_a, n, 50)
    reg.record(key_b, n, 999)

    snap = reg.snapshot()
    assert len(snap) == 2

    found = {}
    for s in snap:
        stats, ok = s.buf.stats_for(n, 60)
        assert ok
        found[s.key.device_id] = stats.moving_avg

    assert found["device-a"] == 50
    assert found["device-b"] == 999
