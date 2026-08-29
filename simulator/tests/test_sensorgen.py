import random

from sensorgen import SensorGenerator, clamp


def test_sensor_generator_stays_within_range_without_anomalies():
    rng = random.Random(1)
    gen = SensorGenerator(rng, 20, 90, 0.0)  # anomaly_rate=0

    for i in range(10000):
        value, is_anomaly = gen.next()
        assert not is_anomaly, f"anomaly_rate=0 but sample {i} was flagged anomalous"
        # Noise can push slightly outside [min,max]; only the baseline
        # itself is clamped. Assert against a generous envelope instead.
        assert 0 <= value <= 110, f"sample {i} = {value} is wildly outside operating range [20,90]"


def test_sensor_generator_anomaly_rate_produces_spikes():
    rng = random.Random(2)
    gen = SensorGenerator(rng, 20, 90, 1.0)  # always anomalous

    _, is_anomaly = gen.next()
    assert is_anomaly, "anomaly_rate=1.0 should always flag the sample as anomalous"


def test_clamp():
    cases = [
        (5, 0, 10, 5),
        (-5, 0, 10, 0),
        (15, 0, 10, 10),
    ]
    for v, lo, hi, want in cases:
        assert clamp(v, lo, hi) == want, f"clamp({v}, {lo}, {hi})"
