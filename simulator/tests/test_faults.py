import random

from config import Config
from faults import decide_faults, sensor_should_fail


def _cfg(**overrides) -> Config:
    base = dict(
        postgres_dsn="", postgres_max_conns=1, mqtt_broker_url="", mqtt_client_id="", mqtt_qos=1,
        sensor_count=1, messages_per_sec=1, anomaly_rate=0.0, duplicate_rate=0.0, out_of_order_rate=0.0,
        network_delay_rate=0.0, sensor_failure_rate=0.0, publisher_workers=1, queue_capacity=1,
    )
    base.update(overrides)
    return Config(**base)


def assert_approx(got: float, want: float, tolerance: float, label: str) -> None:
    assert want - tolerance <= got <= want + tolerance, f"{label} rate = {got:.4f}, want ~{want:.4f} (+/- {tolerance})"


def test_decide_faults_rates_approximate_configured_probability():
    rng = random.Random(42)
    cfg = _cfg(duplicate_rate=0.10, network_delay_rate=0.10, out_of_order_rate=0.10)

    trials = 20000
    duplicates = delayed = out_of_order = 0
    for _ in range(trials):
        d = decide_faults(rng, cfg)
        if d.duplicate:
            duplicates += 1
        if d.delayed:
            delayed += 1
        if d.out_of_order:
            out_of_order += 1

    assert_approx(duplicates / trials, cfg.duplicate_rate, 0.02, "duplicate")
    assert_approx(out_of_order / trials, cfg.out_of_order_rate, 0.02, "out_of_order")
    # delayed is the union of network_delay_rate and out_of_order_rate
    # triggers, so it should be at least the network delay rate and no
    # more than the sum.
    got = delayed / trials
    assert cfg.network_delay_rate - 0.02 <= got <= cfg.network_delay_rate + cfg.out_of_order_rate + 0.02


def test_out_of_order_always_implies_delayed():
    rng = random.Random(7)
    cfg = _cfg(out_of_order_rate=1.0)
    for _ in range(100):
        d = decide_faults(rng, cfg)
        if d.out_of_order:
            assert d.delayed, "out-of-order sample must also be marked delayed"
            assert d.delay_for > 0, "out-of-order sample must have a positive delay"


def test_sensor_should_fail_recovers_from_failure():
    rng = random.Random(3)
    recovered = False
    for _ in range(200):
        if not sensor_should_fail(rng, True, 0):
            recovered = True
            break
    assert recovered, "a failed sensor should eventually recover within 200 ticks (~10% recovery chance/tick)"
