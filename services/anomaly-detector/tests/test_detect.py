from config import Config
from detect import combine_detections, isolation_check, run_detectors, Detection
from rules import MetricRange
from shared.events import SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_INFO, SEVERITY_WARNING


def detect_config() -> Config:
    return Config(
        kafka_brokers=[], consumer_group_id="", topic_processed="", topic_anomalies="", topic_dead_letter="",
        postgres_dsn="", postgres_max_conns=1, catalog_refresh_every_seconds=1,
        kafka_max_retries=1, kafka_retry_base_delay_seconds=0.001,
        breaker_failure_threshold=1, breaker_cooldown_seconds=1,
        ewma_alpha=0.1, zscore_threshold=3.0, min_samples_for_zscore=30,
        forest_training_buffer_size=1, forest_retrain_every_seconds=1,
        forest_num_trees=1, forest_subsample_size=1, forest_score_threshold=0.62,
        http_port="0",
    )


def test_run_detectors_fires_rule_only():
    results = run_detectors(
        1000, MetricRange(min=0, max=100), True,  # rule fires hard
        0.5, 100, detect_config(),  # stat: no fire
        0.1, True,  # forest: below threshold, no fire
    )
    assert len(results) == 1
    assert results[0].method == "RULE"


def test_run_detectors_can_fire_multiple_methods():
    results = run_detectors(
        1000, MetricRange(min=0, max=100), True,  # rule fires
        10.0, 100, detect_config(),  # stat fires (z=10 > 3)
        0.9, True,  # forest fires (0.9 > 0.62 threshold)
    )
    assert len(results) == 3


def test_combine_detections_takes_worst_severity_and_max_score():
    results = [
        Detection(method="RULE", severity=SEVERITY_WARNING, score=0.2, reason="r1"),
        Detection(method="ISOLATION_FOREST", severity=SEVERITY_CRITICAL, score=0.9, reason="r2"),
        Detection(method="STATISTICAL", severity=SEVERITY_HIGH, score=0.5, reason="r3"),
    ]
    severity, score, methods, reason = combine_detections(results)

    assert severity == SEVERITY_CRITICAL
    assert score == 0.9
    assert len(methods) == 3
    assert reason


def test_combine_detections_empty_input_yields_info_severity_zero_score():
    severity, score, methods, _ = combine_detections([])
    assert severity == SEVERITY_INFO
    assert score == 0
    assert len(methods) == 0


def test_isolation_check_severity_bands():
    fired, _, _ = isolation_check(0.5, 0.62)
    assert not fired
    _, sev, _ = isolation_check(0.76, 0.62)
    assert sev == SEVERITY_CRITICAL
