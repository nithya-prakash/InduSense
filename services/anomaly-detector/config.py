"""Environment-variable configuration, the Python port of config.go."""

from __future__ import annotations

import os
from dataclasses import dataclass


def env_str(key: str, default: str) -> str:
    return os.environ.get(key) or default


def env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    if value:
        try:
            return int(value)
        except ValueError:
            pass
    return default


def env_float(key: str, default: float) -> float:
    value = os.environ.get(key)
    if value:
        try:
            return float(value)
        except ValueError:
            pass
    return default


@dataclass
class Config:
    kafka_brokers: list[str]
    consumer_group_id: str
    topic_processed: str
    topic_anomalies: str
    topic_dead_letter: str

    postgres_dsn: str
    postgres_max_conns: int
    catalog_refresh_every_seconds: float

    kafka_max_retries: int
    kafka_retry_base_delay_seconds: float
    breaker_failure_threshold: int
    breaker_cooldown_seconds: float

    # Level 2 -- statistical (EWMA z-score)
    ewma_alpha: float
    zscore_threshold: float
    min_samples_for_zscore: int

    # Level 3 -- Isolation Forest
    forest_training_buffer_size: int
    forest_retrain_every_seconds: float
    forest_num_trees: int
    forest_subsample_size: int
    forest_score_threshold: float

    http_port: str


def load_config() -> Config:
    return Config(
        kafka_brokers=env_str("KAFKA_BROKERS", "localhost:9094").split(","),
        consumer_group_id=env_str("KAFKA_CONSUMER_GROUP_PREFIX", "indusense") + "-anomaly-detector",
        topic_processed=env_str("KAFKA_TOPIC_TELEMETRY_PROCESSED", "telemetry.processed"),
        topic_anomalies=env_str("KAFKA_TOPIC_ANOMALIES_DETECTED", "anomalies.detected"),
        topic_dead_letter=env_str("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),
        postgres_dsn=env_str(
            "ANOMALY_POSTGRES_DSN",
            "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable",
        ),
        postgres_max_conns=env_int("ANOMALY_POSTGRES_MAX_CONNS", 5),
        catalog_refresh_every_seconds=float(env_int("ANOMALY_CATALOG_REFRESH_SECONDS", 300)),
        kafka_max_retries=env_int("ANOMALY_KAFKA_MAX_RETRIES", 5),
        kafka_retry_base_delay_seconds=env_int("ANOMALY_KAFKA_RETRY_BASE_MS", 1000) / 1000,
        breaker_failure_threshold=env_int("ANOMALY_BREAKER_THRESHOLD", 5),
        breaker_cooldown_seconds=float(env_int("ANOMALY_BREAKER_COOLDOWN_S", 15)),
        ewma_alpha=env_float("ANOMALY_EWMA_ALPHA", 0.1),
        zscore_threshold=env_float("ANOMALY_ZSCORE_THRESHOLD", 3.0),
        min_samples_for_zscore=env_int("ANOMALY_MIN_SAMPLES", 30),
        forest_training_buffer_size=env_int("ANOMALY_FOREST_BUFFER_SIZE", 512),
        forest_retrain_every_seconds=float(env_int("ANOMALY_FOREST_RETRAIN_SECONDS", 120)),
        forest_num_trees=env_int("ANOMALY_FOREST_NUM_TREES", 100),
        forest_subsample_size=env_int("ANOMALY_FOREST_SUBSAMPLE_SIZE", 256),
        forest_score_threshold=env_float("ANOMALY_FOREST_SCORE_THRESHOLD", 0.62),
        http_port=env_str("ANOMALY_DETECTOR_PORT", "8083"),
    )
