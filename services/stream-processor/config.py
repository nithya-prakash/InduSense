"""Environment-variable configuration, the Python port of config.go. Same
env var names and defaults, so nothing in docker-compose.yml/.env.example/
the Helm chart needs to change just because the implementation language
changed underneath it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


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


@dataclass
class Config:
    kafka_brokers: list[str]
    consumer_group_id: str
    topic_telemetry_raw: str
    topic_processed: str
    topic_dead_letter: str

    redis_addr: str
    redis_password: str
    redis_db: int
    dedup_ttl_seconds: float

    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str

    window_flush_interval_seconds: float
    windows_seconds: list[float] = field(
        default_factory=lambda: [10.0, 30.0, 60.0, 300.0, 900.0]
    )

    influx_max_retries: int = 5
    influx_retry_base_delay_seconds: float = 1.0
    kafka_max_retries: int = 5
    kafka_retry_base_delay_seconds: float = 1.0
    breaker_failure_threshold: int = 5
    breaker_cooldown_seconds: float = 15.0

    http_port: str = "8082"


def load_config() -> Config:
    return Config(
        kafka_brokers=env_str("KAFKA_BROKERS", "localhost:9094").split(","),
        consumer_group_id=env_str("KAFKA_CONSUMER_GROUP_PREFIX", "indusense") + "-stream-processor",
        topic_telemetry_raw=env_str("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw"),
        topic_processed=env_str("KAFKA_TOPIC_TELEMETRY_PROCESSED", "telemetry.processed"),
        topic_dead_letter=env_str("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),
        redis_addr=f"{env_str('REDIS_HOST', 'localhost')}:{env_str('REDIS_PORT', '6379')}",
        redis_password=env_str("REDIS_PASSWORD", ""),
        redis_db=env_int("REDIS_DB", 0),
        dedup_ttl_seconds=float(env_int("STREAM_DEDUP_TTL_SECONDS", 3600)),
        influx_url=env_str("INFLUXDB_URL", "http://localhost:8086"),
        influx_token=env_str("INFLUXDB_TOKEN", "dev-only-influx-admin-token-change-me"),
        influx_org=env_str("INFLUXDB_ORG", "indusense"),
        influx_bucket=env_str("INFLUXDB_BUCKET", "telemetry"),
        window_flush_interval_seconds=float(env_int("STREAM_WINDOW_FLUSH_SECONDS", 10)),
        influx_max_retries=env_int("STREAM_INFLUX_MAX_RETRIES", 5),
        influx_retry_base_delay_seconds=env_int("STREAM_INFLUX_RETRY_BASE_MS", 1000) / 1000,
        kafka_max_retries=env_int("STREAM_KAFKA_MAX_RETRIES", 5),
        kafka_retry_base_delay_seconds=env_int("STREAM_KAFKA_RETRY_BASE_MS", 1000) / 1000,
        breaker_failure_threshold=env_int("STREAM_BREAKER_THRESHOLD", 5),
        breaker_cooldown_seconds=float(env_int("STREAM_BREAKER_COOLDOWN_S", 15)),
        http_port=env_str("STREAM_PROCESSOR_PORT", "8082"),
    )
