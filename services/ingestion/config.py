"""Environment-variable configuration, the Python port of config.go. Same
env var names and defaults, so nothing in docker-compose.yml/.env.example/
the Helm chart needs to change just because the implementation language
changed underneath it.
"""

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


@dataclass
class Config:
    mqtt_broker_url: str
    mqtt_client_id: str
    mqtt_qos: int

    kafka_brokers: list[str]
    topic_telemetry_raw: str
    topic_device_events: str
    topic_dead_letter: str

    worker_pool_size: int
    queue_capacity: int

    kafka_max_retries: int
    kafka_retry_base_delay_seconds: float

    breaker_failure_threshold: int
    breaker_cooldown_seconds: float

    http_port: str


def load_config() -> Config:
    mqtt_host = env_str("MQTT_BROKER_HOST", "localhost")
    mqtt_port = env_str("MQTT_BROKER_PORT", "1883")

    return Config(
        mqtt_broker_url=env_str("MQTT_BROKER_URL", f"tcp://{mqtt_host}:{mqtt_port}"),
        mqtt_client_id=env_str("MQTT_CLIENT_ID_PREFIX", "indusense") + "-ingestion",
        mqtt_qos=env_int("MQTT_QOS", 1),
        kafka_brokers=env_str("KAFKA_BROKERS", "localhost:9094").split(","),
        topic_telemetry_raw=env_str("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw"),
        topic_device_events=env_str("KAFKA_TOPIC_DEVICE_EVENTS", "device.events"),
        topic_dead_letter=env_str("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),
        worker_pool_size=env_int("INGESTION_WORKER_POOL_SIZE", 50),
        queue_capacity=env_int("INGESTION_QUEUE_CAPACITY", 10000),
        kafka_max_retries=env_int("INGESTION_KAFKA_MAX_RETRIES", 5),
        kafka_retry_base_delay_seconds=env_int("INGESTION_KAFKA_RETRY_BASE_MS", 1000) / 1000,
        breaker_failure_threshold=env_int("INGESTION_BREAKER_THRESHOLD", 5),
        breaker_cooldown_seconds=float(env_int("INGESTION_BREAKER_COOLDOWN_S", 15)),
        http_port=env_str("INGESTION_PORT", "8081"),
    )
