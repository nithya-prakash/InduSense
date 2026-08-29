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
    postgres_dsn: str
    postgres_max_conns: int
    mqtt_broker_url: str
    mqtt_client_id: str
    mqtt_qos: int
    sensor_count: int
    messages_per_sec: int
    anomaly_rate: float
    duplicate_rate: float
    out_of_order_rate: float
    network_delay_rate: float
    sensor_failure_rate: float
    publisher_workers: int
    queue_capacity: int


def load_config() -> Config:
    return Config(
        postgres_dsn=env_str("SIM_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),
        postgres_max_conns=env_int("SIM_POSTGRES_MAX_CONNS", 4),
        mqtt_broker_url=env_str("SIM_MQTT_BROKER_URL", "tcp://localhost:1883"),
        mqtt_client_id=env_str("SIM_MQTT_CLIENT_ID", "indusense-simulator"),
        mqtt_qos=env_int("MQTT_QOS", 1),
        sensor_count=env_int("SENSOR_COUNT", 1000),
        messages_per_sec=env_int("MESSAGES_PER_SECOND", 1000),
        anomaly_rate=env_float("ANOMALY_RATE", 0.02),
        duplicate_rate=env_float("DUPLICATE_RATE", 0.01),
        out_of_order_rate=env_float("OUT_OF_ORDER_RATE", 0.02),
        network_delay_rate=env_float("NETWORK_DELAY_RATE", 0.03),
        sensor_failure_rate=env_float("SENSOR_FAILURE_RATE", 0.005),
        publisher_workers=env_int("SIM_PUBLISHER_WORKERS", 32),
        queue_capacity=env_int("SIM_QUEUE_CAPACITY", 20000),
    )
