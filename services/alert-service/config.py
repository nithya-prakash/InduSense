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


@dataclass
class Config:
    kafka_brokers: list[str]
    consumer_group_id: str
    topic_anomalies: str
    topic_device_events: str
    topic_alerts: str
    topic_dead_letter: str

    postgres_dsn: str
    postgres_max_conns: int
    rule_refresh_every_seconds: float

    escalation_check_every_seconds: float
    escalation_after_seconds: int  # an OPEN alert unacknowledged this long gets escalated one rung

    notification_console_enabled: bool
    notification_webhook_url: str

    http_port: str


def load_config() -> Config:
    return Config(
        kafka_brokers=env_str("KAFKA_BROKERS", "localhost:9094").split(","),
        consumer_group_id=env_str("KAFKA_CONSUMER_GROUP_PREFIX", "indusense") + "-alert-service",
        topic_anomalies=env_str("KAFKA_TOPIC_ANOMALIES_DETECTED", "anomalies.detected"),
        topic_device_events=env_str("KAFKA_TOPIC_DEVICE_EVENTS", "device.events"),
        topic_alerts=env_str("KAFKA_TOPIC_ALERTS", "alerts"),
        topic_dead_letter=env_str("KAFKA_TOPIC_DEAD_LETTER", "dead-letter"),
        postgres_dsn=env_str(
            "ALERT_POSTGRES_DSN",
            "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable",
        ),
        postgres_max_conns=env_int("ALERT_POSTGRES_MAX_CONNS", 10),
        rule_refresh_every_seconds=float(env_int("ALERT_RULE_REFRESH_SECONDS", 60)),
        escalation_check_every_seconds=float(env_int("ALERT_ESCALATION_CHECK_SECONDS", 60)),
        escalation_after_seconds=env_int("ALERT_ESCALATION_AFTER_SECONDS", 900),
        notification_console_enabled=env_str("ALERT_NOTIFY_CONSOLE", "true") == "true",
        notification_webhook_url=env_str("ALERT_NOTIFY_WEBHOOK_URL", ""),
        http_port=env_str("ALERT_SERVICE_PORT", "8084"),
    )
