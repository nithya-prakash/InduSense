"""Environment-variable configuration, the Python port of config.go."""

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


def env_bool(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value:
        return value.strip().lower() in ("1", "t", "true", "yes")
    return default


def env_string_list(key: str, default: list[str] | None) -> list[str] | None:
    """Reads a comma-separated env var into a list, trimming whitespace
    around each entry and dropping empty ones. Returns default (typically
    None) if the var is unset."""
    v = os.environ.get(key, "")
    if not v:
        return default
    return [part.strip() for part in v.split(",") if part.strip()]


@dataclass
class Config:
    port: str

    postgres_dsn: str
    postgres_max_conns: int

    redis_addr: str
    redis_password: str
    redis_db: int

    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str

    kafka_brokers: list[str]
    topic_alerts: str
    mqtt_broker_url: str

    jwt_access_secret: str
    jwt_refresh_secret: str
    jwt_access_ttl_seconds: float
    jwt_refresh_ttl_seconds: float

    cors_allowed_origin: str

    rate_limit_auth_per_minute: int
    rate_limit_default_per_min: int

    # trust_proxy_headers gates whether X-Forwarded-For is ever consulted
    # for the "real" client IP (used for rate limiting and auth audit
    # logging). Defaults to false: an X-Forwarded-For header is trivially
    # spoofable by any direct client, so trusting it unconditionally lets
    # an attacker reset their own rate-limit bucket on every request just
    # by varying the header. Only set this true when the API sits behind
    # a reverse proxy that itself sets/overwrites X-Forwarded-For -- and
    # even then, trusted_proxy_cidrs must list that proxy's address so a
    # client that connects directly (bypassing the proxy) can't still
    # spoof it.
    trust_proxy_headers: bool
    trusted_proxy_cidrs: list[str] = field(default_factory=list)


def load_config() -> Config:
    return Config(
        port=env_str("API_PORT", "8080"),
        postgres_dsn=env_str("API_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"),
        postgres_max_conns=env_int("API_POSTGRES_MAX_CONNS", 10),
        redis_addr=f"{env_str('REDIS_HOST', 'localhost')}:{env_str('REDIS_PORT', '6379')}",
        redis_password=env_str("REDIS_PASSWORD", ""),
        redis_db=env_int("REDIS_DB", 0),
        influx_url=env_str("INFLUXDB_URL", "http://localhost:8086"),
        influx_token=env_str("INFLUXDB_TOKEN", "dev-only-influx-admin-token-change-me"),
        influx_org=env_str("INFLUXDB_ORG", "indusense"),
        influx_bucket=env_str("INFLUXDB_BUCKET", "telemetry"),
        kafka_brokers=env_str("KAFKA_BROKERS", "localhost:9094").split(","),
        topic_alerts=env_str("KAFKA_TOPIC_ALERTS", "alerts"),
        mqtt_broker_url=env_str("MQTT_BROKER_URL", "tcp://localhost:1883"),
        jwt_access_secret=env_str("JWT_ACCESS_SECRET", "change-me-dev-only-access-secret"),
        jwt_refresh_secret=env_str("JWT_REFRESH_SECRET", "change-me-dev-only-refresh-secret"),
        jwt_access_ttl_seconds=env_int("JWT_ACCESS_TTL_MINUTES", 15) * 60,
        jwt_refresh_ttl_seconds=env_int("JWT_REFRESH_TTL_HOURS", 168) * 3600,
        cors_allowed_origin=env_str("API_CORS_ALLOWED_ORIGINS", "http://localhost:3000"),
        rate_limit_auth_per_minute=env_int("API_RATE_LIMIT_AUTH_PER_MIN", 10),
        rate_limit_default_per_min=env_int("API_RATE_LIMIT_DEFAULT_PER_MIN", 120),
        trust_proxy_headers=env_bool("API_TRUST_PROXY_HEADERS", False),
        trusted_proxy_cidrs=env_string_list("API_TRUSTED_PROXY_CIDRS", None) or [],
    )
