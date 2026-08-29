"""Health/readiness endpoints, the Python port of handlers_health.go."""

from __future__ import annotations

from dataclasses import dataclass

import paho.mqtt.client as mqtt
from confluent_kafka.admin import AdminClient
from fastapi import APIRouter, Response
from influxdb_client import InfluxDBClient
from psycopg_pool import ConnectionPool
from redis import Redis

from config import Config

router = APIRouter()


@dataclass
class DepStatus:
    postgres: str = "ok"
    redis: str = "ok"
    kafka: str = "ok"
    mqtt: str = "ok"
    influxdb: str = "ok"


def _check_dependencies(cfg: Config, pool: ConnectionPool, redis_client: Redis) -> DepStatus:
    status = DepStatus()

    try:
        with pool.connection(timeout=2.0) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        status.postgres = f"unreachable: {exc}"

    try:
        redis_client.ping()
    except Exception as exc:  # noqa: BLE001
        status.redis = f"unreachable: {exc}"

    try:
        broker = cfg.kafka_brokers[0]
        admin = AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 2000})
        admin.list_topics(timeout=2.0)
    except Exception as exc:  # noqa: BLE001
        status.kafka = f"unreachable: {exc}"

    try:
        host, _, port = cfg.mqtt_broker_url.replace("tcp://", "").partition(":")
        client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        client.connect(host, int(port) if port else 1883, keepalive=2)
        client.loop(timeout=2.0)
        if not client.is_connected():
            status.mqtt = "unreachable"
        client.disconnect()
    except Exception:  # noqa: BLE001
        status.mqtt = "unreachable"

    try:
        influx_client = InfluxDBClient(url=cfg.influx_url, token=cfg.influx_token, org=cfg.influx_org, timeout=2_000)
        try:
            if not influx_client.ping():
                status.influxdb = "unreachable"
        finally:
            influx_client.close()
    except Exception as exc:  # noqa: BLE001
        status.influxdb = f"unreachable: {exc}"

    return status


def register_health_routes(cfg: Config, pool: ConnectionPool, redis_client: Redis) -> APIRouter:
    @router.get("/live")
    def live() -> Response:
        return Response(content="ok", status_code=200)

    @router.get("/ready")
    def ready(response: Response) -> dict:
        status = _check_dependencies(cfg, pool, redis_client)
        # MQTT is deliberately excluded from readiness: the API doesn't
        # itself consume/publish MQTT, so its outage shouldn't take API
        # readiness down with it -- it's still reported in /health though.
        ready_ = status.postgres == "ok" and status.redis == "ok" and status.kafka == "ok" and status.influxdb == "ok"
        response.status_code = 200 if ready_ else 503
        return {"ready": ready_, "dependencies": status.__dict__}

    @router.get("/health")
    def health() -> dict:
        return _check_dependencies(cfg, pool, redis_client).__dict__

    return router
