"""InfluxDB writes with retry + circuit breaker, the Python port of
influx.go. Wraps the InfluxDB client with retry-with-backoff and a circuit
breaker, matching the same pattern used for Kafka writes in ingestion —
InfluxDB is just as much an external dependency that can be temporarily
unavailable, and deserves the same fail-fast-during-an-outage treatment.
"""

from __future__ import annotations

from datetime import datetime

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from config import Config
from registry import SeriesKey
from shared.reliability import CircuitBreaker, retry_with_backoff
from window import WindowStats


class InfluxSink:
    def __init__(self, cfg: Config):
        self._client = InfluxDBClient(url=cfg.influx_url, token=cfg.influx_token, org=cfg.influx_org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._org = cfg.influx_org
        self._bucket = cfg.influx_bucket
        self._breaker = CircuitBreaker(cfg.breaker_failure_threshold, cfg.breaker_cooldown_seconds)
        self._max_retries = cfg.influx_max_retries
        self._retry_delay = cfg.influx_retry_base_delay_seconds

    def close(self) -> None:
        self._client.close()

    def ping(self) -> bool:
        return self._client.ping()

    def breaker_state(self) -> str:
        return self._breaker.state()

    def write_raw_point(self, key: SeriesKey, at: datetime, value: float) -> None:
        """Writes one sensor_telemetry point. Measurement/tags/fields match
        docs/DATABASE.md exactly: tags identify the hierarchy path + metric,
        the only field is value. Writing the same (tags, timestamp) twice is
        a safe no-op overwrite, which is what makes this naturally
        idempotent under at-least-once delivery."""
        point = (
            Point("sensor_telemetry")
            .tag("factory_id", key.factory_id)
            .tag("production_line_id", key.production_line_id)
            .tag("machine_id", key.machine_id)
            .tag("device_id", key.device_id)
            .tag("sensor_id", key.sensor_id)
            .tag("metric", key.metric)
            .field("value", value)
            .time(at)
        )
        self._write_with_protection(point)

    def write_aggregate_batch(self, points: list[Point]) -> None:
        """Writes many aggregate points (one per series x window, for a
        single flush tick) as a single InfluxDB call instead of one round
        trip per point — with up to ~1000 sensors x 5 windows per flush,
        that difference matters."""
        if not points:
            return
        self._write_with_protection(*points)

    def _write_with_protection(self, *points: Point) -> None:
        if not self._breaker.allow():
            raise RuntimeError("circuit breaker open for influxdb")

        try:
            retry_with_backoff(
                self._max_retries,
                self._retry_delay,
                lambda: self._write_api.write(bucket=self._bucket, org=self._org, record=list(points)),
            )
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()


def build_aggregate_point(key: SeriesKey, window_label: str, at: datetime, stats: WindowStats) -> Point:
    """Constructs (without writing) one sensor_telemetry_agg point for a
    given window (e.g. "1m"), for batching by the caller."""
    return (
        Point("sensor_telemetry_agg")
        .tag("factory_id", key.factory_id)
        .tag("production_line_id", key.production_line_id)
        .tag("machine_id", key.machine_id)
        .tag("device_id", key.device_id)
        .tag("sensor_id", key.sensor_id)
        .tag("metric", key.metric)
        .tag("window", window_label)
        .field("moving_avg", stats.moving_avg)
        .field("moving_stddev", stats.moving_stddev)
        .field("min", stats.min)
        .field("max", stats.max)
        .field("rate_of_change", stats.rate_of_change)
        .field("count", stats.count)
        .time(at)
    )
