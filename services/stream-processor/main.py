"""Command stream-processor consumes telemetry.raw, deduplicates by
event_id, writes each raw reading to InfluxDB, maintains rolling windowed
aggregates (moving average/stddev/min/max/rate-of-change) flushed to
InfluxDB periodically, and republishes a cleaned copy to
telemetry.processed for the anomaly detector.

Python port of main.go. Consumption stays deliberately single-threaded per
instance, same as the Go version: poll() and commit() are called
sequentially from one loop so offset commits always advance in the order
messages were actually processed. Scaling beyond one instance's throughput
is done the standard Kafka way — more partitions, more consumer-group
instances — not more threads committing out of order within one instance.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time

from confluent_kafka import KafkaException, Message

from config import Config, load_config
from dedup import Deduplicator
from health import start_health_server
from influx import InfluxSink, build_aggregate_point
from kafka_io import KafkaIO, KafkaPublishError
from metrics import (
    dlq_messages_total,
    duplicate_events_total,
    kafka_consumer_lag,
    messages_consumed_total,
    messages_failed_total,
    processing_latency_seconds,
    windowed_aggregates_written_total,
)
from registry import SeriesKey, SeriesRegistry
from shared import logging_utils, tracing
from shared.events import NormalizedTelemetryEvent, utc_now

logger = logging_utils.init("stream-processor")


def _duration_label(total_seconds: float) -> str:
    """Matches Go's time.Duration.String() for the whole-second window
    values this service actually configures (10s/30s/1m/5m/15m) — e.g. 60
    seconds formats as "1m0s", not "60s" or "1m". This must stay
    byte-for-byte identical to what the Go version wrote, since it's an
    InfluxDB tag value: a mismatched format would split one logical window
    into two different tag values across the Go-era and Python-era data.
    """
    total = int(total_seconds)
    if total < 60:
        return f"{total}s"
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h{minutes}m{seconds}s"
    return f"{minutes}m{seconds}s"


def main() -> None:
    cfg = load_config()

    shutdown_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda *_: shutdown_event.set())

    shutdown_tracing = tracing.init("stream-processor")

    kio = KafkaIO(cfg)
    dedup = Deduplicator(cfg)
    influx = InfluxSink(cfg)

    if not influx.ping():
        logger.error("stream-processor: cannot reach influxdb at startup")
        sys.exit(1)
    if not dedup.ping():
        logger.error("stream-processor: cannot reach redis at startup")
        sys.exit(1)

    start_health_server(cfg.http_port, dedup, influx, kio)
    logger.info("stream-processor: health/metrics server listening on :%s", cfg.http_port)

    max_window = max(cfg.windows_seconds)
    registry = SeriesRegistry(max_window)

    lag_thread = threading.Thread(target=_run_lag_reporter, args=(shutdown_event, kio), daemon=False)
    flush_thread = threading.Thread(target=_run_window_flusher, args=(shutdown_event, cfg, registry, influx), daemon=False)
    lag_thread.start()
    flush_thread.start()

    logger.info("stream-processor: consuming %s as group %s", cfg.topic_telemetry_raw, cfg.consumer_group_id)
    _consume_loop(shutdown_event, kio, dedup, influx, registry)

    lag_thread.join()
    flush_thread.join()
    kio.close()
    dedup.close()
    influx.close()
    shutdown_tracing()
    logger.info("stream-processor: shutdown complete")


def _consume_loop(shutdown_event: threading.Event, kio: KafkaIO, dedup: Deduplicator, influx: InfluxSink, registry: SeriesRegistry) -> None:
    while not shutdown_event.is_set():
        try:
            msg = kio.poll(1.0)
        except KafkaException as exc:
            logger.error("stream-processor: fetch error: %s", exc)
            continue
        if msg is None:
            continue

        start = time.monotonic()
        should_commit = _process_message(kio, dedup, influx, registry, msg)
        processing_latency_seconds.observe(time.monotonic() - start)

        if should_commit:
            try:
                kio.commit(msg)
            except KafkaException as exc:
                logger.error("stream-processor: commit failed for offset %d: %s", msg.offset(), exc)
        else:
            logger.info("stream-processor: leaving offset %d uncommitted for reprocessing", msg.offset())


def _process_message(kio: KafkaIO, dedup: Deduplicator, influx: InfluxSink, registry: SeriesRegistry, msg: Message) -> bool:
    """Returns whether the offset should be committed. False means the
    message was neither durably processed nor dead-lettered, so it must be
    redelivered on the next fetch (after a restart or rebalance)."""
    messages_consumed_total.inc()

    tracing.extract_kafka(msg.headers())
    with tracing.tracer("stream-processor").start_as_current_span("stream_processor.process"):
        try:
            evt = NormalizedTelemetryEvent.model_validate_json(msg.value())
        except Exception as exc:  # noqa: BLE001 - malformed payload
            messages_failed_total.labels(reason="unmarshal").inc()
            try:
                kio.dead_letter(msg.value(), exc, "unmarshal", "")
            except Exception as dlq_exc:  # noqa: BLE001
                logger.error("dead-letter write failed for malformed message: %s", dlq_exc)
                return False
            dlq_messages_total.inc()
            return True

        try:
            first_seen = dedup.claim(evt.event_id)
        except Exception as exc:  # noqa: BLE001 - Redis unreachable: fail safe, don't commit
            messages_failed_total.labels(reason="dedup_unavailable").inc()
            logger.error(
                "dedup check failed",
                extra={"error": str(exc), "event_id": evt.event_id, "device_id": evt.device_id, "organization_id": evt.organization_id},
            )
            return False
        if not first_seen:
            duplicate_events_total.inc()
            return True  # known duplicate: safe to commit without reprocessing

        key = SeriesKey(
            factory_id=evt.factory_id,
            production_line_id=evt.production_line_id,
            machine_id=evt.machine_id,
            device_id=evt.device_id,
            sensor_id=evt.sensor_id,
            metric=evt.metric,
        )

        try:
            influx.write_raw_point(key, evt.timestamp, evt.value)
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="influx_write").inc()
            try:
                kio.dead_letter(msg.value(), exc, "influxdb_write", evt.event_id)
            except Exception as dlq_exc:  # noqa: BLE001
                logger.error(
                    "dead-letter write also failed, leaving unacked",
                    extra={"error": str(dlq_exc), "event_id": evt.event_id, "device_id": evt.device_id, "organization_id": evt.organization_id},
                )
                return False
            dlq_messages_total.inc()
            return True

        registry.record(key, evt.timestamp, evt.value)

        try:
            kio.publish_processed(evt.device_id, evt)
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="republish").inc()
            try:
                kio.dead_letter(msg.value(), exc, "republish_processed", evt.event_id)
            except Exception as dlq_exc:  # noqa: BLE001
                logger.error(
                    "dead-letter write also failed, leaving unacked",
                    extra={"error": str(dlq_exc), "event_id": evt.event_id, "device_id": evt.device_id, "organization_id": evt.organization_id},
                )
                return False
            dlq_messages_total.inc()
            return True

        return True


def _run_window_flusher(shutdown_event: threading.Event, cfg: Config, registry: SeriesRegistry, influx: InfluxSink) -> None:
    """Periodically computes and writes windowed aggregates for every
    tracked series. Aggregate writes are best-effort (logged, not
    dead-lettered): they're derived observability data recomputable from
    raw points already durably stored, not a primary business record."""
    window_labels = {w: _duration_label(w) for w in cfg.windows_seconds}
    while not shutdown_event.wait(timeout=cfg.window_flush_interval_seconds):
        _flush_once(cfg, registry, influx, window_labels)


def _flush_once(cfg: Config, registry: SeriesRegistry, influx: InfluxSink, window_labels: dict[float, str]) -> None:
    now = utc_now()
    points = []

    for series in registry.snapshot():
        for w in cfg.windows_seconds:
            stats, ok = series.buf.stats_for(now, w)
            if not ok:
                continue
            points.append(build_aggregate_point(series.key, window_labels[w], now, stats))

    try:
        influx.write_aggregate_batch(points)
    except Exception as exc:  # noqa: BLE001
        logger.error("stream-processor: windowed aggregate flush failed (%d points dropped): %s", len(points), exc)
        return
    windowed_aggregates_written_total.inc(len(points))


def _run_lag_reporter(shutdown_event: threading.Event, kio: KafkaIO) -> None:
    while not shutdown_event.wait(timeout=5.0):
        kafka_consumer_lag.set(kio.lag())


if __name__ == "__main__":
    main()
