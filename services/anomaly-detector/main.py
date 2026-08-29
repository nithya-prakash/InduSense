"""Command anomaly-detector consumes telemetry.processed and runs three
independent detection levels on every reading — a rule-based operating-
range check, a statistical EWMA z-score check, and a per-machine-type
isolation forest over each device's multivariate feature vector —
publishing a combined result to anomalies.detected whenever any of them
fires.

Python port of main.go. Consumption stays single-threaded per instance,
same as stream-processor: poll() and commit() are called sequentially from
one loop so offset commits always advance in the order messages were
actually processed.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
import uuid

from confluent_kafka import KafkaException, Message

from catalog import Catalog
from config import Config, load_config
from detect import combine_detections, run_detectors
from featurestore import FeatureStore
from forestregistry import ForestRegistry, run_forest_trainer
from health import start_health_server
from idempotency import claim_telemetry_event_once
from kafka_io import KafkaIO, KafkaPublishError
from metrics import (
    anomalies_detected_total,
    dlq_messages_total,
    kafka_consumer_lag,
    messages_consumed_total,
    messages_failed_total,
    processing_latency_seconds,
)
from rules import MetricRange
from shared import logging_utils, tracing
from shared.events import AnomalyDetected, NormalizedTelemetryEvent, utc_now
from stats import StatisticalTrackers

logger = logging_utils.init("anomaly-detector")


def main() -> None:
    cfg = load_config()

    shutdown_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda *_: shutdown_event.set())

    shutdown_tracing = tracing.init("anomaly-detector")

    try:
        cat = Catalog(cfg.postgres_dsn, cfg.postgres_max_conns)
    except Exception as exc:  # noqa: BLE001
        logger.error("anomaly-detector: failed to load initial catalog: %s", exc)
        sys.exit(1)

    kio = KafkaIO(cfg)
    trackers = StatisticalTrackers(cfg.ewma_alpha)
    fs = FeatureStore(cfg.forest_training_buffer_size)
    forests = ForestRegistry()

    catalog_thread = threading.Thread(target=_run_catalog_refresher, args=(shutdown_event, cfg, cat), daemon=False)
    forest_thread = threading.Thread(target=run_forest_trainer, args=(shutdown_event, cfg, fs, forests), daemon=False)
    lag_thread = threading.Thread(target=_run_lag_reporter, args=(shutdown_event, kio), daemon=False)
    catalog_thread.start()
    forest_thread.start()
    lag_thread.start()

    start_health_server(cfg.http_port, cat, kio, forests)
    logger.info("anomaly-detector: health/metrics server listening on :%s", cfg.http_port)

    logger.info("anomaly-detector: consuming %s as group %s", cfg.topic_processed, cfg.consumer_group_id)
    _consume_loop(shutdown_event, cfg, kio, cat, trackers, fs, forests)

    catalog_thread.join()
    forest_thread.join()
    lag_thread.join()
    kio.close()
    cat.close()
    shutdown_tracing()
    logger.info("anomaly-detector: shutdown complete")


def _run_catalog_refresher(shutdown_event: threading.Event, cfg: Config, cat: Catalog) -> None:
    while not shutdown_event.wait(timeout=cfg.catalog_refresh_every_seconds):
        try:
            cat.refresh()
        except Exception as exc:  # noqa: BLE001
            logger.error("anomaly-detector: catalog refresh failed (keeping stale data): %s", exc)


def _run_lag_reporter(shutdown_event: threading.Event, kio: KafkaIO) -> None:
    while not shutdown_event.wait(timeout=5.0):
        kafka_consumer_lag.set(kio.lag())


def _consume_loop(shutdown_event: threading.Event, cfg: Config, kio: KafkaIO, cat: Catalog, trackers: StatisticalTrackers, fs: FeatureStore, forests: ForestRegistry) -> None:
    while not shutdown_event.is_set():
        try:
            msg = kio.poll(1.0)
        except KafkaException as exc:
            logger.error("anomaly-detector: fetch error: %s", exc)
            continue
        if msg is None:
            continue

        start = time.monotonic()
        should_commit = _process_message(cfg, kio, cat, trackers, fs, forests, msg)
        processing_latency_seconds.observe(time.monotonic() - start)

        if should_commit:
            try:
                kio.commit(msg)
            except KafkaException as exc:
                logger.error("anomaly-detector: commit failed for offset %d: %s", msg.offset(), exc)
        else:
            logger.info("anomaly-detector: leaving offset %d uncommitted for reprocessing", msg.offset())


def _process_message(cfg: Config, kio: KafkaIO, cat: Catalog, trackers: StatisticalTrackers, fs: FeatureStore, forests: ForestRegistry, msg: Message) -> bool:
    messages_consumed_total.inc()

    tracing.extract_kafka(msg.headers())
    with tracing.tracer("anomaly-detector").start_as_current_span("anomaly_detector.detect"):
        try:
            evt = NormalizedTelemetryEvent.model_validate_json(msg.value())
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="unmarshal").inc()
            try:
                kio.dead_letter(msg.value(), exc, "unmarshal", "")
            except Exception as dlq_exc:  # noqa: BLE001
                logger.error("dead-letter write failed for malformed message: %s", dlq_exc)
                return False
            dlq_messages_total.inc()
            return True

        info = cat.lookup(evt.device_id)
        rng = MetricRange()
        has_range = False
        feature_order: list[str] = []
        machine_type = ""
        if info is not None:
            machine_type = info.machine_type
            if evt.metric in info.ranges:
                rng = info.ranges[evt.metric]
                has_range = True
            feature_order = cat.features_for(machine_type)

        z_score, sample_count = trackers.update(evt.device_id, evt.metric, evt.value)

        forest_score = 0.0
        has_forest = False
        if info is not None:
            vector, observed = fs.observe(evt.device_id, machine_type, evt.metric, evt.value, feature_order)
            if observed:
                forest = forests.get(machine_type)
                if forest is not None:
                    forest_score = forest.score(vector)
                    has_forest = True

        results = run_detectors(evt.value, rng, has_range, z_score, sample_count, cfg, forest_score, has_forest)
        if not results:
            return True

        for r in results:
            anomalies_detected_total.labels(method=r.method).inc()

        try:
            claimed = claim_telemetry_event_once(cat.pool(), evt.event_id)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "idempotency claim failed, leaving unacked for retry",
                extra={"error": str(exc), "event_id": evt.event_id, "device_id": evt.device_id},
            )
            return False
        if not claimed:
            # Kafka redelivered a telemetry.processed message we already ran
            # detection for (e.g. after a crash between publish and commit) —
            # re-publishing would create a second AnomalyDetected, and
            # downstream, a second alert/incident, for the same reading.
            logger.info(
                "duplicate telemetry event, anomaly already published, skipping re-publish",
                extra={"event_id": evt.event_id, "device_id": evt.device_id},
            )
            return True

        severity, score, methods, reason = combine_detections(results)
        anomaly = AnomalyDetected(
            anomaly_id=str(uuid.uuid4()),
            event_id=evt.event_id,
            organization_id=evt.organization_id,
            factory_id=evt.factory_id,
            production_line_id=evt.production_line_id,
            machine_id=evt.machine_id,
            device_id=evt.device_id,
            sensor_id=evt.sensor_id,
            metric=evt.metric,
            value=evt.value,
            severity=severity,
            score=score,
            methods=methods,
            reason=reason,
            detected_at=utc_now(),
        )

        try:
            kio.publish_anomaly(evt.device_id, anomaly)
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="publish_anomaly").inc()
            try:
                kio.dead_letter(msg.value(), exc, "publish_anomaly", evt.event_id)
            except Exception as dlq_exc:  # noqa: BLE001
                logger.error(
                    "dead-letter write also failed, leaving unacked",
                    extra={"error": str(dlq_exc), "event_id": evt.event_id, "device_id": evt.device_id, "organization_id": evt.organization_id},
                )
                return False
            dlq_messages_total.inc()
        else:
            logger.info(
                "anomaly detected",
                extra={
                    "anomaly_id": anomaly.anomaly_id,
                    "event_id": evt.event_id,
                    "device_id": evt.device_id,
                    "organization_id": evt.organization_id,
                    "severity": severity,
                    "methods": methods,
                },
            )

        return True


if __name__ == "__main__":
    main()
