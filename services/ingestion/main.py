"""Command ingestion bridges MQTT and Kafka: it subscribes to telemetry and
machine status/event topics, validates and normalizes each message, and
publishes it to the appropriate Kafka topic, partitioned by device_id so
per-device ordering is preserved while different devices process in
parallel. It never performs database writes itself — that's the stream
processor's job downstream.

Python port of main.go. See mqtt_client.py's docstring for the two places
this port's concurrency model genuinely differs from Go's, and the
shutdown sequence comment below for why the specific bug that fix addressed
can't happen here in the first place.
"""

from __future__ import annotations

import logging
import queue
import signal
import sys
import threading
import time
import uuid

from opentelemetry import trace

from config import load_config
from health import start_health_server
from kafka_producer import KafkaPublishError, KafkaSink
from metrics import messages_failed_total, messages_received_total, processing_latency_seconds
from mqtt_client import AtomicBool, InboundMessage, MQTTConnectTimeout, connect_mqtt
from shared import logging_utils, tracing
from shared.events import (
    MachineEvent,
    NormalizedMachineEvent,
    NormalizedTelemetryEvent,
    TelemetryEvent,
    utc_now,
)
from validate import validate_machine_event, validate_telemetry

logger = logging_utils.init("ingestion")


def main() -> None:
    cfg = load_config()

    shutdown_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda *_: shutdown_event.set())

    shutdown_tracing = tracing.init("ingestion")

    sink = KafkaSink(cfg)
    mqtt_connected = AtomicBool()
    jobs: "queue.Queue[InboundMessage]" = queue.Queue(maxsize=cfg.queue_capacity)

    try:
        client = connect_mqtt(cfg, mqtt_connected, jobs, shutdown_event)
    except MQTTConnectTimeout as exc:
        logger.error("ingestion: failed to connect to MQTT broker: %s", exc)
        sys.exit(1)

    start_health_server(cfg.http_port, mqtt_connected, sink)
    logger.info("ingestion: health/metrics server listening on :%s", cfg.http_port)

    workers = [
        threading.Thread(target=_worker, args=(shutdown_event, sink, jobs), daemon=False)
        for _ in range(cfg.worker_pool_size)
    ]
    for w in workers:
        w.start()

    shutdown_event.wait()
    logger.info("ingestion: shutdown signal received, draining queue...")

    # Ordered shutdown. The Go version's history here matters for
    # understanding why this looks the way it does: an early version
    # deferred client.Disconnect, which ran *after* closing the jobs
    # channel and waiting for workers — so the MQTT client was still
    # connected and could still dispatch a handler that sent into an
    # already-closed channel, panicking. The fix was ordering
    # Disconnect-then-drain-then-exit explicitly, and — a second, more
    # subtle problem `go test -race` caught — never closing the channel at
    # all, since paho's own docs warn Disconnect can return before every
    # dispatched handler goroutine finishes, and no synchronization
    # primitive can safely track goroutines whose start time isn't
    # coordinated with the wait.
    #
    # None of that applies here. queue.Queue has no "closed" state to send
    # into unsafely — there is nothing to race. client.disconnect() +
    # loop_stop() below is stronger than Go's fire-and-forget
    # Disconnect(1000ms): loop_stop() blocks until paho's network thread
    # has actually exited, so by the time it returns we know for certain no
    # further on_message calls are coming — no such guarantee exists (or is
    # needed) in the Go version's design, but it doesn't hurt here either.
    client.disconnect()
    client.loop_stop()
    for w in workers:
        w.join()
    sink.close()
    shutdown_tracing()
    logger.info("ingestion: shutdown complete")


def _worker(shutdown_event: threading.Event, sink: KafkaSink, jobs: "queue.Queue[InboundMessage]") -> None:
    while not shutdown_event.is_set():
        try:
            job = jobs.get(timeout=0.5)
        except queue.Empty:
            continue
        _process_message(sink, job)

    # Drain whatever's already buffered rather than abandoning it.
    while True:
        try:
            job = jobs.get_nowait()
        except queue.Empty:
            return
        _process_message(sink, job)


def _process_message(sink: KafkaSink, job: InboundMessage) -> None:
    start = time.monotonic()
    try:
        with tracing.tracer("ingestion").start_as_current_span("ingestion.process_message") as span:
            span.set_attribute("mqtt.topic", job.topic)
            if job.topic.endswith("/telemetry"):
                messages_received_total.labels(topic_kind="telemetry").inc()
                _handle_telemetry(sink, job)
            elif job.topic.endswith("/status"):
                messages_received_total.labels(topic_kind="status").inc()
                _handle_machine_event(sink, job)
            elif job.topic.endswith("/events"):
                messages_received_total.labels(topic_kind="events").inc()
                _handle_machine_event(sink, job)
            else:
                logger.warning("unrecognized MQTT topic, dropping", extra={"mqtt_topic": job.topic})
                job.ack()
    finally:
        processing_latency_seconds.observe(time.monotonic() - start)


def _handle_telemetry(sink: KafkaSink, job: InboundMessage) -> None:
    try:
        raw = TelemetryEvent.model_validate_json(job.payload)
    except Exception as exc:  # noqa: BLE001 - malformed payload, any parse error is equivalent to Go's json.Unmarshal failing
        messages_failed_total.labels(reason="validation").inc()
        try:
            sink.dead_letter_validation_failure(job.payload, exc, str(uuid.uuid4()), job.topic)
        except Exception as dlq_exc:  # noqa: BLE001
            logger.error(
                "dead-letter write failed for malformed telemetry, leaving unacked",
                extra={"error": str(dlq_exc), "mqtt_topic": job.topic},
            )
            return  # do not ack: let MQTT redeliver
        job.ack()
        return

    try:
        validate_telemetry(raw)
    except ValueError as exc:
        messages_failed_total.labels(reason="validation").inc()
        try:
            sink.dead_letter_validation_failure(job.payload, exc, raw.event_id, job.topic)
        except Exception as dlq_exc:  # noqa: BLE001
            logger.error(
                "dead-letter write failed for invalid telemetry, leaving unacked",
                extra={
                    "error": str(dlq_exc),
                    "event_id": raw.event_id,
                    "device_id": raw.device_id,
                    "organization_id": raw.organization_id,
                },
            )
            return
        job.ack()
        return

    normalized = NormalizedTelemetryEvent(
        **raw.model_dump(),
        correlation_id=raw.event_id,
        ingested_at=utc_now(),
    )

    try:
        sink.publish_telemetry(raw.device_id, normalized, job.payload, job.topic)
    except KafkaPublishError as exc:
        logger.error(
            "could not durably record telemetry, leaving unacked for redelivery",
            extra={
                "error": str(exc),
                "event_id": raw.event_id,
                "device_id": raw.device_id,
                "organization_id": raw.organization_id,
            },
        )
        return
    logger.info(
        "telemetry event ingested",
        extra={"event_id": raw.event_id, "device_id": raw.device_id, "organization_id": raw.organization_id},
    )
    job.ack()


def _handle_machine_event(sink: KafkaSink, job: InboundMessage) -> None:
    try:
        raw = MachineEvent.model_validate_json(job.payload)
    except Exception as exc:  # noqa: BLE001
        messages_failed_total.labels(reason="validation").inc()
        try:
            sink.dead_letter_validation_failure(job.payload, exc, str(uuid.uuid4()), job.topic)
        except Exception as dlq_exc:  # noqa: BLE001
            logger.error(
                "dead-letter write failed for malformed machine event, leaving unacked",
                extra={"error": str(dlq_exc), "mqtt_topic": job.topic},
            )
            return
        job.ack()
        return

    correlation_id = str(uuid.uuid4())
    try:
        validate_machine_event(raw)
    except ValueError as exc:
        messages_failed_total.labels(reason="validation").inc()
        try:
            sink.dead_letter_validation_failure(job.payload, exc, correlation_id, job.topic)
        except Exception as dlq_exc:  # noqa: BLE001
            logger.error(
                "dead-letter write failed for invalid machine event, leaving unacked",
                extra={"error": str(dlq_exc), "device_id": raw.device_id, "organization_id": raw.organization_id},
            )
            return
        job.ack()
        return

    normalized = NormalizedMachineEvent(
        **raw.model_dump(),
        correlation_id=correlation_id,
        ingested_at=utc_now(),
    )

    try:
        sink.publish_machine_event(raw.device_id, normalized, job.payload, job.topic)
    except KafkaPublishError as exc:
        logger.error(
            "could not durably record machine event, leaving unacked for redelivery",
            extra={
                "error": str(exc),
                "event_id": correlation_id,
                "device_id": raw.device_id,
                "organization_id": raw.organization_id,
            },
        )
        return
    logger.info(
        "machine event ingested",
        extra={
            "event_id": correlation_id,
            "device_id": raw.device_id,
            "organization_id": raw.organization_id,
            "event_type": raw.event_type,
        },
    )
    job.ack()


if __name__ == "__main__":
    main()
