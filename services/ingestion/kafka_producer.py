"""Kafka publishing with retry + circuit breaker, the Python port of
kafka.go. Owns one Producer per topic role and applies retry-with-backoff
plus a circuit breaker around every publish, so a Kafka outage degrades
gracefully (fail fast, stop hammering the broker) instead of blocking the
whole worker pool indefinitely.
"""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException, Message, Producer

from shared import tracing
from shared.events import (
    ERROR_TYPE_TRANSIENT,
    ERROR_TYPE_VALIDATION,
    DeadLetterRecord,
    NormalizedMachineEvent,
    NormalizedTelemetryEvent,
    utc_now,
)
from shared.reliability import CircuitBreaker, ErrPermanent, retry_with_backoff

from config import Config
from metrics import dlq_messages_total, messages_failed_total, messages_processed_total

_logger = logging.getLogger("ingestion")


class KafkaPublishError(Exception):
    """A Kafka produce failed after exhausting retries/timeout."""


def _new_producer(brokers: list[str]) -> Producer:
    return Producer(
        {
            "bootstrap.servers": ",".join(brokers),
            # acks=all matches segmentio/kafka-go's RequiredAcks: RequireAll
            # in the Go version — every in-sync replica must ack before the
            # produce call is considered durable.
            "acks": "all",
            "linger.ms": 50,  # matches Go's BatchTimeout: 50ms
        }
    )


def produce_and_wait(producer: Producer, topic: str, key: str, value: bytes, headers: list[tuple[str, bytes]], timeout: float = 10.0) -> None:
    """Publishes one message and blocks until the broker has actually
    acknowledged it (or the attempt has definitively failed) — the Python
    equivalent of kafka-go's WriteMessages, which is synchronous by design.
    confluent-kafka's produce() is asynchronous under the hood (librdkafka's
    own internal queue), so flush() is what forces that synchronicity here.
    """
    result: dict[str, KafkaException | None] = {}

    def on_delivery(err: KafkaException | None, _msg: Message) -> None:
        result["err"] = err

    producer.produce(topic, key=key.encode("utf-8"), value=value, headers=headers, on_delivery=on_delivery)
    remaining = producer.flush(timeout)
    if remaining > 0:
        raise KafkaPublishError(f"kafka produce to {topic} timed out waiting for delivery ({remaining} message(s) still outstanding)")
    if result.get("err") is not None:
        raise KafkaPublishError(f"kafka produce to {topic} failed: {result['err']}")


class KafkaSink:
    def __init__(self, cfg: Config):
        self._telemetry_producer = _new_producer(cfg.kafka_brokers)
        self._events_producer = _new_producer(cfg.kafka_brokers)
        self._dlq_producer = _new_producer(cfg.kafka_brokers)
        self._telemetry_topic = cfg.topic_telemetry_raw
        self._events_topic = cfg.topic_device_events
        self._dlq_topic = cfg.topic_dead_letter
        self._breaker = CircuitBreaker(cfg.breaker_failure_threshold, cfg.breaker_cooldown_seconds)
        self._max_retries = cfg.kafka_max_retries
        self._retry_base_delay = cfg.kafka_retry_base_delay_seconds

    def close(self) -> None:
        for producer in (self._telemetry_producer, self._events_producer, self._dlq_producer):
            producer.flush(5.0)

    def breaker_state(self) -> str:
        return self._breaker.state()

    def publish_telemetry(self, key: str, evt: NormalizedTelemetryEvent, raw_payload: bytes, source_topic: str) -> None:
        """Attempts to write to telemetry.raw with retry + circuit breaker.
        On exhaustion, routes the event to the dead-letter topic itself so a
        Kafka blip never silently drops data — the only case that's truly
        unrecoverable is the dead-letter write also failing, in which case
        the caller must not ack the source MQTT message so it gets
        redelivered later."""
        try:
            payload = evt.model_dump_json().encode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise ErrPermanent(f"marshal normalized telemetry: {exc}") from exc

        try:
            self._write_with_protection(self._telemetry_producer, self._telemetry_topic, key, payload)
            messages_processed_total.labels(result="success").inc()
            return
        except Exception as exc:  # noqa: BLE001
            _logger.error("kafka publish to telemetry.raw failed after retries, routing to dead-letter: %s", exc)
            self._dead_letter(raw_payload, exc, ERROR_TYPE_TRANSIENT, "kafka_publish", evt.correlation_id, source_topic)

    def publish_machine_event(self, key: str, evt: NormalizedMachineEvent, raw_payload: bytes, source_topic: str) -> None:
        try:
            payload = evt.model_dump_json().encode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise ErrPermanent(f"marshal normalized machine event: {exc}") from exc

        try:
            self._write_with_protection(self._events_producer, self._events_topic, key, payload)
            messages_processed_total.labels(result="success").inc()
            return
        except Exception as exc:  # noqa: BLE001
            _logger.error("kafka publish to device.events failed after retries, routing to dead-letter: %s", exc)
            self._dead_letter(raw_payload, exc, ERROR_TYPE_TRANSIENT, "kafka_publish", evt.correlation_id, source_topic)

    def dead_letter_validation_failure(self, raw_payload: bytes, validation_err: Exception, correlation_id: str, source_topic: str) -> None:
        self._dead_letter(raw_payload, validation_err, ERROR_TYPE_VALIDATION, "validation", correlation_id, source_topic)

    def _dead_letter(self, raw_payload: bytes, cause: Exception, error_type: str, stage: str, correlation_id: str, source_topic: str) -> None:
        record = DeadLetterRecord(
            original_payload=raw_payload.decode("utf-8", errors="replace"),
            error=str(cause),
            error_type=error_type,
            service="ingestion",
            processing_stage=stage,
            retry_count=self._max_retries,
            timestamp=utc_now(),
            correlation_id=correlation_id,
            source_topic=source_topic,
        )
        payload = record.model_dump_json().encode("utf-8")
        # Dead-letter writes are attempted once without the circuit breaker:
        # if Kafka is down entirely, both the primary topic and dead-letter
        # are unreachable, and there's nothing more ingestion can do but
        # leave the source MQTT message unacked for redelivery.
        try:
            produce_and_wait(self._dlq_producer, self._dlq_topic, correlation_id, payload, headers=[])
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="kafka_unreachable").inc()
            raise KafkaPublishError(f"dead-letter write also failed: {exc}") from exc
        dlq_messages_total.inc()
        messages_processed_total.labels(result="dead_letter").inc()

    def _write_with_protection(self, producer: Producer, topic: str, key: str, payload: bytes) -> None:
        if not self._breaker.allow():
            raise KafkaPublishError(f"circuit breaker open for kafka topic {topic}")

        headers: list[tuple[str, bytes]] = []
        tracing.inject_kafka(headers)

        try:
            retry_with_backoff(
                self._max_retries,
                self._retry_base_delay,
                lambda: produce_and_wait(producer, topic, key, payload, headers),
            )
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()
