"""Kafka consume/produce, the Python port of kafka.go.

Unlike ingestion/stream-processor/anomaly-detector's kafka_io.py, publishing
here is deliberately NOT wrapped in the retry+circuit-breaker pattern —
matching the Go original exactly. An alert is already durably recorded in
Postgres by the time this publish is attempted, so a Kafka hiccup here
means a delayed/missed event notification, not lost alert data; the extra
protection machinery isn't earning its keep for a non-critical, already-
persisted side effect.
"""

from __future__ import annotations

from confluent_kafka import Consumer, KafkaException, Message, Producer

from config import Config
from shared import tracing
from shared.events import ERROR_TYPE_TRANSIENT, AlertEvent, DeadLetterRecord, utc_now


class KafkaPublishError(Exception):
    pass


def _new_consumer(brokers: list[str], group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": ",".join(brokers),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    return consumer


def _new_producer(brokers: list[str]) -> Producer:
    return Producer({"bootstrap.servers": ",".join(brokers), "acks": "all", "linger.ms": 50})


def _produce_and_wait(producer: Producer, topic: str, key: str, value: bytes, headers: list[tuple[str, bytes]], timeout: float = 10.0) -> None:
    result: dict[str, KafkaException | None] = {}

    def on_delivery(err: KafkaException | None, _msg: Message) -> None:
        result["err"] = err

    producer.produce(topic, key=key.encode("utf-8"), value=value, headers=headers, on_delivery=on_delivery)
    remaining = producer.flush(timeout)
    if remaining > 0:
        raise KafkaPublishError(f"kafka produce to {topic} timed out waiting for delivery ({remaining} message(s) still outstanding)")
    if result.get("err") is not None:
        raise KafkaPublishError(f"kafka produce to {topic} failed: {result['err']}")


class KafkaIO:
    def __init__(self, cfg: Config):
        self.anomaly_consumer = _new_consumer(cfg.kafka_brokers, cfg.consumer_group_id, cfg.topic_anomalies)
        self.events_consumer = _new_consumer(cfg.kafka_brokers, cfg.consumer_group_id, cfg.topic_device_events)
        self._alert_producer = _new_producer(cfg.kafka_brokers)
        self._dlq_producer = _new_producer(cfg.kafka_brokers)
        self._alert_topic = cfg.topic_alerts
        self._dlq_topic = cfg.topic_dead_letter

    def close(self) -> None:
        self.anomaly_consumer.close()
        self.events_consumer.close()
        self._alert_producer.flush(5.0)
        self._dlq_producer.flush(5.0)

    def publish_alert(self, key: str, evt: AlertEvent) -> None:
        payload = evt.model_dump_json().encode("utf-8")
        headers: list[tuple[str, bytes]] = []
        tracing.inject_kafka(headers)
        _produce_and_wait(self._alert_producer, self._alert_topic, key, payload, headers)

    def dead_letter(self, raw_payload: bytes, cause: Exception, stage: str, correlation_id: str, source_topic: str) -> None:
        record = DeadLetterRecord(
            original_payload=raw_payload.decode("utf-8", errors="replace"),
            error=str(cause),
            error_type=ERROR_TYPE_TRANSIENT,
            service="alert-service",
            processing_stage=stage,
            retry_count=0,
            timestamp=utc_now(),
            correlation_id=correlation_id,
            source_topic=source_topic,
        )
        payload = record.model_dump_json().encode("utf-8")
        _produce_and_wait(self._dlq_producer, self._dlq_topic, correlation_id, payload, headers=[])
