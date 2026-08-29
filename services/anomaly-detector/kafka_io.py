"""Kafka consume/produce, the Python port of kafka.go.

Consumption is deliberately single-threaded per instance: poll() and
commit() are called sequentially from one loop so offset commits always
advance in the order messages were actually processed.

The publish path (to anomalies.detected) applies retry-with-backoff plus a
circuit breaker, matching the pattern used for Kafka writes in ingestion
and stream-processor — a broker outage should fail fast via the breaker
rather than retry every message individually. dead-letter and the
consumer-side poll/commit are deliberately left unwrapped: if Kafka is
down entirely there's nothing more to do but leave the source message
uncommitted for redelivery once it recovers.
"""

from __future__ import annotations

from confluent_kafka import Consumer, KafkaException, Message, Producer

from config import Config
from shared import tracing
from shared.events import ERROR_TYPE_TRANSIENT, AnomalyDetected, DeadLetterRecord, utc_now
from shared.reliability import CircuitBreaker, ErrPermanent, retry_with_backoff


class KafkaPublishError(Exception):
    pass


def _new_producer(brokers: list[str]) -> Producer:
    return Producer({"bootstrap.servers": ",".join(brokers), "acks": "all", "linger.ms": 50})


def produce_and_wait(producer: Producer, topic: str, key: str, value: bytes, headers: list[tuple[str, bytes]], timeout: float = 10.0) -> None:
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
        self._consumer = Consumer(
            {
                "bootstrap.servers": ",".join(cfg.kafka_brokers),
                "group.id": cfg.consumer_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([cfg.topic_processed])

        self._anomaly_producer = _new_producer(cfg.kafka_brokers)
        self._dlq_producer = _new_producer(cfg.kafka_brokers)
        self._anomaly_topic = cfg.topic_anomalies
        self._dlq_topic = cfg.topic_dead_letter

        self._breaker = CircuitBreaker(cfg.breaker_failure_threshold, cfg.breaker_cooldown_seconds)
        self._max_retries = cfg.kafka_max_retries
        self._retry_delay = cfg.kafka_retry_base_delay_seconds

    def close(self) -> None:
        self._consumer.close()
        self._anomaly_producer.flush(5.0)
        self._dlq_producer.flush(5.0)

    def breaker_state(self) -> str:
        return self._breaker.state()

    def poll(self, timeout: float) -> Message | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise KafkaException(msg.error())
        return msg

    def commit(self, msg: Message) -> None:
        self._consumer.commit(message=msg, asynchronous=False)

    def lag(self) -> int:
        try:
            assigned = self._consumer.assignment()
        except KafkaException:
            return 0
        if not assigned:
            return 0

        total = 0
        try:
            positions = self._consumer.position(assigned)
        except KafkaException:
            return 0
        for tp in positions:
            if tp.offset < 0:
                continue
            watermarks = self._consumer.get_watermark_offsets(tp, timeout=2.0, cached=True)
            if watermarks is None:
                continue
            _low, high = watermarks
            total += max(0, high - tp.offset)
        return total

    def protected_write(self, topic: str, write_fn) -> None:
        """Gates `write_fn` behind the circuit breaker and retries it with
        backoff, recording the outcome — the reusable wiring between
        shared.reliability's two primitives, factored out so it can be
        exercised directly in tests without a real Kafka broker. Mirrors
        kafka.go's protectedWrite."""
        if not self._breaker.allow():
            raise KafkaPublishError(f"circuit breaker open for kafka topic {topic}")

        try:
            retry_with_backoff(self._max_retries, self._retry_delay, write_fn)
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()

    def publish_anomaly(self, key: str, evt: AnomalyDetected) -> None:
        try:
            payload = evt.model_dump_json().encode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise ErrPermanent(f"marshal anomaly: {exc}") from exc

        headers: list[tuple[str, bytes]] = []
        tracing.inject_kafka(headers)
        self.protected_write(
            self._anomaly_topic,
            lambda: produce_and_wait(self._anomaly_producer, self._anomaly_topic, key, payload, headers),
        )

    def dead_letter(self, raw_payload: bytes, cause: Exception, stage: str, correlation_id: str) -> None:
        record = DeadLetterRecord(
            original_payload=raw_payload.decode("utf-8", errors="replace"),
            error=str(cause),
            error_type=ERROR_TYPE_TRANSIENT,
            service="anomaly-detector",
            processing_stage=stage,
            retry_count=0,
            timestamp=utc_now(),
            correlation_id=correlation_id,
            source_topic="telemetry.processed",
        )
        payload = record.model_dump_json().encode("utf-8")
        produce_and_wait(self._dlq_producer, self._dlq_topic, correlation_id, payload, headers=[])
