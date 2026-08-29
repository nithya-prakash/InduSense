"""Kafka consume/produce, the Python port of kafka.go.

Consumption is deliberately single-threaded per instance: poll() and
commit() are called sequentially from one loop so offset commits always
advance in the order messages were actually processed. Scaling beyond one
instance's throughput is done the standard Kafka way — more partitions,
more consumer-group instances — not more threads committing out of order
within one instance.

The publish path (to telemetry.processed) applies retry-with-backoff plus a
circuit breaker, matching the pattern used for InfluxDB writes in this
service (see influx.py) and for Kafka writes in ingestion — a broker outage
should fail fast via the breaker rather than retry every message
individually and pile up latency. dedup/dead-letter and the consumer-side
poll/commit are deliberately left unwrapped: a bad message failing
validation isn't a "dependency down" situation the breaker is for, and if
Kafka is down entirely there's nothing more to do but leave the source
message uncommitted for redelivery once it recovers.
"""

from __future__ import annotations

from confluent_kafka import Consumer, KafkaException, Message, Producer

from config import Config
from shared import tracing
from shared.events import ERROR_TYPE_TRANSIENT, DeadLetterRecord, NormalizedTelemetryEvent, utc_now
from shared.reliability import CircuitBreaker, ErrPermanent, retry_with_backoff


class KafkaPublishError(Exception):
    pass


def _new_producer(brokers: list[str]) -> Producer:
    return Producer({"bootstrap.servers": ",".join(brokers), "acks": "all", "linger.ms": 50})


def produce_and_wait(producer: Producer, topic: str, key: str, value: bytes, headers: list[tuple[str, bytes]], timeout: float = 10.0) -> None:
    """Publishes one message and blocks until the broker has actually
    acknowledged it (or the attempt has definitively failed) — the Python
    equivalent of kafka-go's WriteMessages, which is synchronous by design.
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
        self._consumer.subscribe([cfg.topic_telemetry_raw])

        self._processed_producer = _new_producer(cfg.kafka_brokers)
        self._dlq_producer = _new_producer(cfg.kafka_brokers)
        self._processed_topic = cfg.topic_processed
        self._dlq_topic = cfg.topic_dead_letter

        self._breaker = CircuitBreaker(cfg.breaker_failure_threshold, cfg.breaker_cooldown_seconds)
        self._max_retries = cfg.kafka_max_retries
        self._retry_delay = cfg.kafka_retry_base_delay_seconds

    def close(self) -> None:
        self._consumer.close()
        self._processed_producer.flush(5.0)
        self._dlq_producer.flush(5.0)

    def breaker_state(self) -> str:
        return self._breaker.state()

    def poll(self, timeout: float) -> Message | None:
        """Returns the next message, or None on timeout/no-op events (an
        empty poll is expected and routine — the caller's loop just tries
        again)."""
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise KafkaException(msg.error())
        return msg

    def commit(self, msg: Message) -> None:
        self._consumer.commit(message=msg, asynchronous=False)

    def lag(self) -> int:
        """Sums (high watermark - current position) across every assigned
        partition. confluent-kafka has no single self-reported "lag" stat
        like segmentio/kafka-go's reader.Stats().Lag, so this queries the
        broker directly — arguably a more accurate number, at the cost of
        one extra round trip per partition every reporting tick (this is a
        5s-interval gauge, not a hot path)."""
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
        exercised directly in tests without a real Kafka broker (see
        tests/test_kafka_io.py). Mirrors kafka.go's protectedWrite."""
        if not self._breaker.allow():
            raise KafkaPublishError(f"circuit breaker open for kafka topic {topic}")

        try:
            retry_with_backoff(self._max_retries, self._retry_delay, write_fn)
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()

    def publish_processed(self, key: str, evt: NormalizedTelemetryEvent) -> None:
        try:
            payload = evt.model_dump_json().encode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise ErrPermanent(f"marshal processed telemetry: {exc}") from exc

        headers: list[tuple[str, bytes]] = []
        tracing.inject_kafka(headers)
        self.protected_write(
            self._processed_topic,
            lambda: produce_and_wait(self._processed_producer, self._processed_topic, key, payload, headers),
        )

    def dead_letter(self, raw_payload: bytes, cause: Exception, stage: str, correlation_id: str) -> None:
        record = DeadLetterRecord(
            original_payload=raw_payload.decode("utf-8", errors="replace"),
            error=str(cause),
            error_type=ERROR_TYPE_TRANSIENT,
            service="stream-processor",
            processing_stage=stage,
            retry_count=0,
            timestamp=utc_now(),
            correlation_id=correlation_id,
            source_topic="telemetry.raw",
        )
        payload = record.model_dump_json().encode("utf-8")
        produce_and_wait(self._dlq_producer, self._dlq_topic, correlation_id, payload, headers=[])
