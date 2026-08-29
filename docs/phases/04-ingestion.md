# 4. Ingestion

[services/ingestion](../../services/ingestion/) bridges MQTT and Kafka:
`MQTT (manual ack) → validate → normalize → Kafka`, with a bounded worker
thread pool (`INGESTION_WORKER_POOL_SIZE`, default 50) reading off a bounded
queue (`INGESTION_QUEUE_CAPACITY`, default 10000) fed by the MQTT subscriber.
It never touches Postgres or InfluxDB directly — that's the stream
processor's job (see [Streaming](05-streaming.md)).

**How at-least-once delivery is actually implemented**, not just claimed:
the MQTT client uses a persistent session (`clean_session=False`) with
manual acking (`manual_ack=True`); a message is only acked after it's
durably handed to Kafka — either published to `telemetry.raw` /
`device.events`, or, on Kafka failure, successfully routed to `dead-letter`
instead. If *both* the primary topic and the dead-letter write fail (Kafka
is entirely unreachable), the message is deliberately left unacked.

This was verified live: with Kafka stopped, a telemetry event was published
over MQTT. Ingestion retried with exponential backoff, attempted
dead-letter, failed that too, and correctly left the message unacked instead
of dropping it. Kafka was brought back up, ingestion was restarted, and the
persistent MQTT session **redelivered the still-unacked message on
reconnect** — no data loss across a real Kafka outage plus a real process
restart. It arrived as **two duplicate copies** a fraction of a millisecond
apart, which is exactly why the architecture is built around idempotent
consumers rather than a promise that duplicates can't happen —
deduplication by `event_id` is the stream processor's job, not ingestion's.

Also verified live: malformed telemetry (invalid UUID) is validated at the
boundary and routed to `dead-letter` with the original payload, an
`error`/`error_type`/`processing_stage`, and a `correlation_id` — then
acked, since the failure is durably captured. A circuit breaker
(`CircuitBreaker` in [shared/reliability.py](../../shared/reliability.py),
states CLOSED/OPEN/HALF_OPEN, unit-tested including the half-open
trial-success and trial-failure transitions) wraps every Kafka write so a
sustained outage fails fast instead of retrying every message into a
struggling broker — shared by every service that writes to Kafka or
InfluxDB.

```bash
curl localhost:8081/live      # process liveness — never fails on a dependency
curl localhost:8081/ready     # false if MQTT is disconnected or the Kafka breaker is OPEN
curl localhost:8081/metrics   # Prometheus: messages_received/processed/failed_total, dlq_messages_total, mqtt_connections, processing_latency_seconds
```
