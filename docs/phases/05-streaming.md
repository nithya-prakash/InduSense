# 5. Streaming

[services/stream-processor](../../services/stream-processor/) consumes
`telemetry.raw`, deduplicates by `event_id`, writes each reading to
InfluxDB, maintains rolling windowed aggregates, and republishes to
`telemetry.processed` for the anomaly detector:

```text
telemetry.raw → dedup (Redis SETNX) → InfluxDB raw point → windowed aggregation → InfluxDB agg point → telemetry.processed
```

**Deduplication** claims each `event_id` in Redis (`SETNX` + TTL) *before*
processing; a duplicate is skipped without repeating the InfluxDB write or
aggregate update, but its offset is still committed. This is the mechanism
that catches redelivery duplicates like the one produced live during
[Ingestion](04-ingestion.md)'s own Kafka-outage test.

**Windowed aggregation** ([window.py](../../services/stream-processor/window.py))
keeps a per-`(device_id, metric)` ring buffer trimmed to the longest
configured window (15m) and computes moving average, moving standard
deviation, min, max, and rate of change. A background ticker flushes
aggregates for all five windows (10s/30s/1m/5m/15m) as one batched InfluxDB
write every 10s. Verified live: with real telemetry flowing,
`sensor_telemetry_agg` points appeared in InfluxDB with correct
`moving_avg`/`min`/`max`/`count` values, tagged by window.

**Ordered by event time, not arrival order.** The ring buffer is kept
sorted by each reading's own `timestamp` field regardless of the order
messages are actually consumed in — an early bug had it simply appending on
arrival, which corrupted both the trim logic and rate-of-change's
"first vs. last sample" reasoning whenever out-of-order delivery (simulated
network delay, or ordinary Kafka redelivery) caused a reading to arrive
later than one with an earlier timestamp. Insertion is O(distance from the
correct position) rather than O(1) append — the right trade for a bounded
ring buffer where out-of-order arrivals are the exception, not the norm.
Regression-tested directly (see [Testing](13-testing.md)).

**Consumption is deliberately single-threaded per instance** — reads and
offset commits happen sequentially so commits always advance in the order
messages were actually processed. Scaling beyond one instance's throughput
is the standard Kafka answer — more partitions, more consumer-group
members — not more concurrent workers racing to commit.

**InfluxDB writes are naturally idempotent**: the same `(measurement, tags,
timestamp)` overwrites rather than duplicates, so even if Redis dedup were
bypassed, telemetry records can't become duplicated — Redis dedup exists to
avoid wasted work and to protect the aggregates/republish step from
double-counting, not because the storage layer would otherwise corrupt
itself.

Retries with backoff and a circuit breaker (shared
[reliability.py](../../shared/reliability.py)) wrap every InfluxDB write; on
exhaustion the message is routed to `dead-letter`. Aggregate writes are
best-effort (logged, not dead-lettered) since they're recomputable
observability data, not a primary business record. `dead-letter` writes
themselves are deliberately left unprotected: if Kafka is down entirely,
there's nothing more to do but leave the source message uncommitted for
redelivery once it recovers. `kafka_circuit_breaker` and
`influxdb_circuit_breaker` are both reported by `/ready`.

**Telemetry retention: 30 days (720h) in the reference deployment**, set via
`INFLUXDB_RETENTION` at first-time InfluxDB setup. This only applies when
the `influxdb-data` volume is created fresh — InfluxDB does not
retroactively apply it to an already-existing bucket. To change retention on
a running deployment:

```bash
docker exec indusense-influxdb influx bucket list --org indusense --token "$INFLUXDB_TOKEN"
docker exec indusense-influxdb influx bucket update \
  --id <telemetry-bucket-id> --token "$INFLUXDB_TOKEN" --retention 720h
```

Retention is InfluxDB-only; PostgreSQL retains its own data (devices, users,
incidents, audit log, etc.) indefinitely.

```bash
curl localhost:8082/ready     # false if Redis is unreachable or the InfluxDB breaker is OPEN
curl localhost:8082/metrics   # messages_consumed/failed_total, duplicate_events_total, kafka_consumer_lag, windowed_aggregates_written_total
```
