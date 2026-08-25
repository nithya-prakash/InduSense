# InduSense — Industrial IoT Monitoring Platform

InduSense is a production-grade, event-driven monitoring platform for a
(fictional) German manufacturing company operating multiple factories. It
ingests real-time telemetry from thousands of machine sensors, detects
abnormal behavior, raises alerts, and manages incidents — end to end, through
real MQTT, Kafka, PostgreSQL, and InfluxDB, not mocked stand-ins.

> **Status: Phase 5 of 18 (Streaming) complete.** This README will be
> expanded as each phase lands. See [docs/](docs/) for architecture, ADRs,
> and reliability notes as they are written.

## Why this project exists

This is a portfolio project built to demonstrate real competence in
distributed systems, event-driven architecture, and applied backend
engineering — the kind of system design a Backend/Distributed Systems
Engineer role at a German industrial company would actually require. It
prioritizes:

- **correctness over feature count**
- **working integrations over buzzwords**
- **measured performance over invented benchmarks**

Nothing here is faked. If a capability isn't implemented or a number hasn't
been measured, it's marked `NOT IMPLEMENTED` or `NOT YET MEASURED` rather than
claimed.

## Architecture (target — being built incrementally)

```text
Sensor Simulator (1000+ sensors)
        │ MQTT
        ▼
Eclipse Mosquitto
        │
        ▼
Ingestion Service (MQTT → Kafka)
        │
        ▼
Apache Kafka  ── telemetry.raw / telemetry.processed / anomalies.detected /
                 alerts / incidents / device.events / audit.events / dead-letter
        │
   ┌────┼─────────────┐
   ▼    ▼             ▼
Stream  Anomaly     Alert
Proc.   Detection   Service
   │       │           │
   ▼       ▼           ▼
InfluxDB  Kafka   Notifications
             │
             ▼
        PostgreSQL
             │
             ▼
        FastAPI-style REST/WebSocket API (Go)
             │
             ▼
        Next.js Dashboard
```

## Why Go

Go was chosen for the backend services because this project is meant to
demonstrate concurrency, high-throughput networking, and efficient,
cloud-native deployment — goroutines and channels map directly onto the
worker-pool / bounded-queue patterns needed for MQTT ingestion and Kafka
consumption, and static binaries make container images small and startup
fast. See `docs/ADR-004-why-go.md` (added once ADRs are written in a later
phase).

## Current status (Phase 1 — Foundation)

Infrastructure is defined in [docker-compose.yml](docker-compose.yml) and has
been started and functionally verified (not just container-healthy):

| Component  | Verified |
|------------|----------|
| PostgreSQL 16 | ✅ query executed (`SELECT version()`) |
| Redis 7 | ✅ `SET`/`GET` round trip |
| Eclipse Mosquitto 2.0 (MQTT) | ✅ pub/sub round trip on a test topic |
| Apache Kafka 3.7 (KRaft, single node) | ✅ all 8 topics created; produce/consume round trip on `telemetry.raw` |
| InfluxDB 2.7 | ✅ `/health` returned `ready for queries and writes` |
| Kafka UI | ✅ running at http://localhost:8089 |

Kafka topics created (see [infrastructure/docker/kafka-init-topics.sh](infrastructure/docker/kafka-init-topics.sh)):
`telemetry.raw` (12 partitions), `telemetry.processed` (12), `anomalies.detected` (6),
`alerts` (6), `incidents` (3), `device.events` (3), `audit.events` (3), `dead-letter` (3).

Telemetry topics are partitioned by `device_id` (to be enforced by the
ingestion service in Phase 4) so that events for a given device are strictly
ordered, while different devices process in parallel across partitions.

## Current status (Phase 2 — Domain)

19 PostgreSQL tables created via 8 [golang-migrate](https://github.com/golang-migrate/migrate)
migrations in [migrations/](migrations/): the factory hierarchy
(`organizations` → `factories` → `production_lines` → `machines` → `devices`
→ `sensors`), auth/RBAC scaffolding (`users`, `roles`, `permissions`,
`role_permissions`, `user_roles` — populated in Phase 9), `device_credentials`,
`alert_rules`/`alerts`, `incidents`/`incident_events`, `maintenance_records`,
`audit_logs`, and `idempotency_keys`. 60 indexes/constraints total, including
partial-unique indexes enforcing "one active credential per device" and "one
active incident per machine" at the database level.

[scripts/seed](scripts/seed/main.go) seeds a realistic hierarchy — one
organization ("Musterfabrik GmbH") across 4 German factories (Berlin,
Dresden, Munich, Hamburg), each with 5 production lines of 10 machines drawn
from 5 realistic industrial machine profiles (CNC mill, hydraulic press,
conveyor belt, welding robot, air compressor), each machine with one
provisioned device and 5 sensors — **1000 sensors total**, verified by
running the seed against live Postgres. The seed is idempotent (checks for
an existing organization slug) and transactional (a mid-seed failure leaves
zero partial rows, verified by injecting a duplicate-key failure). Device
credentials are bcrypt-hashed before storage — the plaintext secret exists
only transiently in memory during provisioning.

```bash
make migrate-up    # apply migrations
make seed          # seed the factory hierarchy
make unit-test     # go test ./...
```

## Current status (Phase 3 — Sensor Simulation)

[simulator/](simulator/) is a standalone Go binary that loads the 1000 seeded
sensors from Postgres and publishes realistic telemetry over real MQTT
(Eclipse Mosquitto) — verified with a live broker, not mocked. Each of the
1000 sensors runs as its own goroutine with a per-sensor baseline that drifts
slowly within its operating range (bounded random walk with reversion toward
the midpoint) plus gaussian noise; 200 machine-controller goroutines
independently drive RUNNING/STOPPED transitions per device.

Configurable fault injection (`ANOMALY_RATE`, `DUPLICATE_RATE`,
`OUT_OF_ORDER_RATE`, `NETWORK_DELAY_RATE`, `SENSOR_FAILURE_RATE` — all in
[.env.example](.env.example)) was verified to produce rates matching
configuration (e.g. at 5% each: duplicates landed at ~5.0%, out-of-order at
~5.2%, anomalies at ~4.7%, over a live 10-second run). A bounded channel
(`SIM_QUEUE_CAPACITY`, default 20000) between sensor goroutines and a fixed
pool of MQTT publisher workers provides backpressure — if publishing can't
keep up, new samples are dropped (counted, not silently lost, and never
causing unbounded memory growth) rather than queued indefinitely. Graceful
shutdown on `SIGINT`/`SIGTERM` was verified to drain in-flight sensors and
disconnect cleanly.

Published topics:
- `factory/{factory_id}/machine/{machine_id}/sensor/{sensor_id}/telemetry` — the telemetry event itself
- `factory/{factory_id}/machine/{machine_id}/status` — RUNNING/STOPPED transitions
- `factory/{factory_id}/machine/{machine_id}/events` — `SENSOR_FAILURE`, `SENSOR_RECOVERED`, `MACHINE_STOPPED`, `MACHINE_RUNNING`

```bash
make simulate          # run natively against localhost infra (Ctrl+C to stop gracefully)
make simulate-docker   # run as a container on the compose network (profile: simulate)
```

## Current status (Phase 4 — Ingestion)

[services/ingestion](services/ingestion/) bridges MQTT and Kafka:
`MQTT (manual ack) → validate → normalize → Kafka`, with a bounded worker
pool (`INGESTION_WORKER_POOL_SIZE`, default 50) reading off a bounded queue
(`INGESTION_QUEUE_CAPACITY`, default 10000) fed by the MQTT subscriber. It
never touches Postgres or InfluxDB directly — that's the stream processor's
job (Phase 5).

**How at-least-once delivery is actually implemented here**, not just
claimed: the MQTT client uses `CleanSession(false)` (persistent session) and
disables auto-ack (`SetAutoAckDisabled(true)`); a message is only acked
after it's durably handed to Kafka — either published to `telemetry.raw` /
`device.events`, or, on Kafka failure, successfully routed to `dead-letter`
instead. If *both* the primary topic and the dead-letter write fail (Kafka
is entirely unreachable), the message is deliberately left unacked.

This was verified live, not just reasoned about: with Kafka stopped, a
telemetry event was published over MQTT. Ingestion retried with exponential
backoff (1s/2s/4s/8s), attempted dead-letter, failed that too (Kafka down
means both paths are down), and correctly left the message unacked instead
of dropping it. Kafka was brought back up, ingestion was restarted (killed
and relaunched — the "consumer crashes and restarts" scenario), and the
persistent MQTT session **redelivered the still-unacked message on
reconnect** — no data loss across a real Kafka outage plus a real process
restart. It arrived as **two duplicate copies** a fraction of a millisecond
apart (visible in Kafka by `ingested_at`), which is exactly why the
architecture is built around idempotent consumers rather than a promise that
duplicates can't happen — deduplication by `event_id` is Phase 5's job, not
ingestion's.

Also verified live: malformed telemetry (invalid UUID) is validated at the
boundary and routed to `dead-letter` with the original payload, an
`error`/`error_type`/`processing_stage`, and a `correlation_id` — then
acked, since the failure is durably captured. A circuit breaker
(`CircuitBreaker` in [breaker.go](services/ingestion/breaker.go), states
CLOSED/OPEN/HALF_OPEN, unit-tested including the half-open trial-success and
trial-failure transitions) wraps every Kafka write so a sustained outage
fails fast instead of retrying every message into a struggling broker.

```bash
curl localhost:8081/live      # process liveness — never fails on a dependency
curl localhost:8081/ready     # false if MQTT is disconnected or the Kafka breaker is OPEN
curl localhost:8081/metrics   # Prometheus: messages_received/processed/failed_total, dlq_messages_total, mqtt_connections, processing_latency_seconds
```

## Current status (Phase 5 — Streaming)

[services/stream-processor](services/stream-processor/) consumes
`telemetry.raw`, deduplicates by `event_id`, writes each reading to
InfluxDB, maintains rolling windowed aggregates, and republishes to
`telemetry.processed` for the anomaly detector (Phase 6):

```text
telemetry.raw → dedup (Redis SETNX) → InfluxDB raw point → windowed aggregation → InfluxDB agg point → telemetry.processed
```

**Deduplication** claims each `event_id` in Redis (`SETNX` + TTL) *before*
processing; a duplicate is skipped without repeating the InfluxDB write or
aggregate update, but its offset is still committed. This is the mechanism
that would have caught the literal duplicate produced live in Phase 4 (the
MQTT-redelivery duplicate from the Kafka-outage test) — verified again here
with a fresh run: `duplicate_events_total` incremented for real duplicates
generated by the simulator's `DUPLICATE_RATE`.

**Windowed aggregation** (`window.go`) keeps a per-`(device_id, metric)`
ring buffer trimmed to the longest configured window (15m) and computes
moving average, moving standard deviation, min, max, and rate of change
(the same computation covers "vibration trend" and "energy consumption
rate" from the spec — those are just `rate_of_change` applied to the
vibration/power metrics, not separate calculations). A background ticker
flushes aggregates for all five windows (10s/30s/1m/5m/15m) as one batched
InfluxDB write every 10s. Verified live: with real telemetry flowing,
`sensor_telemetry_agg` points appeared in InfluxDB with correct
`moving_avg`/`min`/`max`/`count` values, tagged by window.

**Consumption is deliberately single-goroutine per instance** — `FetchMessage`
and `CommitMessages` run sequentially so offset commits always advance in
the order messages were actually processed (a pool of goroutines committing
concurrently could commit a higher offset before a lower one finishes,
silently skipping that message on a crash). Scaling beyond one instance's
throughput is the standard Kafka answer — more partitions, more
consumer-group members — not more goroutines racing to commit.

**InfluxDB writes are naturally idempotent**: the same `(measurement, tags,
timestamp)` overwrites rather than duplicates, so even if Redis dedup were
bypassed, telemetry records themselves can't become duplicated — Redis
dedup exists to avoid wasted work and to protect the aggregates/republish
step from double-counting, not because the storage layer would otherwise
corrupt itself. This is explicitly documented rather than assumed, per the
project's at-least-once-plus-idempotency stance (see the note at the bottom
of this README).

Retries with backoff and a circuit breaker (shared `pkg/reliability`, also
used by ingestion) wrap every InfluxDB write; on exhaustion the message is
routed to `dead-letter` (verified live: a deliberately malformed message on
`telemetry.raw` was caught, dead-lettered, and its offset committed).
Aggregate writes are best-effort (logged, not dead-lettered) since they're
recomputable observability data, not a primary business record — a
scope decision documented in code rather than left implicit.

```bash
curl localhost:8082/ready     # false if Redis is unreachable or the InfluxDB breaker is OPEN
curl localhost:8082/metrics   # messages_consumed/failed_total, duplicate_events_total, kafka_consumer_lag, windowed_aggregates_written_total
```

Not yet built: anomaly detection, alerting, incidents, auth/RBAC
enforcement, REST/WS API, frontend, Grafana/Jaeger, Kubernetes manifests,
CI/CD, and most formal testing (tests so far are unit tests for pure logic
in the simulator, ingestion, and stream-processor services — the live
MQTT/Kafka/Redis/InfluxDB verification above was done manually against real
infra, not yet captured as a permanent automated test; that lands in
Phase 13). These land in Phases 6–18.

## Local setup

```bash
git clone <repo>
cd indusense
make setup   # copies .env.example -> .env
make up      # starts Postgres, Redis, Mosquitto, Kafka (+topics), Kafka UI, InfluxDB
make ps      # check container health
make down    # stop everything (data volumes preserved)
```

Kafka UI: http://localhost:8089
InfluxDB UI: http://localhost:8086

## Repository structure

```text
indusense/
├── services/          # api, ingestion, stream-processor, anomaly-detector, alert-service (Go)
├── frontend/          # Next.js + TypeScript dashboard
├── simulator/         # sensor simulator
├── infrastructure/    # docker, kubernetes, helm, prometheus, grafana, jaeger configs
├── migrations/        # PostgreSQL schema migrations
├── tests/             # integration, contract, e2e
├── load-tests/        # k6 scripts
├── scripts/           # dev/ops scripts
├── docs/              # architecture, ADRs, reliability, performance docs
└── docker-compose.yml
```

## Delivery semantics (a note up front)

This system is designed around **at-least-once delivery + idempotent
consumers + deduplication** — not exactly-once semantics across the whole
distributed pipeline. This is a deliberate, documented tradeoff, not an
oversight; see `docs/ADR-005` (added in a later phase) for the reasoning.
