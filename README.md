# InduSense — Industrial IoT Monitoring Platform

InduSense is a production-grade, event-driven monitoring platform for a
(fictional) German manufacturing company operating multiple factories. It
ingests real-time telemetry from thousands of machine sensors, detects
abnormal behavior, raises alerts, and manages incidents — end to end, through
real MQTT, Kafka, PostgreSQL, and InfluxDB, not mocked stand-ins.

> **Status: Phase 2 of 18 (Domain) complete.** This README will be
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

Not yet built: sensor simulator, ingestion service, stream processor,
anomaly detection, alerting, incidents, auth/RBAC enforcement, REST/WS API,
frontend, observability stack, Kubernetes manifests, CI/CD, and most testing.
These land in Phases 3–18.

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
