# InduSense — Industrial IoT Monitoring Platform

InduSense is a production-grade, event-driven monitoring platform for a
(fictional) German manufacturing company operating multiple factories. It
ingests real-time telemetry from thousands of machine sensors, detects
abnormal behavior, raises alerts, and manages incidents — end to end, through
real MQTT, Kafka, PostgreSQL, and InfluxDB, not mocked stand-ins.

> **Status: Phase 14 of 18 (Kubernetes + Helm) complete.** This README will be
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

## Current status (Phase 6 — Anomaly Detection)

[services/anomaly-detector](services/anomaly-detector/) consumes
`telemetry.processed` and runs three independent detection levels on every
reading, publishing a combined result to `anomalies.detected` when any
fire. Full design writeup, including an honest evaluation of the Isolation
Forest (what was measured vs. not), is in
[docs/ANOMALY-DETECTION.md](docs/ANOMALY-DETECTION.md).

All three levels were verified firing on live data in a single test run:

```text
anomalies_detected_total{method="RULE"}              122
anomalies_detected_total{method="STATISTICAL"}          8
anomalies_detected_total{method="ISOLATION_FOREST"}     1
```

- **Rule-based**: value outside the sensor's seeded operating range, severity
  scaled by overshoot fraction.
- **Statistical**: EWMA rolling mean/stddev per `(device_id, metric)`,
  z-score against the pre-sample baseline, suppressed until 30+ samples.
- **Isolation Forest**: a real implementation (not a wrapped library) of
  Liu, Ting & Zhou (2008) — verified with a synthetic-data evaluation
  showing normal points scoring 0.45 vs. outliers scoring 0.68 on average
  (both correctly on their expected side of the 0.6 anomaly threshold). One
  forest is trained **per machine type**, not globally, because different
  machine types report different metric sets — a CNC mill's feature vector
  isn't the same shape or semantics as a hydraulic press's, so a shared
  forest would compare incomparable dimensions. All 5 machine-type forests
  trained successfully from live data in the deployment's first retrain
  cycle (`ANOMALY_FOREST_RETRAIN_SECONDS`, default 120s).

```bash
curl localhost:8083/forests    # which machine types currently have a trained forest
curl localhost:8083/metrics    # anomalies_detected_total{method}, isolation_forests_trained_total
```

## Current status (Phase 7 — Alerting)

[services/alert-service](services/alert-service/) consumes
`anomalies.detected` and `device.events`, matches events against
organization-configured `alert_rules` (Postgres), and creates alerts with
deduplication, cooldown, and escalation — publishing to the `alerts` Kafka
topic and notifying via console (always) and an optional webhook.

**Rule matching** (`conditions.go`) supports the four condition types from
the schema: `GREATER_THAN`/`LESS_THAN`/`OUTSIDE_RANGE` against the anomaly's
raw value, and `ANOMALY_COUNT` — "N anomalies within a window" — backed by
an in-memory per-`(rule, device)` timestamp tracker
(`anomalycount.go`), trimmed to the rule's configured window on every
insert. Four representative rules ship in the seed data (matching the
spec's own examples): a hard temperature threshold (90°C, CRITICAL), a
vibration threshold (HIGH), a power-spike threshold (WARNING), and "three
temperature anomalies within five minutes" (HIGH) — plus a sentinel rule
used only by the direct machine-shutdown handler.

**Deduplication + cooldown** (`store.go`): a new alert is refused if one for
the same `(rule, device, metric)` is still open (an `INSERT ... ON CONFLICT
... WHERE status = 'OPEN' DO NOTHING` matching the partial unique index from
Phase 2, so this holds even under concurrent processing, not just via an
application-level check) — or if the last one for that key resolved more
recently than the rule's `cooldown_seconds`, which is what stops a flapping
condition from re-paging immediately after resolution. Verified live: of
109 qualifying anomalies processed in one run, 65 became new alerts and 25
were correctly suppressed as duplicates of an already-open alert, with zero
duplicate `OPEN` rows ever present in Postgres (checked directly).

**Escalation**: a periodic sweep bumps any `OPEN` alert unacknowledged past
`ALERT_ESCALATION_AFTER_SECONDS` (default 900s) one rung up the
WARNING→HIGH→CRITICAL ladder and re-notifies — verified live with a
shortened interval, including catching and fixing a real bug in the process
(a Postgres parameter-encoding failure in the escalation query's interval
arithmetic, fixed by switching to `make_interval()`).

**Machine shutdown** is a second, independent alert path: `device.events`
messages with `event_type = MACHINE_STOPPED` go straight to alert creation
(bypassing rule matching, since it's not a metric threshold), verified live
end-to-end from a synthetic MQTT publish through ingestion validation to a
real Postgres alert row and console notification.

**Notification providers** (`notify.go`): `ConsoleProvider` (always on,
zero external dependencies) and `WebhookProvider` (retry + circuit breaker
via `pkg/reliability`, optional). A webhook failure is logged and counted,
never dead-lettered or retried against the message pipeline — the alert
itself is already durably recorded in Postgres by the time notification is
attempted, so a webhook outage means a missed notification, not lost alert
data. Email/Slack are intentionally not implemented (no paid service
credentials available in this environment) — the `NotificationProvider`
interface exists specifically so they can be added later without touching
the alert engine.

```bash
curl localhost:8084/metrics   # alerts_generated_total{severity}, alerts_suppressed_total{reason}, alerts_escalated_total, notifications_sent_total{provider}
```

## Current status (Phase 8 — Incidents)

Incident lifecycle management lives inside `alert-service`
([incidents.go](services/alert-service/incidents.go)) rather than as a
separate "incident-service" — incidents originate 1:1 from alerts and there
was no independent workflow to justify a new microservice yet (the spec's
own service list doesn't name one either). The manual lifecycle actions
(acknowledge, assign, resolve, close) are implemented and fully tested now
so Phase 10's REST API can call them directly once there's a human operator
to invoke them — this phase proves the state machine and persistence are
correct ahead of the API surface that will expose them.

**"Don't create unlimited incidents from repeated alerts"** is enforced at
the database level, not just in application code: `openOrAttach` reuses any
incident already active for a machine (`INSERT ... ON CONFLICT (machine_id)
WHERE status IN (...) DO NOTHING`, racing safely against the same partial
unique index from Phase 2) and logs a fresh alert as an `ALERT_ATTACHED`
audit event instead of opening a second incident. Verified live: 7 real
alerts from a fresh traffic burst produced exactly 6 incidents, with 1
correctly attached rather than duplicated — confirmed directly in Postgres
with zero machines ever holding more than one active incident.

**State machine** (`OPEN → ACKNOWLEDGED/INVESTIGATING/RESOLVED → CLOSED`,
with `RESOLVED → INVESTIGATING` allowed for a recurrence but `CLOSED`
terminal) is unit-tested for every valid and invalid transition, then
exercised end-to-end against a real Postgres instance in
[incidents_live_test.go](services/alert-service/incidents_live_test.go) —
open, attach, acknowledge, assign, investigate, resolve, reopen, re-resolve,
close, and verifying a post-closure alert opens a genuinely new incident.
That test caught two real foreign-key constraints the hard way (an
`incidents.alert_id` and `assigned_to` must reference *real* `alerts`/
`users` rows, not placeholder strings) — the schema's referential integrity
doing exactly its job.

## Current status (Phase 9 — Authentication)

[pkg/auth](pkg/auth/) implements password hashing, JWT issuance/validation,
RBAC, and multi-tenancy guards as domain logic with no HTTP dependency —
same pattern as Phases 7–8: build and live-verify the logic before the
REST API (Phase 10) exists to expose it. [pkg/audit](pkg/audit/) is a small
shared writer for the `audit_logs` table, used by `pkg/auth` today and
available to any service later.

**RBAC is claims-based, not a database lookup per request**: `Login`
resolves a user's roles to their full permission set (`RolePermissions` in
[rbac.go](pkg/auth/rbac.go)) and embeds it directly in the JWT, so
authorizing a request only requires validating the token, not a
`role_permissions` join on every call. That same map is also the single
source of truth the seed script uses to populate `role_permissions` in
Postgres — verified live to match exactly: ADMIN 11 permissions,
FACTORY_MANAGER 9, ENGINEER 8, TECHNICIAN 6, VIEWER 5.

**The full auth flow was verified against real Postgres and Redis**, not
mocked, in [service_live_test.go](pkg/auth/service_live_test.go): login
with the real seeded admin user, wrong-password and unknown-email
rejection, refresh-token **rotation** (each refresh both invalidates the
presented token and issues a new one), reuse-of-a-rotated-out-token
rejection, and logout revocation — all checked against Redis state, not
just the code path. The audit trail was checked directly in Postgres:
successful logins, failed logins, and logouts all produced real
`audit_logs` rows.

**Multi-tenancy was verified with two real organizations**, not asserted
against one: the seed script now creates a second organization ("Zweite
Firma GmbH") with its own factory and admin user specifically so isolation
has something concrete to fail against. `TestMultiTenantLoginsAreIsolated`
confirms both organizations' tokens carry distinct `organization_id`
claims, that `RequireSameOrganization` rejects org A's token against org
B's resource, and — as a data-layer ground truth check — that no factory
row's `organization_id` can appear under a different organization's join.

Access tokens are **not** individually revocable (the standard stateless-
JWT tradeoff), mitigated by a short TTL (`JWT_ACCESS_TTL_MINUTES`, default
15) rather than pretending server-side revocation of a bearer token is
free; refresh tokens are revocable because they're tracked in Redis by
design.

## Current status (Phase 10 — APIs)

[services/api](services/api/) is the REST + WebSocket surface: `pkg/auth`
and `pkg/incidents` wired into real HTTP middleware, not reimplemented.
Routing uses the standard library's `net/http.ServeMux` with Go 1.22's
method+path patterns — a deliberate choice over Gin/Fiber, since this API's
needs (path params, method routing) are fully covered by the stdlib now,
and adding a router dependency wouldn't have bought anything.

**Every handler scopes its Postgres/InfluxDB queries by the JWT's
`organization_id`, never by anything the client supplies.** This was
verified live, not just written: an org B user listing factories saw only
their own Stuttgart plant, never org A's four; fetching org A's factory ID
directly by URL returned `404 NOT_FOUND` (not `403`) — correct behavior,
since confirming a cross-tenant resource *exists* is itself a leak. RBAC
was verified the same way: a VIEWER token got `403 FORBIDDEN` provisioning
a device, `200` reading factories.

**Real bugs were found and fixed during live verification** (all three
are the kind of thing that only shows up when you actually run the thing):
1. The Redis-backed rate limiter keyed on `r.RemoteAddr` including the
   port, which is different on every TCP connection — it silently never
   limited anything until fixed to strip the port.
2. `/ws/alerts` failed every upgrade with "response does not implement
   http.Hijacker" — the logging middleware's `ResponseWriter` wrapper
   needed to explicitly forward `Hijack()` to the underlying writer, a
   known gotcha when wrapping `http.ResponseWriter` in front of
   `gorilla/websocket`.
3. Unmatched routes returned Go's default plain-text 404 instead of the
   spec's consistent JSON error envelope — fixed with an explicit
   catch-all handler.

**The full incident lifecycle was verified through the actual HTTP API**:
`OPEN → ACKNOWLEDGED` succeeded (204), `ACKNOWLEDGED → CLOSED` was
correctly rejected (409, skipping straight past RESOLVED), and `GET
/incidents/{id}` returned the complete audit trail with both transitions.
Rate limiting was verified by actually hammering `/auth/login` 15 times:
exactly 10 succeeded, then real `429`s with `Retry-After: 60`.

**`/ws/alerts` streams real-time alerts scoped by organization** — verified
by connecting two WebSocket clients (org A and org B), publishing an org-A
alert event, and confirming org A's client received it while org B's
client received nothing at all after a full 20-second window. Browsers
can't set a custom `Authorization` header on a WebSocket handshake, so the
access token is accepted as a `?token=` query parameter on this one
endpoint specifically — documented as a simplification; a production
system would issue a short-lived, single-use ws ticket instead of reusing
a bearer token in a URL.

`GET /docs` serves a hand-written OpenAPI 3.0 spec (24 paths) through
Swagger UI — hand-written rather than reflection-generated, since a
generator would add a dependency without saving meaningful effort at this
API's size, and a hand-written spec can't accidentally document a field
that wasn't meant to be public.

```bash
curl -X POST localhost:8080/api/v1/auth/login -d '{"email":"admin@musterfabrik-gmbh.de","password":"ChangeMe123!"}'
curl localhost:8080/api/v1/factories -H "Authorization: Bearer <token>"
curl localhost:8080/docs   # Swagger UI
```

**Not implemented in this phase, honestly deferred rather than rushed**:
admin endpoints for browsing/retrying dead-letter queue messages (the DLQ
mechanism itself works — every service already writes to it — only the
admin API surface for browsing it doesn't exist yet); `/ws/incidents`
(incidents don't yet publish to their own Kafka topic — `pkg/incidents`
has a `Publisher` interface ready for this, just not wired to a writer);
device-count-aware pagination cursors (offset pagination only).

## Current status (Phase 11 — Dashboard)

[frontend/](frontend/) is a Next.js 16 + TypeScript app (App Router,
Turbopack, Tailwind CSS) covering every page the spec asks for: Overview,
Factory drill-down, Machine detail with live telemetry charts, Alerts,
Incidents, Devices, Administration.

**Client-rendered by design, not server components everywhere.** Every
data-fetching page is a Client Component that calls the REST API directly
with the browser-held JWT — a deliberate simplification over Next.js
Server Components/Server Actions, since this app's auth model (bearer
tokens in `localStorage`, refreshed by the client) doesn't map cleanly onto
per-request server-side token forwarding without meaningfully more
plumbing. Dynamic route pages (`/factories/[id]`, `/machines/[id]`,
`/incidents/[id]`) still follow Next 16's requirement that `params` be
awaited in an async Server Component — that outer shell just extracts the
id and hands it to a Client Component.

**RBAC drives the UI, not just the API** — verified live by logging in as
each of two different roles and confirming the DOM actually differs, not
just reading the code: as ADMIN, the Alerts page shows "Acknowledge"
buttons and the Incidents detail page shows an Actions panel; as VIEWER
(no `alerts:manage`/`incidents:manage`), those controls are simply absent
while the underlying data remains fully visible — the same
read-without-write split enforced server-side in Phase 10, now visible in
the browser.

**The full incident lifecycle was exercised through the actual UI**, not
just curled: clicking "Move to ACKNOWLEDGED" updated the status badge,
removed the now-invalid action buttons (only `INVESTIGATING`/`RESOLVED`
remained, matching the state machine), and appended a real audit-history
entry with a live timestamp — all sourced from the same Postgres rows
Phase 10's tests already verified.

**`/ws/alerts` drives a live "● live" indicator and the Alerts table** —
acknowledging an alert updates its badge immediately via the REST call's
response, and the WebSocket connection is what the indicator reflects
(verified by watching it read "○ connecting..." then "● live" as the
socket came up).

A real bug surfaced immediately by actually loading the page in a browser
(not by reading the code): the scaffold's default dark-mode CSS block used
un-layered rules that silently beat every Tailwind utility class regardless
of specificity, rendering the whole dashboard near-black. Removed in favor
of an explicitly light-only theme, since no dark theme was designed. A
second, more subtle one: `create-next-app`'s scaffold put the sidebar's
`h-screen` on a flex item without `sticky` positioning, which looked fine
in normal browser scrolling but showed a clipped, overlapping sidebar in a
full-page (beyond-viewport) screenshot — confirmed as a screenshot-capture
artifact, not a real user-facing bug, by resizing to a fixed viewport and
actually scrolling; fixed with `sticky top-0 self-start` regardless, since
it's the more correct pattern either way.

While building the Factory drill-down page, the frontend surfaced a real
API gap: there was no endpoint to list a production line's machines. Added
`GET /api/v1/production-lines/{id}/machines` to `services/api` (Phase 10)
rather than working around it in the UI — exactly the kind of thing you
only find by actually using what you built.

```bash
make up   # frontend included in the default stack
# http://localhost:3000 — demo login: admin@musterfabrik-gmbh.de / ChangeMe123!
```

**Not implemented in this phase**: user creation/role-assignment UI (no
backend endpoint exists yet — the Administration page says so explicitly
rather than faking one); a chart library beyond Recharts line charts (no
gauge/heatmap views); offline/optimistic UI for flaky connections.

Not yet built: Grafana/Jaeger, Kubernetes manifests, CI/CD, and
most formal testing (tests so far are unit tests for pure logic plus
live-Postgres/Redis integration tests for incidents and auth, plus this
phase's live browser verification — none of which is yet captured as a
permanent automated test; that lands in Phase 13). These land in
Phases 12–18.

## Current status (Phase 12 — Observability)

Four pillars, all live-verified against real running services rather than
asserted from reading the code:

**Prometheus** ([infrastructure/prometheus/prometheus.yml](infrastructure/prometheus/prometheus.yml))
scrapes all five Go services (`api`, `ingestion`, `stream-processor`,
`anomaly-detector`, `alert-service`) every 10s. Verified via Prometheus's own
`/api/v1/targets` showing all five as `up`. Two new gauges were added because
the dashboards needed data the existing metrics didn't expose:
`devices_by_status` (`services/api`, refreshed from Postgres every 15s) and
`incidents_open_by_severity` (`services/alert-service`, same pattern) — both
confirmed with real non-zero values (`devices_by_status{status="ACTIVE"} 169`,
`incidents_open_by_severity{severity="CRITICAL"} 3`) rather than just
structurally present. Kafka broker JMX metrics are explicitly **not
implemented** — no JMX exporter sidecar — and the scrape config says so.

**Grafana** ([infrastructure/grafana/](infrastructure/grafana/)) auto-provisions
a Prometheus + Jaeger datasource and four dashboards (Platform, Kafka, IoT,
Anomaly & Alerting) via YAML providers, confirmed present through Grafana's
own `/api/search`. All four were opened in a real browser and watched respond
to a real traffic burst generated with the simulator
(`SENSOR_COUNT=200 MESSAGES_PER_SECOND=200 ANOMALY_RATE=0.1`, 20s): API
throughput/latency/CPU/memory panels spiked and recovered in sync, Kafka
consumer lag rose then drained as anomaly-detector and stream-processor
caught up, and IoT/incident panels tracked the correct live counts — not
just non-empty charts, but charts whose shape matched a traffic pattern
I actually caused.

**Structured logging** (`pkg/logging`, a thin `log/slog` JSON handler) is
wired into the pipeline's key lifecycle events — telemetry/machine-event
ingested, dedup and validation failures, anomaly detected, alert created,
incident opened/attached, and every API request — across all five Go
services. Fields match the spec: `timestamp`, `service`, `level`, `message`,
plus contextual `event_id`/`device_id`/`organization_id` at each call site
and `trace_id` whenever the call happens inside a span. This is a
deliberately partial migration: the remaining ad-hoc `log.Printf` calls
(mostly startup/shutdown lines and background-refresher failures) were left
alone rather than converted wholesale, since they carry no per-event fields
worth structuring.

**OpenTelemetry tracing** (`pkg/tracing`) exports to the Jaeger container
added this phase, over OTLP/HTTP. Getting this right required one real fix:
`otlptracehttp.WithEndpointURL` takes the URL's path as-is and does *not*
default it to `/v1/traces`, so passing `OTEL_EXPORTER_OTLP_ENDPOINT` (an
OTel-standard base URL with no path) straight through 404'd against Jaeger's
receiver — switched to `WithEndpoint(host)`, which does apply the default
path, and confirmed the `traces export: 404` errors stopped appearing in the
logs. Trace context is propagated by hand across Kafka: a `kafka.Header`
carrier adapts `propagation.TextMapCarrier` so the standard W3C
`traceparent` gets injected on every publish (ingestion → telemetry.raw,
stream-processor → telemetry.processed, anomaly-detector →
anomalies.detected, alert-service → alerts) and extracted on every consume.
The `api` service gets per-request spans via `otelhttp` middleware instead,
since HTTP tracing is a standard, separate concern from the Kafka pipeline.

Verified live end-to-end, not just "the exporter didn't error": after a
simulator run, Jaeger's `/api/services` listed all five real services, and
querying `/api/traces` for a single trace ID found a genuine four-span chain
with correct parent/child relationships and monotonically increasing start
times — `ingestion.process_message` (root) → `stream_processor.process`
(child) → `anomaly_detector.detect` (child of that) →
`alert_service.process_anomaly` (child of that) — meaning a `traceparent`
header genuinely rode across three separate Kafka hops and was correctly
extracted each time, not just three isolated same-ID spans. The same trace
ID also showed up verbatim in that request's structured log line
(`anomaly-detector`'s "anomaly detected" record), confirming logs and traces
share an ID a reader could actually pivot between.

**Not implemented in this phase**: tracing on the frontend (no browser-side
OTel SDK); log shipping to any aggregator (Loki, ELK) — logs are stdout-only,
collected by `docker compose logs`; Prometheus alerting rules (Alertmanager)
— Grafana dashboards exist but no alert *rules* fire from Prometheus itself,
only from `alert-service`'s own domain logic; and most `log.Printf` call
sites outside the lifecycle events above remain unstructured, as noted.
These, along with Kubernetes manifests and CI/CD, land in Phases 14–18.

## Current status (Phase 13 — Testing)

[Makefile](Makefile)'s `unit-test`/`integration-test`/`contract-test`/`e2e-test`
targets were scaffolded back in Phase 1 pointing at `tests/integration/`,
`tests/contract/`, and `tests/e2e/` — this phase is what actually populates
them, on top of the unit and live-infra tests already written incrementally
in Phases 1–12 (`pkg/auth`, `pkg/incidents`, and per-service pure-logic
tests). All four tiers, run together, are what `make test` now covers.

**Contract tests** ([tests/contract/](tests/contract/)) lock the JSON wire
format of every event type in `pkg/events` — `NormalizedTelemetryEvent`,
`NormalizedMachineEvent`, `AnomalyDetected`, `AlertEvent`,
`DeadLetterRecord` — against a fixed expected payload, round-tripped in both
directions (struct→JSON and back). There's no schema registry or
consumer-driven-contract framework (Pact) in this stack, so this is a
narrower, honest substitute: it can't catch "the consumer expected a field
the producer stopped sending" across a deploy it doesn't have, but it does
turn "someone renamed `DeviceID` to `Device_Id` in the shared struct" from a
silent runtime failure in some *other* service into an immediate, local test
failure. Fast and infra-free by design — no skip logic needed.

**Integration tests** ([tests/integration/](tests/integration/)) hit the
real running `api` container over HTTP against real Postgres/Redis — not a
mocked handler test — using the demo data `scripts/seed` already puts there.
The centerpiece is `TestTenantIsolation_FactoriesScopedToOrganization`: it
logs in as both seeded organizations' admins for real and asserts
`zweite-firma-gmbh` never sees `musterfabrik-gmbh`'s factories (or vice
versa) — proving the "every query scopes by the JWT's `organization_id`"
claim from Phase 10 against live data, not by re-reading the query.
Alongside it: RBAC (`VIEWER` gets 403 on `POST /api/v1/devices` before the
handler body even runs; `ADMIN` reaches business validation instead),
login success/failure, unauthenticated/malformed-token rejection, and rate
limiting.

Writing the rate-limit test surfaced a real test-suite bug, not a product
bug: the login endpoint's limiter buckets by client IP
(`X-Forwarded-For`-aware), and every test in the package logged in from the
same test-runner IP — so a test that deliberately exhausts the limit (or
just two `make test` runs within the same 60s window) made *unrelated*
tests fail with 429 instead of the status they were actually checking.
Fixed by giving each test its own synthetic `X-Forwarded-For` (RFC 5737
TEST-NET-3, never a real address) — confirmed by running the full suite
twice back to back with no interference, where it previously failed on the
second run.

**End-to-end tests** ([tests/e2e/](tests/e2e/)) are the ones that actually
justify an event-driven architecture existing at all: they publish a real
MQTT message to the same Mosquitto broker ingestion subscribes to, and poll
the real API until the effect that message should have propagates all the
way through — nothing mocked, stubbed, or short-circuited anywhere in
between.

- `TestE2E_TelemetryRoundTrip` looks up a real seeded sensor from Postgres,
  publishes an in-range reading over MQTT, and polls
  `GET /api/v1/telemetry/latest` until that exact value comes back —
  proving MQTT → ingestion → Kafka → stream-processor → InfluxDB → API.
- `TestE2E_AnomalyTriggersAlert` publishes `temperature=150` (the seeded
  "High temperature" rule fires above 90) and polls `GET /api/v1/alerts`
  for the resulting `CRITICAL` alert — proving MQTT → ingestion → Kafka →
  anomaly-detector's rule check → alert-service's rule match → Postgres →
  API, the platform's actual reason for existing. Both passed in under a
  second end to end against the real stack — a genuine (if informal, not a
  load test) latency data point for Phase 15.

Getting the alert test to run reliably took a second, more interesting
fix: `zweite-firma-gmbh` (seeded purely for the tenant-isolation test
above) turned out to have no devices or sensors of its own — only a bare
factory/machine — so it couldn't be used to publish telemetry at all.
Rather than reuse a `musterfabrik-gmbh` device that might already be on
that rule's alert cooldown from earlier simulator runs (which would make
the test flaky depending on unrelated history), the test picks a device
dynamically via `NOT EXISTS (SELECT 1 FROM alerts WHERE device_id = ... AND
title = 'High temperature')` — always a device this specific rule has never
fired for yet, so there's no cooldown state to race against, and the query
is naturally self-renewing across repeated runs.

**Not implemented in this phase**: consumer-driven contract testing (Pact
or similar) — the contract tests here are schema-locking, not full
consumer-driven contracts; frontend tests (no Jest/Playwright suite yet);
`go tool cover` coverage numbers for `services/api`/`ingestion`/etc. remain
low (~3–30%) despite the new integration/E2E coverage, because those tests
exercise the already-running Docker container over the network — coverage
instrumentation only counts code paths executed inside the same test
binary process, so real, meaningful HTTP-level test coverage of the API
doesn't show up in that number at all. Worth stating plainly rather than
letting a low percentage look like an untested service, or citing test
counts as if they meant something they don't. Kubernetes manifests, CI/CD,
and load testing (Makefile's `load-test` target already names itself
Phase 15) land in the remaining phases.

## Current status (Phase 14 — Kubernetes + Helm)

[infrastructure/helm/indusense/](infrastructure/helm/indusense/) is a Helm
chart that deploys the entire stack — every StatefulSet, Deployment, and
one-time setup Job docker-compose.yml already defines — as a single
release, without changing a line of application code: every Go service's
`config.go` just reads environment variables, and doesn't know or care
whether Compose or Kubernetes set them.

**Verified against a real cluster, start to finish**, not `helm template`
alone: Docker Desktop's Kubernetes (kind-based, 1 node), a completely fresh
`helm install --set seed.enabled=true` converging to all 15 pods `Running`
in **66 seconds wall-clock**, real demo data landing in Postgres (2 orgs, 6
users, 200 devices, 1000 sensors — via the same `scripts/seed` binary,
rebuilt as a container image for the first time this phase), a real login
returning a genuine JWT through a `kubectl port-forward`'d API, and —
firing the in-cluster simulator Job — the full pipeline processing real
traffic: stream-processor consuming 1734 messages, anomaly-detector
detecting 131 anomalies and generating 26 real alerts (8 CRITICAL / 13 HIGH
/ 5 WARNING) with genuine reasoning text ("value 90.64 outside safe
operating range [20.00, 90.00]; z-score 3.80 exceeds threshold 3.00..."),
all visible through the actual API. Grafana came up with all 4 dashboards
provisioned from the same JSON as Compose, Prometheus showed all 5 Go
services `up`, and Jaeger recorded real traces from all 5 — proving Phase
12's observability stack works identically under Kubernetes DNS names with
zero code changes.

**Three real bugs were found and fixed by actually deploying, not by
reading the YAML:**

1. **Kafka wouldn't start: a headless-Service DNS chicken-and-egg.** Kafka's
   own KRaft controller-registration step needs to resolve its advertised
   name (`indusense-kafka:9093`) *during startup, before the broker is
   Ready* — but a headless Service excludes not-yet-Ready pods from DNS by
   default, so the broker could never resolve itself and crash-looped
   forever. Fixed with `publishNotReadyAddresses: true` on Kafka's Service,
   the standard fix for this exact problem in self-registering StatefulSet
   members (Kafka, Cassandra, etcd, ZooKeeper all hit it).
2. **Kafka's health probes always failed, even once healthy.**
   `kafka-broker-api-versions.sh` boots a fresh JVM on every invocation —
   routinely well over Kubernetes' 1-second default probe timeout. Fixed
   with an explicit `timeoutSeconds: 10`.
3. **LoadBalancer Services never got an external IP, and worse, blocked
   `helm uninstall`.** Docker Desktop's Kubernetes is kind-based with no
   cloud-provider-kind/MetalLB, so `api`/`frontend`'s LoadBalancer Services
   sat at `<pending>` forever — and their
   `service.kubernetes.io/load-balancer-cleanup` finalizer then blocked
   namespace deletion until manually patched off. Switched the default to
   `ClusterIP` + `kubectl port-forward`; a NodePort was the other option,
   but its 30000-32767 range can't reproduce port 8080, which the frontend
   image already has `NEXT_PUBLIC_API_BASE_URL=http://localhost:8080` baked
   into at build time (Next.js inlines `NEXT_PUBLIC_*` vars into the client
   bundle) — forcing a rebuild just for Kubernetes would have broken the
   one-image-both-substrates property this chart was designed around.

**A fourth issue was observed, not chart-level, and resolved itself:** on
the very first install (before the Kafka probe-timeout fix), Kafka's own
instability during startup left `stream-processor`'s Kafka reader
genuinely stuck — joined its consumer group, then never issued another
fetch, with `messages_consumed_total` stuck at 0 even though the topic had
over a thousand real messages waiting. A pod restart immediately fixed it.
Re-tested after the probe fix, across a completely fresh install with no
manual intervention: the pipeline processed traffic correctly on the first
try. The stuck reader was very likely a downstream symptom of Kafka's own
instability, not an independent bug — but a production hardening step
worth naming honestly: a liveness check tied to consumer progress (not
just process-alive) would catch this class of failure without a human
watching metrics.

**Migrations and topic creation as Helm hooks**
([templates/jobs-migrate.yaml](infrastructure/helm/indusense/templates/jobs-migrate.yaml),
[templates/jobs-kafka-topics.yaml](infrastructure/helm/indusense/templates/jobs-kafka-topics.yaml))
run `post-install,pre-upgrade` — deliberately not `pre-install`, which was
the first thing tried and immediately failed: a pre-install hook runs
*before* the release's own Postgres/Kafka StatefulSets exist, so the
migration Job's wait-for-postgres initContainer had nothing to poll.
Migrations get baked into a small custom image
([infrastructure/docker/migrate/Dockerfile](infrastructure/docker/migrate/Dockerfile))
rather than mounted from a ConfigMap — a 1MiB size limit and decoupling the
schema from the image that applies it are both real problems a ConfigMap
would have created. The Kafka topics script, at under 30 lines, doesn't
carry that argument and is mounted from a ConfigMap instead.

**Demo data seeding**
([templates/jobs-seed.yaml](infrastructure/helm/indusense/templates/jobs-seed.yaml))
reuses `scripts/seed` unmodified, containerized for the first time this
phase. It's a `post-install,post-upgrade` hook, off by default
(`seed.enabled=false`) — the first version was `post-install`-only, which
seemed like the more disciplined choice (why reseed an already-seeded
database on every upgrade?) until it became clear that also meant the
documented way to seed *after* the fact — `helm upgrade --set
seed.enabled=true` — silently did nothing, since post-install hooks never
fire on upgrades. `scripts/seed` was already fully idempotent from Phase
1, so the fix cost nothing: add `post-upgrade` too.

**Traffic generation runs inside the cluster**
([templates/jobs-simulator.yaml](infrastructure/helm/indusense/templates/jobs-simulator.yaml)),
unlike Compose's simulator profile, which runs from the host machine. This
isn't a shortcut — it's a real constraint: Kafka's advertised listener only
resolves inside the cluster (see bug #1's fix), so a client outside it
would get broker metadata pointing at a hostname it can't reach. The Job is
bounded with `activeDeadlineSeconds` rather than a duration flag, since the
simulator binary itself has none — it normally runs until SIGTERM, exactly
like the Compose version.

**Not implemented in this phase**: an ingress controller/Ingress resource
(written and gated behind `ingress.enabled=false`, since Docker Desktop's
Kubernetes has none installed by default — untested against a real one);
Kafka/Postgres clustering or multi-replica stateful components (every
StatefulSet stays at 1 replica, matching docker-compose's topology exactly
rather than pretending this chart does something it doesn't); Horizontal
Pod Autoscaling; NetworkPolicies. `api` runs 2 replicas behind its Service
as the one genuinely meaningful horizontal-scaling demonstration, since
it's the only stateless service worth the point. These, along with CI/CD
and load testing, land in the remaining phases.

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
API: http://localhost:8080 (Swagger docs at `/docs`)

Demo users (`make seed`), one per role, password `ChangeMe123!` for all —
**local development only, never used anywhere real credentials would be**:
`admin@musterfabrik-gmbh.de`, `factory_manager@musterfabrik-gmbh.de`,
`engineer@musterfabrik-gmbh.de`, `technician@musterfabrik-gmbh.de`,
`viewer@musterfabrik-gmbh.de`. A second organization
(`admin@zweite-firma-gmbh.de`, same password) exists specifically for
multi-tenancy testing.

### Alternative: Kubernetes + Helm

Same demo users/password as above. Requires a local cluster (Docker
Desktop's Kubernetes, minikube, or kind) and the app images already built
locally (`make up` builds them, or `docker compose build`):

```bash
docker build -t indusense-migrate:latest -f infrastructure/docker/migrate/Dockerfile .
docker build -t indusense-seed:latest -f scripts/seed/Dockerfile .

helm install indusense infrastructure/helm/indusense \
  --create-namespace -n indusense --set seed.enabled=true

kubectl get pods -n indusense
kubectl port-forward -n indusense svc/indusense-api 8080:8080 &
kubectl port-forward -n indusense svc/indusense-frontend 3000:3000 &
```

To generate traffic (has to run as a pod — see the Phase 14 section above
for why):

```bash
helm upgrade indusense infrastructure/helm/indusense -n indusense \
  --reuse-values --set simulator.enabled=true
```

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
