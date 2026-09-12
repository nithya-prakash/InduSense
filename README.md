# InduSense — Industrial IoT Monitoring Platform

InduSense is a production-grade, event-driven monitoring platform for a
(fictional) German manufacturing company operating multiple factories. It
ingests real-time telemetry from thousands of machine sensors, detects
abnormal behavior, raises alerts, and manages incidents — end to end,
through real MQTT, Kafka, PostgreSQL, and InfluxDB, not mocked stand-ins.

**Dashboard, live** — logging in, watching real-time alerts, drilling into an
incident's audit trail, and a machine's telemetry chart with real InfluxDB
data:

![Dashboard demo](docs/media/dashboard-demo.gif)

**Terminal, real output** — `docker compose ps`, a live health check, a real
login, and the full test suite running against the actual stack (no output
faked or trimmed beyond truncating the JWT for readability):

![Terminal demo](docs/media/terminal-demo.gif)

## Why this project exists

This is a portfolio project built to demonstrate real competence in
distributed systems, event-driven architecture, and applied backend
engineering. It prioritizes:

- **correctness over feature count**
- **working integrations over buzzwords**
- **measured performance over invented benchmarks**

Nothing here is faked. If a capability isn't implemented or a number hasn't
been measured, it's marked `NOT IMPLEMENTED` or `NOT YET MEASURED` rather
than claimed.

## Architecture

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
        FastAPI REST/WebSocket API
             │
             ▼
        Next.js Dashboard
```

## Tech stack

**Backend — Python**, one service per container: FastAPI (`api`), and four
Kafka-consuming workers (`ingestion`, `stream-processor`,
`anomaly-detector`, `alert-service`) sharing a small `shared/` package
(event schemas, circuit breaker/retry, logging, auth, incidents, audit).
paho-mqtt, confluent-kafka-python, psycopg3, redis-py, influxdb-client,
PyJWT + bcrypt, prometheus_client, OpenTelemetry.

**Infrastructure**: PostgreSQL, InfluxDB, Redis, Apache Kafka, Eclipse
Mosquitto — Docker Compose for local dev, Kubernetes + Helm for cluster
deployment. Prometheus + Grafana + Jaeger for observability.

**Frontend**: Next.js 16 (React 19), TypeScript, Tailwind CSS, Recharts.

**Testing/CI**: pytest (unit/contract/integration/e2e against real infra,
not mocks), GitHub Actions, pip-audit, Dependabot, k6 load tests.

**A note on the Go → Python rewrite**: the backend was originally built in
Go across phases 1–16 — a deliberate, working choice, not a mistake — then
rewritten entirely to Python once it was clear a portfolio project only
proves what its owner can personally read, defend, and extend, and Go's
owner doesn't read Go. The rewrite was executed with the same
phase-by-phase, real-infra-verified discipline as the original build, not
a rushed do-over — see
[docs/phases/17-python-rewrite.md](docs/phases/17-python-rewrite.md) for
the full reasoning, not just the mechanics.

## How it was built

The system was built incrementally, one phase at a time, each verified
live against real infrastructure before the next began. The detailed
write-up for every phase — architecture decisions, live verification, real
bugs found and fixed, measured numbers — lives in
**[docs/phases/](docs/phases/README.md)**:

1. [Foundation](docs/phases/01-foundation.md) · 2. [Domain](docs/phases/02-domain.md) ·
3. [Sensor Simulation](docs/phases/03-sensor-simulation.md) · 4. [Ingestion](docs/phases/04-ingestion.md) ·
5. [Streaming](docs/phases/05-streaming.md) · 6. [Anomaly Detection](docs/phases/06-anomaly-detection.md) ·
7. [Alerting](docs/phases/07-alerting.md) · 8. [Incidents](docs/phases/08-incidents.md) ·
9. [Authentication](docs/phases/09-authentication.md) · 10. [APIs](docs/phases/10-apis.md) ·
11. [Dashboard](docs/phases/11-dashboard.md) · 12. [Observability](docs/phases/12-observability.md) ·
13. [Testing](docs/phases/13-testing.md) · 14. [Kubernetes + Helm](docs/phases/14-kubernetes-helm.md) ·
15. [Load Testing](docs/phases/15-load-testing.md) · 16. [CI/CD](docs/phases/16-cicd.md) ·
17. [Python rewrite](docs/phases/17-python-rewrite.md)

See also [docs/ANOMALY-DETECTION.md](docs/ANOMALY-DETECTION.md) for the
Isolation Forest design and evaluation writeup.

## Delivery semantics — the real exposure, not a footnote

This system is **at-least-once, not exactly-once**, at every hop: MQTT
(persistent session, manual ack), Kafka (manual commit after processing),
and every service-to-service handoff in between. That is a deliberate
architectural choice — distributed exactly-once semantics across
MQTT→Kafka→Postgres/InfluxDB would mean either a two-phase-commit-style
protocol this project doesn't need, or Kafka transactions that only cover
the Kafka legs and still leave MQTT and the database writes outside the
transaction boundary. At-least-once plus idempotent consumers is the
honest, load-bearing choice here — but "idempotent consumers" is a claim
that has to be checked concretely, not asserted, so this section states
exactly what redelivery looks like, what actually catches it, and where
the real remaining gaps are.

**Where a message can genuinely be delivered more than once**, not
hypothetically: an MQTT broker redelivering an unacked message on
reconnect (ingestion's own [documented Kafka-outage test](docs/phases/04-ingestion.md)
reproduced exactly this — a real message came back as two duplicate
copies, milliseconds apart); a Kafka consumer crashing or rebalancing
between finishing work and committing its offset, so the same message is
handed to whichever consumer picks up the partition next; and a producer
retrying a write whose acknowledgment was lost even though the write
itself succeeded (the kind of ambiguous-ack race documented in
[the Python rewrite's evaluation notes](eval/results/FINAL_REPORT.md) after
a Kafka broker died mid-load-test).

**Three separate idempotency mechanisms catch this, each with a narrower
scope than "the whole pipeline" — know which one covers what:**

| Layer | Mechanism | Scope | Known gap |
|---|---|---|---|
| `stream-processor` (event-level) | Redis `SETNX` + TTL on `event_id` ([dedup.py](services/stream-processor/dedup.py)) | Skips the InfluxDB write + windowed-aggregate update for a duplicate `telemetry.raw` message | The TTL is finite (`STREAM_DEDUP_TTL_SECONDS`, default 3600s) — a redelivery arriving *after* the key has expired is treated as a brand-new event. This is an accepted, disclosed tradeoff (unbounded Redis growth is worse), not a claim that duplicates are impossible |
| `anomaly-detector` (detection-level) | Postgres `idempotency_keys` table, atomic `INSERT ... ON CONFLICT DO NOTHING RETURNING` ([idempotency.py](services/anomaly-detector/idempotency.py)) | Guards the *entire* detection run for a given `event_id` — the EWMA statistical baseline and the Isolation Forest's training buffer, not just the final `AnomalyDetected` publish | **This was a real, fixed bug, not a design decision**: an earlier version claimed idempotency only around the publish step, so a redelivered `telemetry.processed` message correctly avoided publishing a duplicate anomaly, but still silently folded the same reading into both statistical baselines a second time — a genuine correctness exposure under an ordinary, not rare, failure mode. Fixed by moving the claim before any detector state is touched; see the regression test in [test_idempotency.py](services/anomaly-detector/tests/test_idempotency.py) that reproduces the exact redelivery and asserts the tracker only updates once |
| `alert-service` (alert-level) | Postgres partial unique index + `ON CONFLICT` on `(rule, device, metric)` while `status='OPEN'` ([store.py](services/alert-service/store.py)) | Prevents a second alert row for a condition that's already open | Does not protect against a *different* rule firing twice for reasons upstream of alert-service — that's what the two layers above are for |

**What is not covered, stated plainly rather than implied away**: the
system does not guarantee a payload published twice under the same
`event_id` is checked for *consistency* — `stream-processor`'s dedup
claims the ID and keeps whichever payload arrived first, silently
discarding a different second payload rather than flagging the mismatch.
This has not caused a problem in practice (an `event_id` is meant to be
generated once, at the true source of a reading), but it means the
idempotency guarantee is "the same ID is only ever processed once," not
"the same ID is guaranteed to have carried the same data every time it was
seen" — a distinction worth being precise about rather than glossing over.

See [Streaming](docs/phases/05-streaming.md),
[Anomaly Detection](docs/phases/06-anomaly-detection.md), and
[Alerting](docs/phases/07-alerting.md) for the full design of each layer,
and [eval/results/FINAL_REPORT.md](eval/results/FINAL_REPORT.md) for the
live, reproducible tests behind every claim above (duplicate delivery,
zero-loss cross-validation, and the exact bug-and-fix account for the
anomaly-detector gap).

## Local setup

```bash
git clone <repo>
cd indusense
make setup   # copies .env.example -> .env
make up      # infra -> migrate -> app services -> frontend, all health-checked
make ps      # check container health
make down    # stop everything (data volumes preserved)
```

Kafka UI: http://localhost:8089 · InfluxDB UI: http://localhost:8086 ·
API: http://localhost:8080 (Swagger docs at `/docs`) ·
Dashboard: http://localhost:3000

Demo users (`make seed`), one per role, password `ChangeMe123!` for all —
**local development only**: `admin@musterfabrik-gmbh.de`,
`factory_manager@musterfabrik-gmbh.de`, `engineer@musterfabrik-gmbh.de`,
`technician@musterfabrik-gmbh.de`, `viewer@musterfabrik-gmbh.de`. A second
organization (`admin@zweite-firma-gmbh.de`, same password) exists
specifically for multi-tenancy testing.

### Alternative: Kubernetes + Helm

Requires a local cluster (Docker Desktop's Kubernetes, minikube, or kind)
and the app images already built locally:

```bash
docker build -t indusense-migrate:latest -f infrastructure/docker/migrate/Dockerfile .
docker build -t indusense-seed:latest -f scripts/seed/Dockerfile .

helm install indusense infrastructure/helm/indusense \
  --create-namespace -n indusense --set seed.enabled=true

kubectl get pods -n indusense
kubectl port-forward -n indusense svc/indusense-api 8080:8080 &
kubectl port-forward -n indusense svc/indusense-frontend 3000:3000 &
```

**Generating traffic here is a real, honest constraint, not an oversight**:
it has to run as an in-cluster pod, not from your host machine — Kafka's
advertised listener only resolves inside the cluster network (see
[Kubernetes + Helm](docs/phases/14-kubernetes-helm.md) for the exact
mechanism). One command handles it:

```bash
make k8s-simulate   # RELEASE/NAMESPACE default to indusense/indusense, override if yours differ
```

(equivalent to `helm upgrade indusense infrastructure/helm/indusense -n
indusense --reuse-values --set simulator.enabled=true`, if you'd rather run
it directly.)

## Repository structure

```text
indusense/
├── services/          # api, ingestion, stream-processor, anomaly-detector, alert-service (Python)
├── shared/            # event schemas, reliability, logging, tracing, auth, incidents, audit
├── frontend/          # Next.js + TypeScript dashboard
├── simulator/         # sensor simulator
├── infrastructure/    # docker, kubernetes, helm, prometheus, grafana, jaeger configs
├── migrations/        # PostgreSQL schema migrations
├── tests/             # integration, contract, e2e (pytest)
├── load-tests/        # k6 scripts
├── scripts/           # dev/ops scripts
├── docs/              # phase-by-phase write-ups, design docs
├── .github/workflows/ # CI/CD (GitHub Actions)
└── docker-compose.yml
```
