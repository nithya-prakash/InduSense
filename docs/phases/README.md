# Build phases

InduSense was built incrementally, one phase at a time, each verified live
against real infrastructure before the next began. This folder holds the
detailed, phase-by-phase write-up that used to live in the top-level
[README](../../README.md) — moved here to keep that README a short
overview, not a 1200-line document.

**A note on language and numbers below.** The backend (`services/*`,
`shared/`, `simulator/`, `scripts/seed`) was originally written in Go
through phases 1–16. It was later rewritten entirely to Python — see
[17 — Python rewrite](17-python-rewrite.md) — so the project's owner could
personally read, defend, and maintain every line. The architecture, schema,
algorithms, and business logic described in phases 1–16 are unchanged and
still accurate; code links point at the current `.py` files. Specific
performance numbers (load-test throughput, Kubernetes convergence time,
Isolation Forest evaluation scores) were measured against the original Go
implementation and have not been independently re-measured against Python —
kept here as the historical record of when each capability was first
verified, not as current Python benchmarks.

1. [Foundation](01-foundation.md) — Docker Compose infra, Kafka topics
2. [Domain](02-domain.md) — Postgres schema, migrations, seed data
3. [Sensor Simulation](03-sensor-simulation.md) — 1000 simulated sensors over real MQTT
4. [Ingestion](04-ingestion.md) — MQTT → Kafka, at-least-once delivery
5. [Streaming](05-streaming.md) — dedup, windowed aggregation, InfluxDB
6. [Anomaly Detection](06-anomaly-detection.md) — rule-based, statistical, Isolation Forest
7. [Alerting](07-alerting.md) — rule matching, dedup/cooldown, escalation
8. [Incidents](08-incidents.md) — incident lifecycle, DB-enforced dedup
9. [Authentication](09-authentication.md) — JWT, RBAC, multi-tenancy
10. [APIs](10-apis.md) — REST + WebSocket surface
11. [Dashboard](11-dashboard.md) — Next.js frontend
12. [Observability](12-observability.md) — Prometheus, Grafana, logging, tracing
13. [Testing](13-testing.md) — contract, integration, e2e tests
14. [Kubernetes + Helm](14-kubernetes-helm.md) — chart, real-cluster verification
15. [Load Testing](15-load-testing.md) — k6 scripts and measured results
16. [CI/CD](16-cicd.md) — GitHub Actions, operational hygiene
17. [Python rewrite](17-python-rewrite.md) — the full Go → Python migration
