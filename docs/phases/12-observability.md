# 12. Observability

Four pillars, all live-verified against real running services:

**Prometheus** ([infrastructure/prometheus/prometheus.yml](../../infrastructure/prometheus/prometheus.yml))
scrapes all five backend services (`api`, `ingestion`, `stream-processor`,
`anomaly-detector`, `alert-service`) every 10s. Verified via Prometheus's own
`/api/v1/targets` showing all five as `up`. Two gauges were added because
the dashboards needed data the base metrics didn't expose:
`devices_by_status` (`services/api`, refreshed from Postgres every 15s) and
`incidents_open_by_severity` (`services/alert-service`, same pattern) — both
confirmed with real non-zero values rather than just structurally present.
Kafka broker JMX metrics are explicitly **not implemented** — no JMX
exporter sidecar.

**Grafana** ([infrastructure/grafana/](../../infrastructure/grafana/))
auto-provisions a Prometheus + Jaeger datasource and four dashboards
(Platform, Kafka, IoT, Anomaly & Alerting) via YAML providers. All four were
opened in a real browser and watched respond to a real traffic burst
generated with the simulator: API throughput/latency/CPU/memory panels
spiked and recovered in sync, Kafka consumer lag rose then drained as
anomaly-detector and stream-processor caught up, and IoT/incident panels
tracked the correct live counts.

**Structured logging** ([shared/logging_utils.py](../../shared/logging_utils.py),
a thin JSON log formatter) is wired into the pipeline's key lifecycle
events — telemetry/machine-event ingested, dedup and validation failures,
anomaly detected, alert created, incident opened/attached, and every API
request — across all five services. Fields match: `timestamp`, `service`,
`level`, `message`, plus contextual `event_id`/`device_id`/
`organization_id` at each call site and `trace_id` whenever the call
happens inside a span.

**OpenTelemetry tracing** ([shared/tracing.py](../../shared/tracing.py))
exports to the Jaeger container over OTLP/HTTP. Trace context is propagated
by hand across Kafka: a header carrier adapts the standard W3C `traceparent`
so it's injected on every publish (ingestion → telemetry.raw,
stream-processor → telemetry.processed, anomaly-detector →
anomalies.detected, alert-service → alerts) and extracted on every consume.

Verified live end-to-end: after a simulator run, Jaeger's `/api/services`
listed all five real services, and querying a single trace ID found a
genuine four-span chain with correct parent/child relationships and
monotonically increasing start times — `ingestion.process_message` (root) →
`stream_processor.process` (child) → `anomaly_detector.detect` (child of
that) → `alert_service.process_anomaly` (child of that) — meaning a
`traceparent` header genuinely rode across three separate Kafka hops. The
same trace ID also showed up verbatim in that request's structured log
line, confirming logs and traces share an ID a reader could actually pivot
between.

**Not implemented here**: tracing on the frontend (no browser-side OTel
SDK); log shipping to any aggregator (Loki, ELK) — logs are stdout-only,
collected by `docker compose logs`; Prometheus alerting rules
(Alertmanager) — Grafana dashboards exist but no alert *rules* fire from
Prometheus itself, only from `alert-service`'s own domain logic.
