# InduSense — CV-Safe Metrics

Derived entirely from [FINAL_REPORT.md](FINAL_REPORT.md). Every number here
was measured by running the actual system this session; none were
estimated, rounded up, or reverse-engineered from a target.

## A. Metrics you can confidently put on your CV

- **1,000 simulated industrial sensors**, verified instantiated (not just
  configured) — real Postgres count + simulator startup log confirming
  1000 loaded rows and 1000 asyncio publisher tasks.
- **Zero message loss end-to-end** across a 1000-sensor, 60-second burst at
  the fleet's full nominal rate (127,374 messages), confirmed by two
  independent counters (a Prometheus counter and InfluxDB's own stored
  point count) matching exactly.
- **Real end-to-end latency**: mean 74.3ms / p95 250.3ms from an MQTT
  publish to the resulting alert being visible via the REST API, across
  the full 7-hop pipeline (MQTT → ingestion → Kafka → stream-processor →
  anomaly-detector → alert-service → Postgres → API), n=8, local system.
- **Idempotent deduplication, proven not asserted**: publishing one event
  5 times resulted in exactly 1 stored record, cross-validated via an
  independent database point count.
- **146 automated tests** (unit + contract + integration + e2e) across a
  Python microservice stack, with a green CI run against real
  infrastructure (not mocks) as of 2026-09-02.
- **A working, currently-green CI/CD pipeline**: lint, dependency
  vulnerability scanning across 7 services, unit tests, a full
  integration/e2e suite against real Postgres/Kafka/MQTT/InfluxDB, and
  automated multi-service Docker image publishing to a container registry.

## B. Metrics that are valid but need careful wording

- **"~385 msg/s sustained processing throughput per service instance"** —
  real and precisely measured, but it is a *component* ceiling
  (stream-processor, single instance, single machine), not a system-wide
  claim. Do not say "the system processes 385 msg/s" without the
  "per-instance, single-machine" qualifier — this project's own Kafka
  topics are already partitioned (12 partitions on `telemetry.raw`)
  specifically so this scales horizontally with more instances, which
  wasn't tested this session.
- **"~1,000 msg/s ingested with zero loss"** — true for the MQTT→Kafka
  ingestion boundary specifically (which absorbed the burst and never
  dropped a message), not for the full pipeline's *sustained* throughput
  (which bottlenecks earlier, at stream-processor, and buffers the rest in
  Kafka rather than losing it). Word it as "ingested with zero loss," not
  "processed in real-time at 1,000 msg/s."
- **Test coverage ~40%** — real, but say "unit-test line coverage"
  specifically; a large share of the orchestration/I/O code is exercised
  by the separate integration/e2e tiers (against real infrastructure),
  which weren't measured under coverage instrumentation. Stating the bare
  40% without that context understates what's actually tested; stating it
  as "high coverage" would overstate the unit tier specifically.
- **Anomaly detection (3 methods, 4,613 anomalies detected in one 60s
  burst)** — real counts, real system. Do not imply an accuracy claim
  ("catches anomalies with X% accuracy") — there is no labeled ground
  truth to support that.

## C. Metrics that should NOT be used

- **"68% false-positive reduction"** — does not exist anywhere in this
  repository. No baseline, no formula, no ground truth. Cannot be
  defended if asked "how did you measure that?"
- **"10,000 messages/sec"** — not supported at any layer. The actual
  measured ceiling for the component that would need to sustain it
  (stream-processor) is ~385 msg/s on this hardware — roughly 26x lower.
  If a distributed/scaled-out throughput number is wanted, it would need
  to be genuinely tested with multiple stream-processor instances across
  multiple machines, which has not been done.
- **Any anomaly-detection precision/recall/F1/AUC number** — no labeled
  ground truth exists in this project. Any such number would be
  fabricated.
- **"Production-ready"** — MQTT runs with `allow_anonymous=true` and no
  environment-based production guard (unlike the JWT secret, which now has
  one); this alone disqualifies a "production-ready" claim as stated.
- **"Kubernetes-deployed" or "Kubernetes-verified"** (for the current
  system) — only `helm lint`/`helm template` were run this session; no
  live cluster deployment of the current Python images was performed.

## D. Recommended CV Bullets

1. **"Built an event-driven IoT telemetry pipeline (Python/FastAPI, MQTT,
   Kafka, PostgreSQL, InfluxDB, Redis) simulating 1,000 industrial sensors;
   verified zero message loss across a 127K-message burst via independent
   cross-validation of two separate counters, and measured 74ms mean /
   250ms p95 end-to-end latency from sensor reading to generated alert."**

2. **"Implemented three independent anomaly-detection methods (rule-based,
   EWMA statistical, and a from-scratch Isolation Forest) with
   Kafka-partition-based horizontal scaling and three separate,
   purpose-scoped idempotency layers (Redis event-dedup, Postgres
   alert-dedup, Postgres publish-idempotency); proved deduplication
   empirically — 5 duplicate event publishes reduced to exactly 1 stored
   record, independently confirmed via direct database inspection."**

3. **"Designed and ran a real-infrastructure CI/CD pipeline (GitHub
   Actions) covering 146 automated tests (unit, contract, integration, and
   end-to-end against live Postgres/Kafka/MQTT/InfluxDB — not mocks),
   dependency vulnerability scanning across 7 services, and automated
   multi-service Docker image publishing."**

Each bullet uses only numbers reproduced in [FINAL_REPORT.md](FINAL_REPORT.md)
this session, avoids implying supervised anomaly-detection accuracy,
avoids implying cloud/production-scale throughput, and states the
simulated/local nature of the benchmark implicitly through precise,
defensible technical language rather than marketing terms.
