# InduSense — Repository Audit

Conducted as the first step of a rigorous, reproducible evaluation of every
quantitative claim that could appear on a CV. This file identifies what
exists in the repository and where each claimed metric is supposed to
originate; measured results are in
[results/FINAL_REPORT.md](results/FINAL_REPORT.md).

## Architecture (verified against code, not diagrams)

```
Simulator (simulator/, Python asyncio)
  --MQTT (QoS1)--> Eclipse Mosquitto
Mosquitto --MQTT(manual ack)--> ingestion (services/ingestion/, thread pool + bounded queue)
  --Kafka produce (acks=all)--> telemetry.raw / device.events
telemetry.raw --> stream-processor (services/stream-processor/)
  [Redis SETNX dedup --> InfluxDB write --> windowed aggregation --> republish]
  --> telemetry.processed
telemetry.processed --> anomaly-detector (services/anomaly-detector/)
  [rule-based (rules.py) + EWMA statistical (stats.py) + Isolation Forest (isolationforest.py, from-scratch)]
  --> anomalies.detected
anomalies.detected + device.events --> alert-service (services/alert-service/)
  [rule matching (conditions.py), Postgres dedup/cooldown (store.py), escalation]
  --> alerts (Kafka) + Postgres + incidents (shared/incidents.py)
api (services/api/, FastAPI) --> Postgres/InfluxDB/Redis reads, WebSocket fan-out (ws_alerts.py)
frontend (Next.js) --> api over REST + WebSocket
```

## Component inventory

| Component | Location | Notes |
|---|---|---|
| Telemetry generator/simulator | `simulator/main.py`, `sensorgen.py`, `faults.py`, `model.py` | asyncio tasks (1 per sensor), configurable `SENSOR_COUNT`/`MESSAGES_PER_SECOND` |
| MQTT | `services/ingestion/mqtt_client.py`; broker config `infrastructure/docker/mosquitto.conf` | paho-mqtt v2, persistent session, manual ack, QoS1; broker has `allow_anonymous true` (dev-only, no prod guard — see prior session's security audit) |
| Kafka topics | `infrastructure/docker/kafka-init-topics.sh` | `telemetry.raw`(12p), `telemetry.processed`(12p), `anomalies.detected`(6p), `alerts`(6p), `incidents`(3p), `device.events`(3p), `audit.events`(3p), `dead-letter`(3p) |
| Producers | `services/ingestion/kafka_producer.py` | retry+circuit-breaker wrapped, `acks=all` |
| Consumers | `*/kafka_io.py` in stream-processor/anomaly-detector/alert-service | confluent-kafka-python, single-threaded per instance, manual commit after processing |
| Stream processor | `services/stream-processor/` | dedup.py (Redis SETNX+TTL), window.py (rolling stats), influx.py |
| Feature engineering | `services/anomaly-detector/featurestore.py` | per-machine-type rolling buffer feeding the Isolation Forest |
| Anomaly detection | `services/anomaly-detector/{rules,stats,isolationforest}.py` | 3 independent methods, all run on every sample |
| Alerting | `services/alert-service/{conditions,store,notify}.py` | Postgres-enforced dedup via partial unique index + `ON CONFLICT` |
| Deduplication | `services/stream-processor/dedup.py` (event-level, Redis) + `services/alert-service/store.py` (alert-level, Postgres) + `services/anomaly-detector/idempotency.py` (anomaly-publish level, Postgres) | three independent layers, different scopes |
| Persistence | PostgreSQL (relational: devices/alerts/incidents/users), InfluxDB (time-series telemetry) | |
| FastAPI endpoints | `services/api/handlers_*.py`, `main.py` | 24 authenticated REST routes + `/ws/alerts` |
| Docker Compose | `docker-compose.yml` | 15 services, full local stack |
| Kubernetes manifests | `infrastructure/helm/indusense/templates/` | Deployments, Jobs, Services, ConfigMaps, Secrets |
| CI/CD | `.github/workflows/ci.yml` | lint, compile-check, pip-audit×7, unit-test, full-infra test, image publish |
| Tests | `services/*/tests/`, `shared/tests/`, `simulator/tests/`, `scripts/seed/tests/`, `tests/{contract,integration,e2e}/` | pytest throughout |
| k6/load-testing scripts | `load-tests/{dashboard-read-load,auth-rate-limit,websocket-scale}.js` | **HTTP/WebSocket only — no MQTT/Kafka throughput script exists** |
| Monitoring/observability | `infrastructure/prometheus/`, `infrastructure/grafana/`, Jaeger | Prometheus scrapes all 5 backend services every 10s |
| Existing benchmark/eval scripts | none found | this evaluation adds `eval/` fresh |
| Existing README metrics | see below | |
| Previously generated reports | `docs/phases/15-load-testing.md` (historical k6 HTTP numbers from the original Go build, explicitly marked as not re-measured against Python) | |

## Where the specific CV claims would have to originate — checked directly

```bash
git grep -n -i "68%" -- .        # → no matches
git grep -n -i "false.positive.reduction" -- .   # → no matches
git grep -n -i "10,000 msg\|10000 msg\|10k msg" -- .  # → no matches
```

**None of these three numbers appear anywhere in the repository** — not in
README, not in code comments, not in `docs/`, not in any test or benchmark
script. They do not originate from this codebase. See
[results/CV_SAFE_METRICS.md](results/CV_SAFE_METRICS.md) for the disposition
of each.

## What the repository's own load-testing (`load-tests/*.js`) actually measures

Read directly: all three k6 scripts hit `services/api`'s REST/WebSocket
surface over HTTP. **None of them publish a single MQTT or Kafka message.**
The historical numbers in `docs/phases/15-load-testing.md` (76-78 req/s HTTP
throughput, p95 6.4ms) are REST API numbers from the original Go build, not
MQTT/Kafka ingestion throughput, and are explicitly flagged in that file as
not re-measured against the current Python implementation. Any claim about
"10,000 msg/sec" would have to come from a genuinely new MQTT/Kafka
benchmark — none existed before this evaluation; one was built for this
evaluation (see below) using the real simulator, not k6.

## Anomaly detection ground truth

Read `docs/ANOMALY-DETECTION.md` directly: it states plainly that
precision/recall against labeled ground truth was **never measured** —
only a synthetic-cluster sanity check (normal vs. injected outlier scores)
exists, explicitly marked `NOT YET MEASURED` for real precision/recall. This
evaluation does not fabricate labels or supervised metrics; see
[results/FINAL_REPORT.md](results/FINAL_REPORT.md) §Anomaly Detection.
