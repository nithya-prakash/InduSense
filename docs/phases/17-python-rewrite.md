# 17. Python rewrite

Phases 1–16 built the backend in Go. It was later rewritten entirely to
Python, one service at a time, each verified live against the real running
stack before the next began — the same phased, real-infra-verified
discipline the original Go build used. The motivation was simple: the
project's owner doesn't read Go, which defeats the point of a portfolio
project meant to be personally understood, defended, and maintained.
Everything else — the Next.js frontend, PostgreSQL/InfluxDB/Redis/Kafka/
Mosquitto infrastructure, Docker Compose topology, Helm chart, Kafka
topics, DB schema, and REST/WebSocket contract — stayed identical, so
nothing downstream needed to know the implementation language had changed.

## Library mapping

| Concern | Go (before) | Python (now) |
|---|---|---|
| MQTT | `eclipse/paho.mqtt.golang` | `paho-mqtt` |
| Kafka | `segmentio/kafka-go` | `confluent-kafka-python` |
| Postgres | `jackc/pgx/v5` | `psycopg` v3 + `psycopg_pool` |
| Redis | `redis/go-redis/v9` | `redis-py` |
| InfluxDB | `influxdata/influxdb-client-go` | `influxdb-client` |
| JWT / password hashing | `golang-jwt`, `x/crypto/bcrypt` | `PyJWT`, `bcrypt` |
| Circuit breaker / retry | hand-rolled `pkg/reliability` | ported 1:1 into `shared/reliability.py` |
| Event schemas | Go structs + `encoding/json` | Pydantic v2 models |
| Metrics | `prometheus/client_golang` | `prometheus_client` |
| Tracing | `go.opentelemetry.io/otel` | `opentelemetry-sdk` + OTLP/HTTP exporter |
| Web framework | stdlib `net/http.ServeMux` | **FastAPI** (`api` only — the other four are workers, not HTTP servers) |
| Ingestion worker pool | bounded goroutine pool + channel | `queue.Queue` + `ThreadPoolExecutor` |
| Simulator concurrency | one goroutine per sensor | one asyncio task per sensor, blocking MQTT calls handed to a thread pool |
| Tests | Go `testing` (4 tiers, real infra) | `pytest`, same 4 tiers, same real-infra-not-mocks philosophy |

Order: ingestion → stream-processor → anomaly-detector → alert-service →
api, then `scripts/seed` and `simulator/` (discovered mid-rewrite to also
be Go artifacts, out of the originally-stated scope but ported for the same
reason as everything else), and finally the Go test suites ported to
pytest with `go.mod`/`go.sum`/`pkg/` deleted entirely.

## Real bugs found during the rewrite

Verifying each service live against real infra, rather than trusting that
a structurally-equivalent port behaves identically, surfaced genuine bugs —
none of them hypothetical:

- **anomaly-detector: a UUID-keying bug that silently broke rule-based
  detection.** psycopg3 returns Postgres `uuid` columns as `uuid.UUID`
  objects, not strings; the device catalog's lookup dict was keyed by those
  `UUID` objects while callers always looked up by plain string, so every
  lookup silently missed. Caught because the live end-to-end test failed on
  its very first run against the Python service. Fixed with an explicit
  `str()` cast. See [Anomaly Detection](06-anomaly-detection.md).
- **alert-service: the same class of bug, caught before it shipped.** After
  the anomaly-detector incident, every subsequent Postgres-scanning function
  was checked against `\d <table>` before writing code, which caught two
  more instances proactively: `alert_rules`' `numeric` threshold columns
  return as `decimal.Decimal`, not `float`, and several `RETURNING id`
  `uuid` columns needed explicit `str()` casts.
- **api: FastAPI's `status_code=204` needs `response_model=None`
  explicitly**, even with a `-> None` return annotation, or it raises an
  `AssertionError` at route-registration time. Hit on all six no-content
  routes (logout, decommission, acknowledge, transition, assign, resolve).
- **Contract tests: a wire-format gap Go's `omitempty` had covered for
  free.** Rewriting the Go contract tests as pytest meant generating golden
  JSON strings from the *actual current* Pydantic serializer output rather
  than copy-pasting Go's old fixed strings — and that process caught a real
  divergence: Go's `MachineEvent` struct omits four fields from JSON when
  empty; the Pydantic port had no equivalent, making every message
  chattier than the original. Fixed with a hand-written `model_dump_json`
  override rather than a blanket `exclude_defaults=True`, which would have
  also wrongly dropped a field Go always includes regardless of its value.
- **opentelemetry-instrumentation-fastapi dependency conflict.** A version
  mismatch with modern setuptools broke `api`'s startup
  (`ModuleNotFoundError: No module named 'pkg_resources'`). Rather than
  chase exact pinning for a peripheral auto-tracing feature that was never
  load-bearing, the instrumentation package was dropped entirely — manual
  span creation in `shared/tracing.py` covers the same "never fail on a
  missing collector" init pattern.

## Test infrastructure

Running multiple services' pytest suites in one process breaks silently:
every service has its own `config.py`/`main.py`/`health.py`, and Python's
`sys.modules` cache means a second service's `import config` would silently
reuse the *first* service's already-imported module. `make unit-test` runs
each service's tests as a **separate** `docker run` against a shared test
image ([tests/Dockerfile](../../tests/Dockerfile)) rather than one combined
invocation.

A fixed-window rate limiter also looked broken under a slow sequential
`curl` loop during manual verification — the 60-second window rolled over
mid-test before the count climbed high enough, purely from real subprocess
spawn overhead. Switching to concurrent requests (a `ThreadPoolExecutor`
firing them inside one window) confirmed the limiter itself was correct all
along; the test methodology was the bug.

## Commit history

The rewrite is committed as one commit per service/phase, matching this
project's existing convention (see `git log`): ingestion,
stream-processor, anomaly-detector, alert-service, api, scripts/seed +
simulator, and finally the pytest port + Go removal.
