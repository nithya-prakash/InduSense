# Verification log — 13 Oct 2026

Test and audit results were re-run after the fixes listed under "Changes made while verifying"; the Kubernetes and k6 sections were measured earlier the same day (the k6 WebSocket row was re-measured after its fix).

Everything below was run on one laptop (Apple silicon, Docker Desktop, 8 GB
RAM, 10 CPUs). Nothing here is a production benchmark.

## Tests (Docker Compose stack, fresh volumes)

| Tier | Command | Result |
|---|---|---|
| Unit (per-service, no external services) | `make unit-test` | 131 passed, 8 skipped (live-Postgres tests; the unit run has no network) |
| Contract | `make contract-test` | 6 passed |
| Integration | `make integration-test` | 8 passed |
| End-to-end | `make e2e-test` | 3 passed |

The e2e tier needs a device with no existing CRITICAL alert. Re-running on
volumes left over from earlier runs failed 2 of 3 for that reason; with fresh
volumes (`docker compose down -v`, as CI does) all passed.

## Static checks

| Check | Result |
|---|---|
| `helm lint infrastructure/helm/indusense` (helm 3.16.2) | 0 failed; 37 manifests render |
| `actionlint` 1.7.3 | Found `fail_level`, which is not an input of `reviewdog/action-actionlint@v1`; replaced with `fail_on_error: true`; rerun clean. The workflow has not run on GitHub since the change. |
| `pip-audit` per `requirements.txt` | PyJWT 2.13.0 had 6 advisories (api, tests, seed). Bumped to 2.15.1; all 8 requirement files now report no known vulnerabilities. |
| Frontend | `npm ci`, `npm run lint` (no findings) and `npm run build` succeeded (node 22) |

## Kubernetes (kind v0.24.0, node v1.31.0, Helm chart in this repo)

```bash
kind create cluster --name indusense --image kindest/node:v1.31.0
docker build -t indusense-migrate:latest -f infrastructure/docker/migrate/Dockerfile .
for i in ingestion stream-processor anomaly-detector alert-service api frontend migrate seed simulator; do
  kind load docker-image "indusense-${i}:latest" --name indusense; done
helm upgrade --install indusense infrastructure/helm/indusense -n indusense --create-namespace
helm upgrade indusense infrastructure/helm/indusense -n indusense --reuse-values --set seed.enabled=true
helm upgrade indusense infrastructure/helm/indusense -n indusense --reuse-values \
  --set simulator.enabled=true --set simulator.durationSeconds=60
```

Observed:

- All 15 pods became Ready. Ingestion, stream-processor, anomaly-detector and
  alert-service restarted 2–4 times at first while Kafka and migrations were
  not ready yet; they then stayed up.
- Seed hook created 200 devices.
- API `/health`: postgres, redis, kafka, mqtt, influxdb all `ok` (2 API replicas).
- Login as the seeded admin returned a token; `/api/v1/alerts` returned alerts
  produced by the simulator burst.
- Prometheus: 5 service targets up. After the 60 s simulator run:
  `messages_received_total` = 2772 = `messages_processed_total` (ingestion),
  `anomalies_detected_total` = 79, `alerts_generated_total` = 19,
  `dlq_messages_total` = 0 everywhere.
- Frontend returned HTTP 200 through a port-forward.
- The simulator Job ends as `Failed (DeadlineExceeded)` after its 60 s
  deadline; the simulator runs until stopped, so this is how the Job ends.

Not tested on kind: ingress, persistent volumes surviving a restart, rolling
upgrade, multi-node scheduling.

## Grafana

The 5 provisioned dashboards loaded in the kind deployment. For the 4 that
contain Prometheus queries, 31 of 32 queries returned data after the
simulator run; the one empty query is "Failed messages per service", which is
empty when nothing failed. The dashboard titled "InduSense" has no panel
queries. Datasources: Prometheus, Jaeger. Panels were not inspected visually.

## k6 (Compose stack, simulator running, k6 0.54.0 in Docker)

API behind Docker Desktop port mapping; `API_TRUST_PROXY_HEADERS=true` with the
Docker network CIDRs trusted so the per-VU `X-Forwarded-For` identities are
honoured (otherwise all VUs share one IP and hit the rate limiter).

| Script | Conditions | Result |
|---|---|---|
| `dashboard-read-load.js` | ramp to 50 VUs, 70 s | 5,416 requests (73.9/s), 0.00% failed, 100% checks; p95 65.4 ms overall; per endpoint p95: factories 70.7, devices 56.8, alerts 58.2, telemetry 74.7 ms |
| `auth-rate-limit.js` | 60 logins from one IP, limit 30/min | 30 of 60 returned 429 as expected |
| `websocket-scale.js` | ramp to 200 VUs, 20 s sessions | before the fix: 250/250 connected, 4 not closed cleanly (98.4%), threshold failed; ws connect p95 24.9 ms, 4,325 messages. After the fix: two runs, 250/250 connected and 250/250 clean |

`auth-rate-limit.js` was changed: it sent 30 requests against a 30/min limit
and could never see a 429 (the default limit had been raised from 10).

## Changes made while verifying

- **WebSocket close bug (`services/api/ws_alerts.py`).** The receive loop
  called `receive()` again after Starlette had delivered the disconnect
  message, so ordinary client closes raised an unhandled ASGI exception
  (3 in the one run I counted, in the API log). Fixed; a regression test fails on the
  old handler and passes on the new one; the API log then showed 0 such errors.
- **k6 clean-close check.** The remaining client-side failures all carried the
  k6 error `websocket: close sent` (a write racing the client's own close
  frame). The check now ignores that one error and still fails on any other.
- **PyJWT 2.15.1** in api, tests and seed (see above).
- **Dependency group** applied together: OpenTelemetry sdk and exporter
  1.44 to 1.45 (they must move as a pair), psycopg 3.3.6, psycopg_pool
  3.3.3, confluent-kafka 2.15.1, influxdb-client 1.50.0 (api), pytest 9.1.1.
  Major versions tested separately afterwards: redis 5 to 8 (api, tests),
  bcrypt 4 to 5 (api), confluent-kafka 2.6.1 to 2.15.1 (tests). All suites
  passed with them. bcrypt 5 rejects passwords longer than 72 bytes, and
  passwords are now limited to 72 UTF-8 bytes (`shared/auth.py`): over-long input is rejected with a clear error instead of being truncated (bcrypt 4) or raising (bcrypt 5). Tests cover 72 vs 73 bytes, multi-byte characters, and verifying a real bcrypt 4.2.0 hash with bcrypt 5. A user whose existing password is longer than 72 bytes could no longer log in with the full password; the seeded demo users are unaffected.

## Test environment note

A simulator container left running from the k6 session flooded Kafka
(stream-processor lag in the hundreds of thousands) and made 2 of 3 e2e tests
time out. Removed, and the suite re-run on fresh volumes: all passed.

## Not verified

Ingress, persistence across a pod restart, rolling upgrades, multi-node
scheduling, Alertmanager (not part of the project), a restore drill, real
sensors, and the GitHub Actions run of the changed workflow.

Earlier measurements (stream-processor ceiling, alert latency, zero-loss burst)
are in [eval/results/FINAL_REPORT.md](../eval/results/FINAL_REPORT.md).
