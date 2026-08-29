# 13. Testing

Four tiers, all run via `make unit-test` / `make integration-test` /
`make contract-test` / `make e2e-test` (aliased together as `make test`),
each a separate `docker run` against a shared Python test image
([tests/Dockerfile](../../tests/Dockerfile)) — see
[17 — Python rewrite](17-python-rewrite.md) for why they're split into
per-directory invocations rather than one combined `pytest` run.

**Contract tests** ([tests/contract/](../../tests/contract/)) lock the JSON
wire format of every event type in [shared/events.py](../../shared/events.py) —
`NormalizedTelemetryEvent`, `NormalizedMachineEvent`, `AnomalyDetected`,
`AlertEvent`, `DeadLetterRecord` — against a fixed expected payload,
round-tripped in both directions. There's no schema registry or
consumer-driven-contract framework (Pact) in this stack, so this is a
narrower, honest substitute: it can't catch "the consumer expected a field
the producer stopped sending" across a deploy it doesn't have, but it does
turn "someone renamed a field in the shared event model" from a silent
runtime failure in some *other* service into an immediate, local test
failure. Fast and infra-free by design.

**Integration tests** ([tests/integration/](../../tests/integration/)) hit
the real running `api` container over HTTP against real Postgres/Redis —
not a mocked handler test — using the demo data `scripts/seed` already puts
there. The centerpiece asserts tenant isolation for real: it logs in as
both seeded organizations' admins and confirms `zweite-firma-gmbh` never
sees `musterfabrik-gmbh`'s factories (or vice versa) against live data, not
by re-reading the query. Alongside it: RBAC, login success/failure,
unauthenticated/malformed-token rejection, and rate limiting.

Writing the rate-limit test surfaced a real test-suite bug, not a product
bug: the login endpoint's limiter buckets by client IP, and every test in
the package logged in from the same test-runner IP — so a test that
deliberately exhausts the limit made *unrelated* tests fail with 429
instead of the status they were actually checking. Fixed by giving each
test its own synthetic forwarded-for address (RFC 5737 TEST-NET-3, never a
real address).

**End-to-end tests** ([tests/e2e/](../../tests/e2e/)) publish a real MQTT
message to the same Mosquitto broker ingestion subscribes to, and poll the
real API until the effect that message should have propagates all the way
through — nothing mocked, stubbed, or short-circuited anywhere in between.

- A telemetry round-trip test looks up a real seeded sensor from Postgres,
  publishes an in-range reading over MQTT, and polls the telemetry API
  until that exact value comes back — proving MQTT → ingestion → Kafka →
  stream-processor → InfluxDB → API.
- An anomaly-triggers-alert test publishes a reading well above the seeded
  "High temperature" rule's threshold and polls the alerts API for the
  resulting `CRITICAL` alert — proving MQTT → ingestion → Kafka →
  anomaly-detector's rule check → alert-service's rule match → Postgres →
  API, the platform's actual reason for existing.

**Not implemented here**: consumer-driven contract testing (Pact or
similar); frontend tests (no Jest/Playwright suite yet).
