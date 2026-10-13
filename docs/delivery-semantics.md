# Delivery semantics

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
reconnect (ingestion's own [documented Kafka-outage test](phases/04-ingestion.md)
reproduced exactly this — a real message came back as two duplicate
copies, milliseconds apart); a Kafka consumer crashing or rebalancing
between finishing work and committing its offset, so the same message is
handed to whichever consumer picks up the partition next; and a producer
retrying a write whose acknowledgment was lost even though the write
itself succeeded (the kind of ambiguous-ack race documented in
[the Python rewrite's evaluation notes](../eval/results/FINAL_REPORT.md) after
a Kafka broker died mid-load-test).

**Three separate idempotency mechanisms catch this, each with a narrower
scope than "the whole pipeline" — know which one covers what:**

| Layer | Mechanism | Scope | Known gap |
|---|---|---|---|
| `stream-processor` (event-level) | Redis `SETNX` + TTL on `event_id` ([dedup.py](../services/stream-processor/dedup.py)) | Skips the InfluxDB write + windowed-aggregate update for a duplicate `telemetry.raw` message | The TTL is finite (`STREAM_DEDUP_TTL_SECONDS`, default 3600s) — a redelivery arriving *after* the key has expired is treated as a brand-new event. This is an accepted, disclosed tradeoff (unbounded Redis growth is worse), not a claim that duplicates are impossible |
| `anomaly-detector` (detection-level) | Postgres `idempotency_keys` table, atomic `INSERT ... ON CONFLICT DO NOTHING RETURNING` ([idempotency.py](../services/anomaly-detector/idempotency.py)) | Guards the *entire* detection run for a given `event_id` — the EWMA statistical baseline and the Isolation Forest's training buffer, not just the final `AnomalyDetected` publish | **This was a real, fixed bug, not a design decision**: an earlier version claimed idempotency only around the publish step, so a redelivered `telemetry.processed` message correctly avoided publishing a duplicate anomaly, but still silently folded the same reading into both statistical baselines a second time — a genuine correctness exposure under an ordinary, not rare, failure mode. Fixed by moving the claim before any detector state is touched; see the regression test in [test_idempotency.py](../services/anomaly-detector/tests/test_idempotency.py) that reproduces the exact redelivery and asserts the tracker only updates once |
| `alert-service` (alert-level) | Postgres partial unique index + `ON CONFLICT` on `(rule, device, metric)` while `status='OPEN'` ([store.py](../services/alert-service/store.py)) | Prevents a second alert row for a condition that's already open | Does not protect against a *different* rule firing twice for reasons upstream of alert-service — that's what the two layers above are for |

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

See [Streaming](phases/05-streaming.md),
[Anomaly Detection](phases/06-anomaly-detection.md), and
[Alerting](phases/07-alerting.md) for the full design of each layer,
and [eval/results/FINAL_REPORT.md](../eval/results/FINAL_REPORT.md) for the
live, reproducible tests behind every claim above (duplicate delivery,
zero-loss cross-validation, and the exact bug-and-fix account for the
anomaly-detector gap).

