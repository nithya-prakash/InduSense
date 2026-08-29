# 7. Alerting

[services/alert-service](../../services/alert-service/) consumes
`anomalies.detected` and `device.events`, matches events against
organization-configured `alert_rules` (Postgres), and creates alerts with
deduplication, cooldown, and escalation — publishing to the `alerts` Kafka
topic and notifying via console (always) and an optional webhook.

**Rule matching** ([conditions.py](../../services/alert-service/conditions.py))
supports the four condition types from the schema: `GREATER_THAN`/
`LESS_THAN`/`OUTSIDE_RANGE` against the anomaly's raw value, and
`ANOMALY_COUNT` — "N anomalies within a window" — backed by an in-memory
per-`(rule, device)` timestamp tracker
([anomalycount.py](../../services/alert-service/anomalycount.py)), trimmed
to the rule's configured window on every insert. Four representative rules
ship in the seed data: a hard temperature threshold (90°C, CRITICAL), a
vibration threshold (HIGH), a power-spike threshold (WARNING), and "three
temperature anomalies within five minutes" (HIGH) — plus a sentinel rule
used only by the direct machine-shutdown handler.

**Deduplication + cooldown** ([store.py](../../services/alert-service/store.py)):
a new alert is refused if one for the same `(rule, device, metric)` is
still open (an `INSERT ... ON CONFLICT ... WHERE status = 'OPEN' DO NOTHING`
matching the partial unique index from the [Domain](02-domain.md) schema, so
this holds even under concurrent processing) — or if the last one for that
key resolved more recently than the rule's `cooldown_seconds`, which stops a
flapping condition from re-paging immediately after resolution. Verified
live: of 109 qualifying anomalies processed in one run, 65 became new
alerts and 25 were correctly suppressed as duplicates of an already-open
alert, with zero duplicate `OPEN` rows ever present in Postgres.

**Escalation**: a periodic sweep bumps any `OPEN` alert unacknowledged past
`ALERT_ESCALATION_AFTER_SECONDS` (default 900s) one rung up the
WARNING→HIGH→CRITICAL ladder and re-notifies.

**Machine shutdown** is a second, independent alert path: `device.events`
messages with `event_type = MACHINE_STOPPED` go straight to alert creation
(bypassing rule matching, since it's not a metric threshold), verified live
end-to-end from a synthetic MQTT publish through ingestion validation to a
real Postgres alert row and console notification.

**Notification providers** ([notify.py](../../services/alert-service/notify.py)):
a console provider (always on, zero external dependencies) and a webhook
provider (retry + circuit breaker via [shared/reliability.py](../../shared/reliability.py),
optional). A webhook failure is logged and counted, never dead-lettered or
retried against the message pipeline — the alert is already durably
recorded in Postgres by the time notification is attempted. Email/Slack are
intentionally not implemented (no paid service credentials available in
this environment) — the notification-provider interface exists specifically
so they can be added later without touching the alert engine.

```bash
curl localhost:8084/metrics   # alerts_generated_total{severity}, alerts_suppressed_total{reason}, alerts_escalated_total, notifications_sent_total{provider}
```
