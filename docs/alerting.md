# Alert rules and response workflow

Alert rules live in the `alert_rules` Postgres table and are evaluated by
`services/alert-service` against the events that `anomaly-detector` publishes
to `anomalies.detected` (and machine events on `device.events`). Rules are
reloaded periodically, so a change in the table takes effect without a restart.

## Seeded rules (`scripts/seed`, org `musterfabrik-gmbh`)

| Rule | Metric | Condition | Threshold | Severity | Cooldown |
|---|---|---|---|---|---|
| High temperature | temperature | `GREATER_THAN` | 90 | CRITICAL | 300 s |
| Excessive vibration | vibration | `GREATER_THAN` | 8 | HIGH | 180 s |
| Power spike | power | `GREATER_THAN` | 45 | WARNING | 180 s |
| Repeated temperature anomalies | temperature | `ANOMALY_COUNT` | 3 within 300 s | HIGH | 300 s |
| Unexpected machine shutdown | machine_status | `ANOMALY_COUNT` | 1 within 300 s | WARNING | 300 s |

Conditions: `GREATER_THAN`, `LESS_THAN`, `OUTSIDE_RANGE` (min/max) and
`ANOMALY_COUNT` (N detections inside `window_seconds`). Severities:
`INFO`, `WARNING`, `HIGH`, `CRITICAL`. A rule can be scoped to a machine,
device or sensor; unscoped rules apply to the whole organization.

Values above are the rows queried from the seeded database; the detection
logic that feeds the rules is described in [ANOMALY-DETECTION.md](ANOMALY-DETECTION.md).

## Noise control

- **Cooldown:** while a rule's alert for a device is still open or inside its
  `cooldown_seconds`, repeats are suppressed
  (`alerts_suppressed_total{reason=...}`).
- **Escalation:** an alert that stays `OPEN` (unacknowledged) for
  `ALERT_ESCALATION_AFTER_SECONDS` (default 900 s, checked every 60 s) moves up
  one severity (`alerts_escalated_total`).
- **Incidents:** alerts for the same machine attach to one open incident
  instead of creating a new one (`alerts_attached_to_incident_total`).

## Response workflow

Incident states (enforced in `shared/incidents.py`):

```
OPEN -> ACKNOWLEDGED -> INVESTIGATING -> RESOLVED -> CLOSED
   (OPEN may also go to INVESTIGATING or RESOLVED; RESOLVED may reopen to
    INVESTIGATING; CLOSED is terminal)
```

1. Alert appears in the dashboard Alerts view (live WebSocket feed) and the
   alert-service log/console notification.
2. Operator **acknowledges** it. This stops escalation.
3. Operator moves the incident to **INVESTIGATING**, optionally assigns it, and
   adds a note. Every transition is written to the incident audit trail.
4. After the fix, **RESOLVED**, then **CLOSED**. A recurrence reopens a
   resolved incident to INVESTIGATING.

Permissions (RBAC) decide who may acknowledge, assign or resolve; see the API
docs at `/docs`.

## Metrics to watch

`alerts_generated_total`, `alerts_suppressed_total`, `alerts_escalated_total`,
`alerts_attached_to_incident_total`, `anomalies_detected_total` and
`dlq_messages_total` (should stay 0). The "InduSense — Anomaly & Alerting"
Grafana dashboard charts these.
