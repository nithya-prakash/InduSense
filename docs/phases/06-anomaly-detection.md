# 6. Anomaly Detection

[services/anomaly-detector](../../services/anomaly-detector/) consumes
`telemetry.processed` and runs three independent detection levels on every
reading, publishing a combined result to `anomalies.detected` when any
fire. Full design writeup, including an honest evaluation of the Isolation
Forest, is in [docs/ANOMALY-DETECTION.md](../ANOMALY-DETECTION.md).

- **Rule-based**: value outside the sensor's seeded operating range, severity
  scaled by overshoot fraction.
- **Statistical**: EWMA rolling mean/stddev per `(device_id, metric)`,
  z-score against the pre-sample baseline, suppressed until 30+ samples.
- **Isolation Forest**: a real implementation (not a wrapped library) of
  Liu, Ting & Zhou (2008), ported from scratch to Python rather than
  switched to scikit-learn, to keep the mechanics as legible as the rest of
  the codebase. One forest is trained **per machine type**, not globally,
  because different machine types report different metric sets — a CNC
  mill's feature vector isn't the same shape or semantics as a hydraulic
  press's.

All three levels were verified firing together on live data:

```text
anomalies_detected_total{method="RULE"}              122
anomalies_detected_total{method="STATISTICAL"}          8
anomalies_detected_total{method="ISOLATION_FOREST"}     1
```

```bash
curl localhost:8083/forests    # which machine types currently have a trained forest
curl localhost:8083/metrics    # anomalies_detected_total{method}, isolation_forests_trained_total
```

**Idempotency**: publishing to `anomalies.detected` is guarded by an atomic
claim on the source telemetry event's `event_id`, via the `idempotency_keys`
table ([idempotency.py](../../services/anomaly-detector/idempotency.py),
scope `anomaly_detection`) — the same `INSERT ... ON CONFLICT DO NOTHING
RETURNING` pattern alert-service uses for alert dedup. Without it, Kafka
redelivering a `telemetry.processed` message would run detection again and
publish a second, distinct anomaly ID for the same physical reading — and,
downstream, a second alert/incident. A redelivered event is detected as
already-claimed and its detection result is simply not re-published; the
offset is still committed.

Publishing to `anomalies.detected` is wrapped in the same
retry-with-backoff-plus-circuit-breaker pattern as stream-processor's
InfluxDB/Kafka writes — see `/ready`'s `kafka_circuit_breaker` field.

**A real bug was found here during the later Python rewrite**: psycopg3
returns Postgres `uuid` columns as `uuid.UUID` objects rather than plain
strings, and the device-catalog lookup dict
([catalog.py](../../services/anomaly-detector/catalog.py)) was keyed by
those `UUID` objects while callers always looked up by plain string — every
lookup silently missed, so rule-based detection never fired for any device.
Caught by the live end-to-end test failing on the very first run against
the Python service; fixed with an explicit `str()` cast. See
[17 — Python rewrite](17-python-rewrite.md).
