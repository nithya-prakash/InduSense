# Anomaly Detection

`services/anomaly-detector` consumes `telemetry.processed` and runs three
independent detection levels on every reading. All three run on every
sample (not short-circuited on the first hit); if more than one fires, the
combined `anomalies.detected` record reports the worst severity, the
highest score, and every contributing method, so a reading flagged by both
the rule engine and the isolation forest reads as more actionable than one
flagged by either alone.

## Level 1 — Rule-based

Each sensor was seeded with a `min_operating_value`/`max_operating_value`
(see [migrations/000001_core_hierarchy.up.sql](../migrations/000001_core_hierarchy.up.sql)).
A reading outside that range fires, with severity scaling by how far outside
as a fraction of the range's span (`>=50%` overshoot → CRITICAL, `>=20%` →
HIGH, else WARNING). Implementation: [rules.py](../services/anomaly-detector/rules.py).

This is the cheapest, fastest-firing detector and the one verified most
extensively live — real seeded data with injected spikes produced real
`RULE` anomalies in `anomalies.detected` during Phase 6 verification.

## Level 2 — Statistical (EWMA z-score)

Each `(device_id, metric)` series gets its own exponentially-weighted
moving mean and variance, updated online in O(1) per sample
([stats.py](../services/anomaly-detector/stats.py)). A sample's z-score is
computed against the baseline *before* that sample is folded in, so one
huge spike is judged against the prior baseline rather than a baseline it
just dragged toward itself. EWMA (rather than a plain cumulative average)
was chosen deliberately: it adapts to a genuine regime change instead of
being permanently anchored to whatever the series looked like at startup.
Firing is suppressed until a series has accumulated `ANOMALY_MIN_SAMPLES`
(default 30) — otherwise every series' first few readings would trivially
"deviate" from an unstable baseline.

## Level 3 — Isolation Forest

A real implementation of Liu, Ting & Zhou (2008)
([isolationforest.py](../services/anomaly-detector/isolationforest.py)):
each tree is built by recursively picking a random feature and a random
split value, isolating points in far fewer splits than it takes to isolate
a "normal" point buried in a dense cluster. Anomaly score is
`2^(-avg_path_length / c(subsample_size))`, the standard normalization from
the paper, giving a score in `[0, 1]` where values above ~0.6 are
conventionally anomalous.

**Feature vector.** The spec's suggested feature set
(temperature/vibration/rpm/pressure/power) doesn't uniformly apply here —
different machine types report different metric sets (e.g. a CNC mill
reports `current`, not `pressure`). So the feature vector for a device is
the ordered set of metrics its own machine type's sensors actually report
(5 metrics per profile in this seed data), and **one isolation forest is
trained per machine type**, not one global forest — mixing heterogeneous
metric semantics (comparing a "current" value to a "pressure" value as if
they were the same dimension) would make a single shared forest
statistically meaningless.

**Training is periodic, not one-shot batch.** A rolling buffer
(`ANOMALY_FOREST_BUFFER_SIZE`, default 512 feature vectors) accumulates per
machine type as telemetry arrives (`featurestore.py`), and a background
loop retrains that machine type's forest every `ANOMALY_FOREST_RETRAIN_SECONDS`
(default 120s) once at least 60 samples exist. Between retrains, incoming
events are scored against the current forest. This is what makes the
algorithm viable in a streaming system: no offline batch job, but also no
per-event retraining cost.

### Honest evaluation

**What was measured**: [test_isolationforest.py](../services/anomaly-detector/tests/test_isolationforest.py)
trains a forest on a synthetic 3-feature Gaussian cluster (mean 50, stddev 2
— standing in for a machine's steady operating band) and scores both
held-out in-distribution points and deliberately far-outlier points. Result,
captured directly from a test run:

```
avg normal score  = 0.4495
avg outlier score = 0.6766
```

Both land on the correct side of the conventional 0.6 anomaly threshold, and
the gap is large — this confirms the algorithm's core mechanism
(faster isolation of outliers) actually works in this implementation, not
just that the code compiles.

**What was NOT measured**: precision/recall against a labeled dataset of
real telemetry with known ground-truth anomalies. The simulator's
`ANOMALY_RATE` injects spikes, but that label isn't threaded through Kafka
to the anomaly detector, so there's no automated way yet to compute "of the
anomalies the simulator injected, what fraction did isolation forest catch,
and at what false-positive rate against normal traffic." Building that
evaluation harness (propagate a `ground_truth_anomaly` flag end-to-end
through MQTT → Kafka → detector, purely for offline evaluation, never
consumed by production logic) is a natural follow-up, marked here as
`NOT YET MEASURED` rather than assumed. Live testing during Phase 6
confirmed the mechanism produces plausible-looking flags on real spike data
end-to-end (Kafka → Postgres catalog → feature assembly → scoring →
`anomalies.detected`), which is a functional verification, not a
statistical one.

## Anomaly record schema

Published to `anomalies.detected` (see `AnomalyDetected` in
[shared/events.py](../shared/events.py)):

```json
{
  "anomaly_id": "uuid",
  "event_id": "uuid",
  "organization_id": "...", "factory_id": "...", "production_line_id": "...",
  "machine_id": "...", "device_id": "...", "sensor_id": "...",
  "metric": "temperature",
  "value": 105.3,
  "severity": "HIGH",
  "score": 0.42,
  "methods": ["RULE", "STATISTICAL"],
  "reason": "value 105.30 outside safe operating range [20.00, 90.00]; z-score 4.10 exceeds threshold 3.00 against rolling baseline",
  "detected_at": "2026-08-25T15:00:33Z"
}
```

## Reliability notes

- A malformed `telemetry.processed` message is dead-lettered and its offset
  committed (verified live).
- A Kafka publish failure on `anomalies.detected` is retried at the
  transport level by the writer's own acks/retry behavior, then
  dead-lettered on persistent failure; if the dead-letter write also fails,
  the offset is left uncommitted for redelivery — the same fail-safe pattern
  used throughout ingestion and the stream processor.
- The Postgres-backed catalog (machine type + sensor operating ranges) is
  loaded at startup and refreshed every `ANOMALY_CATALOG_REFRESH_SECONDS`
  (default 300s); a refresh failure logs and keeps serving the previous
  snapshot rather than blocking detection.
