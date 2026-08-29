# 8. Incidents

Incident lifecycle management lives in the shared
[shared/incidents.py](../../shared/incidents.py) module — imported by both
`alert-service` (which opens/attaches incidents from alerts) and `api`
(which exposes the manual lifecycle actions) — rather than as a separate
"incident-service", since incidents originate 1:1 from alerts and there was
no independent workflow to justify a new microservice.

**"Don't create unlimited incidents from repeated alerts"** is enforced at
the database level, not just in application code: opening/attaching reuses
any incident already active for a machine (`INSERT ... ON CONFLICT
(machine_id) WHERE status IN (...) DO NOTHING`, racing safely against the
same partial unique index from the [Domain](02-domain.md) schema) and logs
a fresh alert as an `ALERT_ATTACHED` audit event instead of opening a
second incident. Verified live: 7 real alerts from a fresh traffic burst
produced exactly 6 incidents, with 1 correctly attached rather than
duplicated — confirmed directly in Postgres with zero machines ever holding
more than one active incident.

**State machine** (`OPEN → ACKNOWLEDGED/INVESTIGATING/RESOLVED → CLOSED`,
with `RESOLVED → INVESTIGATING` allowed for a recurrence but `CLOSED`
terminal) is unit-tested for every valid and invalid transition, then
exercised end-to-end against a real Postgres instance in
[shared/tests/test_incidents.py](../../shared/tests/test_incidents.py) —
open, attach, acknowledge, assign, investigate, resolve, reopen, re-resolve,
close, and verifying a post-closure alert opens a genuinely new incident.
That test caught two real foreign-key constraints the hard way (an
`incidents.alert_id` and `assigned_to` must reference *real* `alerts`/
`users` rows, not placeholder strings) — the schema's referential integrity
doing exactly its job.
