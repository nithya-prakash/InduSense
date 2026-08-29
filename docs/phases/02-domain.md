# 2. Domain

19 PostgreSQL tables created via 8 [golang-migrate](https://github.com/golang-migrate/migrate)
migrations in [migrations/](../../migrations/): the factory hierarchy
(`organizations` → `factories` → `production_lines` → `machines` → `devices`
→ `sensors`), auth/RBAC scaffolding (`users`, `roles`, `permissions`,
`role_permissions`, `user_roles` — populated in [Authentication](09-authentication.md)),
`device_credentials`, `alert_rules`/`alerts`, `incidents`/`incident_events`,
`maintenance_records`, `audit_logs`, and `idempotency_keys`. 60
indexes/constraints total, including partial-unique indexes enforcing "one
active credential per device" and "one active incident per machine" at the
database level.

[scripts/seed](../../scripts/seed/main.py) seeds a realistic hierarchy — one
organization ("Musterfabrik GmbH") across 4 German factories (Berlin,
Dresden, Munich, Hamburg), each with 5 production lines of 10 machines drawn
from 5 realistic industrial machine profiles (CNC mill, hydraulic press,
conveyor belt, welding robot, air compressor), each machine with one
provisioned device and 5 sensors — **1000 sensors total**, verified by
running the seed against live Postgres. The seed is idempotent (checks for
an existing organization slug) and transactional (a mid-seed failure leaves
zero partial rows, verified by injecting a duplicate-key failure). Device
credentials are bcrypt-hashed before storage — the plaintext secret exists
only transiently in memory during provisioning.

```bash
make migrate-up    # apply migrations
make seed          # seed the factory hierarchy (runs scripts/seed as a container)
make unit-test      # each service's own pytest suite
```

## Postgres connection pooling

Every service that talks to Postgres sizes its own `psycopg_pool.ConnectionPool`
explicitly via a `*_POSTGRES_MAX_CONNS` env var, rather than relying on a
library default, so pool size is a deliberate choice rather than something
that silently varies with the host machine. Reference-deployment defaults
(well under PostgreSQL's own default `max_connections=100`, leaving
headroom for admin sessions and one-shot jobs):

| Service | Env var | Default | Why |
|---|---|---|---|
| api | `API_POSTGRES_MAX_CONNS` | 10 | Serves concurrent HTTP requests |
| alert-service | `ALERT_POSTGRES_MAX_CONNS` | 10 | One shared pool for the alert store, incident store, and rule cache |
| anomaly-detector | `ANOMALY_POSTGRES_MAX_CONNS` | 5 | Only used for its periodically-refreshed device/sensor catalog, not per-event |
| `scripts/seed` | `SEED_POSTGRES_MAX_CONNS` | 4 | One-shot job |
| simulator | `SIM_POSTGRES_MAX_CONNS` | 4 | Pool exists only for one startup query, then closes |

`ingestion` and `stream-processor` don't appear here — neither talks to
Postgres directly (ingestion only bridges MQTT to Kafka; stream-processor
writes to InfluxDB, not Postgres).
