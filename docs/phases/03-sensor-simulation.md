# 3. Sensor Simulation

[simulator/](../../simulator/) loads the 1000 seeded sensors from Postgres
and publishes realistic telemetry over real MQTT (Eclipse Mosquitto) —
verified with a live broker, not mocked. Each of the 1000 sensors runs as
its own asyncio task with a per-sensor baseline that drifts slowly within
its operating range (bounded random walk with reversion toward the
midpoint) plus gaussian noise; 200 machine-controller tasks independently
drive RUNNING/STOPPED transitions per device. Actual MQTT publish calls are
handed off to a bounded thread pool, since paho-mqtt's confirmation wait is
blocking — asyncio tasks for the timer loops, threads for the one blocking
I/O call each makes.

Configurable fault injection (`ANOMALY_RATE`, `DUPLICATE_RATE`,
`OUT_OF_ORDER_RATE`, `NETWORK_DELAY_RATE`, `SENSOR_FAILURE_RATE` — all in
[.env.example](../../.env.example)) produces rates matching configuration,
verified live against real MQTT traffic. A bounded asyncio queue
(`SIM_QUEUE_CAPACITY`, default 20000) between sensor tasks and the MQTT
publisher thread pool provides backpressure — if publishing can't keep up,
new samples are dropped (counted, not silently lost) rather than queued
indefinitely. Graceful shutdown on `SIGINT`/`SIGTERM` drains in-flight
sensors and disconnects cleanly.

Published topics:
- `factory/{factory_id}/machine/{machine_id}/sensor/{sensor_id}/telemetry` — the telemetry event itself
- `factory/{factory_id}/machine/{machine_id}/status` — RUNNING/STOPPED transitions
- `factory/{factory_id}/machine/{machine_id}/events` — `SENSOR_FAILURE`, `SENSOR_RECOVERED`, `MACHINE_STOPPED`, `MACHINE_RUNNING`

```bash
make simulate          # run as a container on the compose network (profile: simulate)
make simulate-docker   # alias for `simulate` (kept for muscle memory)
```

There is no bare-host invocation anymore (unlike the original Go binary):
the simulator depends on `psycopg`/`paho-mqtt`, which this environment's
local Python toolchain can't install — see
[17 — Python rewrite](17-python-rewrite.md).
