# 15. Load Testing

[load-tests/](../../load-tests/) has three k6 scripts, each testing a
different resource shape. Numbers below are **measured, not invented** —
real `k6 run` output against the running stack — but were captured against
the original Go backend and have not been re-measured against the Python
rewrite (see the note in [docs/phases/README.md](README.md)). They're
single-machine numbers (the k6 client and the stack share one laptop's
CPU), so treat them as functional/correctness proof and relative
comparisons, not a production capacity number.

**[dashboard-read-load.js](../../load-tests/dashboard-read-load.js)**
simulates 50 concurrent logged-in operators polling the same endpoints the
real dashboard calls, each 1–3 seconds — not a tight request loop. Sustained
for 40s after a 20s ramp-up:

- **76–78 req/s aggregate throughput**, ~5600 requests total per run
- **p95 latency 6.4–6.5ms overall**; the one endpoint querying InfluxDB via
  Flux rather than Postgres ran noticeably slower (~10ms), as expected
- **99.92–100% of checks passed**; the failures that did occur were all
  genuine `429 RATE_LIMITED` responses from the per-identity rate limiter
  doing its job at a boundary the test happened to sit near, not
  application errors

**[auth-rate-limit.js](../../load-tests/auth-rate-limit.js)** deliberately
fires 30 login attempts from **one** shared identity to prove the login
limiter holds under a real k6-generated burst: 20 requests got a genuine
401 (wrong password) before the budget ran out, then the remaining 10
correctly got 429.

**[websocket-scale.js](../../load-tests/websocket-scale.js)** ramps to 200
concurrent `/ws/alerts` connections, held open 20s: 100% connected, 100%
clean close across 250 completed connection lifecycles, with WebSocket
handshake p95 of 3.76–3.86ms even at full concurrency. This measures
connection scale, not delivery — real message delivery over this endpoint
was proven separately in the [Dashboard](11-dashboard.md) phase's browser
verification.

```bash
make load-test   # runs all three against whatever's currently running
```

**Not implemented here**: a distributed load-generation setup (k6 on
separate hardware, or k6 Cloud); MQTT/Kafka pipeline throughput under
sustained heavy load specifically via k6 (the simulator already exercises
that path); soak/endurance testing.
