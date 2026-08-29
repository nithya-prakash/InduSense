# 10. APIs

[services/api](../../services/api/) is the REST + WebSocket surface, built
on **FastAPI** — the one deliberate framework swap in the later Python
rewrite (see [17](17-python-rewrite.md)), chosen for built-in OpenAPI docs
and native WebSocket support. `shared/auth.py` and `shared/incidents.py` are
wired in as real middleware/handlers, not reimplemented.

**Every handler scopes its Postgres/InfluxDB queries by the JWT's
`organization_id`, never by anything the client supplies.** Verified live:
an org B user listing factories saw only their own Stuttgart plant, never
org A's four; fetching org A's factory ID directly by URL returned `404
NOT_FOUND` (not `403`) — correct behavior, since confirming a cross-tenant
resource *exists* is itself a leak. RBAC was verified the same way: a
VIEWER token got `403 FORBIDDEN` provisioning a device, `200` reading
factories.

**Real bugs were found and fixed during live verification:**
1. The Redis-backed rate limiter originally keyed on the raw client address
   including the port, which differs on every TCP connection — it silently
   never limited anything until fixed to strip the port.
2. Unmatched routes returned a plain default 404 instead of the platform's
   consistent JSON error envelope — fixed with an explicit exception
   handler.

**The full incident lifecycle was verified through the actual HTTP API**:
`OPEN → ACKNOWLEDGED` succeeded (204), `ACKNOWLEDGED → CLOSED` was correctly
rejected (409, skipping straight past RESOLVED), and `GET /incidents/{id}`
returned the complete audit trail with both transitions. Rate limiting was
verified by hammering `/auth/login` repeatedly: requests within budget
succeeded, the rest got real `429`s with `Retry-After` headers — see
[17](17-python-rewrite.md) for why this needs *concurrent* requests to test
correctly, not a slow sequential loop.

**`/ws/alerts` streams real-time alerts scoped by organization** — verified
by connecting two WebSocket clients (org A and org B), publishing an org-A
alert event, and confirming org A's client received it while org B's client
received nothing at all. Browsers can't set a custom `Authorization` header
on a WebSocket handshake, so the access token is accepted as a `?token=`
query parameter on this one endpoint specifically — documented as a
simplification; a production system would issue a short-lived, single-use
ws ticket instead of reusing a bearer token in a URL.

`GET /docs` serves FastAPI's native Swagger UI, generated from the route
definitions themselves rather than a hand-written spec — the original Go
build hand-wrote its OpenAPI spec; FastAPI's built-in generation replaced
that entirely during the rewrite, since a hand-written spec bought nothing
once the framework produces an accurate one for free.

```bash
curl -X POST localhost:8080/api/v1/auth/login -d '{"email":"admin@musterfabrik-gmbh.de","password":"ChangeMe123!"}'
curl localhost:8080/api/v1/factories -H "Authorization: Bearer <token>"
curl localhost:8080/docs   # Swagger UI
```

**Not implemented here, honestly deferred rather than rushed**:
`/ws/incidents` (incidents don't yet publish to their own Kafka topic);
device-count-aware pagination cursors (offset pagination only).

### Dead-letter queue

Every service that consumes from Kafka (ingestion, stream-processor,
anomaly-detector, alert-service) writes malformed or unrecoverably-failed
messages to the shared `dead-letter` topic — see `DeadLetterRecord` in
[shared/events.py](../../shared/events.py) for the shape every service
uses. **This is write-only: there is no admin API, browsing UI, retry
mechanism, or automated reprocessing built into InduSense itself.** To
inspect what's in `dead-letter`, use standard Kafka tooling:

```bash
open http://localhost:8089    # Kafka UI: Topics -> dead-letter -> Messages

docker exec -it indusense-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic dead-letter --from-beginning
```

Reprocessing a dead-lettered message today means manually re-publishing its
`original_payload` to the appropriate source topic by hand — there is no
one-click retry.
