# 1. Foundation

Infrastructure is defined in [docker-compose.yml](../../docker-compose.yml) and
was started and functionally verified (not just container-healthy):

| Component  | Verified |
|------------|----------|
| PostgreSQL 16 | ✅ query executed (`SELECT version()`) |
| Redis 7 | ✅ `SET`/`GET` round trip |
| Eclipse Mosquitto 2.0 (MQTT) | ✅ pub/sub round trip on a test topic |
| Apache Kafka 3.7 (KRaft, single node) | ✅ all 8 topics created; produce/consume round trip on `telemetry.raw` |
| InfluxDB 2.7 | ✅ `/health` returned `ready for queries and writes` |
| Kafka UI | ✅ running at http://localhost:8089 |

Kafka topics (see [infrastructure/docker/kafka-init-topics.sh](../../infrastructure/docker/kafka-init-topics.sh)):
`telemetry.raw` (12 partitions), `telemetry.processed` (12), `anomalies.detected` (6),
`alerts` (6), `incidents` (3), `device.events` (3), `audit.events` (3), `dead-letter` (3).

Telemetry topics are partitioned by `device_id` (enforced by the ingestion
service — see [Ingestion](04-ingestion.md)) so events for a given device stay
strictly ordered, while different devices process in parallel across
partitions.
