#!/bin/bash
set -e

BOOTSTRAP="kafka:9092"

create_topic() {
  local topic="$1"
  local partitions="$2"
  echo "Creating topic: ${topic} (partitions=${partitions})"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions "${partitions}" --replication-factor 1
}

create_topic "telemetry.raw" 12
create_topic "telemetry.processed" 12
create_topic "anomalies.detected" 6
create_topic "alerts" 6
create_topic "incidents" 3
create_topic "device.events" 3
create_topic "audit.events" 3
create_topic "dead-letter" 3

echo "Topics created:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
