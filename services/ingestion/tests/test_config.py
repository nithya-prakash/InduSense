from config import load_config


def test_load_config_defaults(monkeypatch):
    for key in (
        "MQTT_BROKER_URL", "MQTT_BROKER_HOST", "MQTT_BROKER_PORT", "MQTT_CLIENT_ID_PREFIX",
        "MQTT_QOS", "KAFKA_BROKERS", "KAFKA_TOPIC_TELEMETRY_RAW", "KAFKA_TOPIC_DEVICE_EVENTS",
        "KAFKA_TOPIC_DEAD_LETTER", "INGESTION_WORKER_POOL_SIZE", "INGESTION_QUEUE_CAPACITY",
        "INGESTION_KAFKA_MAX_RETRIES", "INGESTION_KAFKA_RETRY_BASE_MS", "INGESTION_BREAKER_THRESHOLD",
        "INGESTION_BREAKER_COOLDOWN_S", "INGESTION_PORT",
    ):
        monkeypatch.delenv(key, raising=False)

    cfg = load_config()

    assert cfg.mqtt_broker_url == "tcp://localhost:1883"
    assert cfg.mqtt_client_id == "indusense-ingestion"
    assert cfg.mqtt_qos == 1
    assert cfg.kafka_brokers == ["localhost:9094"]
    assert cfg.topic_telemetry_raw == "telemetry.raw"
    assert cfg.topic_device_events == "device.events"
    assert cfg.topic_dead_letter == "dead-letter"
    assert cfg.worker_pool_size == 50
    assert cfg.queue_capacity == 10000
    assert cfg.kafka_max_retries == 5
    assert cfg.kafka_retry_base_delay_seconds == 1.0
    assert cfg.breaker_failure_threshold == 5
    assert cfg.breaker_cooldown_seconds == 15.0
    assert cfg.http_port == "8081"


def test_load_config_respects_overrides(monkeypatch):
    monkeypatch.setenv("MQTT_BROKER_HOST", "mosquitto")
    monkeypatch.setenv("MQTT_BROKER_PORT", "1884")
    monkeypatch.setenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
    monkeypatch.setenv("INGESTION_WORKER_POOL_SIZE", "7")
    monkeypatch.setenv("INGESTION_KAFKA_RETRY_BASE_MS", "250")
    monkeypatch.delenv("MQTT_BROKER_URL", raising=False)

    cfg = load_config()

    assert cfg.mqtt_broker_url == "tcp://mosquitto:1884"
    assert cfg.kafka_brokers == ["kafka-1:9092", "kafka-2:9092"]
    assert cfg.worker_pool_size == 7
    assert cfg.kafka_retry_base_delay_seconds == 0.25


def test_load_config_falls_back_on_unparseable_int(monkeypatch):
    monkeypatch.setenv("INGESTION_WORKER_POOL_SIZE", "not-a-number")

    cfg = load_config()

    assert cfg.worker_pool_size == 50
