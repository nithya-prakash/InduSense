"""MQTT publishing, the Python port of publisher.go."""

from __future__ import annotations

import logging
import time

import paho.mqtt.client as mqtt

_logger = logging.getLogger("simulator")


class MQTTPublishError(Exception):
    pass


class MQTTPublisher:
    """Wraps a paho MQTT client configured for automatic reconnection, so
    a transient broker outage doesn't kill the simulator."""

    def __init__(self, broker_url: str, client_id: str, qos: int, connect_timeout_seconds: float = 15.0):
        self._qos = qos
        self._client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, reconnect_on_failure=True)
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        self._client.on_connect = lambda *a: _logger.info("mqtt: connected to %s", broker_url)
        self._client.on_disconnect = lambda *a: _logger.warning("mqtt: connection lost (will auto-reconnect)")

        host, port = _parse_broker_url(broker_url)
        self._client.connect_async(host, port, keepalive=30)
        self._client.loop_start()

        deadline = time.monotonic() + connect_timeout_seconds
        while time.monotonic() < deadline:
            if self._client.is_connected():
                return
            time.sleep(0.05)
        self._client.loop_stop()
        raise MQTTPublishError(f"mqtt: connect to {broker_url} timed out after {connect_timeout_seconds}s")

    def publish(self, topic: str, payload: bytes, timeout: float = 5.0) -> None:
        info = self._client.publish(topic, payload, qos=self._qos, retain=False)
        try:
            info.wait_for_publish(timeout=timeout)
        except (ValueError, RuntimeError) as exc:
            raise MQTTPublishError(f"publish to {topic} failed: {exc}") from exc
        if not info.is_published():
            raise MQTTPublishError(f"publish to {topic} timed out after {timeout}s")

    def disconnect(self) -> None:
        self._client.disconnect()
        self._client.loop_stop()


def _parse_broker_url(url: str) -> tuple[str, int]:
    without_scheme = url.split("://", 1)[-1]
    host, _, port = without_scheme.partition(":")
    return host, int(port) if port else 1883
