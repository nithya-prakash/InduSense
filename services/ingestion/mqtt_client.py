"""MQTT connection and subscription handling, the Python port of mqtt.go.

Two real differences from the Go version, worth calling out explicitly
rather than pretending they don't exist:

1. Acknowledgment is a *client* method here (`client.ack(mid, qos)`), not a
   method on the message itself like Go's `msg.Ack()` — paho-mqtt's Python
   API ties manual ack to the client, keyed by message ID. InboundMessage's
   `ack` field is a small closure capturing exactly the (client, mid, qos)
   that call needs, so call sites everywhere else still just do `job.ack()`.

2. paho.mqtt.golang dispatches every inbound message on its own goroutine,
   so one slow handler never blocks another message (or the client's own
   keepalive) from being processed. paho-mqtt's Python client instead runs
   on_message callbacks sequentially, one at a time, from its own single
   network-loop thread — if on_message blocked for a long time, it would
   stall that thread, including the PINGREQ keepalive, risking a broker-side
   disconnect under sustained overload. The Go version's handler was
   willing to block up to 5 seconds trying to enqueue a message before
   giving up; this port intentionally does NOT block at all — it tries
   once, non-blockingly, and if the queue is full it immediately leaves the
   message unacked (same outcome: the broker redelivers it later) rather
   than risking that thread stall.

The other Go-specific concern this file's counterpart guards against — a
handler goroutine racing main()'s shutdown and panicking by sending into an
already-closed channel — has no Python equivalent at all: queue.Queue has
no concept of being "closed", so there is nothing to race against. See
main.py's shutdown sequence for the fuller explanation.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import paho.mqtt.client as mqtt

from config import Config
from metrics import mqtt_connections

_logger = logging.getLogger("ingestion")


class MQTTConnectTimeout(Exception):
    """The broker never accepted a connection within the startup timeout."""


@dataclass
class InboundMessage:
    """Carries an MQTT message into the worker pool along with its ack
    function. The message is only acked after it has been durably handed
    off to Kafka (or dead-lettered) — if ingestion crashes before that
    point, the persistent MQTT session redelivers it on reconnect,
    preserving at-least-once delivery end to end."""

    topic: str
    payload: bytes
    ack: Callable[[], None]


class AtomicBool:
    """A small thread-safe boolean flag — the Python equivalent of Go's
    sync/atomic.Bool, used here for the same reason: `connected` is written
    from the MQTT client's callback thread and read from the health-check
    HTTP handler's thread."""

    def __init__(self, value: bool = False):
        self._lock = threading.Lock()
        self._value = value

    def set(self, value: bool) -> None:
        with self._lock:
            self._value = value

    def get(self) -> bool:
        with self._lock:
            return self._value


def connect_mqtt(
    cfg: Config,
    connected: AtomicBool,
    jobs: "queue.Queue[InboundMessage]",
    shutdown_event: threading.Event,
    connect_timeout_seconds: float = 15.0,
) -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=cfg.mqtt_client_id,
        clean_session=False,  # persistent session: broker redelivers on reconnect if we never acked
        manual_ack=True,  # matches SetAutoAckDisabled(true) in the Go version
        reconnect_on_failure=True,
    )

    def on_connect(c: mqtt.Client, _userdata, _connect_flags, reason_code, _properties=None) -> None:
        if reason_code.is_failure:
            _logger.error("mqtt: connect failed: %s", reason_code)
            return
        connected.set(True)
        mqtt_connections.set(1)
        _logger.info("mqtt: connected to %s", cfg.mqtt_broker_url)
        _subscribe(c, cfg, jobs, shutdown_event)

    def on_disconnect(_c: mqtt.Client, _userdata, _disconnect_flags, reason_code, _properties=None) -> None:
        connected.set(False)
        mqtt_connections.set(0)
        if not shutdown_event.is_set():
            _logger.warning("mqtt: connection lost: %s (will auto-reconnect)", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=2, max_delay=30)

    host, port = _parse_broker_url(cfg.mqtt_broker_url)
    # connect_async + loop_start (rather than a plain connect()) is what
    # gives us "keep retrying the initial connection every 2s" — the Python
    # equivalent of Go's SetConnectRetry(true)/SetConnectRetryInterval(2s).
    client.connect_async(host, port, keepalive=30)
    client.loop_start()

    deadline = time.monotonic() + connect_timeout_seconds
    while time.monotonic() < deadline:
        if connected.get():
            return client
        time.sleep(0.05)

    client.loop_stop()
    raise MQTTConnectTimeout(f"mqtt connect to {cfg.mqtt_broker_url} timed out after {connect_timeout_seconds}s")


def _parse_broker_url(url: str) -> tuple[str, int]:
    # cfg.mqtt_broker_url looks like "tcp://mosquitto:1883" — paho's Python
    # client takes host/port separately rather than a single URL.
    without_scheme = url.split("://", 1)[-1]
    host, _, port = without_scheme.partition(":")
    return host, int(port) if port else 1883


def _subscribe(client: mqtt.Client, cfg: Config, jobs: "queue.Queue[InboundMessage]", shutdown_event: threading.Event) -> None:
    def handler(c: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
        if shutdown_event.is_set():
            # Shutting down: deliberately do not enqueue and do not ack —
            # the persistent MQTT session redelivers this message after
            # reconnect, same as any other unacked message.
            return

        def ack() -> None:
            c.ack(msg.mid, msg.qos)

        try:
            jobs.put_nowait(InboundMessage(topic=msg.topic, payload=msg.payload, ack=ack))
        except queue.Full:
            # Queue is full right now: don't ack, so the broker redelivers
            # later — this is the ingestion-side backpressure signal
            # propagating all the way back to MQTT delivery. See this
            # module's docstring for why this doesn't retry/block like the
            # Go version briefly does.
            _logger.warning("ingestion: queue saturated, leaving message on topic %s unacked", msg.topic)

    topics = [
        ("factory/+/machine/+/sensor/+/telemetry", cfg.mqtt_qos),
        ("factory/+/machine/+/status", cfg.mqtt_qos),
        ("factory/+/machine/+/events", cfg.mqtt_qos),
    ]
    for topic, qos in topics:
        client.message_callback_add(topic, handler)
        result, _mid = client.subscribe(topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            _logger.error("mqtt: failed to subscribe to %s: %s", topic, mqtt.error_string(result))
        else:
            _logger.info("mqtt: subscribed to %s (qos=%d)", topic, qos)
