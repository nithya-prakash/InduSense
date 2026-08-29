"""Real-time alert WebSocket feed, the Python port of ws_alerts.go.

Fans out alert events to every connected client whose JWT organization
matches the event's -- the same tenant-isolation rule the REST handlers
enforce, applied to a push channel instead of a pull one.

confluent_kafka's Consumer is a blocking/synchronous client, unlike
FastAPI's async WebSocket API, so the Kafka fan-out loop runs in its own
background thread (matching every other service's single-threaded consume
loop) and hands each broadcast over to the asyncio event loop via
run_coroutine_threadsafe -- the one genuinely new piece of plumbing this
port needed that the Go version's goroutines + channels didn't.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid

from confluent_kafka import Consumer, KafkaException
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from metrics import websocket_connections
from shared import auth
from shared.events import AlertEvent

_logger = logging.getLogger("api")


class WSHub:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._clients: dict[WebSocket, str] = {}  # websocket -> organization_id
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Captures the running event loop at startup so the background
        Kafka consumer thread can schedule broadcasts onto it."""
        self._loop = loop

    async def register(self, ws: WebSocket, org_id: str) -> None:
        async with self._lock:
            self._clients[ws] = org_id
            websocket_connections.set(len(self._clients))

    async def unregister(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.pop(ws, None)
            websocket_connections.set(len(self._clients))
        try:
            await ws.close()
        except Exception:  # noqa: BLE001
            pass

    async def _broadcast_async(self, org_id: str, payload: bytes) -> None:
        async with self._lock:
            targets = [ws for ws, connected_org in self._clients.items() if connected_org == org_id]
        for ws in targets:
            try:
                await ws.send_bytes(payload)
            except Exception as exc:  # noqa: BLE001
                _logger.error("api: websocket write failed, dropping client: %s", exc)
                await self.unregister(ws)

    def broadcast_threadsafe(self, org_id: str, payload: bytes) -> None:
        """Called from the background Kafka-consumer thread."""
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._broadcast_async(org_id, payload), self._loop)


def run_alerts_fan_out(shutdown_event: threading.Event, brokers: list[str], topic: str, hub: WSHub) -> None:
    """Consumes the `alerts` Kafka topic and broadcasts every event to
    matching WebSocket clients. Each API instance uses its own unique,
    never-reused consumer group so every instance receives a full copy of
    the topic -- correct for fan-out-to-many-viewers, unlike a shared
    consumer group (which would split messages across instances, the
    right behavior for work queues but the wrong one for broadcast)."""
    consumer = Consumer(
        {
            "bootstrap.servers": ",".join(brokers),
            # A fresh, never-reused group.id on every process start means
            # there is never a committed offset to resume from, so
            # auto.offset.reset is what actually decides where a new
            # connection starts reading -- "latest" so a freshly-restarted
            # API (or a client that just connected) sees only new alerts
            # from this moment forward, not a replay of the entire topic
            # history flooding in at once.
            "group.id": f"indusense-api-ws-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([topic])

    try:
        while not shutdown_event.is_set():
            try:
                msg = consumer.poll(1.0)
            except KafkaException as exc:
                _logger.error("api: alerts fan-out fetch error: %s", exc)
                continue
            if msg is None:
                continue
            if msg.error():
                _logger.error("api: alerts fan-out fetch error: %s", msg.error())
                continue

            # Best-effort: a missed broadcast on restart is acceptable for
            # a live feed.
            try:
                consumer.commit(message=msg, asynchronous=False)
            except KafkaException:
                pass

            try:
                evt = AlertEvent.model_validate_json(msg.value())
            except Exception as exc:  # noqa: BLE001
                _logger.error("api: alerts fan-out: malformed message: %s", exc)
                continue
            hub.broadcast_threadsafe(evt.organization_id, msg.value())
    finally:
        consumer.close()


def register_ws_routes(hub: WSHub, access_secret: str) -> APIRouter:
    router = APIRouter()

    @router.websocket("/ws/alerts")
    async def ws_alerts(websocket: WebSocket) -> None:
        """Upgrades to a WebSocket and streams every alert event for the
        caller's organization in real time. Browsers can't set a custom
        Authorization header on the WebSocket handshake, so the access
        token is accepted as a query parameter here specifically --
        documented as a simplification; a production system would issue a
        short-lived, single-use ws ticket instead of reusing the bearer
        token in a URL."""
        token_string = websocket.query_params.get("token", "")
        try:
            claims = auth.parse_and_validate(token_string, access_secret, auth.TOKEN_TYPE_ACCESS)
        except Exception:  # noqa: BLE001
            await websocket.close(code=1008)  # policy violation
            return

        await websocket.accept()
        await hub.register(websocket, claims.organization_id)
        _logger.info("api: websocket client connected (org=%s)", claims.organization_id)

        # Drain and discard any client-sent frames (this is a push-only
        # feed) purely to detect disconnects promptly.
        try:
            while True:
                await websocket.receive()
        except WebSocketDisconnect:
            pass
        finally:
            await hub.unregister(websocket)

    return router
