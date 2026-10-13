import asyncio

from ws_alerts import WSHub


class _FakeWebSocket:
    """Stands in for starlette's WebSocket -- only the two send methods
    _broadcast_async can call are implemented, recording which one and
    with what argument."""

    def __init__(self):
        self.sent_text: list[str] = []
        self.sent_bytes: list[bytes] = []

    async def send_text(self, data: str) -> None:
        self.sent_text.append(data)

    async def send_bytes(self, data: bytes) -> None:
        self.sent_bytes.append(data)


def test_broadcast_sends_a_text_frame_not_binary():
    """Regression test for a real, shipped bug: _broadcast_async used to
    call ws.send_bytes(payload), which the browser WebSocket API delivers
    to onmessage as a Blob (its default binaryType) rather than a string.
    JSON.parse(evt.data) on a Blob throws synchronously, and the
    frontend's malformed-frame handler silently swallows it -- so the
    "live" alert feed's WebSocket connection stayed open and healthy while
    never actually updating the UI for a single new alert. Confirmed live
    against a real browser before this fix: the frame arrived at the
    transport layer but every deliver-to-UI attempt failed silently.
    Fixed by sending send_text with the decoded JSON string instead."""
    hub = WSHub()
    org_id = "11111111-1111-1111-1111-111111111111"
    fake_ws = _FakeWebSocket()

    async def run() -> None:
        hub.bind_loop(asyncio.get_running_loop())
        await hub.register(fake_ws, org_id)
        await hub._broadcast_async(org_id, b'{"event_type": "CREATED"}')

    asyncio.run(run())

    assert fake_ws.sent_text == ['{"event_type": "CREATED"}']
    assert fake_ws.sent_bytes == []


def test_normal_client_close_does_not_raise_an_unhandled_asgi_error():
    """Regression: the receive loop kept calling receive() after Starlette
    had already delivered the disconnect message; the next call raises
    WebSocketDisconnected (a RuntimeError), so every ordinary client close
    surfaced as an unhandled ASGI exception. The fake mirrors Starlette:
    one disconnect message, then an error on any further receive()."""
    from shared import auth
    from starlette.websockets import WebSocketDisconnected
    from ws_alerts import register_ws_routes

    secret = "test-secret-that-is-long-enough-for-hs256-use"
    token = auth.issue_token(
        auth._new_base_claims("u1", "11111111-1111-1111-1111-111111111111", "a@b.de", ["viewer"], auth.TOKEN_TYPE_ACCESS, 60, "j1"),
        secret,
    )

    class _ClosingClient:
        query_params = {"token": token}

        def __init__(self):
            self.disconnect_delivered = False
            self.closed = False

        async def accept(self) -> None:
            pass

        async def receive(self) -> dict:
            if self.disconnect_delivered:
                raise WebSocketDisconnected('Cannot call "receive" once a disconnect message has been received.')
            self.disconnect_delivered = True
            return {"type": "websocket.disconnect", "code": 1001}

        async def close(self, code: int = 1000) -> None:
            self.closed = True

    hub = WSHub()
    endpoint = register_ws_routes(hub, secret).routes[0].endpoint
    client = _ClosingClient()

    asyncio.run(endpoint(client))

    assert client.closed
    assert hub._clients == {}
