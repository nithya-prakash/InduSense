"""Auth/permission dependencies, request-ID + logging middleware, CORS,
rate limiting, and client-IP resolution -- the Python port of
middleware.go.
"""

from __future__ import annotations

import ipaddress
import logging
import time
import uuid
from collections.abc import Callable

from fastapi import Depends, Request, Response
from redis import Redis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from metrics import api_request_duration_seconds, api_requests_total
from response import APIError
from shared import auth

_logger = logging.getLogger("api")


def request_id_from_request(request: Request) -> str:
    return getattr(request.state, "request_id", "")


def claims_from_request(request: Request) -> auth.Claims | None:
    return getattr(request.state, "claims", None)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Assigns a request ID (from an incoming X-Request-ID header if
    present, so a caller's own tracing ID is preserved, or a fresh UUID
    otherwise) and echoes it back on the response."""

    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """Logs one structured line per request -- method, path, status,
    duration, request ID -- after the handler completes."""

    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        duration = time.monotonic() - start
        _logger.info(
            "request handled",
            extra={
                "method": request.method, "path": request.url.path, "status": response.status_code,
                "duration_ms": int(duration * 1000), "request_id": request_id_from_request(request),
            },
        )
        from metrics import api_request_duration_seconds, api_requests_total

        api_requests_total.labels(method=request.method, path=request.url.path, status=str(response.status_code)).inc()
        api_request_duration_seconds.labels(method=request.method, path=request.url.path).observe(duration)
        return response




# --- Auth / RBAC dependencies (requireAuth / requirePermission) --------


def _bearer_token(request: Request) -> str:
    h = request.headers.get("Authorization", "")
    prefix = "Bearer "
    if len(h) > len(prefix) and h.startswith(prefix):
        return h[len(prefix):]
    return ""


def make_require_auth(access_secret: str) -> Callable[[Request], auth.Claims]:
    """Parses the Bearer access token, validates it, and returns its
    Claims. Every resource handler downstream scopes its Postgres/InfluxDB
    queries by claims.organization_id -- never by anything the client
    supplies -- which is what makes tenant isolation enforced at the
    backend rather than trusted from the frontend."""

    def dependency(request: Request) -> auth.Claims:
        token_string = _bearer_token(request)
        if not token_string:
            raise APIError(401, "UNAUTHENTICATED", "missing or malformed Authorization header")
        try:
            claims = auth.parse_and_validate(token_string, access_secret, auth.TOKEN_TYPE_ACCESS)
        except Exception:  # noqa: BLE001
            raise APIError(401, "UNAUTHENTICATED", "invalid or expired access token")
        request.state.claims = claims
        return claims

    return dependency


def make_require_permission(access_secret: str, permission: str) -> Callable[..., auth.Claims]:
    """Must run after auth. permission == "" means "authenticated only,
    no specific permission required" -- matches Go's authed(h, "") case."""
    require_auth_dep = make_require_auth(access_secret)

    def dependency(claims: auth.Claims = Depends(require_auth_dep)) -> auth.Claims:
        if permission and not auth.has_permission(claims.permissions, permission):
            raise APIError(403, "FORBIDDEN", f"requires permission {permission!r}")
        return claims

    return dependency


# --- Client IP resolution -----------------------------------------------


class ClientIPResolver:
    """Decides what "the client's IP" means for rate limiting and auth
    audit logging. By default it's just the TCP peer address --
    X-Forwarded-For is never consulted, because it's just a header any
    direct client can set to whatever it wants. When trust_proxy_headers
    is enabled, X-Forwarded-For is trusted ONLY when the actual TCP peer
    connecting to this process is itself in trusted_proxies (e.g. a known
    reverse proxy/load balancer address) -- a client that connects
    directly, bypassing that proxy, still can't spoof its way past rate
    limiting by setting the header itself."""

    def __init__(self, trust_proxy_headers: bool, trusted_cidrs: list[str]):
        self._trust_proxy_headers = trust_proxy_headers
        self._trusted_proxies = []
        for entry in trusted_cidrs:
            cidr = entry
            if "/" not in cidr:
                try:
                    ip = ipaddress.ip_address(entry)
                    cidr = f"{entry}/32" if ip.version == 4 else f"{entry}/128"
                except ValueError as exc:
                    raise ValueError(f"invalid entry {entry!r} in API_TRUSTED_PROXY_CIDRS: {exc}") from exc
            try:
                self._trusted_proxies.append(ipaddress.ip_network(cidr, strict=False))
            except ValueError as exc:
                raise ValueError(f"invalid entry {entry!r} in API_TRUSTED_PROXY_CIDRS: {exc}") from exc

    def resolve(self, request: Request) -> str:
        """Returns just the IP, never the port -- the ephemeral source
        port differs on every new TCP connection, so keying rate limits on
        it would put every single request in its own bucket and never
        actually limit anything."""
        peer = request.client.host if request.client else ""

        if not self._trust_proxy_headers or not self._is_trusted_proxy(peer):
            return peer

        fwd = request.headers.get("X-Forwarded-For", "")
        if not fwd:
            return peer
        # X-Forwarded-For is a comma-separated hop chain appended to by
        # each proxy in the path; the left-most entry is the original
        # client as seen by the first (trusted) proxy.
        first = fwd.split(",")[0].strip()
        return first or peer

    def _is_trusted_proxy(self, peer: str) -> bool:
        try:
            ip = ipaddress.ip_address(peer)
        except ValueError:
            return False
        return any(ip in net for net in self._trusted_proxies)


# --- Rate limiting --------------------------------------------------------

# Makes the increment-and-set-expiry atomic. A plain INCR followed by a
# separate EXPIRE call has a window between the two Redis round-trips
# where a crash or cancelled request leaves the key permanently without a
# TTL -- harmless in practice since the key already encodes its own minute
# and would just become inert clutter, but a Lua script closes the gap for
# free since Redis already guarantees atomic script execution.
_RATE_LIMIT_SCRIPT = """
local count = redis.call("INCR", KEYS[1])
if count == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return count
"""


class RateLimiter:
    """A Redis-backed fixed-window limiter: atomically increment a
    per-window counter keyed by (client IP, route bucket), set its expiry
    only on the first increment of the window, and reject once the limit
    is exceeded. Fixed-window is simpler than a sliding-window/token-
    bucket and sufficient here."""

    def __init__(self, redis_client: Redis, ip_resolver: ClientIPResolver):
        self.redis = redis_client
        self.ip_resolver = ip_resolver
        self._script = redis_client.register_script(_RATE_LIMIT_SCRIPT)

    def dependency(self, bucket: str, limit: int) -> Callable[[Request, Response], None]:
        def dep(request: Request, response: Response) -> None:
            window_key = f"ratelimit:{bucket}:{self.ip_resolver.resolve(request)}:{int(time.time()) // 60}"
            try:
                count = self._script(keys=[window_key], args=[60])
            except Exception as exc:  # noqa: BLE001
                # Fail open: Redis being briefly unavailable shouldn't take
                # the whole API down.
                _logger.error("api: rate limiter redis error (failing open): %s", exc)
                return

            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(max(0, limit - int(count)))

            if int(count) > limit:
                raise APIError(
                    429, "RATE_LIMITED", "too many requests, try again later",
                    headers={"Retry-After": "60", "X-RateLimit-Limit": str(limit), "X-RateLimit-Remaining": "0"},
                )

        return dep
