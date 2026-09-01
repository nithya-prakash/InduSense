"""Command api is InduSense's REST + WebSocket API: authentication, RBAC,
tenant-scoped factory/device/telemetry/alert/incident endpoints, and a
real-time alert feed -- all wired against the domain logic ported earlier
(shared.auth, shared.incidents) rather than reimplementing it.

Python port of main.go, using FastAPI in place of Go's stdlib
net/http.ServeMux -- the one deliberate framework swap in this whole
rewrite (see the migration plan), since FastAPI's native OpenAPI
generation and WebSocket support are the closest match to what this
service actually needs. Because of that, openapi.go's hand-written JSON
spec has no equivalent here: FastAPI derives the same information (routes,
request/response shapes, the bearer-auth requirement) directly from the
route declarations below, which is the whole reason it was chosen. /docs
(Swagger UI) and /api/v1/openapi.json are still served at the exact same
paths as the Go version so nothing pointing at those URLs needs to change.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import ConnectionPool
from redis import Redis
from starlette.exceptions import HTTPException as StarletteHTTPException

from config import load_config
from handlers_alerts import register_alert_routes
from handlers_auth import register_auth_routes
from handlers_devices import register_device_routes
from handlers_factories import register_factory_routes
from handlers_health import register_health_routes
from handlers_incidents import register_incident_routes
from handlers_telemetry import register_telemetry_routes
from metrics import devices_by_status
from middleware import ClientIPResolver, LoggingMiddleware, RateLimiter, RequestIDMiddleware, make_require_permission, request_id_from_request
from response import APIError
from shared import auth, incidents, logging_utils, tracing
from ws_alerts import WSHub, register_ws_routes, run_alerts_fan_out

logger = logging_utils.init("api")

cfg = load_config()

if cfg.jwt_secrets_are_default():
    if cfg.environment == "production":
        raise RuntimeError(
            "refusing to start with API_ENVIRONMENT=production and a default "
            "JWT_ACCESS_SECRET/JWT_REFRESH_SECRET: these values are published "
            "in .env.example, so anyone can forge a valid access token for any "
            "user, organization, or permission set. Set both to real random "
            "secrets before running in production."
        )
    logger.critical(
        "JWT_ACCESS_SECRET and/or JWT_REFRESH_SECRET are still the default "
        "dev-only values from .env.example -- anyone who has read this public "
        "repo can forge a valid token for any user/organization/role. This is "
        "expected for local development but must never reach a real "
        "deployment; set API_ENVIRONMENT=production to make this a hard "
        "startup failure instead of a warning."
    )

pool = ConnectionPool(cfg.postgres_dsn, min_size=1, max_size=cfg.postgres_max_conns, open=True, kwargs={"autocommit": True})
redis_client = Redis(host=cfg.redis_addr.rsplit(":", 1)[0], port=int(cfg.redis_addr.rsplit(":", 1)[1]), password=cfg.redis_password or None, db=cfg.redis_db)

auth_svc = auth.Service(pool, redis_client, cfg.jwt_access_secret, cfg.jwt_refresh_secret, cfg.jwt_access_ttl_seconds, cfg.jwt_refresh_ttl_seconds)
incident_store = incidents.Store(pool, None)
ip_resolver = ClientIPResolver(cfg.trust_proxy_headers, cfg.trusted_proxy_cidrs)
rate_limiter = RateLimiter(redis_client, ip_resolver)
influx_client = InfluxDBClient(url=cfg.influx_url, token=cfg.influx_token, org=cfg.influx_org)
query_api = influx_client.query_api()
ws_hub = WSHub()

_shutdown_event = threading.Event()


def _permission_dep(permission: str):
    return make_require_permission(cfg.jwt_access_secret, permission)


def _run_device_gauge_refresher() -> None:
    """Periodically publishes device counts by status across all
    organizations -- deliberately not tenant-scoped, since this is a
    platform-wide operational gauge for Grafana, not a value returned to
    any single tenant's API response."""
    statuses = ["PROVISIONED", "ACTIVE", "OFFLINE", "MAINTENANCE", "DECOMMISSIONED"]
    while not _shutdown_event.wait(timeout=15.0):
        try:
            with pool.connection() as conn:
                rows = conn.execute("SELECT status, count(*) FROM devices GROUP BY status").fetchall()
        except Exception as exc:  # noqa: BLE001
            logger.error("api: device gauge refresh failed: %s", exc)
            continue
        seen = set()
        for status, n in rows:
            devices_by_status.labels(status=status).set(n)
            seen.add(status)
        for s in statuses:
            if s not in seen:
                devices_by_status.labels(status=s).set(0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    shutdown_tracing = tracing.init("api")
    ws_hub.bind_loop(asyncio.get_running_loop())

    fanout_thread = threading.Thread(target=run_alerts_fan_out, args=(_shutdown_event, cfg.kafka_brokers, cfg.topic_alerts, ws_hub), daemon=False)
    gauge_thread = threading.Thread(target=_run_device_gauge_refresher, daemon=False)
    fanout_thread.start()
    gauge_thread.start()

    logger.info("api: listening on :%s", cfg.port)
    yield

    _shutdown_event.set()
    fanout_thread.join()
    gauge_thread.join()
    pool.close()
    redis_client.close()
    influx_client.close()
    shutdown_tracing()
    logger.info("api: shutdown complete")


app = FastAPI(title="InduSense API", version="1.0.0", docs_url="/docs", openapi_url="/api/v1/openapi.json", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[cfg.cors_allowed_origin],
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
)
app.add_middleware(LoggingMiddleware)
app.add_middleware(RequestIDMiddleware)


# --- Error envelope, matching response.go's writeError shape everywhere ---


@app.exception_handler(APIError)
async def _api_error_handler(request: Request, exc: APIError) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "ERROR", "message": str(exc.detail)}
    body = {"error": {"code": detail["code"], "message": detail["message"], "request_id": request_id_from_request(request)}}
    return JSONResponse(status_code=exc.status_code, content=body, headers=exc.headers)


@app.exception_handler(StarletteHTTPException)
async def _http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    # Catches FastAPI's own automatic 404/405 for unmatched routes/methods
    # -- the equivalent of Go's catch-all "/" handler.
    code = "NOT_FOUND" if exc.status_code == 404 else "HTTP_ERROR"
    message = "no such route" if exc.status_code == 404 else (exc.detail if isinstance(exc.detail, str) else "request failed")
    body = {"error": {"code": code, "message": message, "request_id": request_id_from_request(request)}}
    return JSONResponse(status_code=exc.status_code, content=body)


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    body = {"error": {"code": "INVALID_BODY", "message": "request body must be valid JSON", "request_id": request_id_from_request(request)}}
    return JSONResponse(status_code=400, content=body)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    # Converts a panic in any handler into a clean 500 response instead of
    # crashing the process or leaking a stack trace to the client --
    # withRecover's equivalent.
    logger.error("api: recovered panic [request_id=%s]: %s", request_id_from_request(request), exc)
    body = {"error": {"code": "INTERNAL_ERROR", "message": "an unexpected error occurred", "request_id": request_id_from_request(request)}}
    return JSONResponse(status_code=500, content=body)


# --- Unauthenticated ------------------------------------------------------

app.include_router(register_health_routes(cfg, pool, redis_client))


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


auth_limit = rate_limiter.dependency("auth", cfg.rate_limit_auth_per_minute)
default_limit = rate_limiter.dependency("default", cfg.rate_limit_default_per_min)

app.include_router(register_auth_routes(auth_svc, ip_resolver, cfg.jwt_access_secret, auth_limit))
app.include_router(register_ws_routes(ws_hub, cfg.jwt_access_secret))

# --- Authenticated ----------------------------------------------------------
# Rate limiting is applied selectively per route within each handler
# module, matching main.go's exact route-by-route chain(...) calls --
# list/mutating endpoints get default_limit, single-resource GETs and
# drill-down listings don't, and incidents routes get none at all.

app.include_router(register_factory_routes(pool, _permission_dep, default_limit))
app.include_router(register_device_routes(pool, _permission_dep, default_limit))
app.include_router(register_telemetry_routes(pool, query_api, cfg.influx_bucket, _permission_dep, default_limit))
app.include_router(register_alert_routes(pool, _permission_dep, default_limit))
app.include_router(register_incident_routes(incident_store, _permission_dep))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(cfg.port))
