"""Exercises the running api service over real HTTP against real
Postgres/Redis -- not a mocked handler test. Targets the docker-compose
stack's api container (default http://localhost:8080) and the demo data
scripts/seed puts there: two organizations ("musterfabrik-gmbh" and
"zweite-firma-gmbh", the latter seeded specifically so tenant isolation
has something real to fail against) and one demo user per role, password
"ChangeMe123!".

Every test skips (not fails) if the API isn't reachable, so this package
degrades gracefully in an environment where `make up` hasn't been run,
while still catching real regressions whenever it has.

Python port of tests/integration/api_test.go.
"""

from __future__ import annotations

import os

import pytest
import requests

DEMO_PASSWORD = "ChangeMe123!"


def base_url() -> str:
    return os.environ.get("API_BASE_URL", "http://localhost:8080")


def flush_rate_limit_buckets(redis_client) -> None:
    """Clears every rate-limit counter so the call that follows always
    starts from a clean budget -- see this package's module docstring, and
    login() below, for why a single flush at process start isn't enough
    on its own (it wouldn't cover a test in this file, like
    test_rate_limit_login_endpoint_eventually_returns_429, that
    deliberately exhausts the same bucket). Called from login() rather
    than once per test so every login attempt -- regardless of which
    test, or what ran immediately before it -- gets its own clean shot,
    without changing what the rate limiter itself does or how many
    requests it takes to trip it."""
    if redis_client is None:
        return
    for key in redis_client.scan_iter("ratelimit:*"):
        redis_client.delete(key)


@pytest.fixture()
def url(redis_client):
    """Skips (not fails) if the api isn't reachable -- mirrors Go's
    requireLiveAPI helper."""
    u = base_url()
    try:
        resp = requests.get(f"{u}/live", timeout=3.0)
    except requests.RequestException as exc:
        pytest.skip(f"api not reachable at {u}, skipping: {exc}")
    if resp.status_code != 200:
        pytest.skip(f"api at {u} not healthy (status {resp.status_code}), skipping")
    return u


def login(redis_client, api_url: str, email: str, password: str) -> str:
    """Returns a fresh access token for the given demo user, failing the
    test (not skipping) if the seeded credentials don't work -- that's a
    real regression, not an environment problem.

    This suite doesn't set a synthetic X-Forwarded-For to dodge the auth
    endpoint's rate limit -- that would rely on the API trusting a
    client-supplied header for its own rate-limit key, exactly the
    spoofing vulnerability an earlier audit found and closed (see
    ClientIPResolver in services/api/middleware.py). The header is
    correctly ignored by default, so every login in this file legitimately
    shares one real-IP "auth" bucket, same as it would for any real
    client."""
    flush_rate_limit_buckets(redis_client)
    resp = requests.post(f"{api_url}/api/v1/auth/login", json={"email": email, "password": password}, timeout=5.0)
    assert resp.status_code == 200, f"login for {email}: expected 200, got {resp.status_code}"
    token = resp.json().get("access_token", "")
    assert token, f"login for {email} returned an empty access token"
    return token


def authed_get(api_url: str, path: str, token: str) -> requests.Response:
    return requests.get(f"{api_url}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=5.0)


def test_login_valid_credentials_returns_tokens(url, redis_client):
    token = login(redis_client, url, "admin@musterfabrik-gmbh.de", DEMO_PASSWORD)
    assert len(token) >= 20, f"access token looks too short to be a real JWT: {token!r}"


def test_login_wrong_password_returns_401(url, redis_client):
    flush_rate_limit_buckets(redis_client)  # makes its own raw request rather than calling login(), so it needs its own clean budget
    resp = requests.post(f"{url}/api/v1/auth/login", json={"email": "admin@musterfabrik-gmbh.de", "password": "definitely-wrong"}, timeout=5.0)
    assert resp.status_code == 401, f"expected 401 for wrong password, got {resp.status_code}"
    assert resp.json().get("error", {}).get("code") == "INVALID_CREDENTIALS"


def test_protected_endpoint_no_token_returns_401(url):
    resp = requests.get(f"{url}/api/v1/factories", timeout=5.0)
    assert resp.status_code == 401, f"expected 401 with no Authorization header, got {resp.status_code}"


def test_protected_endpoint_malformed_token_returns_401(url):
    resp = requests.get(f"{url}/api/v1/factories", headers={"Authorization": "Bearer not-a-real-jwt"}, timeout=5.0)
    assert resp.status_code == 401, f"expected 401 with a malformed token, got {resp.status_code}"


def test_tenant_isolation_factories_scoped_to_organization(url, redis_client):
    """The single most important test in this file: it proves -- against
    the real API and real Postgres, not by reading the query in
    handlers_factories.py -- that an organization's admin can never see
    another organization's factories. "Zweite Firma GmbH" was seeded
    specifically to make this claim testable: its only factory is
    "Stuttgart Plant" in Stuttgart, a city/name that never appears among
    musterfabrik-gmbh's seeded factories (Berlin, Dresden, Munich,
    Hamburg, ...)."""

    def fetch_factories(token: str) -> list[dict]:
        resp = authed_get(url, "/api/v1/factories?limit=100", token)
        assert resp.status_code == 200, f"GET /api/v1/factories: expected 200, got {resp.status_code}"
        return resp.json()["items"]

    org1_token = login(redis_client, url, "admin@musterfabrik-gmbh.de", DEMO_PASSWORD)
    org2_token = login(redis_client, url, "admin@zweite-firma-gmbh.de", DEMO_PASSWORD)

    org1_factories = fetch_factories(org1_token)
    org2_factories = fetch_factories(org2_token)

    assert org1_factories, "expected musterfabrik-gmbh to have seeded factories, got none"
    for f in org1_factories:
        assert f["name"] != "Stuttgart Plant", f"tenant isolation broken: musterfabrik-gmbh admin can see zweite-firma-gmbh's factory {f['name']!r}"

    assert len(org2_factories) == 1 and org2_factories[0]["name"] == "Stuttgart Plant", (
        f"expected zweite-firma-gmbh to see exactly its own factory (Stuttgart Plant), got {org2_factories!r}"
    )


def test_rbac_viewer_cannot_provision_device_admin_can(url, redis_client):
    """Proves permission enforcement runs before the handler body: a
    VIEWER (no devices:write) is rejected with 403 before any
    request-body validation happens, while an ADMIN with the same
    malformed body reaches the handler and fails on business validation
    instead (400) -- proving the 403 for VIEWER really is about the
    permission, not a coincidentally-invalid request."""
    viewer_token = login(redis_client, url, "viewer@musterfabrik-gmbh.de", DEMO_PASSWORD)
    admin_token = login(redis_client, url, "admin@musterfabrik-gmbh.de", DEMO_PASSWORD)

    body = {"machine_id": "00000000-0000-0000-0000-000000000000", "serial_number": "SN-RBAC-TEST-DOES-NOT-EXIST"}

    def post(token: str) -> requests.Response:
        return requests.post(f"{url}/api/v1/devices", json=body, headers={"Authorization": f"Bearer {token}"}, timeout=5.0)

    viewer_resp = post(viewer_token)
    assert viewer_resp.status_code == 403, f"expected 403 for VIEWER provisioning a device, got {viewer_resp.status_code}"

    admin_resp = post(admin_token)
    assert admin_resp.status_code == 400, f"expected 400 for ADMIN with a non-existent machine_id (permission granted, business validation failed), got {admin_resp.status_code}"


def test_rate_limit_login_endpoint_eventually_returns_429(url, redis_client):
    """Fires far more login attempts than the configured per-minute limit
    (default 30, from API_RATE_LIMIT_AUTH_PER_MIN) and asserts at least
    one is rejected. Deliberately runs after the other logins in this
    file: they all share the same real-IP "auth" bucket now that spoofing
    a different identity via X-Forwarded-For no longer works (correctly).
    The loop bound of 60 is a safety margin, not the actual cost: the
    loop breaks the instant it sees a 429, so it only ever consumes
    ~(limit+1) real attempts in practice.

    Known, accepted consequence, not a bug: this test's whole job is to
    legitimately exhaust the real-IP "auth" bucket, so running this
    file's tests twice within the same 60-second window will make the
    second run's normal logins also see 429s until the window rolls
    over. That's the rate limiter correctly doing its job."""
    flush_rate_limit_buckets(redis_client)

    body = {"email": "rate-limit-probe@musterfabrik-gmbh.de", "password": "wrong"}
    saw_429 = False
    for _ in range(60):
        resp = requests.post(f"{url}/api/v1/auth/login", json=body, timeout=5.0)
        if resp.status_code == 429:
            saw_429 = True
            break
    assert saw_429, "expected at least one 429 Too Many Requests among 60 rapid login attempts, got none"


def test_health_endpoints_report_real_dependency_status(url):
    resp = requests.get(f"{url}/health", timeout=5.0)
    assert resp.status_code == 200, f"expected /health to report 200 with all dependencies up, got {resp.status_code}: {resp.text}"
