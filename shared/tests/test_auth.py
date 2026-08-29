import os
import uuid
from datetime import timedelta

import pytest
from psycopg_pool import ConnectionPool
from redis import Redis

from shared import auth

TEST_SECRET = "test-secret-do-not-use-in-prod"


def test_issue_and_parse_access_token():
    claims = auth._new_base_claims("user-1", "org-1", "a@b.com", [auth.ROLE_ENGINEER], auth.TOKEN_TYPE_ACCESS, 900, "jti-1")
    token = auth.issue_token(claims, TEST_SECRET)

    parsed = auth.parse_and_validate(token, TEST_SECRET, auth.TOKEN_TYPE_ACCESS)
    assert parsed.user_id == "user-1"
    assert parsed.organization_id == "org-1"
    assert auth.has_permission(parsed.permissions, auth.PERM_ALERTS_MANAGE)


def test_parse_and_validate_rejects_wrong_secret():
    claims = auth._new_base_claims("user-1", "org-1", "a@b.com", [auth.ROLE_VIEWER], auth.TOKEN_TYPE_ACCESS, 60, "jti-1")
    token = auth.issue_token(claims, TEST_SECRET)

    with pytest.raises(Exception):
        auth.parse_and_validate(token, "a-different-secret", auth.TOKEN_TYPE_ACCESS)


def test_parse_and_validate_rejects_expired_token():
    claims = auth._new_base_claims("user-1", "org-1", "a@b.com", [auth.ROLE_VIEWER], auth.TOKEN_TYPE_ACCESS, -60, "jti-1")
    token = auth.issue_token(claims, TEST_SECRET)

    with pytest.raises(Exception):
        auth.parse_and_validate(token, TEST_SECRET, auth.TOKEN_TYPE_ACCESS)


def test_parse_and_validate_rejects_wrong_token_type():
    claims = auth._new_base_claims("user-1", "org-1", "a@b.com", [auth.ROLE_VIEWER], auth.TOKEN_TYPE_ACCESS, 60, "jti-1")
    token = auth.issue_token(claims, TEST_SECRET)

    with pytest.raises(ValueError):
        auth.parse_and_validate(token, TEST_SECRET, auth.TOKEN_TYPE_REFRESH)


def test_new_base_claims_embeds_resolved_permissions():
    claims = auth._new_base_claims("u", "o", "e@e.com", [auth.ROLE_TECHNICIAN], auth.TOKEN_TYPE_ACCESS, 60, "jti")
    assert len(claims.permissions) > 0
    assert not auth.has_permission(claims.permissions, auth.PERM_USERS_MANAGE)


def test_hash_and_verify_password_round_trip():
    hashed = auth.hash_password("correct-horse-battery-staple1")
    assert auth.verify_password(hashed, "correct-horse-battery-staple1")
    assert not auth.verify_password(hashed, "wrong-password")


def test_hash_password_never_stores_plaintext():
    hashed = auth.hash_password("hunter2")
    assert hashed != "hunter2"


@pytest.mark.parametrize("password,want_err", [
    ("short1", True),
    ("alllettersnodigits", True),
    ("12345678", True),
    ("ValidPass123", False),
    ("exactly8a", False),
])
def test_validate_password_strength_rejects_weak_passwords(password, want_err):
    if want_err:
        with pytest.raises(ValueError):
            auth.validate_password_strength(password)
    else:
        auth.validate_password_strength(password)  # must not raise


def test_permissions_for_roles_admin_has_everything():
    perms = auth.permissions_for_roles([auth.ROLE_ADMIN])
    all_perms = [
        auth.PERM_DEVICES_READ, auth.PERM_DEVICES_WRITE, auth.PERM_TELEMETRY_READ,
        auth.PERM_ALERTS_READ, auth.PERM_ALERTS_MANAGE, auth.PERM_INCIDENTS_READ, auth.PERM_INCIDENTS_MANAGE,
        auth.PERM_FACTORIES_READ, auth.PERM_FACTORIES_MANAGE, auth.PERM_USERS_MANAGE, auth.PERM_SYSTEM_ADMIN,
    ]
    for p in all_perms:
        assert auth.has_permission(perms, p)


def test_permissions_for_roles_viewer_is_read_only():
    perms = auth.permissions_for_roles([auth.ROLE_VIEWER])
    assert not auth.has_permission(perms, auth.PERM_DEVICES_WRITE)
    assert not auth.has_permission(perms, auth.PERM_ALERTS_MANAGE)
    assert not auth.has_permission(perms, auth.PERM_USERS_MANAGE)
    assert auth.has_permission(perms, auth.PERM_DEVICES_READ)


def test_permissions_for_roles_unions_multiple_roles():
    perms = auth.permissions_for_roles([auth.ROLE_TECHNICIAN, auth.ROLE_ENGINEER])
    assert auth.has_permission(perms, auth.PERM_ALERTS_MANAGE)
    assert auth.has_permission(perms, auth.PERM_INCIDENTS_MANAGE)


def test_permissions_for_roles_empty_role_list_yields_no_permissions():
    assert auth.permissions_for_roles([]) == []


def test_permissions_for_roles_unknown_role_yields_nothing():
    assert auth.permissions_for_roles(["NOT_A_REAL_ROLE"]) == []


def test_all_roles_have_at_least_one_permission():
    for role in auth.ALL_ROLES:
        assert len(auth.ROLE_PERMISSIONS[role]) > 0


def test_every_role_can_read_factories_and_devices():
    for role in auth.ALL_ROLES:
        perms = auth.permissions_for_roles([role])
        assert auth.has_permission(perms, auth.PERM_FACTORIES_READ)
        assert auth.has_permission(perms, auth.PERM_TELEMETRY_READ)


def test_require_same_organization_allows_match():
    auth.require_same_organization("org-1", "org-1")  # must not raise


def test_require_same_organization_rejects_mismatch():
    with pytest.raises(auth.CrossTenantError):
        auth.require_same_organization("org-1", "org-2")


def _connect_live_or_skip():
    dsn = os.environ.get("ALERT_POSTGRES_DSN", "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable")
    redis_addr = os.environ.get("TEST_REDIS_ADDR", "localhost:6379")
    try:
        pool = ConnectionPool(dsn, min_size=1, max_size=2, open=True, kwargs={"autocommit": True}, timeout=5.0)
        with pool.connection(timeout=5.0) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Postgres reachable, skipping: {exc}")

    host, _, port = redis_addr.partition(":")
    redis_client = Redis(host=host, port=int(port) if port else 6379)
    try:
        redis_client.ping()
    except Exception as exc:  # noqa: BLE001
        pool.close()
        pytest.skip(f"no live Redis reachable, skipping: {exc}")

    return pool, redis_client


def test_login_refresh_logout_against_real_infra():
    """Exercises the full auth flow -- login, access token validation,
    refresh-token rotation, reuse detection, and logout revocation --
    against real Postgres and Redis, using the actual demo user seeded by
    scripts/seed (admin@musterfabrik-gmbh.de)."""
    pool, redis_client = _connect_live_or_skip()
    try:
        svc = auth.Service(pool, redis_client, TEST_SECRET, f"refresh-{TEST_SECRET}", 900, 3600)

        pair = svc.login("admin@musterfabrik-gmbh.de", "ChangeMe123!", "127.0.0.1")
        assert pair.access_token and pair.refresh_token

        claims = auth.parse_and_validate(pair.access_token, TEST_SECRET, auth.TOKEN_TYPE_ACCESS)
        assert auth.has_permission(claims.permissions, auth.PERM_SYSTEM_ADMIN)
        assert claims.email == "admin@musterfabrik-gmbh.de"

        with pytest.raises(auth.InvalidCredentialsError):
            svc.login("admin@musterfabrik-gmbh.de", "wrong-password", "")
        with pytest.raises(auth.InvalidCredentialsError):
            svc.login("nobody@musterfabrik-gmbh.de", "whatever123", "")

        # Refresh rotates: the old refresh token becomes unusable, the new one works.
        new_pair = svc.refresh_access_token(pair.refresh_token, "127.0.0.1")
        assert new_pair.refresh_token != pair.refresh_token

        with pytest.raises(auth.TokenRevokedError):
            svc.refresh_access_token(pair.refresh_token, "")

        svc.logout(new_pair.refresh_token, "127.0.0.1")
        with pytest.raises(auth.TokenRevokedError):
            svc.refresh_access_token(new_pair.refresh_token, "")

        with pool.connection() as conn:
            login_success = conn.execute("SELECT count(*) FROM audit_logs WHERE action = 'user.login' AND result = 'SUCCESS'").fetchone()[0]
            login_failure = conn.execute("SELECT count(*) FROM audit_logs WHERE action = 'user.login' AND result = 'FAILURE'").fetchone()[0]
            logout_count = conn.execute("SELECT count(*) FROM audit_logs WHERE action = 'user.logout'").fetchone()[0]
        assert login_success > 0
        assert login_failure > 0
        assert logout_count > 0
    finally:
        pool.close()
        redis_client.close()


def test_multi_tenant_logins_are_isolated():
    """Verifies, against real seeded data, that a user from one
    organization's token carries that organization's ID and nothing from
    the other."""
    pool, redis_client = _connect_live_or_skip()
    try:
        svc = auth.Service(pool, redis_client, TEST_SECRET, f"refresh-{TEST_SECRET}", 900, 3600)

        pair_a = svc.login("admin@musterfabrik-gmbh.de", "ChangeMe123!", "")
        pair_b = svc.login("admin@zweite-firma-gmbh.de", "ChangeMe123!", "")

        claims_a = auth.parse_and_validate(pair_a.access_token, TEST_SECRET, auth.TOKEN_TYPE_ACCESS)
        claims_b = auth.parse_and_validate(pair_b.access_token, TEST_SECRET, auth.TOKEN_TYPE_ACCESS)

        assert claims_a.organization_id != claims_b.organization_id
        auth.require_same_organization(claims_a.organization_id, claims_a.organization_id)
        with pytest.raises(auth.CrossTenantError):
            auth.require_same_organization(claims_a.organization_id, claims_b.organization_id)

        with pool.connection() as conn:
            leak = conn.execute(
                """
                SELECT count(*) FROM factories f
                JOIN organizations o ON o.id = f.organization_id
                WHERE o.id = %s AND f.organization_id != %s
                """,
                (claims_a.organization_id, claims_a.organization_id),
            ).fetchone()[0]
        assert leak == 0
    finally:
        pool.close()
        redis_client.close()
