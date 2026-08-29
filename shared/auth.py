"""Password hashing, JWT issuance/validation with Redis-backed refresh-token
revocation, and RBAC permission resolution -- the Python port of pkg/auth
(and its one dependency, pkg/audit, folded in here as shared.audit).

Has no HTTP dependency itself, same as the Go version -- the api service
wires this into FastAPI dependencies.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from psycopg_pool import ConnectionPool
from redis import Redis

from shared import audit

TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"

# --- RBAC (rbac.go) ---------------------------------------------------

PERM_DEVICES_READ = "devices:read"
PERM_DEVICES_WRITE = "devices:write"
PERM_TELEMETRY_READ = "telemetry:read"
PERM_ALERTS_READ = "alerts:read"
PERM_ALERTS_MANAGE = "alerts:manage"
PERM_INCIDENTS_READ = "incidents:read"
PERM_INCIDENTS_MANAGE = "incidents:manage"
PERM_FACTORIES_READ = "factories:read"
PERM_FACTORIES_MANAGE = "factories:manage"
PERM_USERS_MANAGE = "users:manage"
PERM_SYSTEM_ADMIN = "system:admin"

ROLE_ADMIN = "ADMIN"
ROLE_FACTORY_MANAGER = "FACTORY_MANAGER"
ROLE_ENGINEER = "ENGINEER"
ROLE_TECHNICIAN = "TECHNICIAN"
ROLE_VIEWER = "VIEWER"

# The single source of truth for what each role can do. The seed script
# inserts role_permissions rows directly from this map (so the reference
# table in Postgres can never drift from what the runtime actually
# enforces), and Service.login() resolves a user's roles to this same set
# of permissions to embed in the JWT -- authorization is claims-based:
# once issued, a token carries its own permission list rather than
# requiring a role_permissions lookup on every request.
ROLE_PERMISSIONS: dict[str, list[str]] = {
    ROLE_ADMIN: [
        PERM_DEVICES_READ, PERM_DEVICES_WRITE, PERM_TELEMETRY_READ,
        PERM_ALERTS_READ, PERM_ALERTS_MANAGE, PERM_INCIDENTS_READ, PERM_INCIDENTS_MANAGE,
        PERM_FACTORIES_READ, PERM_FACTORIES_MANAGE, PERM_USERS_MANAGE, PERM_SYSTEM_ADMIN,
    ],
    ROLE_FACTORY_MANAGER: [
        PERM_DEVICES_READ, PERM_DEVICES_WRITE, PERM_TELEMETRY_READ,
        PERM_ALERTS_READ, PERM_ALERTS_MANAGE, PERM_INCIDENTS_READ, PERM_INCIDENTS_MANAGE,
        PERM_FACTORIES_READ, PERM_FACTORIES_MANAGE,
    ],
    ROLE_ENGINEER: [
        PERM_DEVICES_READ, PERM_DEVICES_WRITE, PERM_TELEMETRY_READ,
        PERM_ALERTS_READ, PERM_ALERTS_MANAGE, PERM_INCIDENTS_READ, PERM_INCIDENTS_MANAGE,
        PERM_FACTORIES_READ,
    ],
    ROLE_TECHNICIAN: [
        PERM_DEVICES_READ, PERM_TELEMETRY_READ,
        PERM_ALERTS_READ, PERM_INCIDENTS_READ, PERM_INCIDENTS_MANAGE,
        PERM_FACTORIES_READ,
    ],
    ROLE_VIEWER: [
        PERM_DEVICES_READ, PERM_TELEMETRY_READ, PERM_ALERTS_READ, PERM_INCIDENTS_READ, PERM_FACTORIES_READ,
    ],
}

ALL_ROLES = [ROLE_ADMIN, ROLE_FACTORY_MANAGER, ROLE_ENGINEER, ROLE_TECHNICIAN, ROLE_VIEWER]


def permissions_for_roles(roles: list[str]) -> list[str]:
    """Unions the permission sets of every given role, de-duplicated. A
    user with multiple roles gets the union, not the intersection -- the
    more permissive combination, which matches how RBAC systems
    conventionally combine multiple role grants."""
    seen: set[str] = set()
    out: list[str] = []
    for role in roles:
        for perm in ROLE_PERMISSIONS.get(role, []):
            if perm not in seen:
                seen.add(perm)
                out.append(perm)
    return out


def has_permission(permissions: list[str], required: str) -> bool:
    """system:admin is NOT an implicit wildcard -- the permission list
    from permissions_for_roles already expands ADMIN to every concrete
    permission, so a caller checking for e.g. devices:write against an
    ADMIN's token finds it listed explicitly rather than needing a
    separate admin-bypass check that could be forgotten in a new
    handler."""
    return required in permissions


# --- Tenancy (tenancy.go) ----------------------------------------------


class CrossTenantError(Exception):
    """Raised when a token's organization doesn't match the resource
    being accessed."""


def require_same_organization(token_org_id: str, resource_org_id: str) -> None:
    """The one-line guard every resource-scoped handler is expected to
    call before returning data: a user from Organization A must never be
    able to read or modify Organization B's data, regardless of what
    permissions their role grants."""
    if token_org_id != resource_org_id:
        raise CrossTenantError("resource belongs to a different organization")


# --- Password hashing (password.go) -------------------------------------


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(hashed: str, plain: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def validate_password_strength(password: str) -> None:
    """Enforces a minimum bar: 8+ characters, at least one letter and one
    digit. Raises ValueError naming the violation. Intentionally simple --
    this is a portfolio-scale system, not a compliance-driven one, so it
    demonstrates the control exists without pretending to implement a
    full password policy engine (breach-list checks, entropy scoring,
    etc.)."""
    if len(password) < 8:
        raise ValueError("password must be at least 8 characters")
    has_letter = any(c.isalpha() for c in password)
    has_digit = any(c.isdigit() for c in password)
    if not has_letter or not has_digit:
        raise ValueError("password must contain at least one letter and one digit")


# --- JWT (jwt.go) --------------------------------------------------------


@dataclass
class Claims:
    """Embeds the resolved permission list directly in the token
    (claims-based authorization) so a request can be authorized from the
    token alone, without a database round trip on every call."""

    user_id: str
    organization_id: str
    email: str
    roles: list[str]
    permissions: list[str]
    token_type: str
    jti: str = ""
    issued_at: datetime | None = None
    expires_at: datetime | None = None


def issue_token(claims: Claims, secret: str) -> str:
    payload = {
        "sub": claims.user_id,
        "jti": claims.jti,
        "iat": int(claims.issued_at.timestamp()) if claims.issued_at else int(time.time()),
        "exp": int(claims.expires_at.timestamp()) if claims.expires_at else int(time.time()),
        "user_id": claims.user_id,
        "organization_id": claims.organization_id,
        "email": claims.email,
        "roles": claims.roles,
        "permissions": claims.permissions,
        "token_type": claims.token_type,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def parse_and_validate(token_string: str, secret: str, expected_type: str) -> Claims:
    """Verifies signature, expiry, and that the token's embedded type
    matches expected_type (an access token presented where a refresh
    token is required, or vice versa, is rejected even though the
    signature is valid -- the two are not interchangeable). Raises
    jwt.PyJWTError (or ValueError for a type mismatch) on any failure."""
    payload = jwt.decode(token_string, secret, algorithms=["HS256"])
    if payload.get("token_type") != expected_type:
        raise ValueError(f"expected token_type {expected_type!r}, got {payload.get('token_type')!r}")
    return Claims(
        user_id=payload.get("user_id", ""),
        organization_id=payload.get("organization_id", ""),
        email=payload.get("email", ""),
        roles=payload.get("roles") or [],
        permissions=payload.get("permissions") or [],
        token_type=payload.get("token_type", ""),
        jti=payload.get("jti", ""),
        issued_at=datetime.fromtimestamp(payload["iat"], tz=timezone.utc) if "iat" in payload else None,
        expires_at=datetime.fromtimestamp(payload["exp"], tz=timezone.utc) if "exp" in payload else None,
    )


def _new_base_claims(user_id: str, org_id: str, email: str, roles: list[str], token_type: str, ttl_seconds: float, jti: str) -> Claims:
    now = datetime.now(timezone.utc)
    return Claims(
        user_id=user_id, organization_id=org_id, email=email, roles=roles,
        permissions=permissions_for_roles(roles), token_type=token_type,
        jti=jti, issued_at=now, expires_at=now + timedelta(seconds=ttl_seconds),
    )


# --- Service (service.go) -----------------------------------------------


class InvalidCredentialsError(Exception):
    pass


class UserInactiveError(Exception):
    pass


class TokenRevokedError(Exception):
    pass


@dataclass
class TokenPair:
    access_token: str
    refresh_token: str
    expires_at: datetime


class Service:
    def __init__(
        self,
        pool: ConnectionPool,
        redis_client: Redis,
        access_secret: str,
        refresh_secret: str,
        access_ttl_seconds: float,
        refresh_ttl_seconds: float,
    ):
        self._pool = pool
        self._redis = redis_client
        self._audit_log = audit.Logger(pool)
        self._access_secret = access_secret
        self._refresh_secret = refresh_secret
        self._access_ttl = access_ttl_seconds
        self._refresh_ttl = refresh_ttl_seconds

    def login(self, email: str, password: str, ip_address: str) -> TokenPair:
        """Verifies credentials against Postgres, resolves the user's
        roles into a permission set, issues an access+refresh token pair,
        records the refresh token's jti in Redis (so it can later be
        revoked on logout), and writes an audit_logs row for the attempt
        -- success or failure alike."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT id, organization_id, password_hash, is_active FROM users WHERE email = %s", (email,)
            ).fetchone()

        if row is None:
            self._log_auth(None, None, "user.login", audit.RESULT_FAILURE, ip_address, {"email": email, "reason": "no such user"})
            raise InvalidCredentialsError("invalid email or password")

        user_id, org_id, password_hash, is_active = str(row[0]), str(row[1]), row[2], row[3]

        if not verify_password(password_hash, password):
            self._log_auth(org_id, user_id, "user.login", audit.RESULT_FAILURE, ip_address, {"reason": "bad password"})
            raise InvalidCredentialsError("invalid email or password")
        if not is_active:
            self._log_auth(org_id, user_id, "user.login", audit.RESULT_FAILURE, ip_address, {"reason": "inactive account"})
            raise UserInactiveError("user account is inactive")

        roles = self._roles_for_user(user_id)
        pair = self._issue_pair(user_id, org_id, email, roles)

        self._log_auth(org_id, user_id, "user.login", audit.RESULT_SUCCESS, ip_address, {"roles": roles})
        return pair

    def refresh_access_token(self, refresh_token: str, ip_address: str) -> TokenPair:
        """Implements refresh-token rotation: the presented refresh token
        must still be present in Redis (not revoked, not already used),
        and is deleted and replaced with a brand-new one -- reusing an old
        refresh token after rotation is treated identically to using a
        revoked one."""
        try:
            claims = parse_and_validate(refresh_token, self._refresh_secret, TOKEN_TYPE_REFRESH)
        except Exception as exc:
            raise TokenRevokedError(str(exc)) from exc

        key = _refresh_redis_key(claims.jti)
        deleted = self._redis.delete(key)
        if deleted == 0:
            self._log_auth(claims.organization_id, claims.user_id, "user.refresh_token_reuse", audit.RESULT_FAILURE, ip_address, {"jti": claims.jti})
            raise TokenRevokedError("refresh token has been revoked or already used")

        roles = self._roles_for_user(claims.user_id)
        return self._issue_pair(claims.user_id, claims.organization_id, claims.email, roles)

    def logout(self, refresh_token: str, ip_address: str) -> None:
        """Revokes a refresh token immediately (removes it from Redis) and
        audits the action. Access tokens are not individually revocable --
        the standard stateless-JWT tradeoff, mitigated by a short
        access-token TTL rather than pretending server-side revocation of
        a bearer access token is free."""
        try:
            claims = parse_and_validate(refresh_token, self._refresh_secret, TOKEN_TYPE_REFRESH)
        except Exception as exc:
            raise TokenRevokedError(str(exc)) from exc
        self._redis.delete(_refresh_redis_key(claims.jti))
        self._log_auth(claims.organization_id, claims.user_id, "user.logout", audit.RESULT_SUCCESS, ip_address, {})

    def _issue_pair(self, user_id: str, org_id: str, email: str, roles: list[str]) -> TokenPair:
        access_claims = _new_base_claims(user_id, org_id, email, roles, TOKEN_TYPE_ACCESS, self._access_ttl, str(uuid.uuid4()))
        access_token = issue_token(access_claims, self._access_secret)

        refresh_jti = str(uuid.uuid4())
        refresh_claims = _new_base_claims(user_id, org_id, email, roles, TOKEN_TYPE_REFRESH, self._refresh_ttl, refresh_jti)
        refresh_token = issue_token(refresh_claims, self._refresh_secret)

        self._redis.set(_refresh_redis_key(refresh_jti), user_id, ex=int(self._refresh_ttl))

        return TokenPair(access_token=access_token, refresh_token=refresh_token, expires_at=access_claims.expires_at)

    def _roles_for_user(self, user_id: str) -> list[str]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id = %s", (user_id,)
            ).fetchall()
        return [r[0] for r in rows]

    def _log_auth(self, org_id: str | None, user_id: str | None, action: str, result: str, ip_address: str, metadata: dict) -> None:
        try:
            self._audit_log.log(audit.Entry(
                organization_id=org_id, user_id=user_id, action=action, resource_type="user",
                ip_address=ip_address or None, result=result, metadata=metadata,
            ))
        except Exception as exc:  # noqa: BLE001 - audit failure must never block the auth flow itself
            print(f"auth: failed to write audit log for action={action}: {exc}")


def _refresh_redis_key(jti: str) -> str:
    return f"refresh_token:{jti}"
