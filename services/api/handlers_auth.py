"""Login/refresh/logout/me endpoints, the Python port of handlers_auth.go."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from middleware import ClientIPResolver, make_require_auth
from response import APIError, internal_error
from shared import auth

HTTP_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


class LoginRequest(BaseModel):
    email: str = ""
    password: str = ""


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_at: str
    token_type: str = "Bearer"


class RefreshRequest(BaseModel):
    refresh_token: str = ""


def _format_time(dt) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def register_auth_routes(auth_svc: auth.Service, ip_resolver: ClientIPResolver, access_secret: str, auth_limit) -> APIRouter:
    router = APIRouter()

    @router.post("/api/v1/auth/login", dependencies=[Depends(auth_limit)])
    def login(req: LoginRequest, request: Request) -> TokenResponse:
        if not req.email or not req.password:
            raise APIError(400, "VALIDATION_ERROR", "email and password are required")
        try:
            pair = auth_svc.login(req.email, req.password, ip_resolver.resolve(request))
        except (auth.InvalidCredentialsError, auth.UserInactiveError):
            raise APIError(401, "INVALID_CREDENTIALS", "invalid email or password")
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        return TokenResponse(access_token=pair.access_token, refresh_token=pair.refresh_token, expires_at=_format_time(pair.expires_at))

    @router.post("/api/v1/auth/refresh", dependencies=[Depends(auth_limit)])
    def refresh(req: RefreshRequest, request: Request) -> TokenResponse:
        if not req.refresh_token:
            raise APIError(400, "INVALID_BODY", "refresh_token is required")
        try:
            pair = auth_svc.refresh_access_token(req.refresh_token, ip_resolver.resolve(request))
        except auth.TokenRevokedError:
            raise APIError(401, "TOKEN_REVOKED", "refresh token is invalid, expired, or already used")
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)
        return TokenResponse(access_token=pair.access_token, refresh_token=pair.refresh_token, expires_at=_format_time(pair.expires_at))

    @router.post("/api/v1/auth/logout", status_code=204, response_model=None, dependencies=[Depends(auth_limit)])
    def logout(req: RefreshRequest, request: Request) -> None:
        if not req.refresh_token:
            raise APIError(400, "INVALID_BODY", "refresh_token is required")
        try:
            auth_svc.logout(req.refresh_token, ip_resolver.resolve(request))
        except auth.TokenRevokedError:
            # Already logged out / invalid token: logout is idempotent
            # from the client's point of view.
            return
        except Exception as exc:  # noqa: BLE001
            raise internal_error(exc)

    @router.get("/api/v1/auth/me")
    def me(claims: auth.Claims = Depends(make_require_auth(access_secret))) -> dict:
        return {
            "user_id": claims.user_id,
            "organization_id": claims.organization_id,
            "email": claims.email,
            "roles": claims.roles,
            "permissions": claims.permissions,
        }

    return router
