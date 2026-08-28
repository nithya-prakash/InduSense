package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/nithya-prakash/indusense/pkg/auth"
)

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    string `json:"expires_at"`
	TokenType    string `json:"token_type"`
}

func handleLogin(authSvc *auth.Service, ipResolver *clientIPResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req loginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "request body must be valid JSON")
			return
		}
		if req.Email == "" || req.Password == "" {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "email and password are required")
			return
		}

		pair, err := authSvc.Login(r.Context(), req.Email, req.Password, ipResolver.resolve(r))
		if err != nil {
			switch {
			case errors.Is(err, auth.ErrInvalidCredentials), errors.Is(err, auth.ErrUserInactive):
				writeError(w, r, http.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid email or password")
			default:
				writeInternalError(w, r, err)
			}
			return
		}

		writeJSON(w, http.StatusOK, tokenResponse{
			AccessToken:  pair.AccessToken,
			RefreshToken: pair.RefreshToken,
			ExpiresAt:    pair.ExpiresAt.Format(httpTimeFormat),
			TokenType:    "Bearer",
		})
	}
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func handleRefresh(authSvc *auth.Service, ipResolver *clientIPResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req refreshRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RefreshToken == "" {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "refresh_token is required")
			return
		}

		pair, err := authSvc.RefreshAccessToken(r.Context(), req.RefreshToken, ipResolver.resolve(r))
		if err != nil {
			if errors.Is(err, auth.ErrTokenRevoked) {
				writeError(w, r, http.StatusUnauthorized, "TOKEN_REVOKED", "refresh token is invalid, expired, or already used")
				return
			}
			writeInternalError(w, r, err)
			return
		}

		writeJSON(w, http.StatusOK, tokenResponse{
			AccessToken:  pair.AccessToken,
			RefreshToken: pair.RefreshToken,
			ExpiresAt:    pair.ExpiresAt.Format(httpTimeFormat),
			TokenType:    "Bearer",
		})
	}
}

func handleLogout(authSvc *auth.Service, ipResolver *clientIPResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req refreshRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RefreshToken == "" {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "refresh_token is required")
			return
		}
		if err := authSvc.Logout(r.Context(), req.RefreshToken, ipResolver.resolve(r)); err != nil {
			if errors.Is(err, auth.ErrTokenRevoked) {
				// Already logged out / invalid token: logout is idempotent
				// from the client's point of view.
				w.WriteHeader(http.StatusNoContent)
				return
			}
			writeInternalError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleMe() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{
			"user_id":         claims.UserID,
			"organization_id": claims.OrganizationID,
			"email":           claims.Email,
			"roles":           claims.Roles,
			"permissions":     claims.Permissions,
		})
	}
}

const httpTimeFormat = "2006-01-02T15:04:05Z07:00"
