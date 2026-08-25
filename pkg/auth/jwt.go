package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	TokenTypeAccess  = "access"
	TokenTypeRefresh = "refresh"
)

// Claims embeds the resolved permission list directly in the token
// (claims-based authorization — see rbac.go) so a request can be authorized
// from the token alone, without a database round trip on every call.
type Claims struct {
	jwt.RegisteredClaims
	UserID         string   `json:"user_id"`
	OrganizationID string   `json:"organization_id"`
	Email          string   `json:"email"`
	Roles          []string `json:"roles"`
	Permissions    []string `json:"permissions"`
	TokenType      string   `json:"token_type"`
}

// IssueToken signs a new JWT. jti (JWT ID) is the caller's responsibility —
// for refresh tokens it doubles as the Redis revocation key.
func IssueToken(claims Claims, secret string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}

// ParseAndValidate verifies signature, expiry, and that the token's
// embedded type matches expectedType (an access token presented where a
// refresh token is required, or vice versa, is rejected even though the
// signature is valid — the two are not interchangeable).
func ParseAndValidate(tokenString, secret, expectedType string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("token is not valid")
	}
	if claims.TokenType != expectedType {
		return nil, fmt.Errorf("expected token_type %q, got %q", expectedType, claims.TokenType)
	}
	return claims, nil
}

func newBaseClaims(userID, orgID, email string, roles []string, tokenType string, ttl time.Duration, jti string) Claims {
	now := time.Now()
	return Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
		UserID:         userID,
		OrganizationID: orgID,
		Email:          email,
		Roles:          roles,
		Permissions:    PermissionsForRoles(roles),
		TokenType:      tokenType,
	}
}
