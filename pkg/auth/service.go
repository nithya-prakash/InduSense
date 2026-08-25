package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/audit"
	"github.com/redis/go-redis/v9"
)

var (
	ErrInvalidCredentials = errors.New("invalid email or password")
	ErrUserInactive       = errors.New("user account is inactive")
	ErrTokenRevoked       = errors.New("refresh token has been revoked or already used")
)

type TokenPair struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
}

type Service struct {
	pool          *pgxpool.Pool
	redis         *redis.Client
	auditLog      *audit.Logger
	accessSecret  string
	refreshSecret string
	accessTTL     time.Duration
	refreshTTL    time.Duration
}

func NewService(pool *pgxpool.Pool, redisClient *redis.Client, accessSecret, refreshSecret string, accessTTL, refreshTTL time.Duration) *Service {
	return &Service{
		pool:          pool,
		redis:         redisClient,
		auditLog:      audit.NewLogger(pool),
		accessSecret:  accessSecret,
		refreshSecret: refreshSecret,
		accessTTL:     accessTTL,
		refreshTTL:    refreshTTL,
	}
}

// Login verifies credentials against Postgres, resolves the user's roles
// into a permission set, issues an access+refresh token pair, records the
// refresh token's jti in Redis (so it can later be revoked on logout), and
// writes an audit_logs row for the attempt — success or failure alike, per
// the spec's own list of security-sensitive actions.
func (s *Service) Login(ctx context.Context, email, password, ipAddress string) (*TokenPair, error) {
	var userID, orgID, passwordHash string
	var isActive bool
	err := s.pool.QueryRow(ctx,
		`SELECT id, organization_id, password_hash, is_active FROM users WHERE email = $1`, email,
	).Scan(&userID, &orgID, &passwordHash, &isActive)

	if errors.Is(err, pgx.ErrNoRows) {
		s.logAuth(ctx, nil, nil, "user.login", audit.ResultFailure, ipAddress, map[string]any{"email": email, "reason": "no such user"})
		return nil, ErrInvalidCredentials
	}
	if err != nil {
		return nil, fmt.Errorf("look up user: %w", err)
	}

	if !VerifyPassword(passwordHash, password) {
		s.logAuth(ctx, &orgID, &userID, "user.login", audit.ResultFailure, ipAddress, map[string]any{"reason": "bad password"})
		return nil, ErrInvalidCredentials
	}
	if !isActive {
		s.logAuth(ctx, &orgID, &userID, "user.login", audit.ResultFailure, ipAddress, map[string]any{"reason": "inactive account"})
		return nil, ErrUserInactive
	}

	roles, err := s.rolesForUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	pair, err := s.issuePair(ctx, userID, orgID, email, roles)
	if err != nil {
		return nil, err
	}

	s.logAuth(ctx, &orgID, &userID, "user.login", audit.ResultSuccess, ipAddress, map[string]any{"roles": roles})
	return pair, nil
}

// RefreshAccessToken implements refresh-token rotation: the presented
// refresh token must still be present in Redis (not revoked, not already
// used), and is deleted and replaced with a brand-new one atomically from
// the caller's point of view — reusing an old refresh token after rotation
// is treated identically to using a revoked one.
func (s *Service) RefreshAccessToken(ctx context.Context, refreshToken, ipAddress string) (*TokenPair, error) {
	claims, err := ParseAndValidate(refreshToken, s.refreshSecret, TokenTypeRefresh)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTokenRevoked, err)
	}

	key := refreshRedisKey(claims.ID)
	deleted, err := s.redis.Del(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("check refresh token revocation state: %w", err)
	}
	if deleted == 0 {
		s.logAuth(ctx, &claims.OrganizationID, &claims.UserID, "user.refresh_token_reuse", audit.ResultFailure, ipAddress, map[string]any{"jti": claims.ID})
		return nil, ErrTokenRevoked
	}

	roles, err := s.rolesForUser(ctx, claims.UserID)
	if err != nil {
		return nil, err
	}
	return s.issuePair(ctx, claims.UserID, claims.OrganizationID, claims.Email, roles)
}

// Logout revokes a refresh token immediately (removes it from Redis) and
// audits the action. Access tokens are not individually revocable — this
// is the standard stateless-JWT tradeoff, mitigated by a short access-token
// TTL (JWT_ACCESS_TTL_MINUTES, default 15) rather than pretending
// server-side revocation of a bearer access token is free.
func (s *Service) Logout(ctx context.Context, refreshToken, ipAddress string) error {
	claims, err := ParseAndValidate(refreshToken, s.refreshSecret, TokenTypeRefresh)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTokenRevoked, err)
	}
	if err := s.redis.Del(ctx, refreshRedisKey(claims.ID)).Err(); err != nil {
		return fmt.Errorf("revoke refresh token: %w", err)
	}
	s.logAuth(ctx, &claims.OrganizationID, &claims.UserID, "user.logout", audit.ResultSuccess, ipAddress, nil)
	return nil
}

func (s *Service) issuePair(ctx context.Context, userID, orgID, email string, roles []string) (*TokenPair, error) {
	accessClaims := newBaseClaims(userID, orgID, email, roles, TokenTypeAccess, s.accessTTL, uuid.NewString())
	accessToken, err := IssueToken(accessClaims, s.accessSecret)
	if err != nil {
		return nil, err
	}

	refreshJTI := uuid.NewString()
	refreshClaims := newBaseClaims(userID, orgID, email, roles, TokenTypeRefresh, s.refreshTTL, refreshJTI)
	refreshToken, err := IssueToken(refreshClaims, s.refreshSecret)
	if err != nil {
		return nil, err
	}

	if err := s.redis.Set(ctx, refreshRedisKey(refreshJTI), userID, s.refreshTTL).Err(); err != nil {
		return nil, fmt.Errorf("store refresh token: %w", err)
	}

	return &TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresAt:    accessClaims.ExpiresAt.Time,
	}, nil
}

func (s *Service) rolesForUser(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT r.name FROM roles r JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id = $1`, userID)
	if err != nil {
		return nil, fmt.Errorf("load roles for user %s: %w", userID, err)
	}
	defer rows.Close()

	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, fmt.Errorf("scan role: %w", err)
		}
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

func (s *Service) logAuth(ctx context.Context, orgID, userID *string, action, result, ipAddress string, metadata map[string]any) {
	var ipPtr *string
	if ipAddress != "" {
		ipPtr = &ipAddress
	}
	if err := s.auditLog.Log(ctx, audit.Entry{
		OrganizationID: orgID,
		UserID:         userID,
		Action:         action,
		ResourceType:   "user",
		IPAddress:      ipPtr,
		Result:         result,
		Metadata:       metadata,
	}); err != nil {
		// Audit logging failure must never block the auth flow itself — it's
		// logged for operator visibility but doesn't fail Login/Logout.
		fmt.Printf("auth: failed to write audit log for action=%s: %v\n", action, err)
	}
}

func refreshRedisKey(jti string) string { return "refresh_token:" + jti }
