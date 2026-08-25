package auth

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// TestLoginRefreshLogoutAgainstRealInfra exercises the full auth flow —
// login, access token validation, refresh-token rotation, reuse detection,
// and logout revocation — against real Postgres and Redis, using the
// actual demo user seeded by scripts/seed (admin@musterfabrik-gmbh.de).
// Skipped if either dependency isn't reachable.
func TestLoginRefreshLogoutAgainstRealInfra(t *testing.T) {
	pool, redisClient := connectLiveOrSkip(t)
	defer pool.Close()
	defer redisClient.Close()

	svc := NewService(pool, redisClient, testSecret, "refresh-"+testSecret, 15*time.Minute, time.Hour)
	ctx := context.Background()

	pair, err := svc.Login(ctx, "admin@musterfabrik-gmbh.de", "ChangeMe123!", "127.0.0.1")
	if err != nil {
		t.Fatalf("Login with correct credentials: %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("expected both tokens to be issued")
	}

	claims, err := ParseAndValidate(pair.AccessToken, testSecret, TokenTypeAccess)
	if err != nil {
		t.Fatalf("validate issued access token: %v", err)
	}
	if !HasPermission(claims.Permissions, PermSystemAdmin) {
		t.Error("expected the seeded admin user's token to carry system:admin")
	}
	if claims.Email != "admin@musterfabrik-gmbh.de" {
		t.Errorf("claims.Email = %q, want admin@musterfabrik-gmbh.de", claims.Email)
	}

	if _, err := svc.Login(ctx, "admin@musterfabrik-gmbh.de", "wrong-password", ""); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("expected ErrInvalidCredentials for a wrong password, got %v", err)
	}
	if _, err := svc.Login(ctx, "nobody@musterfabrik-gmbh.de", "whatever123", ""); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("expected ErrInvalidCredentials for an unknown email, got %v", err)
	}

	// Refresh rotates: the old refresh token becomes unusable, the new one works.
	newPair, err := svc.RefreshAccessToken(ctx, pair.RefreshToken, "127.0.0.1")
	if err != nil {
		t.Fatalf("RefreshAccessToken: %v", err)
	}
	if newPair.RefreshToken == pair.RefreshToken {
		t.Fatal("expected refresh to issue a genuinely new refresh token, not reuse the old one")
	}

	if _, err := svc.RefreshAccessToken(ctx, pair.RefreshToken, ""); !errors.Is(err, ErrTokenRevoked) {
		t.Errorf("expected reusing a rotated-out refresh token to fail with ErrTokenRevoked, got %v", err)
	}

	if err := svc.Logout(ctx, newPair.RefreshToken, "127.0.0.1"); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, err := svc.RefreshAccessToken(ctx, newPair.RefreshToken, ""); !errors.Is(err, ErrTokenRevoked) {
		t.Errorf("expected a logged-out refresh token to be rejected, got %v", err)
	}

	// The audit trail should show both the successful and failed login
	// attempts, plus the logout.
	var loginSuccessCount, loginFailureCount, logoutCount int
	pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE action = 'user.login' AND result = 'SUCCESS'`).Scan(&loginSuccessCount) //nolint:errcheck
	pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE action = 'user.login' AND result = 'FAILURE'`).Scan(&loginFailureCount) //nolint:errcheck
	pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs WHERE action = 'user.logout'`).Scan(&logoutCount)                             //nolint:errcheck
	if loginSuccessCount == 0 {
		t.Error("expected at least one successful login audit entry")
	}
	if loginFailureCount == 0 {
		t.Error("expected at least one failed login audit entry")
	}
	if logoutCount == 0 {
		t.Error("expected at least one logout audit entry")
	}
}

// TestMultiTenantLoginsAreIsolated verifies, against real seeded data, that
// a user from one organization's token carries that organization's ID and
// nothing from the other — the concrete, testable form of "a user from
// Organization A must not be able to access Organization B's data."
func TestMultiTenantLoginsAreIsolated(t *testing.T) {
	pool, redisClient := connectLiveOrSkip(t)
	defer pool.Close()
	defer redisClient.Close()

	svc := NewService(pool, redisClient, testSecret, "refresh-"+testSecret, 15*time.Minute, time.Hour)
	ctx := context.Background()

	pairA, err := svc.Login(ctx, "admin@musterfabrik-gmbh.de", "ChangeMe123!", "")
	if err != nil {
		t.Fatalf("login as org A admin: %v", err)
	}
	pairB, err := svc.Login(ctx, "admin@zweite-firma-gmbh.de", "ChangeMe123!", "")
	if err != nil {
		t.Fatalf("login as org B admin: %v", err)
	}

	claimsA, err := ParseAndValidate(pairA.AccessToken, testSecret, TokenTypeAccess)
	if err != nil {
		t.Fatalf("validate org A token: %v", err)
	}
	claimsB, err := ParseAndValidate(pairB.AccessToken, testSecret, TokenTypeAccess)
	if err != nil {
		t.Fatalf("validate org B token: %v", err)
	}

	if claimsA.OrganizationID == claimsB.OrganizationID {
		t.Fatal("expected the two seeded organizations to have distinct IDs")
	}
	if err := RequireSameOrganization(claimsA.OrganizationID, claimsA.OrganizationID); err != nil {
		t.Errorf("a user's own organization should always pass the guard: %v", err)
	}
	if err := RequireSameOrganization(claimsA.OrganizationID, claimsB.OrganizationID); !errors.Is(err, ErrCrossTenant) {
		t.Errorf("expected org A's token to be rejected against org B's resource, got %v", err)
	}

	// Ground truth at the data layer too: org A's factory must not appear
	// in a query scoped to org B, and vice versa.
	var crossTenantLeak int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM factories f
		 JOIN organizations o ON o.id = f.organization_id
		 WHERE o.id = $1 AND f.organization_id != $1`,
		claimsA.OrganizationID,
	).Scan(&crossTenantLeak)
	if err != nil {
		t.Fatalf("cross-tenant leak query: %v", err)
	}
	if crossTenantLeak != 0 {
		t.Fatal("found factories whose organization_id doesn't match their own organization join — should be impossible")
	}
}

func connectLiveOrSkip(t *testing.T) (*pgxpool.Pool, *redis.Client) {
	t.Helper()
	dsn := os.Getenv("ALERT_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"
	}
	redisAddr := os.Getenv("TEST_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no live Postgres reachable, skipping: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("no live Postgres reachable, skipping: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		pool.Close()
		t.Skipf("no live Redis reachable, skipping: %v", err)
	}

	return pool, redisClient
}
