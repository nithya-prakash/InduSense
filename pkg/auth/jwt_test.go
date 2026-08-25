package auth

import (
	"testing"
	"time"
)

const testSecret = "test-secret-do-not-use-in-prod"

func TestIssueAndParseAccessToken(t *testing.T) {
	claims := newBaseClaims("user-1", "org-1", "a@b.com", []string{RoleEngineer}, TokenTypeAccess, 15*time.Minute, "jti-1")
	token, err := IssueToken(claims, testSecret)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}

	parsed, err := ParseAndValidate(token, testSecret, TokenTypeAccess)
	if err != nil {
		t.Fatalf("ParseAndValidate: %v", err)
	}
	if parsed.UserID != "user-1" || parsed.OrganizationID != "org-1" {
		t.Errorf("parsed claims mismatch: %+v", parsed)
	}
	if !HasPermission(parsed.Permissions, PermAlertsManage) {
		t.Error("expected ENGINEER's permissions to be embedded in the token")
	}
}

func TestParseAndValidateRejectsWrongSecret(t *testing.T) {
	claims := newBaseClaims("user-1", "org-1", "a@b.com", []string{RoleViewer}, TokenTypeAccess, time.Minute, "jti-1")
	token, _ := IssueToken(claims, testSecret)

	if _, err := ParseAndValidate(token, "a-different-secret", TokenTypeAccess); err == nil {
		t.Fatal("expected a token signed with a different secret to fail validation")
	}
}

func TestParseAndValidateRejectsExpiredToken(t *testing.T) {
	claims := newBaseClaims("user-1", "org-1", "a@b.com", []string{RoleViewer}, TokenTypeAccess, -time.Minute, "jti-1")
	token, _ := IssueToken(claims, testSecret)

	if _, err := ParseAndValidate(token, testSecret, TokenTypeAccess); err == nil {
		t.Fatal("expected an already-expired token to fail validation")
	}
}

func TestParseAndValidateRejectsWrongTokenType(t *testing.T) {
	// An access token presented where a refresh token is required must be
	// rejected even though the signature is perfectly valid — the two
	// token types are not interchangeable.
	claims := newBaseClaims("user-1", "org-1", "a@b.com", []string{RoleViewer}, TokenTypeAccess, time.Minute, "jti-1")
	token, _ := IssueToken(claims, testSecret)

	if _, err := ParseAndValidate(token, testSecret, TokenTypeRefresh); err == nil {
		t.Fatal("expected an access token to be rejected when a refresh token is required")
	}
}

func TestNewBaseClaimsEmbedsResolvedPermissions(t *testing.T) {
	claims := newBaseClaims("u", "o", "e@e.com", []string{RoleTechnician}, TokenTypeAccess, time.Minute, "jti")
	if len(claims.Permissions) == 0 {
		t.Fatal("expected permissions to be resolved and embedded at claim-construction time")
	}
	if HasPermission(claims.Permissions, PermUsersManage) {
		t.Error("TECHNICIAN should not have users:manage")
	}
}
