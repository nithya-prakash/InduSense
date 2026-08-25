package auth

import (
	"errors"
	"testing"
)

func TestRequireSameOrganizationAllowsMatch(t *testing.T) {
	if err := RequireSameOrganization("org-1", "org-1"); err != nil {
		t.Errorf("expected matching organizations to pass, got %v", err)
	}
}

func TestRequireSameOrganizationRejectsMismatch(t *testing.T) {
	err := RequireSameOrganization("org-1", "org-2")
	if !errors.Is(err, ErrCrossTenant) {
		t.Errorf("expected ErrCrossTenant for mismatched organizations, got %v", err)
	}
}
