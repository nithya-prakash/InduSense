package auth

import "fmt"

// ErrCrossTenant is returned when a token's organization doesn't match the
// resource being accessed. Handlers must call RequireSameOrganization (or
// scope their queries by the token's organization_id directly) on every
// resource access — there is no global bypass except an explicit
// system:admin check where that's genuinely intended.
var ErrCrossTenant = fmt.Errorf("resource belongs to a different organization")

// RequireSameOrganization is the one-line guard every resource-scoped
// handler is expected to call before returning data: a user from
// Organization A must never be able to read or modify Organization B's
// data, regardless of what permissions their role grants.
func RequireSameOrganization(tokenOrgID, resourceOrgID string) error {
	if tokenOrgID != resourceOrgID {
		return ErrCrossTenant
	}
	return nil
}
