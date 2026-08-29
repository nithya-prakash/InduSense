# 9. Authentication

[shared/auth.py](../../shared/auth.py) implements password hashing, JWT
issuance/validation, RBAC, and multi-tenancy guards as domain logic with no
HTTP dependency — same pattern as [Alerting](07-alerting.md) and
[Incidents](08-incidents.md): build and live-verify the logic before the
REST API exists to expose it.
[shared/audit.py](../../shared/audit.py) is a small shared writer for the
`audit_logs` table.

**RBAC is claims-based, not a database lookup per request**: login resolves
a user's roles to their full permission set and embeds it directly in the
JWT, so authorizing a request only requires validating the token, not a
`role_permissions` join on every call. That same map is also the single
source of truth the seed script uses to populate `role_permissions` in
Postgres — verified live to match exactly: ADMIN 11 permissions,
FACTORY_MANAGER 9, ENGINEER 8, TECHNICIAN 6, VIEWER 5.

**The full auth flow was verified against real Postgres and Redis**, not
mocked, in [shared/tests/test_auth.py](../../shared/tests/test_auth.py):
login with the real seeded admin user, wrong-password and unknown-email
rejection, refresh-token **rotation** (each refresh both invalidates the
presented token and issues a new one), reuse-of-a-rotated-out-token
rejection, and logout revocation — all checked against Redis state. The
audit trail was checked directly in Postgres: successful logins, failed
logins, and logouts all produced real `audit_logs` rows.

**Multi-tenancy was verified with two real organizations**, not asserted
against one: the seed script creates a second organization ("Zweite Firma
GmbH") with its own factory and admin user specifically so isolation has
something concrete to fail against. Both organizations' tokens carry
distinct `organization_id` claims, cross-organization access is rejected,
and — as a data-layer ground truth check — no factory row's
`organization_id` can appear under a different organization's join.

Access tokens are **not** individually revocable (the standard stateless-JWT
tradeoff), mitigated by a short TTL (`JWT_ACCESS_TTL_MINUTES`, default 15)
rather than pretending server-side revocation of a bearer token is free;
refresh tokens are revocable because they're tracked in Redis by design.
