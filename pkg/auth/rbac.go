package auth

// Permission codes, matching the spec exactly.
const (
	PermDevicesRead     = "devices:read"
	PermDevicesWrite    = "devices:write"
	PermTelemetryRead   = "telemetry:read"
	PermAlertsRead      = "alerts:read"
	PermAlertsManage    = "alerts:manage"
	PermIncidentsRead   = "incidents:read"
	PermIncidentsManage = "incidents:manage"
	PermFactoriesRead   = "factories:read"
	PermFactoriesManage = "factories:manage"
	PermUsersManage     = "users:manage"
	PermSystemAdmin     = "system:admin"
)

const (
	RoleAdmin          = "ADMIN"
	RoleFactoryManager = "FACTORY_MANAGER"
	RoleEngineer       = "ENGINEER"
	RoleTechnician     = "TECHNICIAN"
	RoleViewer         = "VIEWER"
)

// RolePermissions is the single source of truth for what each role can do.
// The seed script inserts role_permissions rows directly from this map (so
// the reference table in Postgres can never drift from what the runtime
// actually enforces), and Login() resolves a user's roles to this same set
// of permissions to embed in the JWT — authorization is claims-based:
// once issued, a token carries its own permission list rather than
// requiring a role_permissions lookup on every request.
var RolePermissions = map[string][]string{
	RoleAdmin: {
		PermDevicesRead, PermDevicesWrite, PermTelemetryRead,
		PermAlertsRead, PermAlertsManage, PermIncidentsRead, PermIncidentsManage,
		PermFactoriesRead, PermFactoriesManage, PermUsersManage, PermSystemAdmin,
	},
	RoleFactoryManager: {
		PermDevicesRead, PermDevicesWrite, PermTelemetryRead,
		PermAlertsRead, PermAlertsManage, PermIncidentsRead, PermIncidentsManage,
		PermFactoriesRead, PermFactoriesManage,
	},
	RoleEngineer: {
		PermDevicesRead, PermDevicesWrite, PermTelemetryRead,
		PermAlertsRead, PermAlertsManage, PermIncidentsRead, PermIncidentsManage,
		PermFactoriesRead,
	},
	RoleTechnician: {
		PermDevicesRead, PermTelemetryRead,
		PermAlertsRead, PermIncidentsRead, PermIncidentsManage,
		PermFactoriesRead,
	},
	RoleViewer: {
		PermDevicesRead, PermTelemetryRead, PermAlertsRead, PermIncidentsRead, PermFactoriesRead,
	},
}

// AllRoles lists every valid role name, for seeding and validation.
var AllRoles = []string{RoleAdmin, RoleFactoryManager, RoleEngineer, RoleTechnician, RoleViewer}

// PermissionsForRoles unions the permission sets of every given role,
// de-duplicated. A user with multiple roles gets the union, not the
// intersection — the more permissive combination, which matches how RBAC
// systems conventionally combine multiple role grants.
func PermissionsForRoles(roles []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, role := range roles {
		for _, perm := range RolePermissions[role] {
			if !seen[perm] {
				seen[perm] = true
				out = append(out, perm)
			}
		}
	}
	return out
}

// HasPermission reports whether permissions contains the required one.
// system:admin is NOT an implicit wildcard — the permission list from
// PermissionsForRoles already expands ADMIN to every concrete permission,
// so a caller checking for e.g. devices:write against an ADMIN's token
// finds it listed explicitly rather than needing a separate admin-bypass
// check that could be forgotten in a new handler.
func HasPermission(permissions []string, required string) bool {
	for _, p := range permissions {
		if p == required {
			return true
		}
	}
	return false
}
