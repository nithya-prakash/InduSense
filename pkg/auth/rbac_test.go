package auth

import "testing"

func TestPermissionsForRolesAdminHasEverything(t *testing.T) {
	perms := PermissionsForRoles([]string{RoleAdmin})
	allPerms := []string{
		PermDevicesRead, PermDevicesWrite, PermTelemetryRead,
		PermAlertsRead, PermAlertsManage, PermIncidentsRead, PermIncidentsManage,
		PermFactoriesRead, PermFactoriesManage, PermUsersManage, PermSystemAdmin,
	}
	for _, p := range allPerms {
		if !HasPermission(perms, p) {
			t.Errorf("expected ADMIN to have permission %s", p)
		}
	}
}

func TestPermissionsForRolesViewerIsReadOnly(t *testing.T) {
	perms := PermissionsForRoles([]string{RoleViewer})
	if HasPermission(perms, PermDevicesWrite) {
		t.Error("VIEWER should not have devices:write")
	}
	if HasPermission(perms, PermAlertsManage) {
		t.Error("VIEWER should not have alerts:manage")
	}
	if HasPermission(perms, PermUsersManage) {
		t.Error("VIEWER should not have users:manage")
	}
	if !HasPermission(perms, PermDevicesRead) {
		t.Error("VIEWER should have devices:read")
	}
}

func TestPermissionsForRolesUnionsMultipleRoles(t *testing.T) {
	// TECHNICIAN alone lacks alerts:manage; ENGINEER alone lacks nothing
	// unusual, but combining should still just be the union, deduplicated.
	perms := PermissionsForRoles([]string{RoleTechnician, RoleEngineer})
	if !HasPermission(perms, PermAlertsManage) {
		t.Error("expected the union of TECHNICIAN+ENGINEER to include alerts:manage (from ENGINEER)")
	}
	if !HasPermission(perms, PermIncidentsManage) {
		t.Error("expected the union to include incidents:manage (present in both roles)")
	}
}

func TestPermissionsForRolesEmptyRoleListYieldsNoPermissions(t *testing.T) {
	perms := PermissionsForRoles(nil)
	if len(perms) != 0 {
		t.Errorf("expected no permissions for an empty role list, got %v", perms)
	}
}

func TestPermissionsForRolesUnknownRoleYieldsNothing(t *testing.T) {
	perms := PermissionsForRoles([]string{"NOT_A_REAL_ROLE"})
	if len(perms) != 0 {
		t.Errorf("expected no permissions for an unrecognized role, got %v", perms)
	}
}

func TestAllRolesHaveAtLeastOnePermission(t *testing.T) {
	for _, role := range AllRoles {
		if len(RolePermissions[role]) == 0 {
			t.Errorf("role %s has no permissions configured", role)
		}
	}
}

func TestEveryRoleCanReadFactoriesAndDevices(t *testing.T) {
	// Sanity check on the model itself: every role should at least be able
	// to see the factory hierarchy and telemetry — a role that can't do
	// even that would be useless in this system.
	for _, role := range AllRoles {
		perms := PermissionsForRoles([]string{role})
		if !HasPermission(perms, PermFactoriesRead) {
			t.Errorf("role %s cannot even read factories", role)
		}
		if !HasPermission(perms, PermTelemetryRead) {
			t.Errorf("role %s cannot even read telemetry", role)
		}
	}
}
