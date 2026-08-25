"use client";

import { useAuth } from "@/lib/auth-context";

// Mirrors pkg/auth/rbac.go's RolePermissions — the backend's single source
// of truth. Shown here for reference since there's no /api/v1/roles
// endpoint yet (no page has needed to list roles generically; see the
// honest "not implemented" note below rather than a fabricated one).
const ROLE_PERMISSIONS: Record<string, string[]> = {
  ADMIN: [
    "devices:read", "devices:write", "telemetry:read", "alerts:read", "alerts:manage",
    "incidents:read", "incidents:manage", "factories:read", "factories:manage", "users:manage", "system:admin",
  ],
  FACTORY_MANAGER: [
    "devices:read", "devices:write", "telemetry:read", "alerts:read", "alerts:manage",
    "incidents:read", "incidents:manage", "factories:read", "factories:manage",
  ],
  ENGINEER: [
    "devices:read", "devices:write", "telemetry:read", "alerts:read", "alerts:manage",
    "incidents:read", "incidents:manage", "factories:read",
  ],
  TECHNICIAN: ["devices:read", "telemetry:read", "alerts:read", "incidents:read", "incidents:manage", "factories:read"],
  VIEWER: ["devices:read", "telemetry:read", "alerts:read", "incidents:read", "factories:read"],
};

const ALL_PERMISSIONS = [
  "devices:read", "devices:write", "telemetry:read", "alerts:read", "alerts:manage",
  "incidents:read", "incidents:manage", "factories:read", "factories:manage", "users:manage", "system:admin",
];

export default function AdminPage() {
  const { me } = useAuth();

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Administration</h1>

      <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
        <h2 className="mb-3 text-sm font-semibold text-gray-900">Your identity</h2>
        <dl className="grid grid-cols-2 gap-2 text-sm">
          <dt className="text-gray-500">Email</dt>
          <dd className="text-gray-900">{me?.email}</dd>
          <dt className="text-gray-500">Organization ID</dt>
          <dd className="font-mono text-xs text-gray-900">{me?.organization_id}</dd>
          <dt className="text-gray-500">Roles</dt>
          <dd className="text-gray-900">{me?.roles?.join(", ")}</dd>
        </dl>
      </div>

      <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
        <h2 className="mb-3 text-sm font-semibold text-gray-900">Role → permission matrix</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 text-xs">
            <thead>
              <tr>
                <th className="px-2 py-2 text-left font-medium uppercase text-gray-500">Permission</th>
                {Object.keys(ROLE_PERMISSIONS).map((role) => (
                  <th key={role} className="px-2 py-2 text-center font-medium uppercase text-gray-500">
                    {role.replaceAll("_", " ")}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {ALL_PERMISSIONS.map((perm) => (
                <tr key={perm}>
                  <td className="px-2 py-1.5 font-mono text-gray-700">{perm}</td>
                  {Object.entries(ROLE_PERMISSIONS).map(([role, perms]) => (
                    <td key={role} className="px-2 py-1.5 text-center">
                      {perms.includes(perm) ? <span className="text-green-600">✓</span> : <span className="text-gray-300">–</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-md border border-yellow-200 bg-yellow-50 p-4 text-sm text-yellow-800">
        User creation and role assignment via the UI are not yet implemented — the API only exposes read
        (<code>GET /api/v1/auth/me</code>) today. Demo users and their role assignments are created by{" "}
        <code>scripts/seed</code>. A full user-management API and this page&apos;s corresponding CRUD UI are honest
        follow-up work, not something faked here.
      </div>
    </div>
  );
}
