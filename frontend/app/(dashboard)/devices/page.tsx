"use client";

import { useEffect, useState } from "react";
import { api, ApiError } from "@/lib/api";
import type { Paginated, Device } from "@/lib/types";
import { StatusBadge } from "@/components/Badge";
import { useAuth } from "@/lib/auth-context";

export default function DevicesPage() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const { hasPermission } = useAuth();

  function load() {
    setLoading(true);
    const q = statusFilter ? `?status=${statusFilter}&limit=100` : "?limit=100";
    api
      .get<Paginated<Device>>(`/api/v1/devices${q}`)
      .then((res) => setDevices(res.items))
      .finally(() => setLoading(false));
  }

  useEffect(load, [statusFilter]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Devices</h1>
        <div className="flex items-center gap-3">
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-md border border-gray-300 px-2 py-1 text-sm"
          >
            <option value="">All statuses</option>
            <option value="ACTIVE">Active</option>
            <option value="OFFLINE">Offline</option>
            <option value="MAINTENANCE">Maintenance</option>
            <option value="PROVISIONED">Provisioned</option>
            <option value="DECOMMISSIONED">Decommissioned</option>
          </select>
          {hasPermission("devices:write") && (
            <button
              onClick={() => setShowForm((s) => !s)}
              className="rounded-md bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700"
            >
              {showForm ? "Cancel" : "Provision device"}
            </button>
          )}
        </div>
      </div>

      {showForm && <ProvisionForm onDone={() => { setShowForm(false); load(); }} />}

      {loading ? (
        <div className="text-sm text-gray-400">Loading...</div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Serial number</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Status</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Firmware</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {devices.map((d) => (
                <tr key={d.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-sm text-gray-800">{d.serial_number}</td>
                  <td className="px-4 py-2">
                    <StatusBadge status={d.status} />
                  </td>
                  <td className="px-4 py-2 text-sm text-gray-500">{d.firmware_version}</td>
                </tr>
              ))}
              {devices.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-4 py-6 text-center text-sm text-gray-400">
                    No devices
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function ProvisionForm({ onDone }: { onDone: () => void }) {
  const [machineId, setMachineId] = useState("");
  const [serialNumber, setSerialNumber] = useState("");
  const [secret, setSecret] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const res = await api.post<{ device: Device; secret: string }>("/api/v1/devices", {
        machine_id: machineId,
        serial_number: serialNumber,
      });
      setSecret(res.secret);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Provisioning failed");
    } finally {
      setSubmitting(false);
    }
  }

  if (secret) {
    return (
      <div className="rounded-lg border border-green-200 bg-green-50 p-4">
        <p className="text-sm font-medium text-green-800">Device provisioned. Copy this secret now — it will never be shown again:</p>
        <code className="mt-2 block rounded-md bg-white px-3 py-2 text-sm">{secret}</code>
        <button onClick={onDone} className="mt-3 text-xs font-medium text-green-700 hover:underline">
          Done
        </button>
      </div>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-3 rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-700">Machine ID</label>
          <input
            required
            value={machineId}
            onChange={(e) => setMachineId(e.target.value)}
            placeholder="uuid of an existing machine"
            className="mt-1 w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm"
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700">Serial number</label>
          <input
            required
            value={serialNumber}
            onChange={(e) => setSerialNumber(e.target.value)}
            className="mt-1 w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm"
          />
        </div>
      </div>
      {error && <div className="rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div>}
      <button
        type="submit"
        disabled={submitting}
        className="rounded-md bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
      >
        {submitting ? "Provisioning..." : "Provision"}
      </button>
    </form>
  );
}
