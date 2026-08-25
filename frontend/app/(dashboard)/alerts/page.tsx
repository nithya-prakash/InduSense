"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import type { Paginated, Alert } from "@/lib/types";
import { SeverityBadge, StatusBadge } from "@/components/Badge";
import { useAuth } from "@/lib/auth-context";
import { useAlertsFeed } from "@/lib/use-alerts-feed";

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const { hasPermission } = useAuth();
  const { events, connected } = useAlertsFeed();

  function load() {
    setLoading(true);
    const q = statusFilter ? `?status=${statusFilter}&limit=100` : "?limit=100";
    api
      .get<Paginated<Alert>>(`/api/v1/alerts${q}`)
      .then((res) => setAlerts(res.items))
      .finally(() => setLoading(false));
  }

  useEffect(load, [statusFilter]);

  // A live WebSocket event for a brand-new alert re-triggers a full reload
  // so it appears in the table immediately, rather than waiting for the
  // user to refresh — the actual "sensor -> ... -> alert -> dashboard"
  // real-time path this platform is built around.
  useEffect(() => {
    if (events.length > 0 && events[0].event_type === "CREATED") load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events]);

  async function acknowledge(id: string) {
    await api.post(`/api/v1/alerts/${id}/acknowledge`);
    load();
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Alerts</h1>
        <div className="flex items-center gap-3 text-xs">
          <span className={connected ? "text-green-600" : "text-gray-400"}>
            {connected ? "● live" : "○ connecting..."}
          </span>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-md border border-gray-300 px-2 py-1 text-sm"
          >
            <option value="">All statuses</option>
            <option value="OPEN">Open</option>
            <option value="ACKNOWLEDGED">Acknowledged</option>
            <option value="RESOLVED">Resolved</option>
          </select>
        </div>
      </div>

      {loading ? (
        <div className="text-sm text-gray-400">Loading...</div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Severity</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Title</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Status</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Triggered</th>
                <th className="px-4 py-2"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {alerts.map((a) => (
                <tr key={a.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2">
                    <SeverityBadge severity={a.severity} />
                  </td>
                  <td className="max-w-md truncate px-4 py-2 text-sm text-gray-800" title={a.description}>
                    {a.title}
                  </td>
                  <td className="px-4 py-2">
                    <StatusBadge status={a.status} />
                  </td>
                  <td className="px-4 py-2 text-sm text-gray-500">{new Date(a.triggered_at).toLocaleString()}</td>
                  <td className="px-4 py-2 text-right">
                    {a.status === "OPEN" && hasPermission("alerts:manage") && (
                      <button
                        onClick={() => acknowledge(a.id)}
                        className="rounded-md border border-gray-300 px-2 py-1 text-xs font-medium text-gray-700 hover:bg-gray-100"
                      >
                        Acknowledge
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {alerts.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-6 text-center text-sm text-gray-400">
                    No alerts
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
