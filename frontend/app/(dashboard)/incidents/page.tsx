"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "@/lib/api";
import type { Paginated, Incident } from "@/lib/types";
import { SeverityBadge, StatusBadge } from "@/components/Badge";

export default function IncidentsPage() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    const q = statusFilter ? `?status=${statusFilter}&limit=100` : "?limit=100";
    api
      .get<Paginated<Incident>>(`/api/v1/incidents${q}`)
      .then((res) => setIncidents(res.items))
      .finally(() => setLoading(false));
  }, [statusFilter]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Incidents</h1>
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="rounded-md border border-gray-300 px-2 py-1 text-sm"
        >
          <option value="">All statuses</option>
          <option value="OPEN">Open</option>
          <option value="ACKNOWLEDGED">Acknowledged</option>
          <option value="INVESTIGATING">Investigating</option>
          <option value="RESOLVED">Resolved</option>
          <option value="CLOSED">Closed</option>
        </select>
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
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Opened</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {incidents.map((i) => (
                <tr key={i.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2">
                    <SeverityBadge severity={i.severity} />
                  </td>
                  <td className="px-4 py-2 text-sm">
                    <Link href={`/incidents/${i.id}`} className="font-medium text-blue-600 hover:underline">
                      {i.title}
                    </Link>
                  </td>
                  <td className="px-4 py-2">
                    <StatusBadge status={i.status} />
                  </td>
                  <td className="px-4 py-2 text-sm text-gray-500">{new Date(i.opened_at).toLocaleString()}</td>
                </tr>
              ))}
              {incidents.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-6 text-center text-sm text-gray-400">
                    No incidents
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
