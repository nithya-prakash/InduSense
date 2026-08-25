"use client";

import { useEffect, useState, useCallback } from "react";
import { api, ApiError } from "@/lib/api";
import type { Incident, IncidentEvent } from "@/lib/types";
import { SeverityBadge, StatusBadge } from "@/components/Badge";
import { useAuth } from "@/lib/auth-context";

// Mirrors pkg/incidents' state machine (services/alert-service / api) —
// duplicated here only to decide which action buttons make sense to show;
// the backend remains the sole source of truth and re-validates every
// transition regardless of what the UI offers.
const VALID_TRANSITIONS: Record<string, string[]> = {
  OPEN: ["ACKNOWLEDGED", "INVESTIGATING", "RESOLVED"],
  ACKNOWLEDGED: ["INVESTIGATING", "RESOLVED"],
  INVESTIGATING: ["RESOLVED"],
  RESOLVED: ["CLOSED", "INVESTIGATING"],
  CLOSED: [],
};

export function IncidentDetailClient({ id }: { id: string }) {
  const [incident, setIncident] = useState<Incident | null>(null);
  const [history, setHistory] = useState<IncidentEvent[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [resolutionNotes, setResolutionNotes] = useState("");
  const { hasPermission } = useAuth();

  const load = useCallback(() => {
    api
      .get<{ incident: Incident; history: IncidentEvent[] }>(`/api/v1/incidents/${id}`)
      .then((res) => {
        setIncident(res.incident);
        setHistory(res.history);
      })
      .catch(() => setError("Incident not found"));
  }, [id]);

  useEffect(load, [load]);

  async function transition(status: string) {
    setBusy(true);
    setError(null);
    try {
      if (status === "RESOLVED") {
        await api.post(`/api/v1/incidents/${id}/resolve`, { resolution_notes: resolutionNotes });
      } else {
        await api.post(`/api/v1/incidents/${id}/transition`, { status });
      }
      load();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Transition failed");
    } finally {
      setBusy(false);
    }
  }

  if (error && !incident) return <div className="text-sm text-red-600">{error}</div>;
  if (!incident) return <div className="text-sm text-gray-400">Loading...</div>;

  const canManage = hasPermission("incidents:manage");
  const nextStates = VALID_TRANSITIONS[incident.status] ?? [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{incident.title}</h1>
          <p className="text-sm text-gray-500">{incident.description}</p>
        </div>
        <div className="flex items-center gap-2">
          <SeverityBadge severity={incident.severity} />
          <StatusBadge status={incident.status} />
        </div>
      </div>

      {error && <div className="rounded-md bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div>}

      {canManage && nextStates.length > 0 && (
        <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold text-gray-900">Actions</h2>
          {nextStates.includes("RESOLVED") && (
            <div className="mb-3 flex gap-2">
              <input
                type="text"
                placeholder="Resolution notes..."
                value={resolutionNotes}
                onChange={(e) => setResolutionNotes(e.target.value)}
                className="flex-1 rounded-md border border-gray-300 px-2 py-1 text-sm"
              />
            </div>
          )}
          <div className="flex flex-wrap gap-2">
            {nextStates.map((status) => (
              <button
                key={status}
                disabled={busy}
                onClick={() => transition(status)}
                className="rounded-md border border-gray-300 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-100 disabled:opacity-50"
              >
                Move to {status}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
        <h2 className="mb-3 text-sm font-semibold text-gray-900">Audit history</h2>
        <ol className="space-y-3 border-l-2 border-gray-100 pl-4">
          {history.map((e, i) => (
            <li key={i} className="text-sm">
              <div className="font-medium text-gray-800">
                {e.event_type}
                {e.old_value && e.new_value ? ` (${e.old_value} → ${e.new_value})` : ""}
              </div>
              {e.note && <div className="text-gray-600">{e.note}</div>}
              <div className="text-xs text-gray-400">{new Date(e.created_at).toLocaleString()}</div>
            </li>
          ))}
        </ol>
      </div>
    </div>
  );
}
