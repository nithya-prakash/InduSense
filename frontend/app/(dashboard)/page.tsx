"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, API_BASE } from "@/lib/api";
import type { Paginated, Factory, Device, Alert, Incident, HealthStatus } from "@/lib/types";
import { StatCard } from "@/components/StatCard";
import { SeverityBadge } from "@/components/Badge";

export default function OverviewPage() {
  const [factories, setFactories] = useState<Paginated<Factory> | null>(null);
  const [activeDevices, setActiveDevices] = useState<Paginated<Device> | null>(null);
  const [openAlerts, setOpenAlerts] = useState<Paginated<Alert> | null>(null);
  const [openIncidents, setOpenIncidents] = useState<Paginated<Incident> | null>(null);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [recentAlerts, setRecentAlerts] = useState<Alert[]>([]);

  useEffect(() => {
    api.get<Paginated<Factory>>("/api/v1/factories?limit=100").then(setFactories).catch(() => {});
    api
      .get<Paginated<Device>>("/api/v1/devices?status=ACTIVE&limit=100")
      .then(setActiveDevices)
      .catch(() => {});
    api
      .get<Paginated<Alert>>("/api/v1/alerts?status=OPEN&limit=100")
      .then((res) => {
        setOpenAlerts(res);
        setRecentAlerts(res.items.slice(0, 5));
      })
      .catch(() => {});
    api
      .get<Paginated<Incident>>("/api/v1/incidents?status=OPEN&limit=100")
      .then(setOpenIncidents)
      .catch(() => {});
    fetch(`${API_BASE}/health`)
      .then((r) => r.json())
      .then(setHealth)
      .catch(() => {});
  }, []);

  const countLabel = (p: Paginated<unknown> | null) =>
    p == null ? "…" : p.returned_count >= p.limit ? `${p.returned_count}+` : `${p.returned_count}`;

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Overview</h1>
        <p className="text-sm text-gray-500">Musterfabrik GmbH — real-time platform status</p>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatCard label="Factories" value={countLabel(factories)} accent="blue" />
        <StatCard label="Active devices" value={countLabel(activeDevices)} accent="green" />
        <StatCard label="Open alerts" value={countLabel(openAlerts)} accent="red" />
        <StatCard label="Open incidents" value={countLabel(openIncidents)} accent="yellow" />
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold text-gray-900">Platform health</h2>
          {!health ? (
            <div className="text-sm text-gray-400">Loading...</div>
          ) : (
            <div className="grid grid-cols-2 gap-2 text-sm">
              {Object.entries(health).map(([dep, status]) => (
                <div key={dep} className="flex items-center justify-between rounded-md border border-gray-100 px-3 py-2">
                  <span className="font-medium capitalize text-gray-700">{dep}</span>
                  <span className={status === "ok" ? "text-green-600" : "text-red-600"}>{status === "ok" ? "● ok" : "● down"}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-gray-900">Recent open alerts</h2>
            <Link href="/alerts" className="text-xs font-medium text-blue-600 hover:underline">
              View all
            </Link>
          </div>
          {recentAlerts.length === 0 ? (
            <div className="text-sm text-gray-400">No open alerts</div>
          ) : (
            <ul className="divide-y divide-gray-100">
              {recentAlerts.map((a) => (
                <li key={a.id} className="flex items-center justify-between py-2 text-sm">
                  <span className="truncate text-gray-800">{a.title}</span>
                  <SeverityBadge severity={a.severity} />
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
        <h2 className="mb-3 text-sm font-semibold text-gray-900">Factories</h2>
        {!factories ? (
          <div className="text-sm text-gray-400">Loading...</div>
        ) : (
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            {factories.items.map((f) => (
              <Link
                key={f.id}
                href={`/factories/${f.id}`}
                className="rounded-md border border-gray-200 p-3 text-sm hover:border-blue-300 hover:bg-blue-50"
              >
                <div className="font-medium text-gray-900">{f.name}</div>
                <div className="text-xs text-gray-500">
                  {f.city}, {f.country}
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
