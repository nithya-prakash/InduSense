"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import type { Paginated, Machine, Device, Sensor, TelemetryPoint, Alert, Incident } from "@/lib/types";
import { StatusBadge, SeverityBadge } from "@/components/Badge";
import { TelemetryChart } from "@/components/TelemetryChart";

interface SensorSeries {
  sensor: Sensor;
  points: TelemetryPoint[];
}

export function MachineDetailClient({ id }: { id: string }) {
  const [machine, setMachine] = useState<Machine | null>(null);
  const [devices, setDevices] = useState<Device[]>([]);
  const [series, setSeries] = useState<Record<string, SensorSeries[]>>({});
  const [range, setRange] = useState<"5m" | "1h" | "24h">("1h");
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [incidents, setIncidents] = useState<Incident[]>([]);

  useEffect(() => {
    api.get<Machine>(`/api/v1/machines/${id}`).then(setMachine).catch(() => {});
    api.get<Paginated<Device>>(`/api/v1/machines/${id}/devices`).then((res) => setDevices(res.items)).catch(() => {});
    api.get<Paginated<Alert>>(`/api/v1/alerts?limit=100`).then((res) =>
      setAlerts(res.items.filter((a) => a.machine_id === id))
    );
    api.get<Paginated<Incident>>(`/api/v1/incidents?limit=100`).then((res) =>
      setIncidents(res.items.filter((i) => i.machine_id === id))
    );
  }, [id]);

  useEffect(() => {
    devices.forEach((device) => {
      api.get<Paginated<Sensor>>(`/api/v1/devices/${device.id}/sensors`).then(async (res) => {
        const withPoints = await Promise.all(
          res.items.map(async (sensor) => {
            try {
              const range_res = await api.get<Paginated<TelemetryPoint>>(
                `/api/v1/telemetry/range?device_id=${device.id}&metric=${sensor.metric}&range=${range}`
              );
              return { sensor, points: range_res.items };
            } catch {
              return { sensor, points: [] };
            }
          })
        );
        setSeries((prev) => ({ ...prev, [device.id]: withPoints }));
      });
    });
  }, [devices, range]);

  if (!machine) return <div className="text-sm text-gray-400">Loading...</div>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{machine.name}</h1>
          <p className="text-sm text-gray-500">{machine.machine_type.replaceAll("_", " ")}</p>
        </div>
        <StatusBadge status={machine.status} />
      </div>

      <div className="flex gap-2">
        {(["5m", "1h", "24h"] as const).map((r) => (
          <button
            key={r}
            onClick={() => setRange(r)}
            className={`rounded-md border px-3 py-1 text-xs font-medium ${
              range === r ? "border-blue-600 bg-blue-50 text-blue-700" : "border-gray-300 text-gray-600"
            }`}
          >
            {r}
          </button>
        ))}
      </div>

      {devices.map((device) => (
        <div key={device.id} className="space-y-3">
          <h2 className="text-sm font-semibold text-gray-700">
            Device {device.serial_number} <StatusBadge status={device.status} />
          </h2>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
            {(series[device.id] ?? []).map((s) => (
              <TelemetryChart key={s.sensor.id} metric={s.sensor.metric} unit={s.sensor.unit} points={s.points} />
            ))}
          </div>
        </div>
      ))}

      <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
        <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <h2 className="mb-2 text-sm font-semibold text-gray-900">Alerts for this machine</h2>
          {alerts.length === 0 ? (
            <div className="text-sm text-gray-400">None</div>
          ) : (
            <ul className="divide-y divide-gray-100">
              {alerts.map((a) => (
                <li key={a.id} className="flex items-center justify-between py-2 text-sm">
                  <span className="truncate text-gray-800">{a.title}</span>
                  <SeverityBadge severity={a.severity} />
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <h2 className="mb-2 text-sm font-semibold text-gray-900">Incidents for this machine</h2>
          {incidents.length === 0 ? (
            <div className="text-sm text-gray-400">None</div>
          ) : (
            <ul className="divide-y divide-gray-100">
              {incidents.map((i) => (
                <li key={i.id} className="flex items-center justify-between py-2 text-sm">
                  <span className="truncate text-gray-800">{i.title}</span>
                  <StatusBadge status={i.status} />
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}
