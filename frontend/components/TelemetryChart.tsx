"use client";

import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import type { TelemetryPoint } from "@/lib/types";

export function TelemetryChart({ metric, unit, points }: { metric: string; unit: string; points: TelemetryPoint[] }) {
  const data = points.map((p) => ({
    time: new Date(p.time).toLocaleTimeString(),
    value: p.value,
  }));

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
      <h3 className="mb-2 text-sm font-semibold capitalize text-gray-900">
        {metric} <span className="font-normal text-gray-400">({unit})</span>
      </h3>
      {data.length === 0 ? (
        <div className="flex h-48 items-center justify-center text-sm text-gray-400">No data in this range</div>
      ) : (
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="time" tick={{ fontSize: 10 }} minTickGap={30} />
            <YAxis tick={{ fontSize: 10 }} width={40} />
            <Tooltip />
            <Line type="monotone" dataKey="value" stroke="#2563eb" strokeWidth={2} dot={false} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
