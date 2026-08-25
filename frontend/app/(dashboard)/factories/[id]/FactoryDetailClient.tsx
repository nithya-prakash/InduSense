"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "@/lib/api";
import type { Paginated, Factory, ProductionLine, Machine } from "@/lib/types";
import { StatusBadge } from "@/components/Badge";

export function FactoryDetailClient({ id }: { id: string }) {
  const [factory, setFactory] = useState<Factory | null>(null);
  const [lines, setLines] = useState<ProductionLine[]>([]);
  const [machinesByLine, setMachinesByLine] = useState<Record<string, Machine[]>>({});
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .get<Factory>(`/api/v1/factories/${id}`)
      .then(setFactory)
      .catch(() => setError("Factory not found"));

    api.get<Paginated<ProductionLine>>(`/api/v1/factories/${id}/production-lines`).then((res) => {
      setLines(res.items);
      res.items.forEach((line) => {
        api
          .get<Paginated<Machine>>(`/api/v1/production-lines/${line.id}/machines`)
          .then((r) => setMachinesByLine((prev) => ({ ...prev, [line.id]: r.items })))
          .catch(() => {});
      });
    });
  }, [id]);

  if (error) return <div className="text-sm text-red-600">{error}</div>;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">{factory?.name ?? "Loading..."}</h1>
        {factory && <p className="text-sm text-gray-500">{factory.city}, {factory.country}</p>}
      </div>

      <div className="space-y-4">
        {lines.map((line) => (
          <div key={line.id} className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
            <h2 className="mb-3 text-sm font-semibold text-gray-900">{line.name}</h2>
            {!machinesByLine[line.id] ? (
              <div className="text-sm text-gray-400">Loading machines...</div>
            ) : (
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-5">
                {machinesByLine[line.id].map((m) => (
                  <Link
                    key={m.id}
                    href={`/machines/${m.id}`}
                    className="rounded-md border border-gray-200 p-3 text-sm hover:border-blue-300 hover:bg-blue-50"
                  >
                    <div className="truncate font-medium text-gray-900">{m.name}</div>
                    <div className="text-xs text-gray-500">{m.machine_type.replaceAll("_", " ")}</div>
                    <div className="mt-1">
                      <StatusBadge status={m.status} />
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
