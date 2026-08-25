"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "@/lib/api";
import type { Paginated, Factory } from "@/lib/types";

export default function FactoriesPage() {
  const [factories, setFactories] = useState<Paginated<Factory> | null>(null);

  useEffect(() => {
    api.get<Paginated<Factory>>("/api/v1/factories?limit=100").then(setFactories).catch(() => {});
  }, []);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Factories</h1>

      {!factories ? (
        <div className="text-sm text-gray-400">Loading...</div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Name</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">City</th>
                <th className="px-4 py-2 text-left text-xs font-medium uppercase text-gray-500">Country</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {factories.items.map((f) => (
                <tr key={f.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 text-sm">
                    <Link href={`/factories/${f.id}`} className="font-medium text-blue-600 hover:underline">
                      {f.name}
                    </Link>
                  </td>
                  <td className="px-4 py-2 text-sm text-gray-700">{f.city}</td>
                  <td className="px-4 py-2 text-sm text-gray-700">{f.country}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
