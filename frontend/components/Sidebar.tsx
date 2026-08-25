"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useAuth } from "@/lib/auth-context";

const NAV_ITEMS = [
  { href: "/", label: "Overview" },
  { href: "/factories", label: "Factories" },
  { href: "/devices", label: "Devices" },
  { href: "/alerts", label: "Alerts" },
  { href: "/incidents", label: "Incidents" },
  { href: "/admin", label: "Administration" },
];

export function Sidebar() {
  const pathname = usePathname();
  const { me, logout } = useAuth();

  return (
    <aside className="sticky top-0 flex h-screen w-60 shrink-0 flex-col self-start overflow-y-auto border-r border-gray-200 bg-white">
      <div className="border-b border-gray-200 px-4 py-4">
        <div className="text-lg font-bold text-gray-900">InduSense</div>
        <div className="text-xs text-gray-500">Industrial IoT Monitoring</div>
      </div>
      <nav className="flex-1 space-y-1 px-2 py-4">
        {NAV_ITEMS.map((item) => {
          const active = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`block rounded-md px-3 py-2 text-sm font-medium ${
                active ? "bg-blue-50 text-blue-700" : "text-gray-700 hover:bg-gray-50"
              }`}
            >
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="border-t border-gray-200 px-4 py-4">
        <div className="text-sm font-medium text-gray-900">{me?.email}</div>
        <div className="text-xs text-gray-500">{me?.roles?.join(", ")}</div>
        <button
          onClick={() => logout()}
          className="mt-2 text-xs font-medium text-gray-500 hover:text-gray-800"
        >
          Log out
        </button>
      </div>
    </aside>
  );
}
