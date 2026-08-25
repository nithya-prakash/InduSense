const SEVERITY_COLORS: Record<string, string> = {
  CRITICAL: "bg-red-100 text-red-800 border-red-300",
  HIGH: "bg-orange-100 text-orange-800 border-orange-300",
  WARNING: "bg-yellow-100 text-yellow-800 border-yellow-300",
  INFO: "bg-blue-100 text-blue-800 border-blue-300",
};

const STATUS_COLORS: Record<string, string> = {
  OPEN: "bg-red-100 text-red-800 border-red-300",
  ACKNOWLEDGED: "bg-yellow-100 text-yellow-800 border-yellow-300",
  INVESTIGATING: "bg-blue-100 text-blue-800 border-blue-300",
  RESOLVED: "bg-green-100 text-green-800 border-green-300",
  CLOSED: "bg-gray-100 text-gray-700 border-gray-300",
  RUNNING: "bg-green-100 text-green-800 border-green-300",
  IDLE: "bg-gray-100 text-gray-700 border-gray-300",
  MAINTENANCE: "bg-blue-100 text-blue-800 border-blue-300",
  FAULT: "bg-red-100 text-red-800 border-red-300",
  STOPPED: "bg-gray-100 text-gray-700 border-gray-300",
  ACTIVE: "bg-green-100 text-green-800 border-green-300",
  OFFLINE: "bg-gray-100 text-gray-700 border-gray-300",
  PROVISIONED: "bg-blue-100 text-blue-800 border-blue-300",
  DECOMMISSIONED: "bg-gray-100 text-gray-500 border-gray-300",
};

function Badge({ label, colorMap }: { label: string; colorMap: Record<string, string> }) {
  const cls = colorMap[label] ?? "bg-gray-100 text-gray-700 border-gray-300";
  return (
    <span className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${cls}`}>
      {label}
    </span>
  );
}

export function SeverityBadge({ severity }: { severity: string }) {
  return <Badge label={severity} colorMap={SEVERITY_COLORS} />;
}

export function StatusBadge({ status }: { status: string }) {
  return <Badge label={status} colorMap={STATUS_COLORS} />;
}
