export function StatCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: string | number;
  accent?: "red" | "yellow" | "green" | "blue" | "gray";
}) {
  const accentClass =
    {
      red: "border-l-red-500",
      yellow: "border-l-yellow-500",
      green: "border-l-green-500",
      blue: "border-l-blue-500",
      gray: "border-l-gray-400",
    }[accent ?? "gray"] ?? "border-l-gray-400";

  return (
    <div className={`rounded-lg border border-gray-200 border-l-4 ${accentClass} bg-white p-4 shadow-sm`}>
      <div className="text-sm text-gray-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold text-gray-900">{value}</div>
    </div>
  );
}
