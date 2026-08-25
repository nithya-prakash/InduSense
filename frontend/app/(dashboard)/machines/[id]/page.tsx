import { MachineDetailClient } from "./MachineDetailClient";

export default async function MachineDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <MachineDetailClient id={id} />;
}
