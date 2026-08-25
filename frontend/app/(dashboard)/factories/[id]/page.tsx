import { FactoryDetailClient } from "./FactoryDetailClient";

// Next.js 16 requires params to be awaited even in a page that just passes
// the id down to a client component — see the version-16 upgrade guide's
// "Async Request APIs" section.
export default async function FactoryDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <FactoryDetailClient id={id} />;
}
