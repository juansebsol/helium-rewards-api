import { NextResponse } from "next/server";
import { resolveEntityNames } from "@/lib/entity-names";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function POST(req: Request) {
  let body: { device_ids?: string[] };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON" }, { status: 400 });
  }

  const ids = Array.from(
    new Set((body.device_ids || []).map((id) => String(id || "").trim()).filter(Boolean))
  ).slice(0, 100);

  if (!ids.length) {
    return NextResponse.json({ ok: true, names: {} });
  }

  const map = await resolveEntityNames(ids, {
    maxLookups: ids.length,
    concurrency: 12,
  });

  const names: Record<string, string> = {};
  for (const id of ids) {
    const name = map.get(id);
    if (name) names[id] = name;
  }

  return NextResponse.json({ ok: true, names });
}
