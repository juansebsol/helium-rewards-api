import { NextResponse } from "next/server";
import { buildIndexHistory, isBuilding } from "@/lib/build-index";
import { env } from "@/lib/env";

export const dynamic = "force-dynamic";

export async function POST(req: Request) {
  if (env.rebuildSecret) {
    const auth = req.headers.get("authorization") || "";
    const token = auth.replace(/^Bearer\s+/i, "").trim();
    if (token !== env.rebuildSecret) {
      return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
    }
  } else {
    return NextResponse.json(
      { ok: false, error: "Set REBUILD_SECRET to enable manual rebuild" },
      { status: 403 }
    );
  }

  if (isBuilding()) {
    return NextResponse.json({ ok: false, error: "Build already running" }, { status: 409 });
  }

  try {
    const result = await buildIndexHistory();
    return NextResponse.json({ ok: true, result });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}
