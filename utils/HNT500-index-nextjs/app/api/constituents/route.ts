import { NextResponse } from "next/server";
import {
  getConstituentsForDay,
  getIndexDay,
  hasConstituentsForDay,
} from "@/lib/db";
import { env } from "@/lib/env";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const day = url.searchParams.get("day")?.trim();
  if (!day) {
    return NextResponse.json({ ok: false, error: "day is required" }, { status: 400 });
  }

  const n = Math.min(
    500,
    Math.max(1, parseInt(url.searchParams.get("n") || String(env.indexSize), 10) || env.indexSize)
  );
  const weightMode = url.searchParams.get("weight_mode") || env.weightMode;

  const dayMeta = getIndexDay(day, n, weightMode);
  if (!dayMeta) {
    return NextResponse.json(
      { ok: false, error: `No index row for ${day}. Run npm run rebuild.` },
      { status: 404 }
    );
  }

  if (!hasConstituentsForDay(day, n, weightMode)) {
    return NextResponse.json({
      ok: false,
      error: "Constituent breakdown not stored for this day. Run npm run rebuild.",
      day,
      day_meta: dayMeta,
    }, { status: 404 });
  }

  const items = getConstituentsForDay(day, n, weightMode);
  const sumHnt = items.reduce((s, r) => s + r.total_hnt, 0);

  return NextResponse.json({
    ok: true,
    day,
    params: { n, weight_mode: weightMode },
    day_meta: {
      basket_hnt: dayMeta.basket_hnt,
      basket_usd: dayMeta.basket_usd,
      hnt_price: dayMeta.hnt_price,
      constituent_count: dayMeta.constituent_count,
      fetch_complete: dayMeta.fetch_complete !== 0,
    },
    summary: {
      count: items.length,
      sum_hnt: sumHnt,
    },
    items,
  });
}
