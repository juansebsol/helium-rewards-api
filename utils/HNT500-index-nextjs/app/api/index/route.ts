import { NextResponse } from "next/server";
import { getIndexDays, getMeta } from "@/lib/db";
import { env } from "@/lib/env";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const days = Math.min(
    365,
    Math.max(1, parseInt(url.searchParams.get("days") || String(env.historyDays), 10) || env.historyDays)
  );
  const n = Math.min(
    500,
    Math.max(1, parseInt(url.searchParams.get("n") || String(env.indexSize), 10) || env.indexSize)
  );
  const weightMode = url.searchParams.get("weight_mode") || env.weightMode;

  const rows = getIndexDays(days, n, weightMode).reverse();
  const meta = getMeta();

  const series = rows.map((r) => ({
    day: r.day,
    basket_hnt: r.basket_hnt,
    basket_usd: r.basket_usd,
    hnt_price: r.hnt_price,
    constituent_count: r.constituent_count,
    requested_n: r.n,
    fetch_complete: r.fetch_complete !== 0,
    updated_at: r.updated_at,
  }));

  return NextResponse.json({
    ok: true,
    params: { days, n, weight_mode: weightMode },
    range: series.length
      ? { start_day: series[0].day, end_day: series[series.length - 1].day }
      : null,
    series,
    meta: {
      db_path: meta.dbPath,
      row_count: meta.rowCount,
      latest_day: meta.latestDay,
      last_build: meta.latestBuild,
    },
  });
}
