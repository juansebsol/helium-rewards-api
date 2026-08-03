import { NextResponse } from "next/server";
import { getDeviceHistory } from "@/lib/db";
import { env } from "@/lib/env";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const deviceId = url.searchParams.get("device_id")?.trim();
  if (!deviceId) {
    return NextResponse.json(
      { ok: false, error: "device_id is required" },
      { status: 400 }
    );
  }

  const throughDay =
    url.searchParams.get("through")?.trim() ||
    url.searchParams.get("day")?.trim();
  if (!throughDay) {
    return NextResponse.json(
      { ok: false, error: "through (or day) is required" },
      { status: 400 }
    );
  }

  const n = Math.min(
    500,
    Math.max(
      1,
      parseInt(url.searchParams.get("n") || String(env.indexSize), 10) ||
        env.indexSize
    )
  );
  const weightMode = url.searchParams.get("weight_mode") || env.weightMode;
  const days = Math.min(
    90,
    Math.max(1, parseInt(url.searchParams.get("days") || "14", 10) || 14)
  );

  const history = getDeviceHistory(deviceId, n, weightMode, throughDay, days);
  const sumHnt = history.reduce((s, r) => s + r.total_hnt, 0);
  const withDelta = history.map((row, i) => {
    // history is DESC; next index is older day
    const older = history[i + 1];
    const delta_hnt = older ? row.total_hnt - older.total_hnt : null;
    const share =
      row.basket_hnt && row.basket_hnt > 0
        ? (row.total_hnt / row.basket_hnt) * 100
        : null;
    return { ...row, delta_hnt, share };
  });

  return NextResponse.json({
    ok: true,
    device_id: deviceId,
    through: throughDay,
    params: { n, weight_mode: weightMode, days },
    summary: {
      days_in_window: history.length,
      window_days: days,
      sum_hnt: sumHnt,
      avg_hnt: history.length ? sumHnt / history.length : 0,
    },
    items: withDelta,
  });
}
