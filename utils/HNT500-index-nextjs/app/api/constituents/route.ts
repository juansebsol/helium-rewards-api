import { NextResponse } from "next/server";
import {
  getConstituentsForDay,
  getDaysInIndexByDevice,
  getIndexDay,
  getPreviousConstituentDay,
  getStreakByDevice,
  hasConstituentsForDay,
  type ConstituentRow,
} from "@/lib/db";
import { env } from "@/lib/env";
import {
  resolveEntityNames,
  upsertEntityNames,
} from "@/lib/entity-names";

export const dynamic = "force-dynamic";
export const maxDuration = 60;

type Status = "new" | "stayed";

type EnrichedItem = ConstituentRow & {
  entity_name: string | null;
  status: Status;
  prev_hnt: number | null;
  prev_rank: number | null;
  delta_hnt: number | null;
  delta_rank: number | null;
  days_in_index: number;
  streak_days: number;
};

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
  const resolveNames = url.searchParams.get("resolve_names") !== "0";

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
  const prevDay = getPreviousConstituentDay(day, n, weightMode);
  const prevItems = prevDay
    ? getConstituentsForDay(prevDay, n, weightMode)
    : [];
  const prevById = new Map(prevItems.map((r) => [r.device_id, r]));

  const deviceIds = items.map((r) => r.device_id);
  const daysIn = getDaysInIndexByDevice(deviceIds, n, weightMode, day);
  const streaks = getStreakByDevice(deviceIds, n, weightMode, day);

  // Prefer stored names; fill gaps from local cache / Helium entities API
  const storedNames = items
    .filter((r) => r.entity_name)
    .map((r) => ({ device_id: r.device_id, entity_name: r.entity_name! }));
  if (storedNames.length) upsertEntityNames(storedNames);

  let nameMap = new Map<string, string>();
  if (resolveNames) {
    nameMap = await resolveEntityNames(deviceIds, {
      maxLookups: 120,
      concurrency: 12,
    });
  }

  const enriched: EnrichedItem[] = items.map((row) => {
    const prev = prevById.get(row.device_id);
    const status: Status = prev ? "stayed" : "new";
    const delta_hnt = prev ? row.total_hnt - prev.total_hnt : null;
    const delta_rank = prev ? prev.rank - row.rank : null; // positive = moved up
    const entity_name =
      row.entity_name || nameMap.get(row.device_id) || null;

    return {
      ...row,
      entity_name,
      status,
      prev_hnt: prev?.total_hnt ?? null,
      prev_rank: prev?.rank ?? null,
      delta_hnt,
      delta_rank,
      days_in_index: daysIn.get(row.device_id) ?? 1,
      streak_days: streaks.get(row.device_id) ?? 1,
    };
  });

  const newItems = enriched.filter((r) => r.status === "new");
  const stayed = enriched.filter((r) => r.status === "stayed");
  const outperformers = stayed
    .filter((r) => (r.delta_hnt ?? 0) > 0)
    .sort((a, b) => (b.delta_hnt ?? 0) - (a.delta_hnt ?? 0));
  const underperformers = stayed
    .filter((r) => (r.delta_hnt ?? 0) < 0)
    .sort((a, b) => (a.delta_hnt ?? 0) - (b.delta_hnt ?? 0));

  const exited = prevItems
    .filter((p) => !items.some((t) => t.device_id === p.device_id))
    .map((p) => ({
      rank: p.rank,
      device_id: p.device_id,
      total_hnt: p.total_hnt,
      entity_name: p.entity_name || nameMap.get(p.device_id) || null,
    }));

  const newHnt = newItems.reduce((s, r) => s + r.total_hnt, 0);
  const exitedHnt = exited.reduce((s, r) => s + r.total_hnt, 0);
  const outperformHnt = outperformers.reduce((s, r) => s + (r.delta_hnt ?? 0), 0);
  const underperformHnt = underperformers.reduce(
    (s, r) => s + (r.delta_hnt ?? 0),
    0
  );
  const sumHnt = items.reduce((s, r) => s + r.total_hnt, 0);
  const prevSumHnt = prevItems.reduce((s, r) => s + r.total_hnt, 0);
  const basketDeltaHnt = prevDay ? sumHnt - prevSumHnt : null;

  const topN = 8;

  return NextResponse.json({
    ok: true,
    day,
    prev_day: prevDay,
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
      prev_sum_hnt: prevDay ? prevSumHnt : null,
      basket_delta_hnt: basketDeltaHnt,
      new_count: newItems.length,
      exited_count: exited.length,
      stayed_count: stayed.length,
      new_hnt: newHnt,
      exited_hnt: exitedHnt,
      outperform_hnt: outperformHnt,
      underperform_hnt: underperformHnt,
    },
    movers: {
      top_new: [...newItems]
        .sort((a, b) => b.total_hnt - a.total_hnt)
        .slice(0, topN)
        .map(pickMover),
      top_outperformers: outperformers.slice(0, topN).map(pickMover),
      top_underperformers: underperformers.slice(0, topN).map(pickMover),
      top_exited: [...exited]
        .sort((a, b) => b.total_hnt - a.total_hnt)
        .slice(0, topN),
    },
    items: enriched,
  });
}

function pickMover(r: EnrichedItem) {
  return {
    rank: r.rank,
    device_id: r.device_id,
    entity_name: r.entity_name,
    total_hnt: r.total_hnt,
    delta_hnt: r.delta_hnt,
    delta_rank: r.delta_rank,
    days_in_index: r.days_in_index,
    streak_days: r.streak_days,
    status: r.status,
  };
}
