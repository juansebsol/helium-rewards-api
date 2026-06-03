import { getIndexDays, getMeta, type IndexDayRow } from "@/lib/db";
import { env } from "@/lib/env";
import type { IndexPoint, IndexStats } from "@/lib/types";

export type { IndexPoint, IndexStats } from "@/lib/types";

export function loadIndexSeries(days = env.historyDays): IndexPoint[] {
  const rows = getIndexDays(days, env.indexSize, env.weightMode).reverse();
  return rows.map((r: IndexDayRow) => ({
    day: r.day,
    basket_hnt: r.basket_hnt,
    basket_usd: r.basket_usd,
    hnt_price: r.hnt_price,
    constituent_count: r.constituent_count,
    requested_n: r.n,
    fetch_complete: r.fetch_complete !== 0,
  }));
}

export function computeStats(series: IndexPoint[]): IndexStats {
  const latest = series.length ? series[series.length - 1] : null;
  const previous = series.length > 1 ? series[series.length - 2] : null;
  const changePct =
    latest && previous && previous.basket_usd > 0
      ? ((latest.basket_usd - previous.basket_usd) / previous.basket_usd) * 100
      : null;

  return {
    latest,
    previous,
    changePct,
    periodSumUsd: series.reduce((s, r) => s + r.basket_usd, 0),
    periodSumHnt: series.reduce((s, r) => s + r.basket_hnt, 0),
    days: series.length,
    incompleteDays: series.filter((r) => !r.fetch_complete).length,
  };
}

export function loadIndexPayload(days = env.historyDays) {
  const series = loadIndexSeries(days);
  const stats = computeStats(series);
  const meta = getMeta();
  return {
    series,
    stats,
    meta,
    config: {
      n: env.indexSize,
      weightMode: env.weightMode,
      historyDays: days,
    },
  };
}
