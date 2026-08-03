import { env } from "@/lib/env";
import {
  finishBuildLog,
  replaceConstituentsForDay,
  startBuildLog,
  upsertIndexDay,
} from "@/lib/db";
import {
  enumerateDays,
  fetchHntPricesByDay,
  fillPricesForDays,
  getHistoryRange,
} from "@/lib/dates";
import { fetchTopEarnersForDay } from "@/lib/nexus";
import { resolveEntityNames } from "@/lib/entity-names";
import { basketHntTotal, computeWeights } from "@/lib/weights";

let building = false;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export type BuildResult = {
  daysBuilt: number;
  daysAttempted: number;
  incompleteDays: number;
  startDay: string;
  endDay: string;
  n: number;
  weightMode: string;
  issues: string[];
};

export async function buildIndexHistory(): Promise<BuildResult> {
  if (building) {
    throw new Error("Build already in progress");
  }

  building = true;
  const logId = startBuildLog();
  const n = env.indexSize;
  const weightMode = env.weightMode;
  const range = getHistoryRange(env.historyDays);
  const days = enumerateDays(range.startDay, range.endDay);
  const issues: string[] = [];

  let daysBuilt = 0;
  let incompleteDays = 0;

  try {
    console.log(
      `[index] Building ${days.length} days (${range.startDay} → ${range.endDay}), N=${n}`
    );

    const rawPrices = await fetchHntPricesByDay(env.historyDays);
    const prices = fillPricesForDays(days, rawPrices);
    const now = new Date().toISOString();

    for (let i = 0; i < days.length; i++) {
      const day = days[i];
      const { items, fetchMeta } = await fetchTopEarnersForDay(day, n);

      if (!items.length) {
        const msg = `${day}: no data (${fetchMeta.errors.join("; ") || "empty"})`;
        console.warn(`[index] ${msg}`);
        issues.push(msg);
        continue;
      }

      if (!fetchMeta.complete) {
        incompleteDays++;
        const msg =
          `${day}: ${fetchMeta.loaded}/${fetchMeta.requested} loaded, ` +
          `${fetchMeta.sumHnt.toFixed(0)} HNT — ${fetchMeta.errors.join("; ")}`;
        console.warn(`[index] ${msg}`);
        issues.push(msg);
      }

      const basketHnt = basketHntTotal(items);
      const hntPrice = prices.get(day) || 0;
      const basketUsd = basketHnt * hntPrice;

      upsertIndexDay({
        day,
        n,
        weight_mode: weightMode,
        basket_hnt: basketHnt,
        basket_usd: basketUsd,
        hnt_price: hntPrice,
        constituent_count: items.length,
        fetch_complete: fetchMeta.complete ? 1 : 0,
        updated_at: now,
      });

      const weighted = computeWeights(items, weightMode);
      // Resolve animal names for this day's basket (cached across days)
      const nameMap = await resolveEntityNames(
        weighted.map((c) => c.device_id),
        { maxLookups: weighted.length, concurrency: 12, writeConstituents: false }
      );

      replaceConstituentsForDay(
        day,
        n,
        weightMode,
        weighted.map((c) => ({
          rank: c.rank,
          device_id: c.device_id,
          total_hnt: c.total_hnt,
          total_dc: c.total_dc,
          weight: c.weight,
          entity_name: c.entity_name || nameMap.get(c.device_id) || null,
        }))
      );

      daysBuilt++;
      console.log(
        `[index] ${day} ${fetchMeta.loaded}/${n} devices ` +
          `basket_hnt=${basketHnt.toFixed(2)} usd=${basketUsd.toFixed(2)}` +
          (fetchMeta.complete ? "" : " INCOMPLETE")
      );

      if (i < days.length - 1) await sleep(env.dayDelayMs);
    }

    const status = daysBuilt > 0 ? "ok" : "error";
    const summary =
      issues.length > 0
        ? `${issues.length} issue(s); ${incompleteDays} incomplete day(s)`
        : undefined;
    finishBuildLog(logId, status, daysBuilt, summary);

    return {
      daysBuilt,
      daysAttempted: days.length,
      incompleteDays,
      startDay: range.startDay,
      endDay: range.endDay,
      n,
      weightMode,
      issues,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    finishBuildLog(logId, "error", daysBuilt, msg);
    throw err;
  } finally {
    building = false;
  }
}

export function isBuilding(): boolean {
  return building;
}
