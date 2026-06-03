import { env } from "@/lib/env";

function formatDateYmd(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export function enumerateDays(startDay: string, endDay: string): string[] {
  const days: string[] = [];
  const cur = new Date(`${startDay}T00:00:00`);
  const end = new Date(`${endDay}T00:00:00`);
  while (cur <= end) {
    days.push(formatDateYmd(cur));
    cur.setDate(cur.getDate() + 1);
  }
  return days;
}

/** End = yesterday; start = end − (days − 1). */
export function getHistoryRange(historyDays: number): {
  startDay: string;
  endDay: string;
  days: number;
} {
  const end = new Date();
  end.setHours(0, 0, 0, 0);
  end.setDate(end.getDate() - 1);
  const start = new Date(end);
  start.setDate(start.getDate() - (historyDays - 1));
  return {
    startDay: formatDateYmd(start),
    endDay: formatDateYmd(end),
    days: historyDays,
  };
}

export async function fetchHntPricesByDay(
  historyDays: number
): Promise<Map<string, number>> {
  const id = env.coingeckoCoinId;
  const days = Math.max(1, Math.min(365, historyDays + 2));
  const url = `https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}/market_chart?vs_currency=usd&days=${days}`;

  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`CoinGecko HTTP ${res.status}`);
  }

  const json = (await res.json()) as { prices?: [number, number][] };
  const byDay = new Map<string, number>();
  for (const [ts, price] of json.prices || []) {
    if (!Number.isFinite(price)) continue;
    byDay.set(formatDateYmd(new Date(ts)), price);
  }
  return byDay;
}

export function fillPricesForDays(
  allDays: string[],
  priceByDay: Map<string, number>
): Map<string, number> {
  const filled = new Map<string, number>();
  let last = priceByDay.size ? [...priceByDay.values()][0] : 0;
  for (const day of allDays) {
    if (priceByDay.has(day)) last = priceByDay.get(day)!;
    filled.set(day, last);
  }
  return filled;
}
