import type { TopEarnerItem } from "@/lib/nexus";
import type { WeightMode } from "@/lib/env";

export function computeWeights(
  items: TopEarnerItem[],
  mode: WeightMode
): Array<TopEarnerItem & { weight: number }> {
  const n = items.length;
  if (!n) return [];

  if (mode === "equal") {
    const w = 1 / n;
    return items.map((it) => ({ ...it, weight: w }));
  }

  if (mode === "rank") {
    const denom = (n * (n + 1)) / 2;
    return items.map((it) => ({
      ...it,
      weight: (n - it.rank + 1) / denom,
    }));
  }

  const totalHnt = items.reduce((s, it) => s + it.total_hnt, 0);
  if (totalHnt <= 0) {
    const w = 1 / n;
    return items.map((it) => ({ ...it, weight: w }));
  }

  return items.map((it) => ({
    ...it,
    weight: it.total_hnt / totalHnt,
  }));
}

/** Total HNT earned by all top-N constituents that day (what users expect). */
export function basketHntTotal(items: TopEarnerItem[]): number {
  return items.reduce((s, c) => s + (c.total_hnt || 0), 0);
}

/** Legacy: Σ(weight × HNT) — ~average per device, not top-N total. */
export function basketHntFromWeighted(
  weighted: Array<TopEarnerItem & { weight: number }>
): number {
  return weighted.reduce((s, c) => s + c.weight * c.total_hnt, 0);
}
