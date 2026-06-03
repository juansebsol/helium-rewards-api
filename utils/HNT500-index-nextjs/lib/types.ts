/** Serializable index point (safe for client components). */
export type IndexPoint = {
  day: string;
  basket_hnt: number;
  basket_usd: number;
  hnt_price: number;
  constituent_count: number;
  requested_n: number;
  fetch_complete: boolean;
};

export type IndexStats = {
  latest: IndexPoint | null;
  previous: IndexPoint | null;
  changePct: number | null;
  periodSumUsd: number;
  periodSumHnt: number;
  days: number;
  incompleteDays: number;
};
