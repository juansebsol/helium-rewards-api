import { env } from "@/lib/env";

export type TopEarnerItem = {
  rank: number;
  device_id: string;
  total_hnt: number;
  total_dc: number;
  entity_name: string | null;
};

export type TopEarnersFetchMeta = {
  day: string;
  requested: number;
  loaded: number;
  complete: boolean;
  pagesOk: number;
  pagesFailed: number;
  apiTotal: number | null;
  sumHnt: number;
  errors: string[];
};

export type TopEarnersDayResult = {
  items: TopEarnerItem[];
  fetchMeta: TopEarnersFetchMeta;
};

const PAGE_SIZE = 100;

function headers(): Record<string, string> {
  const h: Record<string, string> = {
    Accept: "application/json",
    "x-api-key": env.nexusApiKey(),
  };
  if (env.nexusToken) h["x-nexus-token"] = env.nexusToken;
  return h;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function mapBatch(
  items?: Array<{
    rank: number;
    device_key?: string;
    device_id?: string;
    total_hnt?: number;
    total_dc?: number;
    name?: string;
    entity_name?: string;
  }>
): TopEarnerItem[] {
  return (items || [])
    .map((row) => {
      const nameRaw = row.entity_name || row.name || "";
      const entity_name = nameRaw.trim() ? nameRaw.trim() : null;
      return {
        rank: row.rank,
        device_id: String(row.device_key || row.device_id || ""),
        total_hnt: Number(row.total_hnt) || 0,
        total_dc: Number(row.total_dc) || 0,
        entity_name,
      };
    })
    .filter((x) => x.device_id);
}

async function fetchPageJson(
  url: string,
  attempt = 1
): Promise<{
  ok?: boolean;
  error?: string;
  items?: Array<{
    rank: number;
    device_key?: string;
    device_id?: string;
    total_hnt?: number;
    total_dc?: number;
  }>;
  total?: number;
}> {
  const maxAttempts = 3;
  const res = await fetch(url, { headers: headers() });
  const text = await res.text();
  let json: {
    ok?: boolean;
    error?: string;
    items?: Array<{
      rank: number;
      device_key?: string;
      device_id?: string;
      total_hnt?: number;
      total_dc?: number;
    }>;
    total?: number;
  };

  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Nexus non-JSON (${res.status}): ${text.slice(0, 120)}`);
  }

  const retryable = res.status === 429 || res.status >= 500;
  if ((!res.ok || json.ok === false) && retryable && attempt < maxAttempts) {
    await sleep(env.pageDelayMs * attempt * 2);
    return fetchPageJson(url, attempt + 1);
  }

  if (!res.ok || json.ok === false) {
    throw new Error(json.error || `Nexus HTTP ${res.status}`);
  }

  return json;
}

export async function fetchTopEarnersForDay(
  day: string,
  totalWanted: number
): Promise<TopEarnersDayResult> {
  const all: TopEarnerItem[] = [];
  const errors: string[] = [];
  let pagesOk = 0;
  let pagesFailed = 0;
  let apiTotal: number | null = null;
  const totalPages = Math.ceil(totalWanted / PAGE_SIZE);
  let page = 1;

  while (all.length < totalWanted && page <= totalPages + 2) {
    const perPage = Math.min(PAGE_SIZE, totalWanted - all.length);
    const url = new URL(`${env.nexusBaseUrl}/api/nexus/helium/top-earners`);
    url.searchParams.set("day", day);
    url.searchParams.set("page", String(page));
    url.searchParams.set("per_page", String(perPage));

    try {
      const json = await fetchPageJson(url.toString());
      const batch = mapBatch(json.items);
      if (typeof json.total === "number") apiTotal = json.total;

      if (!batch.length) {
        pagesFailed++;
        errors.push(`page ${page}: empty items`);
        break;
      }

      all.push(...batch);
      pagesOk++;

      const cap = Math.min(totalWanted, apiTotal ?? totalWanted);
      if (all.length >= cap) break;

      if (batch.length < perPage) {
        errors.push(`page ${page}: short page (${batch.length}/${perPage})`);
        break;
      }

      page++;
      await sleep(env.pageDelayMs);
    } catch (e) {
      pagesFailed++;
      errors.push(`page ${page}: ${e instanceof Error ? e.message : String(e)}`);
      break;
    }
  }

  const items = all.slice(0, totalWanted);
  const sumHnt = items.reduce((s, c) => s + c.total_hnt, 0);

  return {
    items,
    fetchMeta: {
      day,
      requested: totalWanted,
      loaded: items.length,
      complete: items.length >= totalWanted,
      pagesOk,
      pagesFailed,
      apiTotal,
      sumHnt,
      errors,
    },
  };
}
