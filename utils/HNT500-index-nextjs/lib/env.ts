function req(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function opt(name: string, fallback: string): string {
  return process.env[name]?.trim() || fallback;
}

function optInt(name: string, fallback: number): number {
  const n = parseInt(process.env[name] || "", 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function optBool(name: string, fallback: boolean): boolean {
  const v = process.env[name]?.trim().toLowerCase();
  if (v === "true" || v === "1" || v === "yes") return true;
  if (v === "false" || v === "0" || v === "no") return false;
  return fallback;
}

export const env = {
  nexusBaseUrl: opt("NEXUS_BASE_URL", "https://nexus.fractals.finance").replace(/\/$/, ""),
  nexusApiKey: () => req("NEXUS_API_KEY"),
  nexusToken: opt("NEXUS_TOKEN", ""),
  indexSize: optInt("INDEX_SIZE", 500),
  weightMode: opt("WEIGHT_MODE", "earnings") as "earnings" | "equal" | "rank",
  historyDays: optInt("INDEX_HISTORY_DAYS", 30),
  coingeckoCoinId: opt("COINGECKO_COIN_ID", "helium"),
  sqlitePath: opt("SQLITE_PATH", "./data/hnt500.db"),
  cronSchedule: opt("CRON_SCHEDULE", "0 */1 * * *"),
  runBuildOnStart: optBool("RUN_BUILD_ON_START", true),
  rebuildSecret: opt("REBUILD_SECRET", ""),
  pageDelayMs: optInt("TOP_EARNERS_PAGE_DELAY_MS", 400),
  dayDelayMs: optInt("TOP_EARNERS_DAY_DELAY_MS", 350),
};

export type WeightMode = typeof env.weightMode;
