import fs from "fs";
import path from "path";
import Database from "better-sqlite3";
import { env } from "@/lib/env";

let db: Database.Database | null = null;

export type IndexDayRow = {
  day: string;
  n: number;
  weight_mode: string;
  basket_hnt: number;
  basket_usd: number;
  hnt_price: number;
  constituent_count: number;
  fetch_complete: number;
  updated_at: string;
};

export type ConstituentRow = {
  rank: number;
  device_id: string;
  total_hnt: number;
  total_dc: number;
  weight: number;
  entity_name: string | null;
};

function migrateSchema(database: Database.Database): void {
  const dayCols = database
    .prepare(`PRAGMA table_info(index_days)`)
    .all() as { name: string }[];
  if (!dayCols.some((c) => c.name === "fetch_complete")) {
    database.exec(
      `ALTER TABLE index_days ADD COLUMN fetch_complete INTEGER NOT NULL DEFAULT 1`
    );
  }

  const constCols = database
    .prepare(`PRAGMA table_info(index_constituents)`)
    .all() as { name: string }[];
  if (constCols.length && !constCols.some((c) => c.name === "entity_name")) {
    database.exec(
      `ALTER TABLE index_constituents ADD COLUMN entity_name TEXT`
    );
  }

  database.exec(`
    CREATE INDEX IF NOT EXISTS idx_constituents_device
      ON index_constituents (n, weight_mode, device_id, day);
  `);
}

export type BuildLogRow = {
  id: number;
  started_at: string;
  finished_at: string | null;
  status: string;
  days_built: number;
  message: string | null;
};

function resolveDbPath(): string {
  const p = env.sqlitePath;
  return path.isAbsolute(p) ? p : path.join(process.cwd(), p);
}

export function getDb(): Database.Database {
  if (db) return db;

  const file = resolveDbPath();
  fs.mkdirSync(path.dirname(file), { recursive: true });

  db = new Database(file);
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS index_days (
      day TEXT NOT NULL,
      n INTEGER NOT NULL,
      weight_mode TEXT NOT NULL,
      basket_hnt REAL NOT NULL,
      basket_usd REAL NOT NULL,
      hnt_price REAL NOT NULL,
      constituent_count INTEGER NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (day, n, weight_mode)
    );

    CREATE TABLE IF NOT EXISTS build_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      status TEXT NOT NULL,
      days_built INTEGER NOT NULL DEFAULT 0,
      message TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_index_days_lookup
      ON index_days (n, weight_mode, day DESC);

    CREATE TABLE IF NOT EXISTS index_constituents (
      day TEXT NOT NULL,
      n INTEGER NOT NULL,
      weight_mode TEXT NOT NULL,
      rank INTEGER NOT NULL,
      device_id TEXT NOT NULL,
      total_hnt REAL NOT NULL,
      total_dc REAL NOT NULL,
      weight REAL NOT NULL,
      entity_name TEXT,
      PRIMARY KEY (day, n, weight_mode, rank)
    );

    CREATE INDEX IF NOT EXISTS idx_constituents_day
      ON index_constituents (day, n, weight_mode, rank);
  `);

  migrateSchema(db);

  return db;
}

export function upsertIndexDay(row: IndexDayRow): void {
  getDb()
    .prepare(
      `INSERT INTO index_days (day, n, weight_mode, basket_hnt, basket_usd, hnt_price, constituent_count, fetch_complete, updated_at)
       VALUES (@day, @n, @weight_mode, @basket_hnt, @basket_usd, @hnt_price, @constituent_count, @fetch_complete, @updated_at)
       ON CONFLICT(day, n, weight_mode) DO UPDATE SET
         basket_hnt = excluded.basket_hnt,
         basket_usd = excluded.basket_usd,
         hnt_price = excluded.hnt_price,
         constituent_count = excluded.constituent_count,
         fetch_complete = excluded.fetch_complete,
         updated_at = excluded.updated_at`
    )
    .run(row);
}

export function getIndexDays(
  days: number,
  n: number,
  weightMode: string
): IndexDayRow[] {
  return getDb()
    .prepare(
      `SELECT day, n, weight_mode, basket_hnt, basket_usd, hnt_price, constituent_count, fetch_complete, updated_at
       FROM index_days
       WHERE n = ? AND weight_mode = ?
       ORDER BY day DESC
       LIMIT ?`
    )
    .all(n, weightMode, days) as IndexDayRow[];
}

export function startBuildLog(): number {
  const started_at = new Date().toISOString();
  const r = getDb()
    .prepare(
      `INSERT INTO build_log (started_at, status, days_built) VALUES (?, 'running', 0)`
    )
    .run(started_at);
  return Number(r.lastInsertRowid);
}

export function finishBuildLog(
  id: number,
  status: "ok" | "error",
  daysBuilt: number,
  message?: string
): void {
  getDb()
    .prepare(
      `UPDATE build_log SET finished_at = ?, status = ?, days_built = ?, message = ? WHERE id = ?`
    )
    .run(new Date().toISOString(), status, daysBuilt, message ?? null, id);
}

export function getLatestBuildLog(): BuildLogRow | null {
  const row = getDb()
    .prepare(`SELECT * FROM build_log ORDER BY id DESC LIMIT 1`)
    .get();
  return (row as BuildLogRow) || null;
}

export function replaceConstituentsForDay(
  day: string,
  n: number,
  weightMode: string,
  rows: ConstituentRow[]
): void {
  const database = getDb();
  const del = database.prepare(
    `DELETE FROM index_constituents WHERE day = ? AND n = ? AND weight_mode = ?`
  );
  const ins = database.prepare(
    `INSERT INTO index_constituents (day, n, weight_mode, rank, device_id, total_hnt, total_dc, weight, entity_name)
     VALUES (@day, @n, @weight_mode, @rank, @device_id, @total_hnt, @total_dc, @weight, @entity_name)`
  );
  const run = database.transaction(() => {
    del.run(day, n, weightMode);
    for (const row of rows) {
      ins.run({
        day,
        n,
        weight_mode: weightMode,
        rank: row.rank,
        device_id: row.device_id,
        total_hnt: row.total_hnt,
        total_dc: row.total_dc,
        weight: row.weight,
        entity_name: row.entity_name ?? null,
      });
    }
  });
  run();
}

export function getConstituentsForDay(
  day: string,
  n: number,
  weightMode: string
): ConstituentRow[] {
  return getDb()
    .prepare(
      `SELECT rank, device_id, total_hnt, total_dc, weight, entity_name
       FROM index_constituents
       WHERE day = ? AND n = ? AND weight_mode = ?
       ORDER BY rank ASC`
    )
    .all(day, n, weightMode) as ConstituentRow[];
}

export function getDeviceHistory(
  deviceId: string,
  n: number,
  weightMode: string,
  throughDay: string,
  lookbackDays: number
): Array<{
  day: string;
  rank: number;
  total_hnt: number;
  total_dc: number;
  weight: number;
  entity_name: string | null;
  basket_hnt: number | null;
  hnt_price: number | null;
}> {
  const start = new Date(`${throughDay}T12:00:00Z`);
  start.setUTCDate(start.getUTCDate() - Math.max(0, lookbackDays - 1));
  const startDay = start.toISOString().slice(0, 10);

  return getDb()
    .prepare(
      `SELECT c.day, c.rank, c.total_hnt, c.total_dc, c.weight, c.entity_name,
              d.basket_hnt, d.hnt_price
       FROM index_constituents c
       LEFT JOIN index_days d
         ON d.day = c.day AND d.n = c.n AND d.weight_mode = c.weight_mode
       WHERE c.device_id = ?
         AND c.n = ?
         AND c.weight_mode = ?
         AND c.day >= ?
         AND c.day <= ?
       ORDER BY c.day DESC`
    )
    .all(deviceId, n, weightMode, startDay, throughDay) as Array<{
    day: string;
    rank: number;
    total_hnt: number;
    total_dc: number;
    weight: number;
    entity_name: string | null;
    basket_hnt: number | null;
    hnt_price: number | null;
  }>;
}

/** Calendar day immediately before `day` that has constituents stored. */
export function getPreviousConstituentDay(
  day: string,
  n: number,
  weightMode: string
): string | null {
  const row = getDb()
    .prepare(
      `SELECT day FROM index_constituents
       WHERE n = ? AND weight_mode = ? AND day < ?
       GROUP BY day
       ORDER BY day DESC
       LIMIT 1`
    )
    .get(n, weightMode, day) as { day: string } | undefined;
  return row?.day ?? null;
}

/** Days each device appears in the stored index window (up to and including `throughDay`). */
export function getDaysInIndexByDevice(
  deviceIds: string[],
  n: number,
  weightMode: string,
  throughDay: string
): Map<string, number> {
  const out = new Map<string, number>();
  if (!deviceIds.length) return out;

  const idSet = new Set(deviceIds);
  for (const id of deviceIds) out.set(id, 0);

  const rows = getDb()
    .prepare(
      `SELECT device_id, COUNT(DISTINCT day) AS c
       FROM index_constituents
       WHERE n = ? AND weight_mode = ? AND day <= ?
       GROUP BY device_id`
    )
    .all(n, weightMode, throughDay) as { device_id: string; c: number }[];

  for (const r of rows) {
    if (idSet.has(r.device_id)) out.set(r.device_id, r.c);
  }
  return out;
}

/**
 * Consecutive days ending at `throughDay` that the device stayed in the index.
 * Walks backward from throughDay using stored days only.
 */
export function getStreakByDevice(
  deviceIds: string[],
  n: number,
  weightMode: string,
  throughDay: string
): Map<string, number> {
  const out = new Map<string, number>();
  if (!deviceIds.length) return out;

  const days = getDb()
    .prepare(
      `SELECT DISTINCT day FROM index_constituents
       WHERE n = ? AND weight_mode = ? AND day <= ?
       ORDER BY day DESC`
    )
    .all(n, weightMode, throughDay) as { day: string }[];

  if (!days.length) return out;

  const presence = new Map<string, Set<string>>();
  for (const id of deviceIds) presence.set(id, new Set());

  const idSet = new Set(deviceIds);
  const rows = getDb()
    .prepare(
      `SELECT day, device_id FROM index_constituents
       WHERE n = ? AND weight_mode = ? AND day <= ?`
    )
    .all(n, weightMode, throughDay) as { day: string; device_id: string }[];

  for (const r of rows) {
    if (!idSet.has(r.device_id)) continue;
    presence.get(r.device_id)!.add(r.day);
  }

  for (const id of deviceIds) {
    const set = presence.get(id)!;
    let streak = 0;
    for (const d of days) {
      if (!set.has(d.day)) break;
      streak++;
    }
    out.set(id, streak);
  }
  return out;
}

export function hasConstituentsForDay(
  day: string,
  n: number,
  weightMode: string
): boolean {
  const row = getDb()
    .prepare(
      `SELECT COUNT(*) AS c FROM index_constituents WHERE day = ? AND n = ? AND weight_mode = ?`
    )
    .get(day, n, weightMode) as { c: number };
  return row.c > 0;
}

export function getIndexDay(
  day: string,
  n: number,
  weightMode: string
): IndexDayRow | null {
  const row = getDb()
    .prepare(
      `SELECT day, n, weight_mode, basket_hnt, basket_usd, hnt_price, constituent_count, fetch_complete, updated_at
       FROM index_days WHERE day = ? AND n = ? AND weight_mode = ?`
    )
    .get(day, n, weightMode);
  return (row as IndexDayRow) || null;
}

export function getMeta(): {
  dbPath: string;
  rowCount: number;
  latestDay: string | null;
  latestBuild: BuildLogRow | null;
} {
  const d = getDb();
  const rowCount = (
    d.prepare(`SELECT COUNT(*) AS c FROM index_days`).get() as { c: number }
  ).c;
  const latest = d
    .prepare(`SELECT day FROM index_days ORDER BY day DESC LIMIT 1`)
    .get() as { day: string } | undefined;

  return {
    dbPath: resolveDbPath(),
    rowCount,
    latestDay: latest?.day ?? null,
    latestBuild: getLatestBuildLog(),
  };
}
