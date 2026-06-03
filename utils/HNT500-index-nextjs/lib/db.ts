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
};

function migrateSchema(database: Database.Database): void {
  const cols = database
    .prepare(`PRAGMA table_info(index_days)`)
    .all() as { name: string }[];
  if (!cols.some((c) => c.name === "fetch_complete")) {
    database.exec(
      `ALTER TABLE index_days ADD COLUMN fetch_complete INTEGER NOT NULL DEFAULT 1`
    );
  }
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
    `INSERT INTO index_constituents (day, n, weight_mode, rank, device_id, total_hnt, total_dc, weight)
     VALUES (@day, @n, @weight_mode, @rank, @device_id, @total_hnt, @total_dc, @weight)`
  );
  const run = database.transaction(() => {
    del.run(day, n, weightMode);
    for (const row of rows) {
      ins.run({ day, n, weight_mode: weightMode, ...row });
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
      `SELECT rank, device_id, total_hnt, total_dc, weight
       FROM index_constituents
       WHERE day = ? AND n = ? AND weight_mode = ?
       ORDER BY rank ASC`
    )
    .all(day, n, weightMode) as ConstituentRow[];
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
