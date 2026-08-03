import { PublicKey } from "@solana/web3.js";
import { sha256 } from "js-sha256";
import bs58 from "bs58";
import { getDb } from "@/lib/db";
import { toExplorerEntityKey } from "@/lib/helium-world";

export { toExplorerEntityKey } from "@/lib/helium-world";

const ENTITIES_BASE = "https://entities.nft.helium.io";

const HEM_PROGRAM_ID = new PublicKey(
  "hemjuPXBpNvggtaUnN1MwT3wrdhttKEfosTcc2P9Pg8"
);
const HDAO_PROGRAM_ID = new PublicKey(
  "hdaoVTCqhfHHo75XdAMxBKdUqvq1i5bF23sisBqVgGR"
);
const HNT_MINT = new PublicKey("hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux");

let _dao: PublicKey | null = null;
function daoKey(): PublicKey {
  if (!_dao) {
    [_dao] = PublicKey.findProgramAddressSync(
      [Buffer.from("dao"), HNT_MINT.toBuffer()],
      HDAO_PROGRAM_ID
    );
  }
  return _dao;
}

function ensureNameCache(): void {
  getDb().exec(`
    CREATE TABLE IF NOT EXISTS entity_name_cache (
      device_id TEXT PRIMARY KEY,
      entity_name TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
}

export function getCachedEntityNames(
  deviceIds: string[]
): Map<string, string> {
  ensureNameCache();
  const out = new Map<string, string>();
  if (!deviceIds.length) return out;

  const db = getDb();
  const stmt = db.prepare(
    `SELECT device_id, entity_name FROM entity_name_cache WHERE device_id = ?`
  );
  for (const id of deviceIds) {
    const row = stmt.get(id) as
      | { device_id: string; entity_name: string }
      | undefined;
    if (row?.entity_name) out.set(row.device_id, row.entity_name);
  }
  return out;
}

export function upsertEntityNames(
  entries: Array<{ device_id: string; entity_name: string }>
): void {
  ensureNameCache();
  const now = new Date().toISOString();
  const stmt = getDb().prepare(
    `INSERT INTO entity_name_cache (device_id, entity_name, updated_at)
     VALUES (@device_id, @entity_name, @updated_at)
     ON CONFLICT(device_id) DO UPDATE SET
       entity_name = excluded.entity_name,
       updated_at = excluded.updated_at`
  );
  const run = getDb().transaction(() => {
    for (const e of entries) {
      if (!e.device_id || !e.entity_name) continue;
      stmt.run({
        device_id: e.device_id,
        entity_name: e.entity_name,
        updated_at: now,
      });
    }
  });
  run();
}

/** Also write names onto constituent rows that share this device_id. */
export function applyNamesToConstituents(
  entries: Array<{ device_id: string; entity_name: string }>
): number {
  if (!entries.length) return 0;
  const stmt = getDb().prepare(
    `UPDATE index_constituents SET entity_name = ?
     WHERE device_id = ? AND (entity_name IS NULL OR entity_name = '')`
  );
  let n = 0;
  const run = getDb().transaction(() => {
    for (const e of entries) {
      const info = stmt.run(e.entity_name, e.device_id);
      n += Number(info.changes) || 0;
    }
  });
  run();
  return n;
}

export function keyToAssetPda(entityKeyStr: string): string {
  const entityKeyBuf = Buffer.from(bs58.decode(entityKeyStr));
  const hashed = Buffer.from(sha256(entityKeyBuf), "hex");
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from("key_to_asset"), daoKey().toBuffer(), hashed],
    HEM_PROGRAM_ID
  );
  return pda.toBase58();
}

function nameFromEntityJson(json: unknown): string | null {
  if (!json || typeof json !== "object") return null;
  const obj = json as {
    name?: unknown;
    content?: { metadata?: { name?: unknown } };
  };
  if (typeof obj.name === "string" && obj.name.trim()) return obj.name.trim();
  const metaName = obj.content?.metadata?.name;
  if (typeof metaName === "string" && metaName.trim()) return metaName.trim();
  return null;
}

async function fetchEntityName(deviceId: string): Promise<string | null> {
  const explorerKey = toExplorerEntityKey(deviceId);
  if (!explorerKey) return null;

  const urls: string[] = [];
  try {
    const pda = keyToAssetPda(explorerKey);
    urls.push(`${ENTITIES_BASE}/v2/hotspot/${encodeURIComponent(pda)}`);
  } catch {
    // PDA derivation can fail for garbage keys
  }
  urls.push(`${ENTITIES_BASE}/${encodeURIComponent(explorerKey)}`);
  if (explorerKey !== deviceId) {
    urls.push(`${ENTITIES_BASE}/${encodeURIComponent(deviceId)}`);
  }

  for (const url of urls) {
    try {
      const res = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "HNT500-index/0.1",
        },
      });
      if (!res.ok) continue;
      const json = await res.json();
      const name = nameFromEntityJson(json);
      if (name && name !== "Unknown") return name;
    } catch {
      // try next
    }
  }
  return null;
}

/** Resolve missing animal names; caches hits in SQLite. Caps live lookups per call. */
export async function resolveEntityNames(
  deviceIds: string[],
  opts?: { maxLookups?: number; concurrency?: number; writeConstituents?: boolean }
): Promise<Map<string, string>> {
  const unique = Array.from(new Set(deviceIds.filter(Boolean)));
  const cached = getCachedEntityNames(unique);
  const missing = unique.filter((id) => !cached.has(id));
  if (!missing.length) return cached;

  const maxLookups = opts?.maxLookups ?? 80;
  const concurrency = opts?.concurrency ?? 10;
  const toFetch = missing.slice(0, maxLookups);
  const found: Array<{ device_id: string; entity_name: string }> = [];

  for (let i = 0; i < toFetch.length; i += concurrency) {
    const batch = toFetch.slice(i, i + concurrency);
    const results = await Promise.all(
      batch.map(async (id) => {
        const name = await fetchEntityName(id);
        return name ? { device_id: id, entity_name: name } : null;
      })
    );
    for (const r of results) {
      if (r) {
        found.push(r);
        cached.set(r.device_id, r.entity_name);
      }
    }
  }

  if (found.length) {
    upsertEntityNames(found);
    if (opts?.writeConstituents !== false) applyNamesToConstituents(found);
  }
  return cached;
}
