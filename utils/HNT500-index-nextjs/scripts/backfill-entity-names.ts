/**
 * Backfill animal names for all device_ids currently in SQLite.
 * Usage: node --env-file=.env --import tsx scripts/backfill-entity-names.ts
 */
import { getDb } from "../lib/db";
import { resolveEntityNames } from "../lib/entity-names";

async function main() {
  const db = getDb();
  const rows = db
    .prepare(
      `SELECT DISTINCT device_id FROM index_constituents
       WHERE device_id IS NOT NULL AND device_id != ''`
    )
    .all() as { device_id: string }[];

  const ids = rows.map((r) => r.device_id);
  console.log(`[backfill] ${ids.length} unique device_ids`);

  const batchSize = 100;
  let named = 0;
  for (let i = 0; i < ids.length; i += batchSize) {
    const batch = ids.slice(i, i + batchSize);
    const map = await resolveEntityNames(batch, {
      maxLookups: batch.length,
      concurrency: 12,
      writeConstituents: true,
    });
    const hit = batch.filter((id) => map.has(id)).length;
    named += hit;
    console.log(
      `[backfill] ${Math.min(i + batchSize, ids.length)}/${ids.length} (+${hit} named this batch, ${named} total hits)`
    );
  }

  const stats = db
    .prepare(
      `SELECT COUNT(*) AS total,
              SUM(CASE WHEN entity_name IS NOT NULL AND entity_name != '' THEN 1 ELSE 0 END) AS named
       FROM index_constituents`
    )
    .get() as { total: number; named: number };

  console.log(
    `[backfill] done. constituents named ${stats.named}/${stats.total}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
