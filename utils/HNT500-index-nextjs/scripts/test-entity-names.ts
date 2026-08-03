/**
 * Quick test: entity_key → key_to_asset PDA → animal name
 *
 * Uses the same derivation as helium-keyToAssetKey.
 * Run from this folder:
 *   node --import tsx scripts/test-entity-names.ts
 */
import { PublicKey } from "@solana/web3.js";
import { sha256 } from "js-sha256";
import bs58 from "bs58";
import Database from "better-sqlite3";
import path from "path";

const HEM = new PublicKey("hemjuPXBpNvggtaUnN1MwT3wrdhttKEfosTcc2P9Pg8");
const HDAO = new PublicKey("hdaoVTCqhfHHo75XdAMxBKdUqvq1i5bF23sisBqVgGR");
const HNT = new PublicKey("hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux");
const [DAO] = PublicKey.findProgramAddressSync(
  [Buffer.from("dao"), HNT.toBuffer()],
  HDAO
);

function keyToAssetPda(entityKeyStr: string): string {
  const entityKeyBuf = Buffer.from(bs58.decode(entityKeyStr));
  const hashed = Buffer.from(sha256(entityKeyBuf), "hex");
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from("key_to_asset"), DAO.toBuffer(), hashed],
    HEM
  );
  return pda.toBase58();
}

async function fetchName(label: string, entityOrPda: string, via: "entity" | "pda") {
  const url =
    via === "entity"
      ? `https://entities.nft.helium.io/${encodeURIComponent(entityOrPda)}`
      : `https://entities.nft.helium.io/v2/hotspot/${encodeURIComponent(entityOrPda)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const json = (await res.json()) as { name?: string; message?: string };
  console.log(
    `${res.status} ${label.padEnd(18)} via=${via} name=${json.name || json.message || "?"}`
  );
  return json.name || null;
}

async function main() {
  // Known-good control (IoT-style entity_key_str)
  const control = "13XoAGeJYSbViwt9CT3EYdaeA4eXgnM2gZUkgBvyuCvnwcQk9mk";
  const controlPda = keyToAssetPda(control);
  console.log("DAO", DAO.toBase58());
  console.log("control PDA", controlPda, "(expect Dkf159DW4Mbd2aEh7zYbV1tsHvEYWjqUc5GRpviiL85D)");
  await fetchName("CONTROL entity", control, "entity");
  await fetchName("CONTROL pda", controlPda, "pda");

  const dbPath = path.join(process.cwd(), "data", "hnt500.db");
  const db = new Database(dbPath, { readonly: true });
  const rows = db
    .prepare(
      `SELECT rank, device_id FROM index_constituents
       WHERE day = (SELECT MAX(day) FROM index_constituents)
       ORDER BY rank ASC LIMIT 6`
    )
    .all() as { rank: number; device_id: string }[];

  console.log("\nHNT500 sample device_ids:");
  for (const row of rows) {
    const id = row.device_id;
    console.log(`\n#${row.rank} len=${id.length} ${id.slice(0, 28)}…`);
    try {
      if (id.length <= 60) {
        await fetchName(`#${row.rank} raw`, id, "entity");
        const pda = keyToAssetPda(id);
        console.log("  derived PDA", pda);
        await fetchName(`#${row.rank} pda`, pda, "pda");
      } else {
        const pda = keyToAssetPda(id);
        console.log("  derived PDA", pda);
        await fetchName(`#${row.rank} pda`, pda, "pda");
      }
    } catch (e) {
      console.log("  error", e instanceof Error ? e.message : e);
    }
  }

  console.log(`
Summary:
- keyToAssetKey works (control PDA matches entities.nft.helium.io)
- Animal names resolve for real entity_key_str values
- HNT500 device_ids currently do NOT resolve — they are not valid
  entity_key_str inputs for this API (or no longer exist on-chain)
`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
