// src/topEarnersScheduledScrape.js
// Daily top earners scraper (DC-only) that scans S3 once and stores top-N snapshots for 1/7/30 days.
/* eslint-disable no-console */
/* global Buffer */

const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const zlib = require("zlib");
const { helium } = require("@helium/proto");
const bs58 = require("bs58");
const crypto = require("crypto");
const { supabase } = require("./supabase");

// -----------------------------------------------------------------------------
// CONFIG (edit these values to change behavior)
// -----------------------------------------------------------------------------
const CONFIG = {
  // S3 oracle source
  AWS_REGION: process.env.AWS_REGION || "us-west-2",
  AWS_BUCKET: process.env.AWS_BUCKET || "foundation-poc-data-requester-pays",
  REWARD_SHARES_PREFIX:
    "foundation-mobile-verified/mobile_network_reward_shares_v1",

  // Windows to compute (in days). Must include the max lookback.
  WINDOWS_DAYS: [1, 7, 30],

  // Output size
  TOP_N: 10,
};

const REGION = CONFIG.AWS_REGION;
const BUCKET = CONFIG.AWS_BUCKET;
const PREFIX = CONFIG.REWARD_SHARES_PREFIX;
const WINDOWS_DAYS = Array.from(new Set(CONFIG.WINDOWS_DAYS)).sort((a, b) => a - b);
const LOOKBACK_DAYS = WINDOWS_DAYS[WINDOWS_DAYS.length - 1];
const TOP_N = CONFIG.TOP_N;

// --- HELPERS ---
async function* framedMessages(stream) {
  let buffer = Buffer.alloc(0);
  for await (const chunk of stream) {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 4) {
      const messageLength = buffer.readUInt32BE(0);
      if (buffer.length < 4 + messageLength) break;
      const message = buffer.subarray(4, 4 + messageLength);
      yield message;
      buffer = buffer.subarray(4 + messageLength);
    }
  }
}

function base58checkEncode(data, version = 0) {
  const versionedData = Buffer.concat([Buffer.from([version]), data]);
  const firstHash = crypto.createHash("sha256").update(versionedData).digest();
  const secondHash = crypto.createHash("sha256").update(firstHash).digest();
  const checksum = secondHash.slice(0, 4);
  const finalData = Buffer.concat([versionedData, checksum]);
  return bs58.encode(finalData);
}

function isBytes(v) {
  return v instanceof Uint8Array || Buffer.isBuffer(v);
}

function pickDeviceBytesFromGatewayReward(gatewayReward) {
  if (!gatewayReward || typeof gatewayReward !== "object") return null;

  const preferred = [
    "gateway",
    "gatewayKey",
    "gatewayPubkey",
    "gatewayPubKey",
    "hotspot",
    "hotspotKey",
    "hotspotPubkey",
    "hotspotPubKey",
  ];
  for (const k of preferred) {
    const v = gatewayReward[k];
    if (isBytes(v)) return Buffer.from(v);
  }

  for (const [k, v] of Object.entries(gatewayReward)) {
    if (k.toLowerCase().includes("reward")) continue;
    if (isBytes(v)) return Buffer.from(v);
  }

  return null;
}

function deviceKeyStringFromDecoded(decoded) {
  const bytes = pickDeviceBytesFromGatewayReward(decoded?.gatewayReward);
  if (!bytes || !bytes.length) return null;
  return base58checkEncode(bytes, 0);
}

function formatHntLike(n) {
  return (n / 1e8).toFixed(2);
}

async function listGzKeysInRange(s3, prefix, startTs, endTs) {
  const keys = [];
  let continuationToken = undefined;

  while (true) {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
        RequestPayer: "requester",
      })
    );

    const contents = resp.Contents || [];
    for (const obj of contents) {
      const key = obj.Key || "";
      if (!key.endsWith(".gz")) continue;
      const m = key.match(/\.(\d{13})\.gz$/);
      const ts = m ? parseInt(m[1], 10) : 0;
      if (ts >= startTs && ts <= endTs) keys.push({ key, ts });
    }

    if (!resp.IsTruncated || !resp.NextContinuationToken) break;
    continuationToken = resp.NextContinuationToken;
  }

  keys.sort((a, b) => a.ts - b.ts);
  return keys;
}

function topNFromTotals(totalsMap, n) {
  const arr = Array.from(totalsMap.entries());
  arr.sort((a, b) => b[1] - a[1]);
  return arr.slice(0, n);
}

async function run() {
  const MobileRewardShare = helium?.poc_mobile?.mobile_reward_share;
  if (!MobileRewardShare) throw new Error("mobile_reward_share decoder not available in @helium/proto");

  const endDate = new Date();
  const startDate = new Date(endDate.getTime() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000);
  const startTs = startDate.getTime();
  const endTs = endDate.getTime();

  const cutoffs = Object.fromEntries(
    WINDOWS_DAYS.map((d) => [String(d), endTs - d * 24 * 60 * 60 * 1000])
  );

  console.log("Top Earners (DC) scrape");
  console.log("=======================");
  console.log(`Bucket: ${BUCKET}`);
  console.log(`Prefix: ${PREFIX}`);
  console.log(`Lookback: ${LOOKBACK_DAYS} days`);
  console.log(`Windows: ${WINDOWS_DAYS.join(", ")} days`);
  console.log(`Top N: ${TOP_N}`);
  console.log("");

  const s3 = new S3Client({ region: REGION });

  console.log("Listing files...");
  const files = await listGzKeysInRange(s3, PREFIX, startTs, endTs);
  console.log(`Files in range: ${files.length}`);
  console.log("");

  const totalsByWindow = Object.fromEntries(WINDOWS_DAYS.map((d) => [String(d), new Map()]));

  let framesScanned = 0;
  let decodedOk = 0;
  let rewardsCounted = 0;
  let deviceKeysExtracted = 0;

  for (const { key, ts } of files) {
    const get = await s3.send(
      new GetObjectCommand({
        Bucket: BUCKET,
        Key: key,
        RequestPayer: "requester",
      })
    );
    if (!get.Body) continue;

    const gunzip = zlib.createGunzip();
    get.Body.pipe(gunzip);

    for await (const frame of framedMessages(gunzip)) {
      framesScanned++;
      let decoded;
      try {
        decoded = MobileRewardShare.decode(frame);
      } catch (_) {
        continue;
      }
      decodedOk++;

      const dcTransfer = decoded.gatewayReward?.dcTransferReward;
      const dc = dcTransfer ? parseInt(dcTransfer.toString(), 10) : 0;
      if (!dc) continue;

      const deviceKey = deviceKeyStringFromDecoded(decoded);
      if (!deviceKey) continue;
      deviceKeysExtracted++;

      rewardsCounted++;

      for (const windowDays of WINDOWS_DAYS) {
        if (ts < cutoffs[String(windowDays)]) continue;
        const m = totalsByWindow[String(windowDays)];
        m.set(deviceKey, (m.get(deviceKey) || 0) + dc);
      }
    }
  }

  const results = {};
  for (const windowDays of WINDOWS_DAYS) {
    const windowKey = String(windowDays);
    const top = topNFromTotals(totalsByWindow[windowKey], TOP_N);
    results[windowKey] = top.map(([device_key, total_dc], idx) => ({
      rank: idx + 1,
      device_key,
      total_dc,
      total_hnt: Number(formatHntLike(total_dc)),
    }));
  }

  const meta = {
    generated_at: new Date().toISOString(),
    bucket: BUCKET,
    prefix: PREFIX,
    lookback_days: LOOKBACK_DAYS,
    windows_days: WINDOWS_DAYS,
    top_n: TOP_N,
    files_in_range: files.length,
    frames_scanned: framesScanned,
    decoded_ok: decodedOk,
    rewards_counted: rewardsCounted,
    device_keys_extracted: deviceKeysExtracted,
    cutoffs_ms: cutoffs,
  };

  console.log("Writing snapshot to DB...");
  const { error } = await supabase.from("top_earners_snapshots").insert({
    source_prefix: PREFIX,
    lookback_days: LOOKBACK_DAYS,
    windows_days: WINDOWS_DAYS,
    top_n: TOP_N,
    results,
    meta,
  });
  if (error) throw error;

  console.log("Done.");
}

run().catch((err) => {
  console.error("Top earners scrape failed:", err?.message || err);
  process.exitCode = 1;
});

