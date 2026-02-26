#!/usr/bin/env node
/**
 * Standalone debug scraper: scan reward-share files and print top N devices by DC rewards.
 *
 * Usage:
 *   node scripts/top-earners-dc.js
 *
 * Notes:
 * - Edit the CONFIG block below (self-contained; no .env loading).
 * - Only scrapes DC transfer rewards from mobile reward share protobufs.
 * - This can be heavy: it scans *all* messages for the selected lookback window.
 */
/* eslint-disable no-console */
/* global Buffer */

const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const zlib = require("zlib");
const { helium } = require("@helium/proto");
const bs58 = require("bs58");
const crypto = require("crypto");
require('dotenv').config();

// -----------------------------------------------------------------------------
// CONFIG (edit this block; AWS keys load from env)
// -----------------------------------------------------------------------------
const CONFIG = {
  // AWS requester-pays credentials (loaded from env; DO NOT COMMIT REAL KEYS)
  AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID || "",
  AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY || "",
  AWS_REGION: "us-west-2",
  AWS_BUCKET: "foundation-poc-data-requester-pays",

  // Reward shares data source prefix
  REWARD_SHARES_PREFIX: "foundation-mobile-verified/mobile_network_reward_shares_v1",

  // How far back to scan
  DAYS_LOOKBACK: 30,

  // Output size
  TOP_N: 10,
};

const REGION = CONFIG.AWS_REGION;
const BUCKET = CONFIG.AWS_BUCKET;
const PREFIX = CONFIG.REWARD_SHARES_PREFIX;
const DAYS_LOOKBACK = Math.max(1, parseInt(CONFIG.DAYS_LOOKBACK, 10) || 1);
const TOP_N = Math.max(1, parseInt(CONFIG.TOP_N, 10) || 10);

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

function toYyyyMmDd(d) {
  return new Date(d).toISOString().slice(0, 10);
}

function formatDC(dc) {
  if (dc >= 1_000_000_000) return `${(dc / 1_000_000_000).toFixed(2)}B DC`;
  if (dc >= 1_000_000) return `${(dc / 1_000_000).toFixed(2)}M DC`;
  if (dc >= 1_000) return `${(dc / 1_000).toFixed(2)}K DC`;
  return `${dc} DC`;
}

function formatHntLike(n) {
  return (n / 1e8).toFixed(2);
}

function isBytes(v) {
  return v instanceof Uint8Array || Buffer.isBuffer(v);
}

function pickDeviceBytesFromGatewayReward(gatewayReward) {
  if (!gatewayReward || typeof gatewayReward !== "object") return null;

  // Prefer common field names if present
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

  // Otherwise, take the first bytes-like field (excluding reward amount fields)
  for (const [k, v] of Object.entries(gatewayReward)) {
    if (k.toLowerCase().includes("reward")) continue;
    if (isBytes(v)) return Buffer.from(v);
  }

  return null;
}

function deviceKeyStringFromDecoded(decoded) {
  const gr = decoded?.gatewayReward;
  const bytes = pickDeviceBytesFromGatewayReward(gr);
  if (!bytes || !bytes.length) return null;
  // In this codebase device keys are treated as base58check strings.
  return base58checkEncode(bytes, 0);
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

async function main() {
  if (!CONFIG.AWS_ACCESS_KEY_ID || !CONFIG.AWS_SECRET_ACCESS_KEY) {
    console.error("Error: AWS credentials are required in the CONFIG block.");
    console.error("Set CONFIG.AWS_ACCESS_KEY_ID and CONFIG.AWS_SECRET_ACCESS_KEY in scripts/top-earners-dc.js");
    process.exitCode = 1;
    return;
  }

  const MobileRewardShare = helium?.poc_mobile?.mobile_reward_share;
  if (!MobileRewardShare) {
    console.error("Error: mobile_reward_share decoder not available in @helium/proto.");
    process.exitCode = 1;
    return;
  }

  const endDate = new Date();
  const startDate = new Date(endDate.getTime() - DAYS_LOOKBACK * 24 * 60 * 60 * 1000);
  const startTs = startDate.getTime();
  const endTs = endDate.getTime();

  console.log("Helium Rewards Local Debug (Top Earners)");
  console.log("=======================================");
  console.log(`Bucket: ${BUCKET}`);
  console.log(`Prefix: ${PREFIX}`);
  console.log(`Range:  ${toYyyyMmDd(startDate)} -> ${toYyyyMmDd(endDate)} (${DAYS_LOOKBACK}d)`);
  console.log(`Top N:  ${TOP_N}`);
  console.log("");

  const s3 = new S3Client({
    region: REGION,
    credentials: {
      accessKeyId: CONFIG.AWS_ACCESS_KEY_ID,
      secretAccessKey: CONFIG.AWS_SECRET_ACCESS_KEY,
      ...(CONFIG.AWS_SESSION_TOKEN ? { sessionToken: CONFIG.AWS_SESSION_TOKEN } : {}),
    },
  });

  console.log("Listing files...");
  const files = await listGzKeysInRange(s3, PREFIX, startTs, endTs);
  console.log(`Files in range: ${files.length}`);
  if (!files.length) return;
  console.log("");

  const totalsByDevice = new Map(); // deviceKey -> totalDC
  let framesScanned = 0;
  let decodedOk = 0;
  let rewardsCounted = 0;

  for (const { key } of files) {
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

      rewardsCounted++;
      totalsByDevice.set(deviceKey, (totalsByDevice.get(deviceKey) || 0) + dc);
    }
  }

  const top = topNFromTotals(totalsByDevice, TOP_N);

  console.log("Top earners by DC");
  console.log("-----------------");
  if (!top.length) {
    console.log("No rewards found in the selected range.");
  } else {
    top.forEach(([deviceKey, totalDc], i) => {
      const rank = String(i + 1).padStart(2, " ");
      console.log(
        `${rank}. ${formatDC(totalDc).padStart(12)}  (HNT: ${formatHntLike(totalDc)})  ${deviceKey.slice(0, 70)}...`
      );
    });
  }
  console.log("");

  console.log("Summary");
  console.log("-------");
  console.log(`Frames scanned:  ${framesScanned.toLocaleString()}`);
  console.log(`Decoded OK:      ${decodedOk.toLocaleString()}`);
  console.log(`Rewards counted: ${rewardsCounted.toLocaleString()}`);
  console.log(`Unique devices:  ${totalsByDevice.size.toLocaleString()}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exitCode = 1;
});

