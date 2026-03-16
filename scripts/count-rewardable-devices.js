#!/usr/bin/env node
/**
 * Count how many distinct devices earned DC rewards in the last N days.
 *
 * This is a standalone debug script and does NOT touch the app framework.
 *
 * Usage:
 *   node scripts/count-rewardable-devices.js
 *
 * Edit the CONFIG block below to adjust lookback, bucket, prefix, etc.
 */
/* eslint-disable no-console */
/* global Buffer */

const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const zlib = require("zlib");
const { helium } = require("@helium/proto");
require("dotenv").config();

// -----------------------------------------------------------------------------
// CONFIG (edit this block; AWS keys load from env)
// -----------------------------------------------------------------------------
const CONFIG = {
  // AWS requester-pays credentials (loaded from env; DO NOT COMMIT REAL KEYS)
  AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID || "",
  AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY || "",
  AWS_REGION: "us-west-2",
  AWS_BUCKET: "foundation-poc-data-requester-pays",

  // Reward shares data source prefix (same as main scraper)
  REWARD_SHARES_PREFIX: "foundation-mobile-verified/mobile_network_reward_shares_v1",

  // How far back to scan (in days). 30 = last 30 days.
  DAYS_LOOKBACK: 30,
};

const REGION = CONFIG.AWS_REGION;
const BUCKET = CONFIG.AWS_BUCKET;
const PREFIX = CONFIG.REWARD_SHARES_PREFIX;
const DAYS_LOOKBACK = Math.max(1, parseInt(CONFIG.DAYS_LOOKBACK, 10) || 1);

// --- HELPERS (mirrors src/topEarnersScheduledScrape.js where it matters) ---
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

function toYyyyMmDd(d) {
  return new Date(d).toISOString().slice(0, 10);
}

async function listGzKeysInRange(s3, prefix, startTs, endTs) {
  const out = [];
  let continuationToken;

  do {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: continuationToken,
        RequestPayer: "requester",
      })
    );

    const contents = resp.Contents || [];
    for (const obj of contents) {
      if (!obj.Key || !obj.LastModified) continue;
      const ts = obj.LastModified instanceof Date ? obj.LastModified.getTime() : new Date(obj.LastModified).getTime();
      if (ts >= startTs && ts <= endTs && obj.Key.endsWith(".gz")) {
        out.push({ key: obj.Key, ts });
      }
    }
    continuationToken = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (continuationToken);

  // Sort by time ascending (oldest first)
  out.sort((a, b) => a.ts - b.ts);
  return out;
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
  // For counting distinct devices, we just need a stable string; hex is fine.
  return bytes.toString("hex");
}

// -----------------------------------------------------------------------------
// MAIN
// -----------------------------------------------------------------------------

async function run() {
  const MobileRewardShare = helium?.poc_mobile?.mobile_reward_share;
  if (!MobileRewardShare) {
    throw new Error("mobile_reward_share decoder not available in @helium/proto");
  }

  const endDate = new Date();
  const startDate = new Date(endDate.getTime() - DAYS_LOOKBACK * 24 * 60 * 60 * 1000);
  const startTs = startDate.getTime();
  const endTs = endDate.getTime();

  console.log("Count rewardable devices (DC transfer) over last N days");
  console.log("========================================================");
  console.log(`Bucket:   ${BUCKET}`);
  console.log(`Prefix:   ${PREFIX}`);
  console.log(`Range:    ${toYyyyMmDd(startDate)} → ${toYyyyMmDd(endDate)} (${DAYS_LOOKBACK} days)`);
  console.log("");

  const s3 = new S3Client({
    region: REGION,
    credentials: {
      accessKeyId: CONFIG.AWS_ACCESS_KEY_ID,
      secretAccessKey: CONFIG.AWS_SECRET_ACCESS_KEY,
    },
  });

  console.log("Listing files in range...");
  const files = await listGzKeysInRange(s3, PREFIX, startTs, endTs);
  console.log(`Files in range: ${files.length}`);
  console.log("");

  const deviceSet = new Set();
  let framesScanned = 0;
  let decodedOk = 0;
  let rewardsCounted = 0;

  for (const { key } of files) {
    console.log(`Processing ${key}...`);
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

      deviceSet.add(deviceKey);
      rewardsCounted++;
    }
  }

  console.log("");
  console.log("Summary");
  console.log("=======");
  console.log(`Distinct rewardable devices: ${deviceSet.size.toLocaleString()}`);
  console.log(`Frames scanned:              ${framesScanned.toLocaleString()}`);
  console.log(`Frames decoded OK:           ${decodedOk.toLocaleString()}`);
  console.log(`Rewards counted (non-zero):  ${rewardsCounted.toLocaleString()}`);
}

run().catch((err) => {
  console.error("Count rewardable devices failed:", err?.message || err);
  process.exitCode = 1;
});

