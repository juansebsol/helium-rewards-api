#!/usr/bin/env node
/**
 * Standalone debug scraper: scrape a single device's DC rewards from S3 reward-share files.
 *
 * Usage:
 *   node scripts/scrape-device.js "<DEVICE_KEY>" 30
 *
 * Notes:
 * - Edit the CONFIG block below (self-contained; no .env loading).
 * - Only scrapes DC transfer rewards from mobile reward share protobufs.
 * - Intended as a lightweight local debug tool (no DB writes, no PoC aggregation).
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

  // Default target + lookback (you can also pass CLI args to override)
  DEVICE_KEY: "1494dwLhWf1SH2d7nUoBMyNsXMWMXcmRwChwQUA8zg7gaLp7nuz",
  DAYS_LOOKBACK: 30,
};

const REGION = CONFIG.AWS_REGION;
const BUCKET = CONFIG.AWS_BUCKET;
const PREFIX = CONFIG.REWARD_SHARES_PREFIX;

const DEVICE_KEY = (process.argv[2] || CONFIG.DEVICE_KEY || "").trim();
const DAYS_LOOKBACK = Math.max(1, parseInt(process.argv[3] || CONFIG.DAYS_LOOKBACK, 10) || 30);

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

function getTargetDeviceFormats(targetKey) {
  try {
    const decoded = bs58.decode(targetKey);
    const data = decoded.slice(1, -4); // Remove version and checksum
    const rawBase58 = bs58.encode(data);

    return {
      original: targetKey,
      rawBase58,
      hex: Buffer.from(data).toString("hex"),
      base64: Buffer.from(data).toString("base64"),
      base58check: base58checkEncode(data, 0),
    };
  } catch (e) {
    // If base58check decode fails, treat as raw base58
    const data = bs58.decode(targetKey);
    return {
      original: targetKey,
      rawBase58: targetKey,
      hex: Buffer.from(data).toString("hex"),
      base64: Buffer.from(data).toString("base64"),
    };
  }
}

function containsTargetDevice(decodedMessage, targetFormats) {
  const messageStr = JSON.stringify(decodedMessage);
  for (const value of Object.values(targetFormats)) {
    if (typeof value === "string" && value && messageStr.includes(value)) return true;
  }
  return false;
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

async function scrape() {
  if (!DEVICE_KEY) {
    console.error("Error: DEVICE_KEY is required.");
    console.error('Usage: node scripts/scrape-device.js "<DEVICE_KEY>" 30');
    process.exitCode = 1;
    return;
  }

  if (!CONFIG.AWS_ACCESS_KEY_ID || !CONFIG.AWS_SECRET_ACCESS_KEY) {
    console.error("Error: AWS credentials are required in the CONFIG block.");
    console.error("Set CONFIG.AWS_ACCESS_KEY_ID and CONFIG.AWS_SECRET_ACCESS_KEY in scripts/scrape-device.js");
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

  console.log("Helium Rewards Local Debug Scrape");
  console.log("================================");
  console.log(`Bucket: ${BUCKET}`);
  console.log(`Prefix: ${PREFIX}`);
  console.log(`Device: ${DEVICE_KEY.slice(0, 60)}...`);
  console.log(`Range:  ${toYyyyMmDd(startDate)} -> ${toYyyyMmDd(endDate)} (${DAYS_LOOKBACK}d)`);
  console.log("");

  const s3 = new S3Client({
    region: REGION,
    credentials: {
      accessKeyId: CONFIG.AWS_ACCESS_KEY_ID,
      secretAccessKey: CONFIG.AWS_SECRET_ACCESS_KEY,
      ...(CONFIG.AWS_SESSION_TOKEN ? { sessionToken: CONFIG.AWS_SESSION_TOKEN } : {}),
    },
  });
  const targetFormats = getTargetDeviceFormats(DEVICE_KEY);

  console.log("Listing files...");
  const files = await listGzKeysInRange(s3, PREFIX, startTs, endTs);
  console.log(`Files in range: ${files.length}`);
  if (!files.length) return;
  console.log("");

  const totalsByDay = new Map(); // day -> dc
  let totalDc = 0;
  let totalMatches = 0;
  let totalFrames = 0;

  for (const { key, ts } of files) {
    const fileDate = new Date(ts);
    const day = toYyyyMmDd(fileDate);

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
      totalFrames++;
      let decoded;
      try {
        decoded = MobileRewardShare.decode(frame);
      } catch (_) {
        continue;
      }

      if (!containsTargetDevice(decoded, targetFormats)) continue;
      totalMatches++;

      const dcTransfer = decoded.gatewayReward?.dcTransferReward;
      const dc = dcTransfer ? parseInt(dcTransfer.toString(), 10) : 0;
      if (!dc) continue;

      totalDc += dc;
      totalsByDay.set(day, (totalsByDay.get(day) || 0) + dc);
    }
  }

  const days = Array.from(totalsByDay.entries()).sort((a, b) => a[0].localeCompare(b[0]));

  console.log("Daily totals");
  console.log("-----------");
  if (!days.length) {
    console.log("No rewards found for device in the selected range.");
  } else {
    for (const [day, dc] of days) {
      console.log(`${day}  ${formatDC(dc).padStart(12)}   (HNT: ${formatHntLike(dc)})`);
    }
  }
  console.log("");

  console.log("Summary");
  console.log("-------");
  console.log(`Frames scanned: ${totalFrames.toLocaleString()}`);
  console.log(`Matches found:  ${totalMatches.toLocaleString()}`);
  console.log(`Total DC:       ${totalDc.toLocaleString()} (${formatDC(totalDc)})`);
  console.log(`Total (HNT):    ${formatHntLike(totalDc)}`);
}

scrape().catch((err) => {
  console.error("Fatal error:", err);
  process.exitCode = 1;
});

