// src/scrapePocRewards.js
// PoC rewards scraper for Helium device rewards
// - Fetches PoC reward data from AWS S3 bucket (radio_reward_v2 messages)
// - Processes compressed .gz files and extracts PoC rewards
// - Returns structured PoC reward information
// - Based on reference implementation from mobile-deployer-poc-rewards.ts

const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const zlib = require("zlib");
const bs58 = require("bs58");
const crypto = require("crypto");
const protobuf = require("protobufjs/minimal");

// --- CONFIG ---
const REGION = process.env.AWS_REGION || "us-west-2";
const BUCKET = process.env.AWS_BUCKET || "foundation-poc-data-requester-pays";

// DATA SOURCES - PoC rewards are in the same prefix as DC rewards
const DATA_SOURCES = {
  MOBILE_VERIFIED: {
    prefix: "foundation-mobile-verified/mobile_network_reward_shares_v1",
    description: "Mobile network reward shares (PoC rewards in radio_reward_v2 messages)"
  }
};

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
  const firstHash = crypto.createHash('sha256').update(versionedData).digest();
  const secondHash = crypto.createHash('sha256').update(firstHash).digest();
  const checksum = secondHash.slice(0, 4);
  const finalData = Buffer.concat([versionedData, checksum]);
  return bs58.encode(finalData);
}

function base58checkEncodeRaw(payload, version = 0) {
  const data = Buffer.concat([Buffer.from([version]), Buffer.from(payload)]);
  const chk = crypto.createHash("sha256").update(
                crypto.createHash("sha256").update(data).digest()
              ).digest().subarray(0, 4);
  return bs58.encode(Buffer.concat([data, chk]));
}

// Extract submessage from protobuf frame
function extractSubmessage(frame, fieldNo) {
  const r = protobuf.Reader.create(frame);
  while (r.pos < r.len) {
    const tag = r.uint32();
    const field = tag >>> 3;
    const wire = tag & 7;
    if (wire === 2) {
      const len = r.uint32();
      const start = r.pos, end = start + len;
      const slice = frame.subarray(start, end);
      r.pos = end;
      if (field === fieldNo) return slice;
    } else if (wire === 0) r.uint64();
      else if (wire === 1) r.skip(8);
      else if (wire === 5) r.skip(4);
      else throw new Error("unexpected wire type " + wire);
  }
  return null;
}

// Extract period information from mobile_reward_share frame
function getPeriodFromFrame(frame) {
  const r = protobuf.Reader.create(frame);
  let start = null, end = null;

  while (r.pos < r.len) {
    const tag = r.uint32();
    const field = tag >>> 3;
    const wire = tag & 7;

    if (wire === 0) { // varint
      const v = r.uint64();
      const n = BigInt(v.toString());
      if (field === 1) start = n;
      if (field === 2) end = n;
    } else if (wire === 2) { // skip length-delimited
      const len = r.uint32(); r.pos += len;
    } else if (wire === 1) r.skip(8);
      else if (wire === 5) r.skip(4);
  }

  if (start === null || end === null) return null;
  return { start: Number(start), end: Number(end) };
}

// Parse radio_reward_v2 to extract PoC rewards
function parseRadioRewardV2(rr2) {
  const r = protobuf.Reader.create(rr2);
  let hotspotPayload = null;
  let cbsdId = null;
  let basePoc = 0n, boostedPoc = 0n;

  while (r.pos < r.len) {
    const tag = r.uint32();
    const field = tag >>> 3;
    const wire = tag & 7;

    if (wire === 2) {
      const len = r.uint32();
      const start = r.pos, end = start + len;
      // field 1: hotspot_key (bytes)
      if (field === 1) hotspotPayload = rr2.subarray(start, end);
      // field 2: cbsd_id (string)
      if (field === 2) cbsdId = new TextDecoder().decode(rr2.subarray(start, end));
      r.pos = end;
    } else if (wire === 0) {
      const v = r.uint64();
      const n = BigInt(v.toString());
      if (field === 7) basePoc = n;
      if (field === 8) boostedPoc = n;
    } else if (wire === 1) r.skip(8);
      else if (wire === 5) r.skip(4);
  }

  // Convert hotspot payload to display format
  let hotspotId = null;
  if (hotspotPayload) {
    if (hotspotPayload.length === 264) {
      hotspotId = base58checkEncodeRaw(hotspotPayload, 0);   // Explorer "mobile pub key"
    } else if (hotspotPayload.length === 32) {
      hotspotId = bs58.encode(Buffer.from(hotspotPayload));  // plain base58 Ed25519 pubkey
    } else {
      hotspotId = `<unexpected hotspot_key length ${hotspotPayload.length} bytes>`;
    }
  }
  
  return { 
    hotspotId, 
    cbsdId, 
    basePoc: Number(basePoc), 
    boostedPoc: Number(boostedPoc),
    totalPoc: Number(basePoc) + Number(boostedPoc)
  };
}

// Convert target device key to multiple formats for matching
function getTargetDeviceFormats(targetKey) {
  try {
    // Try to decode as base58check first
    const decoded = bs58.decode(targetKey);
    const data = decoded.slice(1, -4); // Remove version and checksum
    const rawBase58 = bs58.encode(data);
    
    return {
      original: targetKey,
      rawBase58: rawBase58,
      hex: Buffer.from(data).toString('hex'),
      base64: Buffer.from(data).toString('base64')
    };
  } catch (e) {
    // If base58check decode fails, treat as raw base58
    return {
      original: targetKey,
      rawBase58: targetKey,
      hex: Buffer.from(bs58.decode(targetKey)).toString('hex'),
      base64: Buffer.from(bs58.decode(targetKey)).toString('base64')
    };
  }
}

// Check if a device key matches our target (supports different formats)
function matchesTargetDevice(deviceKey, targetFormats) {
  if (!deviceKey) return false;
  
  // Direct match
  if (deviceKey === targetFormats.original) return true;
  
  // Try to decode and compare in different formats
  try {
    // Try as base58 decoded bytes
    const decoded = bs58.decode(deviceKey);
    const targetDecoded = bs58.decode(targetFormats.original);
    if (Buffer.compare(decoded, targetDecoded) === 0) return true;
  } catch (e) {
    // Ignore decode errors
  }
  
  // Check against all target formats
  for (const [format, value] of Object.entries(targetFormats)) {
    if (typeof value === 'string' && deviceKey.includes(value)) {
      return true;
    }
  }
  
  return false;
}

// Format numbers with commas
function formatNumber(num) {
  return num.toLocaleString();
}

// Format PoC to readable format
function formatPoc(poc) {
  if (poc >= 1000000000) {
    return `${(poc / 1000000000).toFixed(2)}B PoC`;
  } else if (poc >= 1000000) {
    return `${(poc / 1000000).toFixed(2)}M PoC`;
  } else if (poc >= 1000) {
    return `${(poc / 1000).toFixed(2)}K PoC`;
  } else {
    return `${poc} PoC`;
  }
}

// --- MAIN SCRAPER FUNCTION ---
async function scrapePocRewards(deviceKey, customDateRange = null) {
  if (!deviceKey) {
    throw new Error('Device key is required');
  }

  console.log("🔍 HELIUM POC REWARDS SCRAPER");
  console.log("=" .repeat(100));
  console.log(`📦 Bucket: ${BUCKET}`);
  console.log(`🎯 Target Device: ${deviceKey.substring(0, 60)}...`);
  
  // Calculate date range
  const endDate = new Date();
  const startDate = customDateRange?.start || new Date(endDate.getTime() - 29 * 24 * 60 * 60 * 1000); // Default 30 days
  const actualEndDate = customDateRange?.end || endDate;
  
  const startTimestamp = startDate.getTime();
  const endTimestamp = actualEndDate.getTime();
  
  console.log(`📅 Date Range: ${startDate.toISOString().split('T')[0]} → ${actualEndDate.toISOString().split('T')[0]}`);
  console.log(`📊 Data Sources:`);
  for (const [key, source] of Object.entries(DATA_SOURCES)) {
    console.log(`   • ${key}: ${source.prefix}`);
    console.log(`     Description: ${source.description}`);
  }
  console.log("=" .repeat(100));

  const s3 = new S3Client({ region: REGION });
  
  // Get target device in multiple formats
  const targetFormats = getTargetDeviceFormats(deviceKey);
  console.log(`\n🔑 Target Device Formats:`);
  console.log(`   Original: ${targetFormats.original.substring(0, 60)}...`);
  console.log(`   Raw Base58: ${targetFormats.rawBase58.substring(0, 60)}...`);
  console.log(`   Hex: ${targetFormats.hex.substring(0, 60)}...`);
  console.log(`   Base64: ${targetFormats.base64.substring(0, 60)}...`);
  
  // ============================================================================
  // SEARCH MOBILE VERIFIED REWARDS FOR POC DATA
  // ============================================================================
  console.log(`\n📁 SEARCHING FOR POC REWARDS (radio_reward_v2)`);
  console.log("-".repeat(80));
  console.log(`📂 Prefix: ${DATA_SOURCES.MOBILE_VERIFIED.prefix}`);
  
  const pocRewards = [];
  let totalFilesProcessed = 0;
  let targetDateFiles = [];
  
  try {
    // Get all files in the prefix
    const files = [];
    let continuationToken;
    
    while (true) {
      const resp = await s3.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: DATA_SOURCES.MOBILE_VERIFIED.prefix,
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
        RequestPayer: "requester"
      }));
      
      const contents = resp.Contents || [];
      for (const obj of contents) {
        const key = obj.Key || "";
        if (key.endsWith('.gz')) {
          files.push(key);
        }
      }
      
      if (!resp.IsTruncated || !resp.NextContinuationToken) break;
      continuationToken = resp.NextContinuationToken;
    }
    
    console.log(`📄 Found ${files.length} total files in prefix`);
    totalFilesProcessed = files.length;
    
    // Filter for target date range files
    targetDateFiles = files
      .map(file => {
        const timestampMatch = file.match(/\.(\d{13})\.gz$/);
        const timestamp = timestampMatch ? parseInt(timestampMatch[1]) : 0;
        return { file, timestamp, date: new Date(timestamp) };
      })
      .filter(item => item.timestamp >= startTimestamp && item.timestamp <= endTimestamp)
      .sort((a, b) => a.timestamp - b.timestamp);
    
    console.log(`📅 Found ${targetDateFiles.length} files in target date range`);
    
    if (targetDateFiles.length === 0) {
      console.log("❌ No files found in target date range");
      return {
        deviceKey,
        pocRewards: [],
        summary: {
          totalBasePoc: 0,
          totalBoostedPoc: 0,
          totalPoc: 0,
          filesProcessed: totalFilesProcessed,
          filesInRange: 0,
          dateRange: { start: startDate, end: actualEndDate }
        }
      };
    }
    
    // Process each file in target date range
    for (const fileInfo of targetDateFiles) {
      console.log(`\n🔄 Processing: ${fileInfo.file.split('/').pop()}`);
      console.log(`📅 Date: ${fileInfo.date.toISOString()}`);
      
      try {
        const get = await s3.send(new GetObjectCommand({
          Bucket: BUCKET,
          Key: fileInfo.file,
          RequestPayer: "requester"
        }));

        if (!get.Body) {
          console.log("   ❌ Empty file");
          continue;
        }

        const gunzip = zlib.createGunzip();
        const stream = get.Body;
        stream.pipe(gunzip);

        let messageCount = 0;
        let foundCount = 0;
        
        for await (const frame of framedMessages(gunzip)) {
          messageCount++;
          
          try {
            // Extract period information from the frame
            const period = getPeriodFromFrame(frame);
            
            // Try radio_reward_v2 (outer field 8) using wire-level parsing
            const rr2 = extractSubmessage(frame, 8);
            if (rr2) {
              const { hotspotId, cbsdId, basePoc, boostedPoc, totalPoc } = parseRadioRewardV2(rr2);
              
              // Check if this message matches our target device
              const isTargetDevice = matchesTargetDevice(cbsdId, targetFormats) || 
                                   matchesTargetDevice(hotspotId, targetFormats);
              
              if (isTargetDevice) {
                foundCount++;
                
                pocRewards.push({
                  file: fileInfo.file,
                  timestamp: fileInfo.timestamp,
                  date: fileInfo.date,
                  basePoc: basePoc,
                  boostedPoc: boostedPoc,
                  totalPoc: totalPoc,
                  rewardType: 'mobile_verified_poc',
                  dataSource: DATA_SOURCES.MOBILE_VERIFIED.prefix,
                  period: period,
                  hotspotId: hotspotId,
                  cbsdId: cbsdId
                });
                
                console.log(`   ✅ Found PoC rewards! Base: ${formatPoc(basePoc)}, Boosted: ${formatPoc(boostedPoc)}, Total: ${formatPoc(totalPoc)}`);
              }
            }
          } catch (e) {
            // Ignore decode errors
          }
        }
        
        console.log(`   📊 Processed ${messageCount} messages, found ${foundCount} PoC matches`);
        
      } catch (e) {
        console.log(`   ❌ Error processing file: ${e.message}`);
      }
    }
    
  } catch (e) {
    console.error(`❌ Error accessing PoC rewards: ${e.message}`);
    throw e;
  }
  
  // Calculate summary
  const totalBasePoc = pocRewards.reduce((sum, reward) => sum + reward.basePoc, 0);
  const totalBoostedPoc = pocRewards.reduce((sum, reward) => sum + reward.boostedPoc, 0);
  const totalPoc = pocRewards.reduce((sum, reward) => sum + reward.totalPoc, 0);
  
  console.log(`\n📊 POC SCRAPING RESULTS`);
  console.log("=" .repeat(100));
  console.log(`✅ Found ${pocRewards.length} PoC reward entries`);
  console.log(`💰 Total Base PoC: ${formatNumber(totalBasePoc)} (${formatPoc(totalBasePoc)})`);
  console.log(`🚀 Total Boosted PoC: ${formatNumber(totalBoostedPoc)} (${formatPoc(totalBoostedPoc)})`);
  console.log(`💎 Total PoC: ${formatNumber(totalPoc)} (${formatPoc(totalPoc)})`);
  console.log(`📁 Files Processed: ${totalFilesProcessed}`);
  console.log(`📅 Date Range: ${startDate.toISOString().split('T')[0]} → ${actualEndDate.toISOString().split('T')[0]}`);
  
  return {
    deviceKey,
    pocRewards: pocRewards,
    summary: {
      totalBasePoc,
      totalBoostedPoc,
      totalPoc,
      rewardCount: pocRewards.length,
      filesProcessed: totalFilesProcessed,
      filesInRange: targetDateFiles.length,
      dateRange: { start: startDate, end: actualEndDate },
      averageDaily: pocRewards.length > 0 ? totalPoc / pocRewards.length : 0
    }
  };
}

// convenience direct-run
if (require.main === module) {
  require('dotenv').config();
  const deviceKey = process.argv[2] || process.env.DEFAULT_DEVICE_KEY;
  if (!deviceKey) {
    console.error('❌ Usage: node src/scrapePocRewards.js <DEVICE_KEY>');
    console.error('❌ Or set DEFAULT_DEVICE_KEY in .env file');
    process.exit(1);
  }
  
  scrapePocRewards(deviceKey)
    .then((result) => {
      console.log('✅ PoC scraping completed successfully!');
      console.log(`🎯 Device: ${result.deviceKey.substring(0, 60)}...`);
      console.log(`💎 Total PoC: ${formatPoc(result.summary.totalPoc)}`);
      process.exit(0);
    })
    .catch((e) => {
      console.error('❌ PoC scraping failed:', e.message);
      process.exit(1);
    });
}

module.exports = { scrapePocRewards, formatPoc, formatNumber };
