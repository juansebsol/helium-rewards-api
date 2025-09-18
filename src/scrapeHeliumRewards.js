// src/scrapeHeliumRewards.js
// Core AWS S3 scraper for Helium device rewards
// - Fetches protobuf data from AWS S3 bucket
// - Processes compressed .gz files
// - Extracts reward data for target devices
// - Returns structured reward information

const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const zlib = require("zlib");
const { helium } = require("@helium/proto");
const bs58 = require("bs58");
const crypto = require("crypto");

// --- CONFIG ---
const REGION = process.env.AWS_REGION || "us-west-2";
const BUCKET = process.env.AWS_BUCKET || "foundation-poc-data-requester-pays";

// CUSTOMIZABLE PARAMETERS
const DAYS_TO_AGGREGATE = parseInt(process.env.DAYS_TO_AGGREGATE) || 30;

// DATA SOURCES - CLEARLY DEFINED
const DATA_SOURCES = {
  MOBILE_VERIFIED: {
    prefix: "foundation-mobile-verified/mobile_network_reward_shares_v1",
    proto: "helium.poc_mobile.mobile_reward_share",
    description: "Mobile network reward shares (verified rewards for mobile hotspots)"
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

function enrichBytes(value) {
  if (value instanceof Uint8Array || value instanceof Buffer) {
    const buf = Buffer.from(value);
    return {
      _bytes_base64: buf.toString("base64"),
      _bytes_hex: buf.toString("hex"),
      _bytes_base58: bs58.encode(buf),
      _bytes_base58check: base58checkEncode(buf, 0),
    };
  }
  if (Array.isArray(value)) return value.map(enrichBytes);
  if (value && typeof value === "object" && !(value instanceof Date)) {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = enrichBytes(v);
    return out;
  }
  return value;
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

// Check if a message contains our target device
function containsTargetDevice(message, targetFormats) {
  const messageStr = JSON.stringify(message);
  
  // Check all formats
  for (const [format, value] of Object.entries(targetFormats)) {
    if (typeof value === 'string' && messageStr.includes(value)) {
      return true;
    }
  }
  
  // Also check enriched bytes fields
  const enriched = enrichBytes(message);
  const enrichedStr = JSON.stringify(enriched);
  
  for (const [format, value] of Object.entries(targetFormats)) {
    if (typeof value === 'string' && enrichedStr.includes(value)) {
      return true;
    }
  }
  
  return false;
}

// Format numbers with commas
function formatNumber(num) {
  return num.toLocaleString();
}

// Format DC to readable format
function formatDC(dc) {
  if (dc >= 1000000000) {
    return `${(dc / 1000000000).toFixed(2)}B DC`;
  } else if (dc >= 1000000) {
    return `${(dc / 1000000).toFixed(2)}M DC`;
  } else if (dc >= 1000) {
    return `${(dc / 1000).toFixed(2)}K DC`;
  } else {
    return `${dc} DC`;
  }
}

// --- MAIN SCRAPER FUNCTION ---
async function scrapeHeliumRewards(deviceKey, customDateRange = null) {
  if (!deviceKey) {
    throw new Error('Device key is required');
  }

  console.log("🔍 HELIUM REWARDS SCRAPER");
  console.log("=" .repeat(100));
  console.log(`📦 Bucket: ${BUCKET}`);
  console.log(`🎯 Target Device: ${deviceKey.substring(0, 60)}...`);
  
  // Calculate date range
  const endDate = new Date();
  const startDate = customDateRange?.start || new Date(endDate.getTime() - (DAYS_TO_AGGREGATE - 1) * 24 * 60 * 60 * 1000);
  const actualEndDate = customDateRange?.end || endDate;
  
  const startTimestamp = startDate.getTime();
  const endTimestamp = actualEndDate.getTime();
  
  console.log(`📅 Date Range: ${startDate.toISOString().split('T')[0]} → ${actualEndDate.toISOString().split('T')[0]} (${DAYS_TO_AGGREGATE} days)`);
  console.log(`📊 Data Sources:`);
  for (const [key, source] of Object.entries(DATA_SOURCES)) {
    console.log(`   • ${key}: ${source.prefix}`);
    console.log(`     Description: ${source.description}`);
    console.log(`     Proto: ${source.proto}`);
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
  
  // Check if protobuf decoder is available
  const MobileRewardShare = helium?.poc_mobile?.mobile_reward_share;
  if (!MobileRewardShare) {
    throw new Error("❌ mobile_reward_share decoder not available in @helium/proto");
  }
  
  console.log("\n✅ mobile_reward_share decoder found");
  
  // ============================================================================
  // SEARCH MOBILE VERIFIED REWARDS
  // ============================================================================
  console.log(`\n📁 SEARCHING MOBILE VERIFIED REWARDS`);
  console.log("-".repeat(80));
  console.log(`📂 Prefix: ${DATA_SOURCES.MOBILE_VERIFIED.prefix}`);
  console.log(`🧩 Proto: ${DATA_SOURCES.MOBILE_VERIFIED.proto}`);
  
  const mobileRewards = [];
  let totalFilesProcessed = 0;
  
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
    const targetDateFiles = files
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
        rewards: [],
        summary: {
          totalRewards: 0,
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
            const decoded = MobileRewardShare.decode(frame);
            
            // Check if this message contains our target device
            if (containsTargetDevice(decoded, targetFormats)) {
              foundCount++;
              
              // Extract reward amount
              const rewardAmount = decoded.gatewayReward?.dcTransferReward?.low || 0;
              
              mobileRewards.push({
                file: fileInfo.file,
                timestamp: fileInfo.timestamp,
                date: fileInfo.date,
                message: decoded,
                enriched: enrichBytes(decoded),
                rewardAmount: rewardAmount,
                rewardType: 'mobile_verified',
                dataSource: DATA_SOURCES.MOBILE_VERIFIED.prefix
              });
              
              console.log(`   ✅ Found device! Reward: ${formatDC(rewardAmount)}`);
            }
          } catch (e) {
            // Ignore decode errors
          }
        }
        
        console.log(`   📊 Processed ${messageCount} messages, found ${foundCount} matches`);
        
      } catch (e) {
        console.log(`   ❌ Error processing file: ${e.message}`);
      }
    }
    
  } catch (e) {
    console.error(`❌ Error accessing mobile verified rewards: ${e.message}`);
    throw e;
  }
  
  // Calculate summary
  const totalRewards = mobileRewards.reduce((sum, reward) => sum + reward.rewardAmount, 0);
  
  console.log(`\n📊 SCRAPING RESULTS`);
  console.log("=" .repeat(100));
  console.log(`✅ Found ${mobileRewards.length} reward entries`);
  console.log(`💰 Total DC Rewards: ${formatNumber(totalRewards)} (${formatDC(totalRewards)})`);
  console.log(`📁 Files Processed: ${totalFilesProcessed}`);
  console.log(`📅 Date Range: ${startDate.toISOString().split('T')[0]} → ${actualEndDate.toISOString().split('T')[0]}`);
  
  return {
    deviceKey,
    rewards: mobileRewards,
    summary: {
      totalRewards,
      rewardCount: mobileRewards.length,
      filesProcessed: totalFilesProcessed,
      filesInRange: targetDateFiles.length,
      dateRange: { start: startDate, end: actualEndDate },
      averageDaily: mobileRewards.length > 0 ? totalRewards / mobileRewards.length : 0
    }
  };
}

// convenience direct-run
if (require.main === module) {
  require('dotenv').config();
  const deviceKey = process.argv[2] || process.env.DEFAULT_DEVICE_KEY;
  if (!deviceKey) {
    console.error('❌ Usage: node src/scrapeHeliumRewards.js <DEVICE_KEY>');
    console.error('❌ Or set DEFAULT_DEVICE_KEY in .env file');
    process.exit(1);
  }
  
  scrapeHeliumRewards(deviceKey)
    .then((result) => {
      console.log('✅ Scraping completed successfully!');
      console.log(`🎯 Device: ${result.deviceKey.substring(0, 60)}...`);
      console.log(`💰 Total Rewards: ${formatDC(result.summary.totalRewards)}`);
      process.exit(0);
    })
    .catch((e) => {
      console.error('❌ Scraping failed:', e.message);
      process.exit(1);
    });
}

module.exports = { scrapeHeliumRewards, formatDC, formatNumber };
