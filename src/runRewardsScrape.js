// src/runRewardsScrape.js
// Main orchestrator for Helium rewards scraping
// - Coordinates scraper, parser, and database operations
// - Handles error recovery and retries
// - Provides progress reporting
// - Used for local testing and development

const { scrapeHeliumRewards } = require('./scrapeHeliumRewards');
const { scrapePocRewards } = require('./scrapePocRewards');
const { parseRewardsData, validateRewardData, enrichRewardData } = require('./parseRewardsData');
const { upsertHeliumRewards } = require('./upsertHeliumRewards');
const { testConnection } = require('./supabase');

// Main orchestration function
async function runRewardsScrape(deviceKey, options = {}) {
  const startTime = Date.now();
  
  console.log('🚀 HELIUM REWARDS SCRAPE ORCHESTRATOR (DC + PoC)');
  console.log('=' .repeat(100));
  console.log(`🎯 Target Device: ${deviceKey.substring(0, 60)}...`);
  console.log(`⏰ Started at: ${new Date().toISOString()}`);
  
  if (options.dateRange) {
    console.log(`📅 Custom Date Range: ${options.dateRange.start.toISOString().split('T')[0]} → ${options.dateRange.end.toISOString().split('T')[0]}`);
  }
  
  console.log('=' .repeat(100));
  
  let scrapedData, pocData, parsedData, upsertResult;
  
  try {
    // Step 1: Test database connection
    console.log('\n🔧 STEP 1: Testing database connection...');
    const connectionOk = await testConnection();
    if (!connectionOk) {
      throw new Error('Database connection test failed');
    }
    
    // Step 2: Scrape DC rewards data from AWS S3
    console.log('\n📡 STEP 2: Scraping DC rewards data from AWS S3...');
    scrapedData = await scrapeHeliumRewards(deviceKey, options.dateRange);
    
    // Step 3: Scrape PoC rewards data from AWS S3
    console.log('\n💎 STEP 3: Scraping PoC rewards data from AWS S3...');
    pocData = await scrapePocRewards(deviceKey, options.dateRange);
    
    if ((!scrapedData.rewards || scrapedData.rewards.length === 0) && 
        (!pocData.pocRewards || pocData.pocRewards.length === 0)) {
      console.log('⚠️ No rewards data found for this device and date range');
      return {
        success: true,
        deviceKey,
        message: 'No rewards data found',
        summary: scrapedData.summary,
        executionTime: Date.now() - startTime
      };
    }
    
    // Step 4: Parse and aggregate rewards data (DC + PoC)
    console.log('\n📊 STEP 4: Parsing and aggregating rewards data (DC + PoC)...');
    parsedData = parseRewardsData(scrapedData, pocData);
    
    // Step 5: Validate parsed data
    console.log('\n✅ STEP 5: Validating parsed data...');
    validateRewardData(parsedData);
    
    // Step 6: Enrich data with additional fields
    console.log('\n🔍 STEP 6: Enriching data with additional fields...');
    parsedData = enrichRewardData(parsedData);
    
    // Step 7: Upsert to database
    console.log('\n💾 STEP 7: Upserting to database...');
    
    // Add execution time to metadata
    parsedData.metadata.executionTime = Date.now() - startTime;
    
    upsertResult = await upsertHeliumRewards(parsedData);
    
    const executionTime = Date.now() - startTime;
    
    // Final summary
    console.log('\n🎉 SCRAPE OPERATION COMPLETED SUCCESSFULLY!');
    console.log('=' .repeat(100));
    console.log(`🎯 Device: ${deviceKey.substring(0, 60)}...`);
    console.log(`💰 Total DC Rewards: ${scrapedData.summary.totalRewards.toLocaleString()}`);
    if (pocData && pocData.summary) {
      console.log(`💎 Total PoC Rewards: ${pocData.summary.totalPoc.toLocaleString()} (Base: ${pocData.summary.totalBasePoc.toLocaleString()}, Boosted: ${pocData.summary.totalBoostedPoc.toLocaleString()})`);
    }
    console.log(`📅 Daily Entries: ${parsedData.dailyRewards.length}`);
    console.log(`➕ Database Inserts: ${upsertResult.insertedCount}`);
    console.log(`🔄 Database Updates: ${upsertResult.updatedCount}`);
    console.log(`⏱️ Total Execution Time: ${(executionTime / 1000).toFixed(2)}s`);
    console.log(`✅ Status: ${upsertResult.errorCount > 0 ? 'Partial Success' : 'Complete Success'}`);
    console.log('=' .repeat(100));
    
    return {
      success: true,
      deviceKey,
      scrapedData,
      pocData,
      parsedData,
      upsertResult,
      executionTime,
      summary: {
        totalRewards: scrapedData.summary.totalRewards,
        totalPocRewards: pocData?.summary?.totalPoc || 0,
        dailyEntries: parsedData.dailyRewards.length,
        databaseInserts: upsertResult.insertedCount,
        databaseUpdates: upsertResult.updatedCount,
        errors: upsertResult.errorCount
      }
    };
    
  } catch (error) {
    const executionTime = Date.now() - startTime;
    
    console.error('\n❌ SCRAPE OPERATION FAILED!');
    console.error('=' .repeat(100));
    console.error(`🎯 Device: ${deviceKey.substring(0, 60)}...`);
    console.error(`❌ Error: ${error.message}`);
    console.error(`⏱️ Execution Time: ${(executionTime / 1000).toFixed(2)}s`);
    
    // Log partial results if available
    if (scrapedData) {
      console.error(`📊 Scraped DC: ${scrapedData.rewards?.length || 0} reward entries`);
    }
    if (pocData) {
      console.error(`💎 Scraped PoC: ${pocData.pocRewards?.length || 0} reward entries`);
    }
    if (parsedData) {
      console.error(`📅 Parsed: ${parsedData.dailyRewards?.length || 0} daily entries`);
    }
    if (upsertResult) {
      console.error(`💾 Database: ${upsertResult.insertedCount + upsertResult.updatedCount} successful operations`);
    }
    
    console.error('=' .repeat(100));
    
    return {
      success: false,
      deviceKey,
      error: error.message,
      executionTime,
      partialResults: {
        scrapedData: scrapedData || null,
        pocData: pocData || null,
        parsedData: parsedData || null,
        upsertResult: upsertResult || null
      }
    };
  }
}

// Batch scrape multiple devices
async function runBatchRewardsScrape(deviceKeys, options = {}) {
  console.log('🔄 BATCH REWARDS SCRAPING');
  console.log('=' .repeat(100));
  console.log(`📱 Devices: ${deviceKeys.length}`);
  console.log(`⏰ Started at: ${new Date().toISOString()}`);
  console.log('=' .repeat(100));
  
  const results = [];
  const batchStartTime = Date.now();
  
  for (let i = 0; i < deviceKeys.length; i++) {
    const deviceKey = deviceKeys[i];
    console.log(`\n📱 Processing device ${i + 1}/${deviceKeys.length}: ${deviceKey.substring(0, 60)}...`);
    
    try {
      const result = await runRewardsScrape(deviceKey, options);
      results.push(result);
      
      // Add delay between devices to avoid rate limiting
      if (i < deviceKeys.length - 1) {
        console.log('⏳ Waiting 10 seconds before next device...');
        await new Promise(resolve => setTimeout(resolve, 10000));
      }
      
    } catch (error) {
      console.error(`❌ Failed to process device ${deviceKey.substring(0, 60)}...: ${error.message}`);
      results.push({
        success: false,
        deviceKey,
        error: error.message
      });
    }
  }
  
  const batchExecutionTime = Date.now() - batchStartTime;
  const successCount = results.filter(r => r.success).length;
  const failureCount = results.length - successCount;
  
  console.log('\n🎉 BATCH SCRAPING COMPLETED!');
  console.log('=' .repeat(100));
  console.log(`📱 Total Devices: ${deviceKeys.length}`);
  console.log(`✅ Successful: ${successCount}`);
  console.log(`❌ Failed: ${failureCount}`);
  console.log(`⏱️ Total Batch Time: ${(batchExecutionTime / 1000 / 60).toFixed(2)} minutes`);
  console.log('=' .repeat(100));
  
  return {
    success: failureCount === 0,
    totalDevices: deviceKeys.length,
    successCount,
    failureCount,
    results,
    batchExecutionTime
  };
}

// convenience direct-run
if (require.main === module) {
  require('dotenv').config();
  
  const deviceKey = process.argv[2] || process.env.DEFAULT_DEVICE_KEY;
  
  if (!deviceKey) {
    console.error('❌ Usage: node src/runRewardsScrape.js <DEVICE_KEY>');
    console.error('❌ Or set DEFAULT_DEVICE_KEY in .env file');
    console.error('');
    console.error('Examples:');
    console.error('  node src/runRewardsScrape.js 1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi');
    process.exit(1);
  }
  
  // Parse optional date range
  let dateRange = null;
  if (process.argv[3] && process.argv[4]) {
    dateRange = {
      start: new Date(process.argv[3]),
      end: new Date(process.argv[4])
    };
    
    if (isNaN(dateRange.start.getTime()) || isNaN(dateRange.end.getTime())) {
      console.error('❌ Invalid date format. Use YYYY-MM-DD');
      process.exit(1);
    }
  }
  
  runRewardsScrape(deviceKey, { dateRange })
    .then((result) => {
      if (result.success) {
        console.log('🎉 Scraping completed successfully!');
        process.exit(0);
      } else {
        console.error('❌ Scraping failed');
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error('❌ Unexpected error:', error.message);
      process.exit(1);
    });
}

module.exports = { runRewardsScrape, runBatchRewardsScrape };
