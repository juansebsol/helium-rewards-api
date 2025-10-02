// test-local-scraping.js
// Local testing script for Helium rewards scraping
// Provides various test scenarios and debugging utilities
//
// USAGE COMMANDS:
// ===============
// 
// QUICK COMMANDS (using npm scripts - uses your .env automatically):
// ------------------------------------------------------------------
// npm run test:local          - Run all tests (environment, database, devices, scrape, pipeline)
// npm run test:scrape         - Test scraping only (uses DEFAULT_DEVICE_KEY from .env, 7 days)
// npm run test:query          - Test device queries only
// npm run test:scheduled      - Test scheduled scraping functionality
//
// DIRECT COMMANDS (manual with custom parameters):
// ------------------------------------------------
// 1. Test scraping only (no database):
//    node -r dotenv/config test-local-scraping-2.js scrape <DEVICE_KEY> <DAYS> <START_DATE>
//    Example: node -r dotenv/config test-local-scraping-2.js scrape 14dhJinBmTD6rY7oqaJafaHzHPRXzsTk4vFGSujLPd5MEKB3qzb 30 2025-09-04
//
// 2. Test full pipeline (scrape + parse + database):
//    node -r dotenv/config test-local-scraping-2.js pipeline <DEVICE_KEY> <DAYS> <START_DATE>
//    Example: node -r dotenv/config test-local-scraping-2.js pipeline 14dhJinBmTD6rY7oqaJafaHzHPRXzsTk4vFGSujLPd5MEKB3qzb 30 2025-09-04
//
// 3. Run all tests:
//    node -r dotenv/config test-local-scraping-2.js all
//
// 4. Test specific components:
//    node -r dotenv/config test-local-scraping-2.js env      - Check environment variables
//    node -r dotenv/config test-local-scraping-2.js db       - Test database connection
//    node -r dotenv/config test-local-scraping-2.js devices  - List tracked devices
//
// PARAMETERS:
// - DEVICE_KEY: Your Helium device key (required for direct commands)
// - DAYS: Number of days to scrape backwards from START_DATE (default: 7)
// - START_DATE: Start date in YYYY-MM-DD format (default: today)
//
// NOTES:
// - npm scripts automatically load your .env file and use DEFAULT_DEVICE_KEY
// - Direct commands require you to specify device key manually
// - All commands use dotenv to load environment variables automatically
//
// HOW IT WORKS:
// - Scrapes AWS S3 for Helium blockchain data
// - Looks for DC transfer rewards for your device
// - Processes protobuf messages to extract reward amounts
// - Can optionally save to database (pipeline mode)
// - Shows detailed logs of what it finds

require('dotenv').config();

const { runRewardsScrape } = require('./src/runRewardsScrape');
const { scrapeHeliumRewards } = require('./src/scrapeHeliumRewards');
const { parseRewardsData } = require('./src/parseRewardsData');
const { upsertHeliumRewards, getTrackedDevices } = require('./src/upsertHeliumRewards');
const { testConnection } = require('./src/supabase');

// Test configuration
const DEFAULT_DEVICE_KEY = process.env.DEFAULT_DEVICE_KEY || '1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi';

// CONFIGURATION - Change these values to test different scenarios
const DEFAULT_DAYS_TO_SCRAPE = 30; // Number of days to scrape in test suite
const DEFAULT_TEST_DAYS = 7; // Default days for individual test commands

// Test scenarios
async function testDatabaseConnection() {
  console.log('🔧 TESTING DATABASE CONNECTION');
  console.log('=' .repeat(60));
  
  try {
    const result = await testConnection();
    if (result) {
      console.log('✅ Database connection successful');
      return true;
    } else {
      console.log('❌ Database connection failed');
      return false;
    }
  } catch (error) {
    console.error('❌ Database connection error:', error.message);
    return false;
  }
}

async function testScrapeOnly(deviceKey = DEFAULT_DEVICE_KEY, days = DEFAULT_TEST_DAYS, startDate = null) {
  console.log('📡 TESTING SCRAPE ONLY (NO DATABASE)');
  console.log('=' .repeat(60));
  console.log(`🎯 Device: ${deviceKey.substring(0, 60)}...`);
  console.log(`📅 Days: ${days}`);
  if (startDate) {
    console.log(`📅 Start Date: ${startDate.toISOString().split('T')[0]}`);
  }
  console.log('=' .repeat(60));
  
  try {
    // Create custom date range for testing
    const endDate = startDate || new Date();
    const actualStartDate = new Date(endDate.getTime() - days * 24 * 60 * 60 * 1000);
    
    const result = await scrapeHeliumRewards(deviceKey, {
      start: actualStartDate,
      end: endDate
    });
    
    console.log('\n📊 SCRAPE RESULTS');
    console.log('-' .repeat(60));
    console.log(`✅ Rewards found: ${result.rewards.length}`);
    console.log(`💰 Total DC: ${result.summary.totalRewards.toLocaleString()}`);
    console.log(`📁 Files processed: ${result.summary.filesProcessed}`);
    console.log(`📅 Date range: ${actualStartDate.toISOString().split('T')[0]} → ${endDate.toISOString().split('T')[0]}`);
    
    if (result.rewards.length > 0) {
      console.log('\n🎯 SAMPLE REWARDS:');
      result.rewards.slice(0, 3).forEach((reward, i) => {
        console.log(`   ${i + 1}. ${reward.date.toISOString().split('T')[0]}: ${reward.rewardAmount.toLocaleString()} DC`);
      });
    }
    
    return result;
    
  } catch (error) {
    console.error('❌ Scrape test failed:', error.message);
    return null;
  }
}

async function testFullPipeline(deviceKey = DEFAULT_DEVICE_KEY, days = DEFAULT_TEST_DAYS, startDate = null) {
  console.log('🚀 TESTING FULL PIPELINE');
  console.log('=' .repeat(60));
  console.log(`🎯 Device: ${deviceKey.substring(0, 60)}...`);
  console.log(`📅 Days: ${days}`);
  if (startDate) {
    console.log(`📅 Start Date: ${startDate.toISOString().split('T')[0]}`);
  }
  console.log('=' .repeat(60));
  
  try {
    // Create custom date range for testing
    const endDate = startDate || new Date();
    const actualStartDate = new Date(endDate.getTime() - days * 24 * 60 * 60 * 1000);
    
    const result = await runRewardsScrape(deviceKey, {
      dateRange: { start: actualStartDate, end: endDate }
    });
    
    if (result.success) {
      console.log('\n🎉 PIPELINE TEST SUCCESSFUL');
      console.log('-' .repeat(60));
      console.log(`💰 Total rewards: ${result.summary.totalRewards.toLocaleString()} DC`);
      console.log(`📅 Daily entries: ${result.summary.dailyEntries}`);
      console.log(`➕ Database inserts: ${result.summary.databaseInserts}`);
      console.log(`🔄 Database updates: ${result.summary.databaseUpdates}`);
      console.log(`⏱️ Execution time: ${(result.executionTime / 1000).toFixed(2)}s`);
    } else {
      console.log('\n❌ PIPELINE TEST FAILED');
      console.log('-' .repeat(60));
      console.log(`❌ Error: ${result.error}`);
    }
    
    return result;
    
  } catch (error) {
    console.error('❌ Full pipeline test failed:', error.message);
    return null;
  }
}

async function testQueryDevices() {
  console.log('📋 TESTING DEVICE QUERIES');
  console.log('=' .repeat(60));
  
  try {
    const devices = await getTrackedDevices();
    
    console.log(`✅ Found ${devices.length} tracked devices:`);
    
    if (devices.length === 0) {
      console.log('⚠️ No devices are currently tracked');
      console.log('💡 Add a device with: npm run device:add <DEVICE_KEY> "Device Name"');
    } else {
      devices.forEach((device, i) => {
        const lastScraped = device.last_scraped 
          ? new Date(device.last_scraped).toLocaleDateString()
          : 'Never';
        
        console.log(`\n${i + 1}. ${device.devices.device_name || 'Unnamed'}`);
        console.log(`   🎯 Key: ${device.device_key.substring(0, 60)}...`);
        console.log(`   📅 Added: ${new Date(device.added_to_tracked_at).toLocaleDateString()}`);
        console.log(`   🔄 Last Scraped: ${lastScraped}`);
        console.log(`   ✅ Active: ${device.is_active ? 'Yes' : 'No'}`);
      });
    }
    
    return devices;
    
  } catch (error) {
    console.error('❌ Device query test failed:', error.message);
    return null;
  }
}

async function testEnvironmentVariables() {
  console.log('🔧 TESTING ENVIRONMENT VARIABLES');
  console.log('=' .repeat(60));
  
  const requiredVars = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_REGION',
    'AWS_BUCKET',
    'SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY'
  ];
  
  const optionalVars = [
    'DEFAULT_DEVICE_KEY',
    'DAYS_TO_AGGREGATE',
    'GITHUB_TOKEN',
    'GITHUB_REPOSITORY'
  ];
  
  let allRequired = true;
  
  console.log('📋 Required Variables:');
  requiredVars.forEach(varName => {
    const value = process.env[varName];
    if (value) {
      const displayValue = varName.includes('KEY') || varName.includes('SECRET') 
        ? `${value.substring(0, 8)}...` 
        : value;
      console.log(`   ✅ ${varName}: ${displayValue}`);
    } else {
      console.log(`   ❌ ${varName}: NOT SET`);
      allRequired = false;
    }
  });
  
  console.log('\n📋 Optional Variables:');
  optionalVars.forEach(varName => {
    const value = process.env[varName];
    if (value) {
      const displayValue = varName.includes('KEY') || varName.includes('TOKEN')
        ? `${value.substring(0, 8)}...`
        : value;
      console.log(`   ✅ ${varName}: ${displayValue}`);
    } else {
      console.log(`   ⚠️ ${varName}: NOT SET`);
    }
  });
  
  if (allRequired) {
    console.log('\n✅ All required environment variables are set');
  } else {
    console.log('\n❌ Some required environment variables are missing');
    console.log('💡 Copy env.sample to .env and fill in your values');
  }
  
  return allRequired;
}

async function runTestSuite() {
  console.log('🧪 HELIUM REWARDS API TEST SUITE');
  console.log('=' .repeat(80));
  console.log(`⏰ Started at: ${new Date().toISOString()}`);
  console.log('=' .repeat(80));
  
  const results = {};
  
  // Test 1: Environment variables
  console.log('\n1️⃣ Testing environment variables...');
  results.environment = await testEnvironmentVariables();
  
  if (!results.environment) {
    console.log('\n❌ Environment test failed - stopping test suite');
    return results;
  }
  
  // Test 2: Database connection
  console.log('\n2️⃣ Testing database connection...');
  results.database = await testDatabaseConnection();
  
  if (!results.database) {
    console.log('\n❌ Database test failed - stopping test suite');
    return results;
  }
  
  // Test 3: Device queries
  console.log('\n3️⃣ Testing device queries...');
  results.devices = await testQueryDevices();
  
  // Test 4: Scrape only (no database)
  console.log('\n4️⃣ Testing scrape functionality...');
  results.scrape = await testScrapeOnly(DEFAULT_DEVICE_KEY, DEFAULT_DAYS_TO_SCRAPE);
  
  // Test 5: Full pipeline (if scrape worked)
  if (results.scrape && results.scrape.rewards.length > 0) {
    console.log('\n5️⃣ Testing full pipeline...');
    results.pipeline = await testFullPipeline(DEFAULT_DEVICE_KEY, DEFAULT_DAYS_TO_SCRAPE);
  } else {
    console.log('\n5️⃣ Skipping full pipeline test (no scraped data)');
    results.pipeline = null;
  }
  
  // Summary
  console.log('\n📊 TEST SUITE SUMMARY');
  console.log('=' .repeat(80));
  console.log(`✅ Environment: ${results.environment ? 'PASS' : 'FAIL'}`);
  console.log(`✅ Database: ${results.database ? 'PASS' : 'FAIL'}`);
  console.log(`✅ Device Queries: ${results.devices ? 'PASS' : 'FAIL'}`);
  console.log(`✅ Scraping: ${results.scrape ? 'PASS' : 'FAIL'}`);
  console.log(`✅ Full Pipeline: ${results.pipeline ? 'PASS' : 'SKIP'}`);
  
  const passCount = Object.values(results).filter(r => r !== null && r !== false).length;
  const totalTests = Object.keys(results).length;
  
  console.log(`\n🎯 Overall: ${passCount}/${totalTests} tests passed`);
  console.log('=' .repeat(80));
  
  return results;
}

// Command line interface
if (require.main === module) {
  const command = process.argv[2];
  const deviceKey = process.argv[3] || DEFAULT_DEVICE_KEY;
  const days = parseInt(process.argv[4]) || DEFAULT_TEST_DAYS;
  const startDateStr = process.argv[5];
  const startDate = startDateStr ? new Date(startDateStr) : null;
  
  switch (command) {
    case 'env':
      testEnvironmentVariables()
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;
      
    case 'db':
      testDatabaseConnection()
        .then(result => process.exit(result ? 0 : 1))
        .catch(() => process.exit(1));
      break;
      
    case 'scrape':
      testScrapeOnly(deviceKey, days, startDate)
        .then(result => process.exit(result ? 0 : 1))
        .catch(() => process.exit(1));
      break;
      
    case 'pipeline':
      testFullPipeline(deviceKey, days, startDate)
        .then(result => process.exit(result?.success ? 0 : 1))
        .catch(() => process.exit(1));
      break;
      
    case 'devices':
      testQueryDevices()
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;
      
    case 'all':
    default:
      runTestSuite()
        .then(results => {
          const failed = Object.values(results).some(r => r === false);
          process.exit(failed ? 1 : 0);
        })
        .catch(() => process.exit(1));
      break;
  }
}

module.exports = {
  testDatabaseConnection,
  testScrapeOnly,
  testFullPipeline,
  testQueryDevices,
  testEnvironmentVariables,
  runTestSuite
};
