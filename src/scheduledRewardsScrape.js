// src/scheduledRewardsScrape.js
// Scheduled scraping logic for Helium rewards
// - Manages automated scraping schedules
// - Handles multiple device processing
// - Provides device management commands
// - Used by GitHub Actions for daily automation

const { runRewardsScrape, runBatchRewardsScrape } = require('./runRewardsScrape');
const { getTrackedDevices, addDeviceToTracking, removeDeviceFromTracking } = require('./upsertHeliumRewards');

// Run scheduled scraping for all tracked devices
async function runScheduledScraping(options = {}) {
  console.log('📅 SCHEDULED HELIUM REWARDS SCRAPING');
  console.log('=' .repeat(100));
  console.log(`⏰ Started at: ${new Date().toISOString()}`);
  console.log('=' .repeat(100));
  
  try {
    // Get all tracked devices
    console.log('📋 Fetching tracked devices...');
    const trackedDevices = await getTrackedDevices();
    
    if (trackedDevices.length === 0) {
      console.log('⚠️ No devices are currently tracked for scraping');
      console.log('💡 Add devices using: npm run device:add <DEVICE_KEY> "<DEVICE_NAME>"');
      return {
        success: true,
        message: 'No tracked devices',
        devicesProcessed: 0
      };
    }
    
    console.log(`📱 Found ${trackedDevices.length} tracked devices:`);
    trackedDevices.forEach((device, i) => {
      const lastScraped = device.last_scraped 
        ? new Date(device.last_scraped).toLocaleDateString()
        : 'Never';
      console.log(`   ${i + 1}. ${device.devices.device_name || 'Unnamed'} (${device.device_key.substring(0, 20)}...) - Last: ${lastScraped}`);
    });
    
    // Extract device keys
    const deviceKeys = trackedDevices.map(d => d.device_key);
    
    // Run batch scraping
    console.log('\n🚀 Starting batch scraping...');
    const batchResult = await runBatchRewardsScrape(deviceKeys, options);
    
    // Summary
    console.log('\n📊 SCHEDULED SCRAPING SUMMARY');
    console.log('=' .repeat(100));
    console.log(`📱 Total Tracked Devices: ${trackedDevices.length}`);
    console.log(`✅ Successfully Processed: ${batchResult.successCount}`);
    console.log(`❌ Failed: ${batchResult.failureCount}`);
    console.log(`⏱️ Total Time: ${(batchResult.batchExecutionTime / 1000 / 60).toFixed(2)} minutes`);
    
    // Log failures
    if (batchResult.failureCount > 0) {
      console.log('\n❌ FAILED DEVICES:');
      batchResult.results
        .filter(r => !r.success)
        .forEach(result => {
          console.log(`   • ${result.deviceKey.substring(0, 20)}...: ${result.error}`);
        });
    }
    
    console.log('=' .repeat(100));
    
    return {
      success: batchResult.success,
      devicesProcessed: trackedDevices.length,
      successCount: batchResult.successCount,
      failureCount: batchResult.failureCount,
      executionTime: batchResult.batchExecutionTime,
      results: batchResult.results
    };
    
  } catch (error) {
    console.error('❌ Scheduled scraping failed:', error.message);
    return {
      success: false,
      error: error.message,
      devicesProcessed: 0
    };
  }
}

// Run scraping for specific devices
async function runSpecificDevicesScraping(deviceKeys, options = {}) {
  console.log('🎯 SPECIFIC DEVICES SCRAPING');
  console.log('=' .repeat(100));
  console.log(`📱 Devices: ${deviceKeys.length}`);
  console.log(`⏰ Started at: ${new Date().toISOString()}`);
  
  deviceKeys.forEach((key, i) => {
    console.log(`   ${i + 1}. ${key.substring(0, 60)}...`);
  });
  
  console.log('=' .repeat(100));
  
  try {
    const batchResult = await runBatchRewardsScrape(deviceKeys, options);
    
    console.log('\n📊 SPECIFIC DEVICES SCRAPING SUMMARY');
    console.log('=' .repeat(100));
    console.log(`📱 Total Devices: ${deviceKeys.length}`);
    console.log(`✅ Successfully Processed: ${batchResult.successCount}`);
    console.log(`❌ Failed: ${batchResult.failureCount}`);
    console.log(`⏱️ Total Time: ${(batchResult.batchExecutionTime / 1000 / 60).toFixed(2)} minutes`);
    console.log('=' .repeat(100));
    
    return batchResult;
    
  } catch (error) {
    console.error('❌ Specific devices scraping failed:', error.message);
    return {
      success: false,
      error: error.message,
      devicesProcessed: 0
    };
  }
}

// Command line interface for device management
async function handleDeviceManagement(command, deviceKey, deviceName, notes) {
  switch (command) {
    case 'add':
      if (!deviceKey) {
        console.error('❌ Device key is required for add command');
        console.error('Usage: npm run device:add <DEVICE_KEY> "<DEVICE_NAME>" "<NOTES>"');
        process.exit(1);
      }
      
      console.log('➕ ADDING DEVICE TO TRACKING');
      console.log('=' .repeat(60));
      
      try {
        const result = await addDeviceToTracking(deviceKey, deviceName, notes);
        console.log('✅ Device added successfully!');
        console.log(`🎯 Device Key: ${deviceKey.substring(0, 60)}...`);
        console.log(`📝 Device Name: ${deviceName || 'Auto-generated'}`);
        if (notes) console.log(`📋 Notes: ${notes}`);
        return result;
      } catch (error) {
        console.error('❌ Failed to add device:', error.message);
        process.exit(1);
      }
      
    case 'remove':
      if (!deviceKey) {
        console.error('❌ Device key is required for remove command');
        console.error('Usage: npm run device:remove <DEVICE_KEY>');
        process.exit(1);
      }
      
      console.log('➖ REMOVING DEVICE FROM TRACKING');
      console.log('=' .repeat(60));
      
      try {
        await removeDeviceFromTracking(deviceKey);
        console.log('✅ Device removed successfully!');
        console.log(`🎯 Device Key: ${deviceKey.substring(0, 60)}...`);
        return true;
      } catch (error) {
        console.error('❌ Failed to remove device:', error.message);
        process.exit(1);
      }
      
    case 'list':
      console.log('📋 TRACKED DEVICES LIST');
      console.log('=' .repeat(60));
      
      try {
        const devices = await getTrackedDevices();
        
        if (devices.length === 0) {
          console.log('⚠️ No devices are currently being tracked');
          console.log('💡 Add a device with: npm run device:add <DEVICE_KEY> "<DEVICE_NAME>"');
          return [];
        }
        
        console.log(`Found ${devices.length} tracked devices:\n`);
        
        devices.forEach((device, i) => {
          const lastScraped = device.last_scraped 
            ? new Date(device.last_scraped).toLocaleString()
            : 'Never';
          
          console.log(`${i + 1}. ${device.devices.device_name || 'Unnamed Device'}`);
          console.log(`   🎯 Key: ${device.device_key.substring(0, 60)}...`);
          console.log(`   📅 Added: ${new Date(device.added_to_tracked_at).toLocaleDateString()}`);
          console.log(`   🔄 Last Scraped: ${lastScraped}`);
          console.log(`   ✅ Active: ${device.is_active ? 'Yes' : 'No'}`);
          if (device.notes) {
            console.log(`   📋 Notes: ${device.notes}`);
          }
          console.log('');
        });
        
        return devices;
      } catch (error) {
        console.error('❌ Failed to list devices:', error.message);
        process.exit(1);
      }
      
    default:
      console.error('❌ Unknown command:', command);
      console.error('Available commands: add, remove, list');
      process.exit(1);
  }
}

// convenience direct-run
if (require.main === module) {
  require('dotenv').config();
  
  const command = process.argv[2];
  
  if (!command) {
    console.error('❌ Command is required');
    console.error('');
    console.error('Available commands:');
    console.error('  all                           - Scrape all tracked devices');
    console.error('  add <DEVICE_KEY> [NAME]       - Add device to tracking');
    console.error('  remove <DEVICE_KEY>           - Remove device from tracking');
    console.error('  list                          - List tracked devices');
    console.error('  devices <KEY1,KEY2,...>       - Scrape specific devices');
    console.error('');
    console.error('Examples:');
    console.error('  npm run scrape:scheduled all');
    console.error('  npm run device:add 1trSusefVoBGpZF... "My Hotspot"');
    console.error('  npm run device:remove 1trSusefVoBGpZF...');
    console.error('  npm run device:list');
    process.exit(1);
  }
  
  // Handle different commands
  if (command === 'all') {
    runScheduledScraping()
      .then(result => {
        if (result.success) {
          console.log('🎉 Scheduled scraping completed successfully!');
          process.exit(0);
        } else {
          console.error('❌ Scheduled scraping failed');
          process.exit(1);
        }
      })
      .catch(error => {
        console.error('❌ Unexpected error:', error.message);
        process.exit(1);
      });
  } 
  else if (command === 'devices') {
    const deviceKeysArg = process.argv[3];
    if (!deviceKeysArg) {
      console.error('❌ Device keys are required');
      console.error('Usage: npm run scrape:scheduled devices <KEY1,KEY2,...>');
      process.exit(1);
    }
    
    const deviceKeys = deviceKeysArg.split(',').map(key => key.trim());
    
    runSpecificDevicesScraping(deviceKeys)
      .then(result => {
        if (result.success) {
          console.log('🎉 Specific devices scraping completed successfully!');
          process.exit(0);
        } else {
          console.error('❌ Specific devices scraping failed');
          process.exit(1);
        }
      })
      .catch(error => {
        console.error('❌ Unexpected error:', error.message);
        process.exit(1);
      });
  }
  else if (['add', 'remove', 'list'].includes(command)) {
    const deviceKey = process.argv[3];
    const deviceName = process.argv[4];
    const notes = process.argv[5];
    
    handleDeviceManagement(command, deviceKey, deviceName, notes)
      .then(() => {
        process.exit(0);
      })
      .catch(error => {
        console.error('❌ Device management failed:', error.message);
        process.exit(1);
      });
  }
  else {
    console.error('❌ Unknown command:', command);
    process.exit(1);
  }
}

module.exports = { 
  runScheduledScraping, 
  runSpecificDevicesScraping, 
  handleDeviceManagement 
};
