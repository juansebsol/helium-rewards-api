// temp-upsert-rewards.js
// Temporary script to upsert DC rewards data for testing
// This script adds hardcoded reward data for specific dates

require('dotenv').config();
const { supabase } = require('./src/supabase');

// Original device key
const DEVICE_KEY = '14dhJinBmTD6rY7oqaJafaHzHPRXzsTk4vFGSujLPd5MEKB3qzb';
const DEVICE_ID = '92f66950-f198-4737-9014-62c6f6629e73';

// Hardcoded reward data
const REWARD_DATA = [
  { date: '2025-09-03', amount: 6663224679 },
  { date: '2025-09-04', amount: 13333343788 },
  { date: '2025-09-05', amount: 8799854779 },
  { date: '2025-09-06', amount: 8215578544 },
  { date: '2025-09-07', amount: 7253577606 },
  { date: '2025-09-08', amount: 8512613674 },
  { date: '2025-09-09', amount: 11289922238 },
  { date: '2025-09-10', amount: 9037266660 },
  { date: '2025-09-11', amount: 8382862794 },
  { date: '2025-09-12', amount: 4414135135 },
  { date: '2025-09-13', amount: 5681492615 },
  { date: '2025-09-14', amount: 6980692858 },
  { date: '2025-09-15', amount: 4520675467 },
  { date: '2025-09-16', amount: 9420007688 },
  { date: '2025-09-17', amount: 2916930350 },
  { date: '2025-09-18', amount: 4577815044 },
  { date: '2025-09-19', amount: 32101994830 },
  { date: '2025-09-20', amount: 12501659140 },
  { date: '2025-09-21', amount: 78043601 },
  { date: '2025-09-22', amount: 165099483 },
  { date: '2025-09-23', amount: 30188212 },
  { date: '2025-09-24', amount: 16060084 },
  { date: '2025-09-25', amount: 16531850 },
  { date: '2025-09-26', amount: 19259485 },
  { date: '2025-09-27', amount: 117544141 },
  { date: '2025-09-30', amount: 10376456 },
  { date: '2025-10-01', amount: 145525279 }
];

async function upsertTempRewards() {
  console.log('🔄 TEMPORARY REWARDS UPSERT SCRIPT');
  console.log('=' .repeat(60));
  console.log(`🎯 Device: ${DEVICE_KEY}`);
  console.log(`📅 Dates: ${REWARD_DATA.length} entries`);
  console.log('=' .repeat(60));
  
  try {
    // Step 1: Ensure device exists
    console.log('🔍 Checking if device exists...');
    
    let { data: existingDevice, error: deviceSelectError } = await supabase
      .from('devices')
      .select('id, device_key, device_name, is_active')
      .eq('device_key', DEVICE_KEY)
      .single();
    
    if (deviceSelectError && deviceSelectError.code !== 'PGRST116') {
      throw new Error(`Error checking device: ${deviceSelectError.message}`);
    }
    
    if (!existingDevice) {
      console.log('➕ Creating new device record with specific ID...');
      const { data: newDevice, error: deviceInsertError } = await supabase
        .from('devices')
        .insert({
          id: DEVICE_ID,
          device_key: DEVICE_KEY,
          device_name: `TEMP Device ${DEVICE_KEY.substring(0, 12)}...`,
          description: 'Temporary device for testing - hardcoded rewards data',
          is_active: true
        })
        .select()
        .single();
      
      if (deviceInsertError) {
        throw new Error(`Error creating device: ${deviceInsertError.message}`);
      }
      
      console.log(`✅ Device created with ID: ${newDevice.id}`);
    } else {
      console.log(`✅ Device exists with ID: ${existingDevice.id}`);
      // Update device name to indicate it's temporary
      await supabase
        .from('devices')
        .update({ 
          device_name: `TEMP Device ${DEVICE_KEY.substring(0, 12)}...`,
          description: 'Temporary device for testing - hardcoded rewards data'
        })
        .eq('id', existingDevice.id);
      console.log(`🔄 Updated device name to indicate temporary status`);
    }
    
    // Step 2: Upsert reward data
    console.log('\n📅 Upserting reward data...');
    
    // Get the actual device ID (either existing or newly created)
    const actualDeviceId = existingDevice ? existingDevice.id : DEVICE_ID;
    console.log(`🆔 Using device ID: ${actualDeviceId}`);
    
    let successCount = 0;
    let errorCount = 0;
    
    for (const reward of REWARD_DATA) {
      try {
        const transactionDate = new Date(reward.date + 'T00:00:00.000Z');
        
        // Create reward data object - only required fields
        const rewardData = {
          device_key: DEVICE_KEY,
          device_id: actualDeviceId,
          transaction_date: transactionDate.toISOString().split('T')[0],
          reward_amount_dc: reward.amount,
          data_source: 'temp-manual-insert'
        };
        
        console.log(`📅 Processing ${reward.date}: ${reward.amount.toLocaleString()} DC`);
        
        // Insert to database
        const { data, error } = await supabase
          .from('helium_rewards_daily')
          .insert(rewardData)
          .select();
        
        if (error) {
          console.log(`   ❌ Failed: ${error.message}`);
          errorCount++;
        } else {
          console.log(`   ✅ Success: Upserted`);
          successCount++;
        }
        
      } catch (error) {
        console.log(`   ❌ Error processing ${reward.date}: ${error.message}`);
        errorCount++;
      }
    }
    
    console.log('\n📊 UPSERT SUMMARY');
    console.log('=' .repeat(60));
    console.log(`✅ Successful: ${successCount}`);
    console.log(`❌ Failed: ${errorCount}`);
    console.log(`📅 Total dates: ${REWARD_DATA.length}`);
    
    if (successCount === REWARD_DATA.length) {
      console.log('\n🎉 All rewards upserted successfully!');
    } else {
      console.log('\n⚠️  Some rewards failed to upsert. Check errors above.');
    }
    
  } catch (error) {
    console.error('❌ Script failed:', error.message);
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  upsertTempRewards()
    .then(() => {
      console.log('\n✅ Script completed');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Script failed:', error.message);
      process.exit(1);
    });
}

module.exports = { upsertTempRewards, DEVICE_KEY, DEVICE_ID, REWARD_DATA };