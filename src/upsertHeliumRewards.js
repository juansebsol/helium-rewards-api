// src/upsertHeliumRewards.js
// Database operations for Helium device rewards
// - Upserts daily reward aggregations
// - Manages device tracking
// - Creates audit trail entries
// - Handles data conflicts and deduplication

const { supabase } = require('./supabase');

// Upsert daily rewards data into database
async function upsertHeliumRewards(parsedData) {
  if (!parsedData || !parsedData.dailyRewards) {
    throw new Error('Invalid parsed data provided');
  }

  const { deviceKey, dailyRewards, summary, metadata } = parsedData;
  
  console.log('💾 UPSERTING HELIUM REWARDS TO DATABASE');
  console.log('=' .repeat(80));
  console.log(`🎯 Device: ${deviceKey.substring(0, 60)}...`);
  console.log(`📝 Processing ${dailyRewards.length} daily reward entries`);
  
  let deviceId;
  let insertedCount = 0;
  let updatedCount = 0;
  let errorCount = 0;
  
  try {
    // Step 1: Ensure device exists in devices table
    console.log('🔍 Checking if device exists...');
    
    let { data: existingDevice, error: deviceSelectError } = await supabase
      .from('devices')
      .select('id, device_key, device_name, is_active')
      .eq('device_key', deviceKey)
      .single();
    
    if (deviceSelectError && deviceSelectError.code !== 'PGRST116') {
      throw new Error(`Error checking device: ${deviceSelectError.message}`);
    }
    
    if (!existingDevice) {
      console.log('➕ Creating new device record...');
      const { data: newDevice, error: deviceInsertError } = await supabase
        .from('devices')
        .insert({
          device_key: deviceKey,
          device_name: `Device ${deviceKey.substring(0, 12)}...`,
          description: 'Auto-created during rewards scraping',
          is_active: true
        })
        .select('id, device_key')
        .single();
        
      if (deviceInsertError) {
        throw new Error(`Error creating device: ${deviceInsertError.message}`);
      }
      
      deviceId = newDevice.id;
      console.log(`✅ Created new device with ID: ${deviceId}`);
    } else {
      deviceId = existingDevice.id;
      console.log(`✅ Found existing device with ID: ${deviceId}`);
    }
    
    // Step 2: Upsert daily rewards
    console.log('💾 Upserting daily rewards...');
    
    for (const dayData of dailyRewards) {
      try {
        // Check if record already exists
        const { data: existingReward, error: selectError } = await supabase
          .from('helium_rewards_daily')
          .select('id, reward_amount_dc')
          .eq('device_id', deviceId)
          .eq('transaction_date', dayData.date)
          .eq('reward_type', dayData.rewardType)
          .single();
        
        if (selectError && selectError.code !== 'PGRST116') {
          console.warn(`⚠️ Error checking existing reward for ${dayData.date}: ${selectError.message}`);
          errorCount++;
          continue;
        }
        
        const rewardData = {
          transaction_date: dayData.date,
          device_key: deviceKey,
          device_id: deviceId,
          reward_amount_dc: dayData.totalRewardsDC,
          base_poc_reward: dayData.totalBasePoc || 0,
          boosted_poc_reward: dayData.totalBoostedPoc || 0,
          total_poc_reward: dayData.totalPoc || 0,
          reward_type: dayData.rewardType,
          data_source: dayData.dataSource,
          raw_data: {
            rewardCount: dayData.rewardCount,
            pocRewardCount: dayData.pocRewardCount || 0,
            rawEntries: dayData.rawEntries,
            pocRawEntries: dayData.pocRawEntries || [],
            metadata: {
              parsedAt: metadata.parsedAt,
              filesProcessed: summary.filesProcessed
            }
          },
          updated_at: new Date().toISOString()
        };
        
        if (existingReward) {
          // Update existing record if any reward amount changed
          const dcChanged = existingReward.reward_amount_dc !== dayData.totalRewardsDC;
          const pocChanged = (existingReward.base_poc_reward || 0) !== dayData.totalBasePoc || 
                           (existingReward.boosted_poc_reward || 0) !== dayData.totalBoostedPoc ||
                           (existingReward.total_poc_reward || 0) !== dayData.totalPoc;
          
          if (dcChanged || pocChanged) {
            const { error: updateError } = await supabase
              .from('helium_rewards_daily')
              .update(rewardData)
              .eq('id', existingReward.id);
            
            if (updateError) {
              console.warn(`⚠️ Error updating reward for ${dayData.date}: ${updateError.message}`);
              errorCount++;
            } else {
              const changes = [];
              if (dcChanged) changes.push(`DC: ${dayData.totalRewardsDC} (was ${existingReward.reward_amount_dc})`);
              if (pocChanged) changes.push(`PoC: ${dayData.totalPoc} (was ${existingReward.total_poc_reward || 0})`);
              console.log(`🔄 Updated ${dayData.date}: ${changes.join(', ')}`);
              updatedCount++;
            }
          } else {
            console.log(`✅ ${dayData.date}: ${dayData.totalRewardsDC} DC, ${dayData.totalPoc} PoC (no change)`);
          }
        } else {
          // Insert new record
          const { error: insertError } = await supabase
            .from('helium_rewards_daily')
            .insert(rewardData);
          
          if (insertError) {
            console.warn(`⚠️ Error inserting reward for ${dayData.date}: ${insertError.message}`);
            errorCount++;
          } else {
            console.log(`➕ Inserted ${dayData.date}: ${dayData.totalRewardsDC} DC, ${dayData.totalPoc} PoC`);
            insertedCount++;
          }
        }
        
      } catch (dayError) {
        console.warn(`⚠️ Error processing ${dayData.date}: ${dayError.message}`);
        errorCount++;
      }
    }
    
    // Step 3: Update device tracking if this device is tracked
    console.log('🔄 Updating device tracking...');
    
    const { error: trackingUpdateError } = await supabase
      .from('tracked_devices')
      .update({
        last_scraped: new Date().toISOString()
      })
      .eq('device_key', deviceKey);
    
    if (trackingUpdateError) {
      console.warn(`⚠️ Could not update tracking info: ${trackingUpdateError.message}`);
    } else {
      console.log('✅ Updated device tracking timestamp');
    }
    
    // Step 4: Create audit log entry
    console.log('📝 Creating audit log entry...');
    
    const logEntry = {
      device_key: deviceKey,
      scrape_date: new Date().toISOString(),
      status: errorCount > 0 ? 'partial' : 'success',
      records_found: summary.rewardCount || 0,
      records_processed: insertedCount + updatedCount,
      date_range_start: summary.dateRange.start.toISOString().split('T')[0],
      date_range_end: summary.dateRange.end.toISOString().split('T')[0],
      error_message: errorCount > 0 ? `${errorCount} errors occurred during processing` : null,
      execution_time_ms: metadata.executionTime || null,
      data_source: dailyRewards[0]?.dataSource || 'mobile_verified',
      metadata: {
        insertedCount,
        updatedCount,
        errorCount,
        totalDailyEntries: dailyRewards.length,
        filesProcessed: summary.filesProcessed,
        summary: summary.dailyBreakdown
      }
    };
    
    const { error: logError } = await supabase
      .from('helium_rewards_scrape_log')
      .insert(logEntry);
    
    if (logError) {
      console.warn(`⚠️ Could not create audit log: ${logError.message}`);
    } else {
      console.log('✅ Created audit log entry');
    }
    
    // Summary
    console.log('\n📊 UPSERT SUMMARY');
    console.log('-' .repeat(80));
    console.log(`✅ Successfully processed ${dailyRewards.length} daily entries`);
    console.log(`➕ Inserted: ${insertedCount} new records`);
    console.log(`🔄 Updated: ${updatedCount} existing records`);
    if (errorCount > 0) {
      console.log(`⚠️ Errors: ${errorCount} failed operations`);
    }
    console.log(`🎯 Device ID: ${deviceId}`);
    
    return {
      success: true,
      deviceId,
      deviceKey,
      insertedCount,
      updatedCount,
      errorCount,
      totalProcessed: insertedCount + updatedCount,
      auditLogCreated: !logError
    };
    
  } catch (error) {
    console.error('❌ Database upsert failed:', error.message);
    
    // Create error audit log
    try {
      await supabase
        .from('helium_rewards_scrape_log')
        .insert({
          device_key: deviceKey,
          scrape_date: new Date().toISOString(),
          status: 'error',
          records_found: summary?.rewardCount || 0,
          records_processed: insertedCount + updatedCount,
          error_message: error.message,
          data_source: dailyRewards[0]?.dataSource || 'mobile_verified',
          metadata: {
            insertedCount,
            updatedCount,
            errorCount: errorCount + 1,
            failedAt: 'upsert_operation'
          }
        });
    } catch (logError) {
      console.warn('⚠️ Could not create error audit log:', logError.message);
    }
    
    throw error;
  }
}

// Add device to tracking list
async function addDeviceToTracking(deviceKey, deviceName = null, notes = null) {
  console.log(`➕ Adding device to tracking list: ${deviceKey.substring(0, 60)}...`);
  
  try {
    // First ensure device exists
    const { data: device, error: deviceError } = await supabase
      .from('devices')
      .upsert({
        device_key: deviceKey,
        device_name: deviceName || `Device ${deviceKey.substring(0, 12)}...`,
        description: 'Added to tracking list',
        is_active: true
      }, {
        onConflict: 'device_key'
      })
      .select('id')
      .single();
    
    if (deviceError) {
      throw new Error(`Error ensuring device exists: ${deviceError.message}`);
    }
    
    // Add to tracking
    const { data: tracking, error: trackingError } = await supabase
      .from('tracked_devices')
      .upsert({
        device_key: deviceKey,
        notes: notes,
        is_active: true
      }, {
        onConflict: 'device_key'
      })
      .select()
      .single();
    
    if (trackingError) {
      throw new Error(`Error adding to tracking: ${trackingError.message}`);
    }
    
    console.log('✅ Device added to tracking list successfully');
    return tracking;
    
  } catch (error) {
    console.error('❌ Failed to add device to tracking:', error.message);
    throw error;
  }
}

// Remove device from tracking list
async function removeDeviceFromTracking(deviceKey) {
  console.log(`➖ Removing device from tracking list: ${deviceKey.substring(0, 60)}...`);
  
  try {
    const { error } = await supabase
      .from('tracked_devices')
      .update({ is_active: false })
      .eq('device_key', deviceKey);
    
    if (error) {
      throw new Error(`Error removing from tracking: ${error.message}`);
    }
    
    console.log('✅ Device removed from tracking list successfully');
    return true;
    
  } catch (error) {
    console.error('❌ Failed to remove device from tracking:', error.message);
    throw error;
  }
}

// Get list of tracked devices
async function getTrackedDevices() {
  try {
    const { data, error } = await supabase
      .from('tracked_devices')
      .select(`
        *,
        devices!inner(device_key, device_name, is_active)
      `)
      .eq('is_active', true)
      .order('added_to_tracked_at', { ascending: false });
    
    if (error) {
      throw new Error(`Error fetching tracked devices: ${error.message}`);
    }
    
    return data || [];
    
  } catch (error) {
    console.error('❌ Failed to get tracked devices:', error.message);
    throw error;
  }
}

// convenience direct-run
if (require.main === module) {
  console.log('💾 Helium Rewards Database Operations');
  console.log('This module is designed to be used after parsing rewards data');
  console.log('Example usage:');
  console.log('  const { upsertHeliumRewards } = require("./upsertHeliumRewards");');
  console.log('  const result = await upsertHeliumRewards(parsedData);');
}

module.exports = { 
  upsertHeliumRewards, 
  addDeviceToTracking, 
  removeDeviceFromTracking, 
  getTrackedDevices 
};
