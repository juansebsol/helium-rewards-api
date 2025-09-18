// api/manage-devices.js
// GET /api/manage-devices - List tracked devices
// POST /api/manage-devices - Add device to tracking
// DELETE /api/manage-devices - Remove device from tracking

const { supabase } = require('./_supabase');
const { 
  isValidDeviceKey,
  errorResponse,
  successResponse 
} = require('./_util');

module.exports = async (req, res) => {
  // Basic CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();

  try {
    switch (req.method) {
      case 'GET':
        return await handleListDevices(req, res);
      case 'POST':
        return await handleAddDevice(req, res);
      case 'DELETE':
        return await handleRemoveDevice(req, res);
      default:
        return errorResponse(res, 405, 'Method not allowed');
    }
  } catch (error) {
    console.error('Device management API error:', error);
    return errorResponse(res, 500, 'Internal server error', {
      message: error.message
    });
  }
};

// GET - List tracked devices
async function handleListDevices(req, res) {
  const { active_only = 'true', include_stats = 'false' } = req.query;
  
  try {
    let query = supabase
      .from('tracked_devices')
      .select(`
        *,
        devices!inner(device_key, device_name, description, is_active, created_at)
      `)
      .order('added_to_tracked_at', { ascending: false });

    // Filter by active status
    if (active_only === 'true') {
      query = query.eq('is_active', true);
    }

    const { data, error } = await query;

    if (error) {
      console.error('List devices error:', error);
      return errorResponse(res, 500, 'Database error while fetching devices');
    }

    // Optionally include reward statistics
    let devicesWithStats = data || [];
    
    if (include_stats === 'true' && data && data.length > 0) {
      devicesWithStats = await Promise.all(
        data.map(async (device) => {
          try {
            // Get recent reward stats (last 30 days)
            const thirtyDaysAgo = new Date();
            thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
            
            const { data: rewardStats, error: statsError } = await supabase
              .from('helium_rewards_daily')
              .select('reward_amount_dc, transaction_date')
              .eq('device_key', device.device_key)
              .gte('transaction_date', thirtyDaysAgo.toISOString().split('T')[0])
              .order('transaction_date', { ascending: false });

            if (statsError) {
              console.warn(`Stats error for device ${device.device_key}:`, statsError);
              return { ...device, stats: null };
            }

            const totalDC = rewardStats?.reduce((sum, r) => sum + parseInt(r.reward_amount_dc), 0) || 0;
            const activeDays = rewardStats?.length || 0;
            const averageDaily = activeDays > 0 ? totalDC / activeDays : 0;
            
            return {
              ...device,
              stats: {
                last_30_days: {
                  total_dc: totalDC,
                  active_days: activeDays,
                  average_daily: Math.round(averageDaily),
                  last_reward_date: rewardStats?.[0]?.transaction_date || null
                }
              }
            };
          } catch (statsError) {
            console.warn(`Failed to get stats for device ${device.device_key}:`, statsError);
            return { ...device, stats: null };
          }
        })
      );
    }

    // Format response
    const formattedDevices = devicesWithStats.map(device => ({
      id: device.id,
      device_key: device.device_key.substring(0, 60) + '...',
      device_key_full: device.device_key, // Include full key for management operations
      device_name: device.devices.device_name,
      description: device.devices.description,
      added_to_tracked_at: device.added_to_tracked_at,
      last_scraped: device.last_scraped,
      is_active: device.is_active,
      device_active: device.devices.is_active,
      notes: device.notes,
      scrape_frequency: device.scrape_frequency || '1 day',
      stats: device.stats || null
    }));

    return successResponse(res, {
      count: formattedDevices.length,
      filters: {
        active_only: active_only === 'true',
        include_stats: include_stats === 'true'
      },
      devices: formattedDevices
    });

  } catch (error) {
    console.error('List devices error:', error);
    return errorResponse(res, 500, 'Failed to fetch tracked devices');
  }
}

// POST - Add device to tracking
async function handleAddDevice(req, res) {
  const { device_key, device_name, notes, description } = req.body;

  // Validate required fields
  if (!device_key) {
    return errorResponse(res, 400, 'device_key is required', {
      example: {
        device_key: '1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi',
        device_name: 'My Helium Hotspot',
        notes: 'Added via API'
      }
    });
  }

  // Validate device key format
  if (!isValidDeviceKey(device_key)) {
    return errorResponse(res, 400, 'Invalid device_key format', {
      format: 'Expected base58check encoded Helium device key (100+ characters)'
    });
  }

  try {
    // First ensure device exists in devices table
    const { data: device, error: deviceError } = await supabase
      .from('devices')
      .upsert({
        device_key: device_key,
        device_name: device_name || `Device ${device_key.substring(0, 12)}...`,
        description: description || 'Added via API',
        is_active: true
      }, {
        onConflict: 'device_key'
      })
      .select('id, device_key, device_name')
      .single();

    if (deviceError) {
      console.error('Device upsert error:', deviceError);
      return errorResponse(res, 500, 'Failed to create/update device record');
    }

    // Add to tracking list
    const { data: tracking, error: trackingError } = await supabase
      .from('tracked_devices')
      .upsert({
        device_key: device_key,
        notes: notes,
        is_active: true
      }, {
        onConflict: 'device_key'
      })
      .select(`
        *,
        devices!inner(device_key, device_name, description)
      `)
      .single();

    if (trackingError) {
      console.error('Tracking upsert error:', trackingError);
      return errorResponse(res, 500, 'Failed to add device to tracking list');
    }

    return successResponse(res, {
      device: {
        id: tracking.id,
        device_key: device_key.substring(0, 60) + '...',
        device_name: tracking.devices.device_name,
        description: tracking.devices.description,
        added_to_tracked_at: tracking.added_to_tracked_at,
        is_active: tracking.is_active,
        notes: tracking.notes
      }
    }, 'Device added to tracking list successfully');

  } catch (error) {
    console.error('Add device error:', error);
    return errorResponse(res, 500, 'Failed to add device to tracking');
  }
}

// DELETE - Remove device from tracking
async function handleRemoveDevice(req, res) {
  const { device_key } = req.body;

  // Validate required fields
  if (!device_key) {
    return errorResponse(res, 400, 'device_key is required in request body');
  }

  // Validate device key format
  if (!isValidDeviceKey(device_key)) {
    return errorResponse(res, 400, 'Invalid device_key format');
  }

  try {
    // Check if device is currently tracked
    const { data: existingTracking, error: checkError } = await supabase
      .from('tracked_devices')
      .select('id, device_key, is_active')
      .eq('device_key', device_key)
      .single();

    if (checkError) {
      if (checkError.code === 'PGRST116') {
        return errorResponse(res, 404, 'Device not found in tracking list');
      }
      console.error('Check tracking error:', checkError);
      return errorResponse(res, 500, 'Database error while checking device');
    }

    if (!existingTracking.is_active) {
      return errorResponse(res, 400, 'Device is already inactive in tracking list');
    }

    // Deactivate tracking (soft delete)
    const { error: updateError } = await supabase
      .from('tracked_devices')
      .update({ 
        is_active: false,
        notes: (existingTracking.notes || '') + ` [Removed via API on ${new Date().toISOString()}]`
      })
      .eq('device_key', device_key);

    if (updateError) {
      console.error('Remove tracking error:', updateError);
      return errorResponse(res, 500, 'Failed to remove device from tracking');
    }

    return successResponse(res, {
      device_key: device_key.substring(0, 60) + '...',
      removed_at: new Date().toISOString()
    }, 'Device removed from tracking list successfully');

  } catch (error) {
    console.error('Remove device error:', error);
    return errorResponse(res, 500, 'Failed to remove device from tracking');
  }
}
