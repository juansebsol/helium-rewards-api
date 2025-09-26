// api/helium-rewards.js
// GET /api/helium-rewards?device_key=<key>&days=7
// GET /api/helium-rewards?device_key=<key>&start=YYYY-MM-DD&end=YYYY-MM-DD
// GET /api/helium-rewards?device_key=<key> -> all data for that device

const { supabase } = require('./_supabase');
const { 
  calculateDateRange, 
  isValidDeviceKey, 
  formatRewardAmount,
  errorResponse,
  successResponse 
} = require('./_util');

module.exports = async (req, res) => {
  // Basic CORS (public read); tighten if needed
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'GET') return errorResponse(res, 405, 'Method not allowed');

  const { device_key, days, start, end, reward_type } = req.query;

  // Device key is required
  if (!device_key) {
    return errorResponse(res, 400, 'device_key parameter is required', {
      example: '?device_key=1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi&days=30'
    });
  }

  // Validate device key format
  if (!isValidDeviceKey(device_key)) {
    return errorResponse(res, 400, 'Invalid device_key format', {
      format: 'Expected base58check encoded Helium device key (100+ characters)'
    });
  }

  // Calculate date range
  const dateRange = calculateDateRange(days, start, end);
  if (dateRange.error) {
    return errorResponse(res, 400, dateRange.error);
  }

  try {
    // First get the device ID
    const { data: device, error: deviceError } = await supabase
      .from('devices')
      .select('id, device_key, device_name, is_active')
      .eq('device_key', device_key)
      .single();
      
    if (deviceError) {
      if (deviceError.code === 'PGRST116') {
        return errorResponse(res, 404, 'Device not found', {
          device_key: device_key.substring(0, 60) + '...',
          suggestion: 'Make sure the device has been scraped at least once'
        });
      }
      
      console.error('Device lookup error:', deviceError);
      return errorResponse(res, 500, 'Database error while looking up device');
    }
    
    // Build query for rewards
    let query = supabase
      .from('helium_rewards_daily')
      .select(`
        *,
        devices!inner(device_key, device_name, is_active)
      `)
      .eq('device_id', device.id)
      .order('transaction_date', { ascending: false });

    // Apply date range filter
    if (dateRange.startDate && dateRange.endDate) {
      query = query
        .gte('transaction_date', dateRange.startDate)
        .lte('transaction_date', dateRange.endDate);
    }

    // Apply reward type filter
    if (reward_type && reward_type !== 'all') {
      const validTypes = ['mobile_verified', 'iot_verified'];
      if (!validTypes.includes(reward_type)) {
        return errorResponse(res, 400, 'Invalid reward_type', {
          valid_types: validTypes,
          received: reward_type
        });
      }
      query = query.eq('reward_type', reward_type);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Rewards query error:', error);
      return errorResponse(res, 500, 'Database error while fetching rewards');
    }

    // Calculate summary statistics
    let summary = null;
    if (data && data.length > 0) {
      const totalDC = data.reduce((sum, record) => sum + parseInt(record.reward_amount_dc), 0);
      const totalBasePoc = data.reduce((sum, record) => sum + parseInt(record.base_poc_reward || 0), 0);
      const totalBoostedPoc = data.reduce((sum, record) => sum + parseInt(record.boosted_poc_reward || 0), 0);
      const totalPoc = data.reduce((sum, record) => sum + parseInt(record.total_poc_reward || 0), 0);
      const rewardCounts = data.reduce((sum, record) => sum + (record.raw_data?.rewardCount || 1), 0);
      const pocRewardCounts = data.reduce((sum, record) => sum + (record.raw_data?.pocRewardCount || 0), 0);
      
      // Group by reward type for breakdown
      const rewardTypeBreakdown = data.reduce((acc, record) => {
        const type = record.reward_type;
        if (!acc[type]) {
          acc[type] = { count: 0, total_dc: 0, total_poc: 0 };
        }
        acc[type].count += 1;
        acc[type].total_dc += parseInt(record.reward_amount_dc);
        acc[type].total_poc += parseInt(record.total_poc_reward || 0);
        return acc;
      }, {});

      // Calculate daily statistics
      const dailyAmounts = data.map(record => parseInt(record.reward_amount_dc));
      const dailyPocAmounts = data.map(record => parseInt(record.total_poc_reward || 0));
      const maxDaily = Math.max(...dailyAmounts);
      const minDaily = Math.min(...dailyAmounts);
      const averageDaily = totalDC / data.length;
      const maxDailyPoc = dailyPocAmounts.length > 0 ? Math.max(...dailyPocAmounts) : 0;
      const minDailyPoc = dailyPocAmounts.length > 0 ? Math.min(...dailyPocAmounts) : 0;
      const averageDailyPoc = data.length > 0 ? totalPoc / data.length : 0;

      // Calculate uptime (active days vs total days in range)
      let uptimePercentage = null;
      if (dateRange.startDate && dateRange.endDate) {
        const startDate = new Date(dateRange.startDate);
        const endDate = new Date(dateRange.endDate);
        const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;
        const activeDays = data.length;
        uptimePercentage = (activeDays / totalDays) * 100;
      }

      summary = {
        total_dc_rewards: totalDC,
        total_dc_formatted: formatRewardAmount(totalDC),
        total_base_poc_rewards: totalBasePoc,
        total_boosted_poc_rewards: totalBoostedPoc,
        total_poc_rewards: totalPoc,
        total_reward_entries: rewardCounts,
        total_poc_reward_entries: pocRewardCounts,
        active_days: data.length,
        average_daily_dc: Math.round(averageDaily),
        average_daily_formatted: formatRewardAmount(averageDaily),
        max_daily_dc: maxDaily,
        max_daily_formatted: formatRewardAmount(maxDaily),
        min_daily_dc: minDaily,
        min_daily_formatted: formatRewardAmount(minDaily),
        average_daily_poc: Math.round(averageDailyPoc),
        max_daily_poc: maxDailyPoc,
        min_daily_poc: minDailyPoc,
        uptime_percentage: uptimePercentage ? Math.round(uptimePercentage * 100) / 100 : null,
        reward_type_breakdown: rewardTypeBreakdown
      };
    }

    // Format response data
    const formattedData = (data || []).map((record) => ({
      transaction_date: record.transaction_date,
      device_key: record.devices?.device_key || record.device_key,
      device_name: record.devices?.device_name,
      reward_amount_dc: parseInt(record.reward_amount_dc),
      reward_amount_formatted: formatRewardAmount(parseInt(record.reward_amount_dc)),
      base_poc_reward: parseInt(record.base_poc_reward || 0),
      boosted_poc_reward: parseInt(record.boosted_poc_reward || 0),
      total_poc_reward: parseInt(record.total_poc_reward || 0),
      reward_type: record.reward_type,
      data_source: record.data_source,
      reward_entries: record.raw_data?.rewardCount || 1,
      poc_reward_entries: record.raw_data?.pocRewardCount || 0,
      day_of_week: new Date(record.transaction_date).toLocaleDateString('en-US', { weekday: 'short' }),
      created_at: record.created_at,
      updated_at: record.updated_at
    }));

    return successResponse(res, {
      device_key: device_key.substring(0, 60) + '...',
      device_name: device.device_name,
      device_active: device.is_active,
      query_parameters: {
        days: days ? parseInt(days) : null,
        start_date: dateRange.startDate,
        end_date: dateRange.endDate,
        reward_type: reward_type || 'all'
      },
      count: formattedData.length,
      summary,
      data: formattedData
    });

  } catch (error) {
    console.error('API Error:', error);
    return errorResponse(res, 500, 'Internal server error', {
      message: error.message
    });
  }
};
