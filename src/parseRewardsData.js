// src/parseRewardsData.js
// Rewards data parser for Helium device rewards
// - Processes scraped reward data
// - Aggregates daily totals
// - Validates and enriches reward information
// - Prepares data for database storage

const { formatDC, formatNumber } = require('./scrapeHeliumRewards');

// Parse and aggregate rewards data
function parseRewardsData(scrapedData) {
  if (!scrapedData || !scrapedData.rewards) {
    throw new Error('Invalid scraped data provided');
  }

  const { deviceKey, rewards, summary } = scrapedData;
  
  console.log('📊 PARSING REWARDS DATA');
  console.log('=' .repeat(80));
  console.log(`🎯 Device: ${deviceKey.substring(0, 60)}...`);
  console.log(`📝 Processing ${rewards.length} reward entries`);
  
  // Group rewards by date
  const dailyRewards = new Map();
  
  rewards.forEach(reward => {
    const dateKey = reward.date.toISOString().split('T')[0];
    
    if (!dailyRewards.has(dateKey)) {
      dailyRewards.set(dateKey, {
        date: dateKey,
        deviceKey: deviceKey,
        totalRewardsDC: 0,
        rewardCount: 0,
        rewardType: reward.rewardType || 'mobile_verified',
        dataSource: reward.dataSource,
        rawEntries: []
      });
    }
    
    const dayData = dailyRewards.get(dateKey);
    dayData.totalRewardsDC += reward.rewardAmount;
    dayData.rewardCount += 1;
    dayData.rawEntries.push({
      timestamp: reward.timestamp,
      rewardAmount: reward.rewardAmount,
      file: reward.file
    });
  });
  
  // Convert to array and sort by date
  const dailyRewardsArray = Array.from(dailyRewards.values())
    .sort((a, b) => new Date(a.date) - new Date(b.date));
  
  console.log(`📅 Aggregated into ${dailyRewardsArray.length} daily entries`);
  
  // Calculate enhanced statistics
  const totalDC = dailyRewardsArray.reduce((sum, day) => sum + day.totalRewardsDC, 0);
  const activeDays = dailyRewardsArray.length;
  const averageDailyDC = activeDays > 0 ? totalDC / activeDays : 0;
  const maxDailyDC = Math.max(...dailyRewardsArray.map(d => d.totalRewardsDC));
  const minDailyDC = Math.min(...dailyRewardsArray.map(d => d.totalRewardsDC));
  
  // Performance metrics
  const totalDays = Math.ceil((summary.dateRange.end - summary.dateRange.start) / (1000 * 60 * 60 * 24));
  const uptimePercentage = (activeDays / totalDays) * 100;
  
  const enhancedSummary = {
    ...summary,
    dailyBreakdown: {
      activeDays,
      totalDays,
      uptimePercentage: Math.round(uptimePercentage * 100) / 100,
      averageDailyDC: Math.round(averageDailyDC),
      maxDailyDC,
      minDailyDC,
      totalDC
    }
  };
  
  console.log('📈 DAILY BREAKDOWN STATISTICS');
  console.log('-' .repeat(80));
  console.log(`📅 Active Days: ${activeDays}/${totalDays} (${uptimePercentage.toFixed(1)}% uptime)`);
  console.log(`💰 Total DC: ${formatNumber(totalDC)} (${formatDC(totalDC)})`);
  console.log(`📊 Average Daily: ${formatNumber(averageDailyDC)} (${formatDC(averageDailyDC)})`);
  console.log(`📈 Max Daily: ${formatNumber(maxDailyDC)} (${formatDC(maxDailyDC)})`);
  console.log(`📉 Min Daily: ${formatNumber(minDailyDC)} (${formatDC(minDailyDC)})`);
  
  // Show top performing days
  const topDays = dailyRewardsArray
    .sort((a, b) => b.totalRewardsDC - a.totalRewardsDC)
    .slice(0, 5);
    
  console.log('\n🏆 TOP PERFORMING DAYS:');
  topDays.forEach((day, i) => {
    console.log(`   ${i + 1}. ${day.date}: ${formatDC(day.totalRewardsDC)} (${day.rewardCount} entries)`);
  });
  
  console.log('✅ Parsing completed successfully');
  
  return {
    deviceKey,
    dailyRewards: dailyRewardsArray,
    summary: enhancedSummary,
    metadata: {
      parsedAt: new Date(),
      totalEntries: rewards.length,
      dailyEntries: dailyRewardsArray.length,
      dateRange: summary.dateRange
    }
  };
}

// Validate reward data before database insertion
function validateRewardData(parsedData) {
  const errors = [];
  
  if (!parsedData.deviceKey) {
    errors.push('Missing device key');
  }
  
  if (!parsedData.dailyRewards || !Array.isArray(parsedData.dailyRewards)) {
    errors.push('Missing or invalid daily rewards array');
  }
  
  parsedData.dailyRewards?.forEach((day, index) => {
    if (!day.date) {
      errors.push(`Missing date for entry ${index}`);
    }
    
    if (typeof day.totalRewardsDC !== 'number' || day.totalRewardsDC < 0) {
      errors.push(`Invalid reward amount for ${day.date}: ${day.totalRewardsDC}`);
    }
    
    if (!day.rewardType) {
      errors.push(`Missing reward type for ${day.date}`);
    }
  });
  
  if (errors.length > 0) {
    throw new Error(`Validation failed: ${errors.join(', ')}`);
  }
  
  console.log('✅ Reward data validation passed');
  return true;
}

// Enrich reward data with additional calculated fields
function enrichRewardData(parsedData) {
  const enrichedDailyRewards = parsedData.dailyRewards.map(day => ({
    ...day,
    rewardAmountFormatted: formatDC(day.totalRewardsDC),
    dayOfWeek: new Date(day.date).toLocaleDateString('en-US', { weekday: 'short' }),
    weekNumber: getWeekNumber(new Date(day.date)),
    monthYear: new Date(day.date).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
  }));
  
  return {
    ...parsedData,
    dailyRewards: enrichedDailyRewards
  };
}

// Helper function to get week number
function getWeekNumber(date) {
  const firstDayOfYear = new Date(date.getFullYear(), 0, 1);
  const pastDaysOfYear = (date - firstDayOfYear) / 86400000;
  return Math.ceil((pastDaysOfYear + firstDayOfYear.getDay() + 1) / 7);
}

// convenience direct-run
if (require.main === module) {
  // This would typically be called after scraping
  console.log('📊 Rewards Data Parser');
  console.log('This module is designed to be used after scraping data');
  console.log('Example usage:');
  console.log('  const { parseRewardsData } = require("./parseRewardsData");');
  console.log('  const parsed = parseRewardsData(scrapedData);');
}

module.exports = { 
  parseRewardsData, 
  validateRewardData, 
  enrichRewardData 
};
