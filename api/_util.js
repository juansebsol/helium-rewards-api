// api/_util.js
// Utility functions for API endpoints

// Get ISO date string (YYYY-MM-DD)
function isoDate(date = new Date()) {
  return date.toISOString().split('T')[0];
}

// Calculate start date from days parameter
function startDateFromDays(days) {
  const daysNum = parseInt(days);
  if (isNaN(daysNum) || daysNum < 1 || daysNum > 365) {
    return null;
  }
  
  const date = new Date();
  date.setDate(date.getDate() - daysNum + 1);
  return isoDate(date);
}

// Validate device key format
function isValidDeviceKey(deviceKey) {
  if (!deviceKey || typeof deviceKey !== 'string') {
    return false;
  }
  
  // Basic validation for Helium device key format
  // Should be base58check encoded, typically 100+ characters
  return deviceKey.length > 50 && /^[1-9A-HJ-NP-Za-km-z]+$/.test(deviceKey);
}

// Format reward amount for display
function formatRewardAmount(amount) {
  if (typeof amount !== 'number') return '0';
  
  if (amount >= 1000000000) {
    return `${(amount / 1000000000).toFixed(2)}B`;
  } else if (amount >= 1000000) {
    return `${(amount / 1000000).toFixed(2)}M`;
  } else if (amount >= 1000) {
    return `${(amount / 1000).toFixed(2)}K`;
  } else {
    return amount.toString();
  }
}

// Validate date string (YYYY-MM-DD)
function isValidDate(dateString) {
  if (!dateString) return false;
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateString)) return false;
  
  const date = new Date(dateString);
  return date instanceof Date && !isNaN(date) && date.toISOString().split('T')[0] === dateString;
}

// Calculate date range
function calculateDateRange(days, start, end) {
  let startDate = null;
  let endDate = null;
  
  if (days) {
    startDate = startDateFromDays(days);
    if (!startDate) return { error: 'Invalid days parameter (1-365)' };
    endDate = isoDate();
  } else if (start && end) {
    if (!isValidDate(start)) return { error: 'Invalid start date format (YYYY-MM-DD)' };
    if (!isValidDate(end)) return { error: 'Invalid end date format (YYYY-MM-DD)' };
    
    const startDateObj = new Date(start);
    const endDateObj = new Date(end);
    
    if (startDateObj > endDateObj) {
      return { error: 'Start date must be before end date' };
    }
    
    const daysDiff = Math.ceil((endDateObj - startDateObj) / (1000 * 60 * 60 * 24));
    if (daysDiff > 365) {
      return { error: 'Date range cannot exceed 365 days' };
    }
    
    startDate = start;
    endDate = end;
  }
  
  return { startDate, endDate };
}

// Standard error response
function errorResponse(res, status, message, details = null) {
  const response = {
    error: message,
    timestamp: new Date().toISOString()
  };
  
  if (details) {
    response.details = details;
  }
  
  return res.status(status).json(response);
}

// Standard success response
function successResponse(res, data, message = null) {
  const response = {
    success: true,
    timestamp: new Date().toISOString(),
    ...data
  };
  
  if (message) {
    response.message = message;
  }
  
  return res.status(200).json(response);
}

module.exports = {
  isoDate,
  startDateFromDays,
  isValidDeviceKey,
  formatRewardAmount,
  isValidDate,
  calculateDateRange,
  errorResponse,
  successResponse
};
