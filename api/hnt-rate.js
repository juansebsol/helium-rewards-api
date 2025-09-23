// api/hnt-rate.js
// GET /api/hnt-rate?days=30
// GET /api/hnt-rate?start=YYYY-MM-DD&end=YYYY-MM-DD
// Computes DC→HNT per day using HNT/USD from Pyth at exactly 00:00:00 UTC
// If midnight price is not available, the day is returned with status: 'missing'

const DEFAULT_PYTH_HNT_USD_FEED_ID = process.env.PYTH_HNT_USD_FEED_ID || '649fdd7ec08e8e2a20f425729854e90293dcbe2376abc47197a14da6ff339756';
const PYTH_BASE_URL = 'https://benchmarks.pyth.network/v1/updates/price';

const { calculateDateRange, errorResponse, successResponse, isValidDate } = require('./_util');

function toUtcMidnightTimestamp(dateStr) {
  const [y, m, d] = dateStr.split('-').map(Number);
  const ms = Date.UTC(y, m - 1, d, 0, 0, 0, 0);
  return Math.floor(ms / 1000);
}

function toNumberFromPyth(priceObj) {
  // priceObj: { price: "251548394", expo: -8 }
  if (!priceObj || typeof priceObj.price === 'undefined' || typeof priceObj.expo === 'undefined') return null;
  const p = Number(priceObj.price);
  const expo = Number(priceObj.expo);
  if (!isFinite(p) || !isFinite(expo)) return null;
  return p * Math.pow(10, expo); // expo is negative for USD pairs
}

module.exports = async (req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'GET') return errorResponse(res, 405, 'Method not allowed');

  try {
    const { days, start, end, feed_id } = req.query;
    const dateRange = calculateDateRange(days, start, end);
    if (dateRange.error) return errorResponse(res, 400, dateRange.error);

    // If neither provided, default to last 30 days
    let startDate = dateRange.startDate;
    let endDate = dateRange.endDate;
    if (!startDate && !endDate) {
      // last 30 days inclusive
      const todayIso = new Date().toISOString().split('T')[0];
      const startObj = new Date();
      startObj.setUTCDate(startObj.getUTCDate() - 29);
      startDate = startObj.toISOString().split('T')[0];
      endDate = todayIso;
    }

    // Validate dates again defensively
    if (!isValidDate(startDate) || !isValidDate(endDate)) {
      return errorResponse(res, 400, 'Invalid date range');
    }

    // Build array of dates inclusive
    const daysOut = [];
    const cur = new Date(startDate + 'T00:00:00.000Z');
    const endObj = new Date(endDate + 'T00:00:00.000Z');
    while (cur <= endObj) {
      daysOut.push(cur.toISOString().split('T')[0]);
      cur.setUTCDate(cur.getUTCDate() + 1);
    }

    // Fetch Pyth price at exactly 00:00:00 UTC for each day
    const feedId = (typeof feed_id === 'string' && feed_id.length > 0) ? feed_id : DEFAULT_PYTH_HNT_USD_FEED_ID;

    const results = [];
    for (const dateStr of daysOut) {
      const ts = toUtcMidnightTimestamp(dateStr);
      const url = `${PYTH_BASE_URL}/${ts}?ids=${feedId}&encoding=hex&parsed=true`;

      let hntUsd = null;
      let emaUsd = null;
      let meta = null;
      let status = 'ok';
      try {
        const resp = await fetch(url, { method: 'GET', headers: { 'accept': 'application/json' } });
        if (!resp.ok) {
          status = 'missing';
        } else {
          const data = await resp.json();
          const parsed = data?.parsed?.[0];
          const priceNum = toNumberFromPyth(parsed?.price);
          const emaNum = toNumberFromPyth(parsed?.ema_price);
          hntUsd = (priceNum && priceNum > 0) ? priceNum : null;
          emaUsd = (emaNum && emaNum > 0) ? emaNum : null;
          meta = parsed?.metadata || null;
          if (!hntUsd) status = 'missing'; // strictly require midnight price
        }
      } catch (_) {
        status = 'missing';
      }

      // Compute DC→HNT only when we have midnight price
      let dcToHnt = null;
      if (status === 'ok' && hntUsd && hntUsd > 0) {
        dcToHnt = 0.00001 / hntUsd; // 1 DC = $0.00001
      }

      results.push({
        date: dateStr,
        status,
        hnt_usd_price: hntUsd,
        ema_usd_price: emaUsd,
        dc_to_hnt_rate: dcToHnt,
        pyth: {
          feed_id: feedId,
          url,
          metadata: meta
        }
      });
    }

    return successResponse(res, {
      range: { start: startDate, end: endDate, days: days ? Number(days) : null },
      count: results.length,
      data: results
    });

  } catch (err) {
    return errorResponse(res, 500, 'Failed to fetch HNT rate', { message: err.message });
  }
};
