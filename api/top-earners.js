// api/top-earners.js
// GET /api/top-earners
// Returns latest top earners snapshot for DC rewards.

const { supabase } = require('./_supabase');

module.exports = async (req, res) => {
  // Basic CORS (public read); tighten if needed
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'GET') return res.status(405).json({ success: false, error: 'Method not allowed' });

  try {
    const windowDays = String(req.query.window_days || req.query.window || '').trim(); // "1" | "7" | "30" | ""
    const limit = Math.max(1, Math.min(100, parseInt(req.query.limit || '10', 10) || 10));

    const { data, error } = await supabase
      .from('top_earners_snapshots')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) {
      return res.status(500).json({ success: false, error: error.message });
    }

    if (!data) {
      return res.status(404).json({ success: false, error: 'No top earner snapshots found' });
    }

    const results = data.results || {};
    const windows = Object.keys(results).sort((a, b) => Number(a) - Number(b));

    let payloadResults;
    if (windowDays) {
      if (!results[windowDays]) {
        return res.status(400).json({
          success: false,
          error: `Invalid window_days. Available: ${windows.join(', ')}`,
        });
      }
      payloadResults = { [windowDays]: (results[windowDays] || []).slice(0, limit) };
    } else {
      payloadResults = {};
      for (const w of windows) payloadResults[w] = (results[w] || []).slice(0, limit);
    }

    return res.status(200).json({
      success: true,
      timestamp: new Date().toISOString(),
      snapshot: {
        id: data.id,
        created_at: data.created_at,
        source_prefix: data.source_prefix,
        lookback_days: data.lookback_days,
        windows_days: data.windows_days,
        top_n: data.top_n,
      },
      results: payloadResults,
      meta: data.meta || null,
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message || 'Server error' });
  }
};

