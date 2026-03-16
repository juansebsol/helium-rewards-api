// api/top-earners.js
// GET /api/top-earners?window_days=1|7|30&page=1&per_page=10
// Returns latest top earners snapshot, paginated for the requested window.

const { supabase } = require('./_supabase');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'GET') return res.status(405).json({ success: false, error: 'Method not allowed' });

  try {
    const windowDaysRaw = String(req.query.window_days || req.query.window || '1').trim();
    const windowDays = parseInt(windowDaysRaw, 10);
    if (![1, 7, 30].includes(windowDays)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid window_days. Use one of: 1, 7, 30',
      });
    }

    const page = Math.max(1, parseInt(req.query.page || '1', 10) || 1);
    const perPage = Math.max(1, Math.min(100, parseInt(req.query.per_page || req.query.limit || '10', 10) || 10));

    // Get latest snapshot meta (no big JSON)
    const { data: snap, error: snapError } = await supabase
      .from('top_earners_snapshots')
      .select('id, created_at, source_prefix, lookback_days, windows_days, top_n, meta')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (snapError) {
      return res.status(500).json({ success: false, error: snapError.message });
    }

    if (!snap) {
      return res.status(404).json({ success: false, error: 'No top earner snapshots found' });
    }

    const from = (page - 1) * perPage;
    const to = from + perPage - 1;

    const { data: rows, error: rowsError, count } = await supabase
      .from('top_earners_flat')
      .select('rank, device_key, total_dc, total_hnt', { count: 'exact' })
      .eq('snapshot_id', snap.id)
      .eq('window_days', windowDays)
      .order('rank', { ascending: true })
      .range(from, to);

    if (rowsError) {
      return res.status(500).json({ success: false, error: rowsError.message });
    }

    const total = typeof count === 'number' ? count : (rows || []).length;

    return res.status(200).json({
      success: true,
      timestamp: new Date().toISOString(),
      snapshot: {
        id: snap.id,
        created_at: snap.created_at,
        source_prefix: snap.source_prefix,
        lookback_days: snap.lookback_days,
        windows_days: snap.windows_days,
        top_n: snap.top_n,
      },
      window_days: windowDays,
      items: rows || [],
      total,
      page,
      per_page: perPage,
      meta: snap.meta || null,
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err?.message || 'Server error' });
  }
};

