// api/trigger-scrape.js
// POST /api/trigger-scrape
// Triggers the Helium rewards scraper for a specific device key
// This endpoint triggers GitHub Actions instead of scraping directly

module.exports = async (req, res) => {
  // Basic CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { device_key, days, start_date, end_date } = req.body;

    // Validate required parameters
    if (!device_key) {
      return res.status(400).json({ 
        error: 'device_key is required',
        example: { 
          device_key: '1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi',
          days: 30
        }
      });
    }

    // Validate device key format (basic check)
    if (typeof device_key !== 'string' || device_key.length < 50) {
      return res.status(400).json({
        error: 'Invalid device_key format',
        details: 'Expected base58check encoded Helium device key (100+ characters)'
      });
    }

    console.log(`🚀 API: Triggering GitHub Action for device: ${device_key.substring(0, 60)}...`);

    // Check if GitHub token is available
    if (!process.env.GITHUB_TOKEN) {
      return res.status(500).json({
        success: false,
        error: 'GitHub token not configured',
        details: 'GITHUB_TOKEN environment variable is required'
      });
    }

    // Prepare client payload
    const clientPayload = {
      device_key: device_key,
      triggered_by: 'api',
      timestamp: new Date().toISOString()
    };

    // Add optional date range parameters
    if (days) {
      clientPayload.days = parseInt(days);
    }
    if (start_date && end_date) {
      // Validate date format
      const startDate = new Date(start_date);
      const endDate = new Date(end_date);
      
      if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
        return res.status(400).json({
          error: 'Invalid date format',
          details: 'Use YYYY-MM-DD format for start_date and end_date'
        });
      }
      
      if (startDate > endDate) {
        return res.status(400).json({
          error: 'Invalid date range',
          details: 'start_date must be before end_date'
        });
      }
      
      clientPayload.start_date = start_date;
      clientPayload.end_date = end_date;
    }

    // Trigger GitHub Action workflow
    const workflowResponse = await fetch(
      `https://api.github.com/repos/${process.env.GITHUB_REPOSITORY || 'your-username/helium-rewards-api'}/dispatches`,
      {
        method: 'POST',
        headers: {
          'Authorization': `token ${process.env.GITHUB_TOKEN}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
          'User-Agent': 'helium-rewards-api/1.0'
        },
        body: JSON.stringify({
          event_type: 'helium-rewards-scrape',
          client_payload: clientPayload
        })
      }
    );

    if (!workflowResponse.ok) {
      const errorText = await workflowResponse.text();
      console.error('GitHub API error:', workflowResponse.status, errorText);
      
      // Parse GitHub API error if possible
      let githubError = errorText;
      try {
        const errorJson = JSON.parse(errorText);
        githubError = errorJson.message || errorText;
      } catch (e) {
        // Keep original error text
      }
      
      return res.status(500).json({
        success: false,
        error: 'Failed to trigger GitHub Action',
        details: {
          github_status: workflowResponse.status,
          github_error: githubError,
          repository: process.env.GITHUB_REPOSITORY || 'not-configured'
        }
      });
    }

    console.log(`✅ GitHub Action workflow triggered successfully for device: ${device_key.substring(0, 60)}...`);

    // Return success response
    res.status(200).json({
      success: true,
      message: 'GitHub Action workflow triggered successfully',
      details: {
        device_key: device_key.substring(0, 60) + '...',
        workflow: 'helium-rewards-scraper',
        event_type: 'helium-rewards-scrape',
        status: 'queued',
        parameters: {
          days: clientPayload.days || null,
          start_date: clientPayload.start_date || null,
          end_date: clientPayload.end_date || null
        },
        triggered_at: clientPayload.timestamp,
        note: 'Check GitHub Actions tab for progress and results'
      }
    });

  } catch (error) {
    console.error('❌ API Error:', error);
    
    res.status(500).json({ 
      success: false,
      error: 'Failed to trigger GitHub Action',
      details: {
        device_key: req.body?.device_key?.substring(0, 60) + '...' || 'unknown',
        error_message: error.message,
        timestamp: new Date().toISOString()
      }
    });
  }
};
