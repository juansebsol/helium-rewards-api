// scripts/trigger-github-action.js
// Utility script to trigger GitHub Actions from command line
// Usage: node scripts/trigger-github-action.js <DEVICE_KEY> [days]

require('dotenv').config();

async function triggerGitHubAction(deviceKey, days = 30) {
  if (!process.env.GITHUB_TOKEN) {
    throw new Error('GITHUB_TOKEN environment variable is required');
  }

  if (!process.env.GITHUB_REPOSITORY) {
    throw new Error('GITHUB_REPOSITORY environment variable is required (format: username/repo)');
  }

  console.log('🚀 Triggering GitHub Action...');
  console.log(`📱 Device: ${deviceKey.substring(0, 60)}...`);
  console.log(`📅 Days: ${days}`);
  console.log(`📦 Repository: ${process.env.GITHUB_REPOSITORY}`);

  const payload = {
    event_type: 'helium-rewards-scrape',
    client_payload: {
      device_key: deviceKey,
      days: parseInt(days),
      triggered_by: 'script',
      timestamp: new Date().toISOString()
    }
  };

  try {
    const response = await fetch(
      `https://api.github.com/repos/${process.env.GITHUB_REPOSITORY}/dispatches`,
      {
        method: 'POST',
        headers: {
          'Authorization': `token ${process.env.GITHUB_TOKEN}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
          'User-Agent': 'helium-rewards-api-script/1.0'
        },
        body: JSON.stringify(payload)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`GitHub API error (${response.status}): ${errorText}`);
    }

    console.log('✅ GitHub Action triggered successfully!');
    console.log('💡 Check the Actions tab in your GitHub repository to see the progress');
    console.log(`🔗 https://github.com/${process.env.GITHUB_REPOSITORY}/actions`);

    return {
      success: true,
      payload: payload.client_payload
    };

  } catch (error) {
    console.error('❌ Failed to trigger GitHub Action:', error.message);
    throw error;
  }
}

// Command line interface
if (require.main === module) {
  const deviceKey = process.argv[2];
  const days = process.argv[3];

  if (!deviceKey) {
    console.error('❌ Device key is required');
    console.error('');
    console.error('Usage:');
    console.error('  node scripts/trigger-github-action.js <DEVICE_KEY> [days]');
    console.error('');
    console.error('Examples:');
    console.error('  node scripts/trigger-github-action.js 1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi');
    console.error('  node scripts/trigger-github-action.js 1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi 7');
    process.exit(1);
  }

  triggerGitHubAction(deviceKey, days)
    .then(() => {
      console.log('🎉 Script completed successfully');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Script failed:', error.message);
      process.exit(1);
    });
}

module.exports = { triggerGitHubAction };
