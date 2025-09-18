# 🚀 Helium Rewards API

> **Helium network device rewards scraper and API - extracts reward data from AWS S3 and provides RESTful API access**

This project scrapes Helium device reward data from AWS S3 protobuf files, processes and aggregates the data, and provides a comprehensive REST API for querying rewards information. Built following the proven architecture pattern from the XNET device offload scraper.

## 📋 What This Does

**Transform raw Helium blockchain data into actionable insights:**

- **🔍 AWS S3 Data Extraction**: Automatically fetches and processes compressed protobuf files from Helium's foundation data bucket
- **📊 Reward Aggregation**: Calculates daily DC (Data Credit) rewards with comprehensive statistics
- **🗄️ Historical Tracking**: Maintains complete audit trails and historical data
- **🔄 Automated Scheduling**: Daily automated scraping via GitHub Actions
- **📡 REST API**: Query rewards data with flexible filtering and date ranges
- **🎯 Device Management**: Easy API to add/remove devices from tracking list

## 🏗️ Architecture Overview

### **Core Components**
```
src/
├── scrapeHeliumRewards.js       # AWS S3 scraper (protobuf processing)
├── parseRewardsData.js          # Data aggregation and validation
├── upsertHeliumRewards.js       # Database operations (Supabase)
├── runRewardsScrape.js          # Orchestration and error handling
├── scheduledRewardsScrape.js    # Multi-device batch processing
└── supabase.js                  # Database client configuration

api/
├── helium-rewards.js            # Query rewards data endpoint
├── manage-devices.js            # Device management endpoint
└── trigger-scrape.js            # GitHub Actions trigger endpoint
```

### **Data Flow**
```
AWS S3 Bucket → Protobuf Decoder → Data Aggregator → Supabase Database → REST API
```

### **Database Schema**
- **`devices`** - Device registry (parent table)
- **`helium_rewards_daily`** - Daily reward aggregations (child table)
- **`tracked_devices`** - Scheduled scraping configuration
- **`helium_rewards_scrape_log`** - Complete audit trail

## 🚀 Quick Start

### 1. **Clone and Install**
```bash
git clone <your-repo-url>
cd helium-rewards-api
npm install
```

### 2. **Environment Setup**
```bash
# Copy the sample environment file
cp env.sample .env

# Edit .env with your credentials
nano .env
```

**Required Environment Variables:**
```bash
# AWS Configuration (for S3 access)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-west-2
AWS_BUCKET=foundation-poc-data-requester-pays

# Database Configuration
SUPABASE_URL=your_supabase_project_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key

# GitHub Actions (for API triggering)
GITHUB_TOKEN=your_github_personal_access_token
GITHUB_REPOSITORY=username/helium-rewards-api

# Optional Configuration
DEFAULT_DEVICE_KEY=your_default_device_key_for_testing
DAYS_TO_AGGREGATE=30
```

### 3. **Database Setup**
```bash
# In your Supabase SQL editor, run:
\i utils/supa-sql-migrate.txt

# This creates all necessary tables, indexes, and policies
```

### 4. **Test Your Setup**
```bash
# Test environment and database connection
npm run test:local env

# Test scraping functionality (uses DEFAULT_DEVICE_KEY)
npm run test:scrape

# Run full test suite
npm run test:local all
```

### 5. **Add Your First Device**
```bash
# Add a device to tracking list
npm run device:add "1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi" "My Helium Hotspot"

# List tracked devices
npm run device:list

# Scrape rewards for a specific device
npm run scrape:rewards "your-device-key-here"
```

## 📖 Usage Guide

### **Local Development**

#### **Scrape Single Device**
```bash
# Scrape last 30 days (default)
npm run scrape:rewards 1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi

# Scrape with custom date range
npm run scrape:rewards 1trSusefVoBGpZF78uAGhqfNNi9jHeZwgfn8WnnGgGhzJJDo1Xer8uQDEryJ7Lu3XKH44M7qReXgGjegznjKa6AHMjJMeNQrcZJViYc7oqwoBHygSWiC5qVKyWgnjQDWsDgvphnRTkYKbZESJrRTMP89TBKz5zgnt4N8JKQaQPNMqv3A1579TpbF2xYM1gBhTDf5PFyNixg5tHKC4WZnJnBxivSEezPiHbewL2NPpsv5z1bEeH8NngitV6aNB3AmC7GjSwn6Zn2TCTubajt9CLmg6E5ap12MGKUHJFtJGnuYczVJ1o1pouqggU9XzEasW3MZFH9KVMo97ukPGv4yTpsG4UrDmQk23s34pfFLWH3sxi 2025-01-01 2025-01-31
```

#### **Device Management**
```bash
# Add device to tracking
npm run device:add "device-key" "Device Name" "Optional notes"

# Remove device from tracking
npm run device:remove "device-key"

# List all tracked devices
npm run device:list
```

#### **Testing & Debugging**
```bash
# Test environment variables
npm run test:local env

# Test database connection
npm run test:local db

# Test scraping only (no database)
npm run test:scrape device-key 7

# Run comprehensive test suite
npm run test:local all
```

### **API Endpoints**

#### **Query Rewards Data**
```bash
# Get last 30 days of rewards
GET /api/helium-rewards?device_key=...&days=30

# Get specific date range
GET /api/helium-rewards?device_key=...&start=2025-01-01&end=2025-01-31

# Filter by reward type
GET /api/helium-rewards?device_key=...&reward_type=mobile_verified
```

## 🔧 Deployment

### **Vercel Deployment (Recommended)**

1. **Deploy to Vercel:**
   ```bash
   # Install Vercel CLI
   npm i -g vercel
   
   # Deploy
   vercel --prod
   ```

2. **Configure Environment Variables:**
   - Go to your Vercel dashboard
   - Add all environment variables from your `.env` file
   - Ensure `GITHUB_REPOSITORY` matches your repo

3. **Test API Endpoints:**
   ```bash
   curl https://your-app.vercel.app/api/helium-rewards?device_key=...&days=7
   ```

### **GitHub Actions Setup**

1. **Configure Repository Secrets:**
   - Go to Settings → Secrets and variables → Actions
   - Add all required secrets:
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`
     - `AWS_REGION`
     - `AWS_BUCKET`
     - `SUPABASE_URL`
     - `SUPABASE_SERVICE_ROLE_KEY`

2. **Scheduled Scraping:**
   - Runs daily at 2 AM UTC automatically
   - Scrapes all devices in your tracking list
   - View progress in Actions tab

## 🎯 Web Interface

### **Device Management UI**
Open `utils/manage-devices-ui.html` in your browser for a user-friendly interface to:
- Add/remove devices from tracking
- View device statistics
- Trigger scraping operations

### **Scraping & Query UI**
Open `utils/trigger-scrape-ui.html` in your browser for:
- Trigger GitHub Actions
- Query rewards data with filters
- View formatted results and charts

## 🗄️ Database Management

### **Fresh Installation**
```sql
-- Run in Supabase SQL editor
\i utils/supa-sql-migrate.txt
```

### **Complete Reset (⚠️ DESTRUCTIVE)**
```sql
-- Only if you need to start completely fresh
\i utils/supa-sql-nuke.txt
\i utils/supa-sql-migrate.txt
```

## 📊 Data Sources

**Currently Supported:**
- **Mobile Verified Rewards**: `foundation-mobile-verified/mobile_network_reward_shares_v1`
  - Protocol: `helium.poc_mobile.mobile_reward_share`
  - Description: Mobile network reward shares for verified mobile hotspots

**Future Support:**
- IoT verified rewards
- Consensus rewards
- Validator rewards

## 🔍 Troubleshooting

### **Common Issues**

#### **Environment Variables Missing**
```bash
npm run test:local env
# This will show which variables are missing
```

#### **Database Connection Failed**
```bash
npm run test:local db
# Check your SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
```

#### **AWS Access Denied**
- Ensure your AWS credentials have S3 read access
- Verify the bucket supports requester-pays
- Check AWS region is correct

#### **No Rewards Found**
- Verify device key format (should be 100+ characters)
- Check if device was active in the specified date range
- Confirm device is a mobile hotspot (not IoT-only)

#### **GitHub Actions Not Triggering**
- Verify `GITHUB_TOKEN` has repository dispatch permissions
- Check `GITHUB_REPOSITORY` format is `username/repo-name`
- Ensure repository secrets are configured correctly

### **Debug Mode**
```bash
# Test specific components
npm run test:scrape device-key 3  # 3 days only
npm run test:query device-key 7   # Query existing data
```

## 🚀 Advanced Usage

### **Custom Date Ranges**
```bash
# Scrape specific month
npm run scrape:rewards device-key 2025-01-01 2025-01-31

# Query last 90 days
curl "https://your-api.vercel.app/api/helium-rewards?device_key=...&days=90"
```

### **API Integration Examples**

#### **JavaScript/Node.js**
```javascript
const response = await fetch('https://your-api.vercel.app/api/helium-rewards', {
  method: 'GET',
  headers: {
    'Content-Type': 'application/json',
  },
  params: new URLSearchParams({
    device_key: 'your-device-key',
    days: '30',
    reward_type: 'mobile_verified'
  })
});

const data = await response.json();
console.log(`Total DC: ${data.summary.total_dc_rewards}`);
```

#### **Python**
```python
import requests

response = requests.get('https://your-api.vercel.app/api/helium-rewards', {
    'device_key': 'your-device-key',
    'days': 30,
    'reward_type': 'mobile_verified'
})

data = response.json()
print(f"Total DC: {data['summary']['total_dc_rewards']}")
```

## 🔒 Security

### **API Security**
- CORS configured for specific origins
- Rate limiting via Vercel
- Input validation on all endpoints
- No sensitive data in API responses

### **Database Security**
- Row Level Security (RLS) enabled
- Service role key for backend operations
- Encrypted connections (SSL/TLS)
- Regular backup and monitoring

### **AWS Security**
- Read-only S3 permissions
- Requester-pays bucket access
- IAM roles with least privilege
- Encrypted data in transit

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes following the existing patterns
4. Test thoroughly: `npm run test:local all`
5. Commit with clear messages: `git commit -m 'Add amazing feature'`
6. Push to your branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

## 📄 License

This project is licensed under the ISC License - see the package.json for details.

## 🙏 Acknowledgments

- **Helium Foundation** for providing open access to blockchain data
- **XNET Project** for the original scraper architecture pattern
- **Supabase** for excellent PostgreSQL hosting
- **Vercel** for seamless serverless deployment

---

## 📞 Support

- **Documentation**: Check the `docs/` folder for detailed guides
- **Issues**: Open GitHub issues for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions

**Happy scraping! 🚀**
