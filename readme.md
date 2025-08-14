# ALPHA Subnet 107 – Verifiable On-Chain Treasury Data via Decentralized AI

ALPHA is a TAO subnet delivering real-time, verifiable crypto market data through decentralized AI research agents.

Agents continuously source, validate, and publish live price feeds and holdings data—anchored on-chain for treasury managers, analysts, and protocol developers.

## Links

1. [taostats #107](https://taostats.io/subnets/107/chart)
2. [Discord - Subnet Channel 107](https://discord.gg/qasY3HA9F9)
3. [Non Technical Explainer](./explainer.md)
4. [Video Demo](https://screen.studio/share/Ux3MCH6H)
5. [Dashboard - Coming Soon](https://screen.studio/share/Ux3MCH6H)

![ALPHA Screenshot](/alpha-screenshot.png "ALPHA Screenshot")

## Overview

The Bittensor Company Intelligence Subnet is a decentralized network designed to provide real-time financial information about public companies, private companies, startups, and other financial entities. This validator serves as a critical component that queries miners for company intelligence data and validates their responses against authoritative sources.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Components](#components)
3. [Setup and Installation](#setup-and-installation)
4. [Configuration](#configuration)
5. [Running the Validator](#running-the-validator)
6. [API Usage](#api-usage)
7. [Monitoring and Maintenance](#monitoring-and-maintenance)
8. [Development](#development)
9. [Troubleshooting](#troubleshooting)

## Architecture Overview

The validator implements a sophisticated three-tier validation system:

```
┌──────────────────────────┐    ┌───────────────────────────┐    ┌───────────────────────┐
│   Query Layer            │    │ Validation Layer          │    │  Incentive Layer      │
│                          │    │                           │    │                       │
│ • Smart Query Generation │────│ • Structure Validation    │────│ • Score Calculation   │
│ • Company DB Management  │    │ • External API Validation │    │ • Weight Distribution │
│                          │    │                           │    │                       │
└──────────────────────────┘    └───────────────────────────┘    └───────────────────────┘
```

### Key Features

- **Intelligent Query Generation**: Uses multiple strategies to generate diverse, meaningful queries
- **Two-Tier Validation**: Combines structural validation with external API verification
- **Company Database Management**: Maintains updated company information with fallback mechanisms
- **HTTP API Interface**: Provides external access to miner capabilities
- **Comprehensive Monitoring**: Tracks performance, errors, and system health

## Components

### Core Components

#### 1. Company Database (`company_database.py`)

Manages company information with intelligent caching and fallback mechanisms.

**Key Features:**

- External API integration for fresh company data
- Fallback to hardcoded company list for reliability
- Sector-based organization
- Market cap filtering for different query strategies

#### 2. Query Generator (`query_generator.py`)

Generates sophisticated queries using multiple strategies:

- **Popular Companies** (40%): Large, well-known companies
- **Emerging Companies** (20%): Smaller, growing companies
- **Sector-Focused** (15%): Industry-specific queries
- **Crypto-Focused** (15%): Companies with cryptocurrency exposure
- **Random Selection** (10%): Diverse sampling

#### 3. Response Validator (`response_validator.py`)

Implements two-tier validation system:

**Tier 1: Structure Validation**

- JSON schema validation
- Required field checking
- Data type validation
- Completeness scoring

**Tier 2: External API Validation**

- Real-time data verification
- Field-by-field accuracy scoring
- Freshness assessment
- Confidence calculation

#### 4. Incentive Mechanism (`incentive_mechanism.py`)

Calculates miner rewards based on:

- Response accuracy
- Response time
- Confidence alignment
- Historical performance

### Support Components

#### 5. External API Client (`external_api_client.py`)

Handles all external API communications with:

- Rate limiting
- Circuit breaker pattern
- Response caching
- Error handling and retry logic

#### 6. Configuration Management (`config.py`)

Centralized configuration with:

- Environment-specific settings
- Validation weights configuration
- Feature flags
- Performance tuning parameters

#### 7. HTTP Server (`server.py`, `routes.py`)

Provides external API access with:

- Bearer token authentication
- RESTful endpoints
- CORS support
- Error handling

## Setup and Installation

### Prerequisites

- Python 3.9+
- Bittensor framework
- Docker (optional)
- External API access credentials

### Installation Steps

1. **Clone the Repository**

```bash
git clone https://github.com/tigerinvests-com/sn107-alpha/
cd sn107-alpha
```

2. **Install Dependencies**

```bash
pip install -r requirements.txt
```

3. **Set Up Bittensor Wallet**

```bash
# Create or import wallet
btcli wallet new_coldkey  --wallet.name validator
btcli wallet new_hotkey   --wallet.name validator --wallet.hotkey default
```

4. **Configure Environment Variables**

```bash
cp .env.example .env
# Edit .env with your configuration
```

### Required Environment Variables

```bash
# External API Configuration
CRYPTO_HOLDINGS_URL=https://your-api.com
CRYPTO_HOLDINGS_API_KEY=your-api-key

# Authentication
API_TOKEN=your-secure-token

# Network Configuration
VALIDATOR_HOST=0.0.0.0
VALIDATOR_PORT=8000
NETUID=107

# Optional: Performance Tuning
MAX_CONCURRENT_MINERS=20
MINER_TIMEOUT=15
CACHE_TTL=300
```

## Configuration

### Core Configuration Options

#### Validation Weights

Controls the balance between validation tiers:

```bash
STRUCTURE_VALIDATION_WEIGHT=0.3  # 30% weight for structure validation
API_VALIDATION_WEIGHT=0.7        # 70% weight for API validation
```

#### Query Strategy Weights

Controls query generation strategy distribution:

```bash
POPULAR_COMPANIES_WEIGHT=0.4     # 40% popular companies
EMERGING_COMPANIES_WEIGHT=0.2    # 20% emerging companies
SECTOR_FOCUSED_WEIGHT=0.15       # 15% sector-focused
CRYPTO_FOCUSED_WEIGHT=0.15       # 15% crypto-focused
RANDOM_SELECTION_WEIGHT=0.1      # 10% random selection
```

#### Analysis Type Distribution

```bash
CRYPTO_ANALYSIS_WEIGHT=0.35      # 35% crypto analysis
FINANCIAL_ANALYSIS_WEIGHT=0.35   # 35% financial analysis
SENTIMENT_ANALYSIS_WEIGHT=0.15   # 15% sentiment analysis
NEWS_ANALYSIS_WEIGHT=0.15        # 15% news analysis
```

### Environment-Specific Configuration

The validator automatically loads environment-specific settings:

- **Production**: Minimal logging, longer cache TTL
- **Staging**: Debug logging, medium cache TTL
- **Development**: Verbose logging, short cache TTL, detailed validation

## Running the Validator

### Option 1: Direct Python Execution

```bash
# Run validator only
python validators/validator.py \
  --wallet.name validator_wallet \
  --wallet.hotkey validator_hotkey \
  --netuid 107 \
  --logging.debug

# Run with HTTP server
python validators/server.py \
  --wallet.name validator_wallet \
  --wallet.hotkey validator_hotkey \
  --netuid 107 \
  --logging.debug
```

### Option 2: Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose  --env-file ./.env.example up -d

# View logs
docker-compose logs -f validator

# Stop
docker-compose down
```

### Command Line Arguments

```bash
python validators/validator.py \
  --wallet.name <wallet_name> \
  --wallet.hotkey <hotkey_name> \
  --netuid <subnet_id> \
  --neuron.timeout <timeout_seconds> \
  --neuron.sample_size <miners_per_round> \
  --validator.max_concurrent_miners <max_concurrent> \
  --logging.debug \
  --logging.trace
```

## API Usage

### Authentication

All API endpoints require Bearer token authentication:

```bash
curl -H "Authorization: Bearer your-api-token" \
  http://localhost:8000/validator/query
```

### Available Endpoints

#### 1. Query Miners

**POST** `/validator/query`

Query subnet miners for company intelligence.

**Request Body:**

```json
{
  "ticker": "AAPL",
  "analysis_type": "crypto"
}
```

**Response:**

```json
{
  "query_id": "query_1673123456_AAPL",
  "ticker": "AAPL",
  "analysis_type": "crypto",
  "total_miners_queried": 15,
  "successful_responses": 12,
  "average_response_time": 2.3,
  "best_response": {
    "uid": 42,
    "score": 0.95,
    "response_time": 1.8,
    "success": true,
    "confidence": 0.92,
    "data": {
      "company": {
        "ticker": "AAPL",
        "companyName": "Apple Inc.",
        "marketCap": 2800000000000,
        "sector": "Technology"
      }
    }
  },
  "responses": [...]
}
```

#### 2. Get Miners

**GET** `/validator/miners`

Get list of available miners.

**Response:**

```json
{
  "total_miners": 25,
  "miners": [
    {
      "uid": 1,
      "ip": "192.168.1.100",
      "port": 8091,
      "stake": 1000.0,
      "hotkey": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
    }
  ]
}
```

#### 3. Get Scores

**GET** `/validator/scores`

Get current miner scores.

**Response:**

```json
{
  "scores": {
    "1": 0.85,
    "2": 0.92,
    "3": 0.78
  },
  "last_updated": "2024-01-15T10:30:00Z"
}
```

#### 4. Health Check

**GET** `/status`

Basic health check.

**Response:**

```json
{
  "status": "ok"
}
```

#### 5. Validator Info

**GET** `/info`

Detailed validator information.

**Response:**

```json
{
  "status": "healthy",
  "validator_address": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
  "netuid": 1,
  "current_step": 1250,
  "available_miners": 25,
  "miner_uids": [1, 2, 3, 4, 5],
  "last_update": "2024-01-15T10:30:00Z",
  "metagraph_block": 1234567
}
```

### Analysis Types

- **crypto**: Cryptocurrency holdings and exposure
- **financial**: Financial metrics and fundamentals
- **sentiment**: Market sentiment analysis
- **news**: News analysis and aggregation

### Error Handling

The API uses standard HTTP status codes:

- **200**: Success
- **400**: Bad request (invalid parameters)
- **401**: Unauthorized (invalid token)
- **404**: Not found
- **500**: Internal server error
- **503**: Service unavailable (validator not ready)

## Monitoring and Maintenance

### Logging

The validator provides comprehensive logging:

```bash
# View logs (Docker)
docker-compose logs -f validator

# View logs (systemd)
sudo journalctl -u validator -f

# Log levels
VALIDATOR_LOG_LEVEL=debug  # debug, info, warning, error
```

### Health Monitoring

Monitor validator health using:

1. **HTTP Health Endpoints**:

   - `/status` - Basic health check
   - `/info` - Detailed status information

2. **Log Analysis**:

   - Look for "✅" success indicators
   - Monitor "💥" error messages
   - Track validation round completion

3. **Key Metrics**:
   - Query success rate
   - Average response time
   - Miner availability
   - API validation score

### Maintenance Tasks

#### State Management

The validator automatically saves and loads state:

```bash
# State files location
ls -la data/
# validator_state.json - Current state
# validation_history/ - Historical validation data
```

## Development

### Project Structure

```
bittensor-company-intelligence-validator/
├── analysis/
│   ├── company_database.py      # Company data management
│   ├── external_api_client.py   # External API integration
│   ├── query_generator.py       # Query generation strategies
│   ├── response_validator.py    # Response validation logic
│   ├── incentive_mechanism.py   # Reward calculation
│   └── validation_schemas.py    # JSON schemas
├── config/
│   └── config.py                # Configuration management
├── miners/
│   ├── api_manager.py           # Miner API management
│   ├── intelligence_provider.py # Intelligence data provider
│   └── miner.py                 # Miner implementation
├── neurons/
│   └── protocol.py              # Network protocol definitions
├── validators/
│   ├── validator.py             # Main validator logic
│   ├── server.py                # HTTP server
│   └── routes.py                # API routes
├── docker-compose.yml           # Docker deployment
├── requirements.txt             # Python dependencies
└── README.md                    # This documentation
```

### Environment Setup

```bash
# Development environment
export ENVIRONMENT=development
export DEBUG_MODE=true
export VERBOSE_LOGGING=true

# Enable detailed validation logging
export SAVE_VALIDATION_DETAILS=true
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Troubleshooting

### Common Issues

#### 1. "No miners available"

**Cause**: No miners are registered or online
**Solution**:

- Check network connectivity
- Verify miners are running
- Check subnet registration

#### 2. "External API validation failed"

**Cause**: External API is unreachable or misconfigured
**Solution**:

- Check API credentials
- Verify API endpoint URL
- Check network connectivity
- Review rate limiting

#### 3. "Weight setting failed"

**Cause**: Insufficient stake or network issues
**Solution**:

- Check wallet balance
- Verify registration status
- Check network connection

#### 4. "Database initialization failed"

**Cause**: Unable to connect to external API
**Solution**:

- Validator will use fallback data
- Check API configuration
- Verify network connectivity

### Debug Mode

Enable debug mode for detailed logging:

```bash
export DEBUG_MODE=true
export VERBOSE_LOGGING=true
export SAVE_VALIDATION_DETAILS=true
```

### Performance Tuning

For high-traffic scenarios:

```bash
# Increase concurrent miners
export MAX_CONCURRENT_MINERS=50

# Adjust timeout
export MINER_TIMEOUT=30

# Optimize cache
export CACHE_TTL=600
export COMPANY_CACHE_DURATION_HOURS=2
```

### Getting Help

1. **Check Logs**: Most issues are logged with clear error messages
2. **Review Configuration**: Verify all environment variables are set
3. **Test Connectivity**: Ensure network access to external APIs
4. **Monitor Resources**: Check CPU, memory, and disk usage
5. **Community Support**: Join the Bittensor community for help

### Logs Analysis

Key log patterns to monitor:

```bash
# Successful validation
✅ Epoch 123 completed in 15.2s

# API issues
💥 Error refreshing from API: Connection timeout

# Miner issues
⚠️ No miners available for validation

# Weight setting success
✅ Successfully set weights for 25 UIDs
```

## License

This project is licensed under the MIT License.

## Support

For technical support:

- GitHub Issues: Report bugs and request features
