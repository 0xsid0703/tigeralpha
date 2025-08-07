import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

import aiohttp
import bittensor as bt
from aiohttp import ClientSession, ClientTimeout, TCPConnector

from config.config import appConfig as config


class RateLimiter:
    """Simple rate limiter for API calls."""

    def __init__(self, max_requests: int = 100, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Wait if rate limit would be exceeded."""
        async with self.lock:
            now = time.time()
            # Remove old requests outside the time window
            self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]

            if len(self.requests) >= self.max_requests:
                # Calculate wait time
                oldest_request = min(self.requests)
                wait_time = self.time_window - (now - oldest_request)
                if wait_time > 0:
                    bt.logging.info(f"⏱️ Rate limit reached, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    return await self.acquire()

            self.requests.append(now)


class ExternalAPIClient:
    """Client for interacting with the external financial API."""

    def __init__(self):
        self.base_url = config.CRYPTO_HOLDINGS_BASE_URL
        self.api_key = config.CRYPTO_HOLDINGS_API_KEY
        self.session: Optional[ClientSession] = None
        self.rate_limiter = RateLimiter(max_requests=100, time_window=60)

        # Connection settings (will be used when session is created)
        self.connection_limit = config.API_MANAGER_CONNECTION_LIMIT
        self.dns_cache_ttl = config.API_MANAGER_DNS_CACHE
        self.client_timeout = config.API_MANAGER_CLIENT_TIMEOUT

        # Cache for API responses
        self.cache = {}
        self.cache_ttl = config.CACHE_TTL

        # Initialize flag
        self._initialized = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def initialize(self):
        """Initialize the HTTP session."""
        if self.session is None or self.session.closed:
            # Create connector and timeout in async context
            connector = TCPConnector(
                limit=self.connection_limit,
                ttl_dns_cache=self.dns_cache_ttl,
                use_dns_cache=True
            )

            timeout = ClientTimeout(total=self.client_timeout)

            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json',
                'User-Agent': 'Bittensor-CompanyIntelligence/1.0'
            }

            self.session = ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )
            self._initialized = True
            bt.logging.info("🌐 External API client initialized")

    async def close(self):
        """Close the HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            self._initialized = False

    def _get_cache_key(self, endpoint: str, params: Dict = None) -> str:
        """Generate cache key for request."""
        key = endpoint
        if params:
            sorted_params = sorted(params.items())
            key += "_" + "_".join([f"{k}={v}" for k, v in sorted_params])
        return key

    def _is_cache_valid(self, cache_entry: Dict) -> bool:
        """Check if cache entry is still valid."""
        if not cache_entry:
            return False

        cached_time = cache_entry.get('timestamp', 0)
        return (time.time() - cached_time) < self.cache_ttl

    async def _make_request(self, endpoint: str, params: Dict = None, use_cache: bool = True) -> Optional[Dict]:
        """Make HTTP request with caching and error handling."""
        # Ensure session is initialized
        if not self._initialized or self.session is None or self.session.closed:
            await self.initialize()

        cache_key = self._get_cache_key(endpoint, params)

        # Check cache first
        if use_cache and cache_key in self.cache:
            cache_entry = self.cache[cache_key]
            if self._is_cache_valid(cache_entry):
                bt.logging.debug(f"📋 Cache hit for {endpoint}")
                return cache_entry['data']

        # Rate limiting
        await self.rate_limiter.acquire()

        url = urljoin(self.base_url, endpoint)

        try:
            bt.logging.debug(f"🌐 Making API request to {endpoint}")

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    if not isinstance(data, dict):
                        bt.logging.error(f"❌ Invalid response format from {endpoint}: {data}")
                        return None

                    if 'result' not in data:
                        bt.logging.error(f"❌ Missing 'result' in response from {endpoint}: {data}")
                        return None

                    # Cache the response
                    if use_cache:
                        self.cache[cache_key] = {
                            'data': data['result'],
                            'timestamp': time.time()
                        }

                    return data['result']

                elif response.status == 429:
                    bt.logging.warning(f"⚠️ Rate limited by external API, status {response.status}")
                    await asyncio.sleep(5)  # Back off
                    return await self._make_request(endpoint, params, use_cache)

                else:
                    bt.logging.error(f"❌ External API error: {response.status} - {await response.text()}")
                    return None

        except asyncio.TimeoutError:
            bt.logging.error(f"⏰ Timeout making request to {endpoint}")
            return None
        except Exception as e:
            bt.logging.error(f"💥 Error making request to {endpoint}: {e}")
            return None

    async def get_companies_list(self) -> List[Dict[str, Any]]:
        """Get list of all available companies."""
        try:
            data = await self._make_request(config.COMPANIES_ENDPOINT)
            if data and isinstance(data, list):
                bt.logging.info(f"📊 Retrieved {len(data)} companies from external API")
                return data
            elif data and isinstance(data, dict) and 'companies' in data:
                companies = data['companies']
                bt.logging.info(f"📊 Retrieved {len(companies)} companies from external API")
                return companies
            else:
                bt.logging.warning("❌ Invalid response format from companies endpoint")
                return []
        except Exception as e:
            bt.logging.error(f"💥 Error fetching companies list: {e}")
            return []

    async def validate_company_data(self, ticker: str, analysis_type: str, miner_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate miner data against external API using POST request."""
        try:
            validation_response = await self._post_validation_request(miner_data, ticker, analysis_type)

            if not validation_response:
                return {
                    'valid': False,
                    'error': 'Could not get validation response from external API',
                    'score': 0.0
                }

            # Process field scores from API response
            validation_result = self._process_validation_scores(validation_response, ticker)

            return validation_result

        except Exception as e:
            bt.logging.error(f"💥 Error validating company data for {ticker}: {e}")
            return {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'score': 0.0
            }

    async def _post_validation_request(self, request_body: Dict[str, Any], ticker: str, analysis_type: str) -> Optional[Dict[str, Any]]:
        """Send POST request to validation endpoint."""
        if not self._initialized or self.session is None or self.session.closed:
            await self.initialize()

        # Rate limiting
        await self.rate_limiter.acquire()

        validation_endpoint = config.VALIDATION_ENDPOINT
        validation_endpoint = validation_endpoint.replace('<ticker>', ticker)
        validation_endpoint = validation_endpoint.replace('<analysis_type>', analysis_type)

        url = urljoin(self.base_url, validation_endpoint)

        try:
            bt.logging.debug(f"🌐 Posting validation request to {validation_endpoint}")

            async with self.session.post(url, json=request_body) as response:
                if response.status == 200:
                    data = await response.json()

                    if 'result' not in data:
                        bt.logging.error(f"❌ Invalid response format from validation API: {data}")
                        return None

                    bt.logging.debug(f"✅ Validation response received: {response.status}")
                    return data['result']

                elif response.status == 429:
                    bt.logging.warning(f"⚠️ Rate limited by validation API, status {response.status}")
                    await asyncio.sleep(5)  # Back off
                    return await self._post_validation_request(request_body, ticker, analysis_type)

                else:
                    error_text = await response.text()
                    bt.logging.error(f"❌ Validation API error: {response.status} - {error_text}")
                    return None

        except asyncio.TimeoutError:
            bt.logging.error(f"⏰ Timeout posting validation request")
            return None
        except Exception as e:
            bt.logging.error(f"💥 Error posting validation request: {e}")
            return None

    def _process_validation_scores(self, api_response: Dict[str, Any], ticker: str) -> Dict[str, Any]:
        """Process field scores from API response into validation result."""
        validation_result = {
            'valid': True,
            'score': 0.0,
            'field_scores': {},
            'details': {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'ticker': ticker
        }

        try:
            bt.logging.debug(f"📊 Processing validation scores for {api_response}")

            # Extract field scores from API response
            field_scores = api_response.get('fieldScores', {})

            if not field_scores:
                bt.logging.warning(f"⚠️ No field scores in validation response for {ticker}")
                return {
                    'valid': False,
                    'error': 'No field scores in validation response',
                    'score': 0.0
                }

            # Store individual field scores
            validation_result['field_scores'] = field_scores

            # Calculate weighted overall score
            total_score = 0.0
            total_weight = 0.0

            # Define field weights for overall score calculation
            field_weights = {
                'company.companyName': 1.5,       # Company name - important
                'company.ticker': 1.0,            # Ticker symbol
                'company.marketCap': 2.0,         # Market cap - very important
                'company.sharePrice': 1.8,        # Share price - important
                'company.sector': 1.2,            # Sector classification
                'company.industry': 1.0,          # Industry classification
                'company.website': 0.8,           # Website URL
                'company.exchange': 1.0,          # Exchange listing
                'company.volume': 1.5,            # Trading volume
                'company.eps': 1.3,               # Earnings per share
                'company.bookValue': 1.0,         # Book value
                'cryptoHoldings': 1.8,    # Crypto holdings - important for crypto analysis
                'totalCryptoValue': 1.8,  # Total crypto value
                'sentiment': 1.0,         # Sentiment analysis
                'sentimentScore': 1.0,    # Sentiment score
                'newsArticles': 0.8,      # News article count
                'totalArticles': 0.8      # Total articles
            }

            # Calculate weighted score
            for field, score in field_scores.items():
                if not isinstance(score, (int, float)):
                    bt.logging.warning(f"⚠️ Invalid score type for field {field}: {type(score)}")
                    continue

                # Ensure score is between 0 and 1
                normalized_score = max(0.0, min(1.0, float(score)))

                # Get weight for this field
                weight = field_weights.get(field, 1.0)

                total_score += normalized_score * weight
                total_weight += weight

                # Add field details
                if normalized_score >= 0.9:
                    validation_result['details'][field] = 'excellent'
                elif normalized_score >= 0.7:
                    validation_result['details'][field] = 'good'
                elif normalized_score >= 0.5:
                    validation_result['details'][field] = 'fair'
                elif normalized_score >= 0.3:
                    validation_result['details'][field] = 'poor'
                else:
                    validation_result['details'][field] = 'very_poor'

            # Calculate final score
            if total_weight > 0:
                validation_result['score'] = total_score / total_weight
            else:
                validation_result['score'] = 0.0

            # Add summary statistics
            validation_result['summary'] = {
                'total_fields_validated': len(field_scores),
                'average_field_score': sum(field_scores.values()) / len(field_scores) if field_scores else 0.0,
                'high_scoring_fields': len([s for s in field_scores.values() if s >= 0.8]),
                'low_scoring_fields': len([s for s in field_scores.values() if s < 0.5]),
                'validation_confidence': min(1.0, len(field_scores) / 10.0)  # Confidence based on number of fields
            }

            # Add data freshness if provided by API
            if 'freshnessScore' in api_response:
                freshness_score = float(api_response['freshnessScore'])
                validation_result['freshnessScore'] = freshness_score
                validation_result['details']['freshness'] = self._classify_freshness(freshness_score)

            # Add completeness score if provided by API
            if 'completenessScore' in api_response:
                completeness_score = float(api_response['completenessScore'])
                validation_result['completenessScore'] = completeness_score
                validation_result['details']['completeness'] = self._classify_completeness(completeness_score)

            bt.logging.debug(f"📊 Validation result for {ticker}: score={validation_result['score']:.3f}, fields={len(field_scores)}")

            return validation_result

        except Exception as e:
            bt.logging.error(f"💥 Error processing validation scores for {ticker}: {e}")
            return {
                'valid': False,
                'error': f'Error processing validation scores: {str(e)}',
                'score': 0.0
            }

    def _classify_freshness(self, score: float) -> str:
        """Classify freshness score into categories."""
        if score >= 0.9:
            return 'very_fresh'
        elif score >= 0.7:
            return 'fresh'
        elif score >= 0.5:
            return 'moderate'
        elif score >= 0.3:
            return 'stale'
        else:
            return 'very_stale'

    def _classify_completeness(self, score: float) -> str:
        """Classify completeness score into categories."""
        if score >= 0.9:
            return 'complete'
        elif score >= 0.7:
            return 'mostly_complete'
        elif score >= 0.5:
            return 'partial'
        elif score >= 0.3:
            return 'limited'
        else:
            return 'minimal'
