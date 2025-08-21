import asyncio
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import bittensor as bt
import httpx

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
            self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]

            if len(self.requests) >= self.max_requests:
                oldest_request = min(self.requests)
                wait_time = self.time_window - (now - oldest_request)
                if wait_time > 0:
                    bt.logging.info(f"⏱️ Rate limit reached, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    return await self.acquire()

            self.requests.append(now)


class ExternalAPIClient:

    def __init__(self):
        self.base_url = config.CRYPTO_HOLDINGS_BASE_URL
        self.api_key = config.CRYPTO_HOLDINGS_API_KEY
        self.client: Optional[httpx.AsyncClient] = None
        self.rate_limiter = RateLimiter(max_requests=100, time_window=60)

        self.cache = {}
        self.cache_ttl = config.CACHE_TTL

        self._initialized = False
        self._client_lock = asyncio.Lock()

        self.max_retries = 2
        self.retry_delay = 1.0

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def initialize(self):
        """Initialize the httpx client with robust SSL handling."""
        async with self._client_lock:
            if self.client is None or self.client.is_closed:
                try:
                    headers = {
                        'Authorization': f'Bearer {self.api_key}',
                        'Content-Type': 'application/json',
                        'User-Agent': 'Bittensor-CompanyIntelligence/1.0',
                        'Connection': 'close'
                    }

                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = True
                    ssl_context.verify_mode = ssl.CERT_REQUIRED

                    # Configure httpx with conservative settings to avoid SSL issues
                    self.client = httpx.AsyncClient(
                        headers=headers,
                        timeout=httpx.Timeout(
                            connect=15.0,
                            read=30.0,
                            write=10.0,
                            pool=60.0
                        ),
                        limits=httpx.Limits(
                            max_keepalive_connections=0,
                            max_connections=3,
                            keepalive_expiry=0
                        ),
                        verify=ssl_context,
                        http1=True,
                        http2=False,
                        follow_redirects=True
                    )

                    self._initialized = True

                except Exception as e:
                    bt.logging.error(f"💥 Failed to initialize httpx client: {e}")
                    self._initialized = False
                    raise

    async def close(self):
        """Close the httpx client."""
        async with self._client_lock:
            if self.client and not self.client.is_closed:
                await self.client.aclose()
                self.client = None
                self._initialized = False
                bt.logging.debug("🔒 httpx API client closed")

    async def _recreate_client(self):
        bt.logging.info("🔄 Recreating httpx client due to connection issues")

        await self.close()
        await asyncio.sleep(0.2)
        await self.initialize()

    def _get_cache_key(self, endpoint: str, params: Dict = None) -> str:
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

    def _is_connection_error(self, error: Exception) -> bool:
        """Check if error is related to connection/SSL issues."""

        error_types = (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
            httpx.ProxyError,
            httpx.UnsupportedProtocol
        )

        # Also check for SSL-related errors in the error message
        error_msg = str(error).lower()
        ssl_keywords = ['ssl', 'certificate', 'handshake', 'close_notify', 'tls']

        return isinstance(error, error_types) or any(keyword in error_msg for keyword in ssl_keywords)

    async def _make_request_with_retry(self, method: str, endpoint: str, params: Dict = None,
                                       json_data: Dict = None, use_cache: bool = True) -> Optional[Dict]:
        """Make HTTP request with comprehensive retry logic."""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    await self._recreate_client()
                    delay = min(0.5 * (2 ** attempt), 5.0)
                    await asyncio.sleep(delay)

                return await self._make_request(method, endpoint, params, json_data, use_cache)

            except Exception as e:
                last_error = e
                error_msg = str(e)

                if self._is_connection_error(e):
                    bt.logging.warning(f"🔒 Connection/SSL error on attempt {attempt + 1}/{self.max_retries}: {error_msg}")
                    if attempt < self.max_retries - 1:
                        await self._recreate_client()
                        delay = self.retry_delay * (2 ** attempt) + 0.5
                        bt.logging.info(f"🔄 Connection retry in {delay:.1f}s...")
                        await asyncio.sleep(delay)
                        continue

                elif isinstance(e, (httpx.TimeoutException, httpx.RequestError)):
                    bt.logging.warning(f"⏰ Request error on attempt {attempt + 1}/{self.max_retries}: {error_msg}")
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delay * (2 ** attempt)
                        bt.logging.info(f"🔄 Request retry in {delay:.1f}s...")
                        await asyncio.sleep(delay)
                        continue

                else:
                    bt.logging.error(f"💥 Unexpected error on attempt {attempt + 1}: {error_msg}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(1)
                        await self._recreate_client()
                        continue

                break

        bt.logging.error(f"💥 All {self.max_retries} retry attempts failed for {endpoint}. Last error: {last_error}")

        return None

    async def _make_request(self, method: str, endpoint: str, params: Dict = None,
                            json_data: Dict = None, use_cache: bool = True) -> Optional[Dict]:
        """Make HTTP request with caching and error handling."""
        # Ensure client is initialized
        if not self._initialized or self.client is None or self.client.is_closed:
            await self.initialize()

        # Check cache for GET requests
        if method.upper() == 'GET' and use_cache:
            cache_key = self._get_cache_key(endpoint, params)
            if cache_key in self.cache:
                cache_entry = self.cache[cache_key]
                if self._is_cache_valid(cache_entry):
                    bt.logging.debug(f"📋 Cache hit for {endpoint}")
                    return cache_entry['data']

        await self.rate_limiter.acquire()

        url = urljoin(self.base_url, endpoint)

        try:
            request_kwargs = {}
            if params:
                request_kwargs['params'] = params
            if json_data:
                request_kwargs['json'] = json_data

            request_kwargs['headers'] = {
                'Connection': 'close',
                'Cache-Control': 'no-cache'
            }

            response = await self.client.request(method, url, **request_kwargs)

            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception as json_error:
                    bt.logging.error(f"❌ Failed to parse JSON response: {json_error}")
                    return None

                if not isinstance(data, dict):
                    bt.logging.error(f"❌ Invalid response format from {endpoint}: {type(data)}")
                    return None

                if 'result' not in data:
                    bt.logging.error(f"❌ Missing 'result' in response from {endpoint}")
                    return None

                if method.upper() == 'GET' and use_cache:
                    cache_key = self._get_cache_key(endpoint, params)
                    self.cache[cache_key] = {
                        'data': data['result'],
                        'timestamp': time.time()
                    }

                return data['result']

            elif response.status_code == 429:
                bt.logging.warning(f"⚠️ Rate limited by external API, status {response.status_code}")
                raise httpx.HTTPStatusError(
                    f"Rate limited: {response.status_code}",
                    request=response.request,
                    response=response
                )

            else:
                error_text = response.text
                bt.logging.error(f"❌ External API error: {response.status_code} - {error_text}")
                return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise
            else:
                bt.logging.error(f"🔴 HTTP status error: {e}")
                return None

        except Exception as e:
            bt.logging.debug(f"🔍 Request exception for {endpoint}: {e}")
            raise

    async def get_companies_list(self) -> List[Dict[str, Any]]:
        """Get list of all available companies."""
        try:
            data = await self._make_request_with_retry('GET', config.COMPANIES_ENDPOINT)
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
        validation_endpoint = config.VALIDATION_ENDPOINT
        validation_endpoint = validation_endpoint.replace('<ticker>', ticker)
        validation_endpoint = validation_endpoint.replace('<analysis_type>', analysis_type)

        return await self._make_request_with_retry('POST', validation_endpoint, json_data=request_body, use_cache=False)

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
            field_scores = api_response.get('fieldScores', {})

            if not field_scores:
                bt.logging.warning(f"⚠️ No field scores in validation response for {ticker}")
                return {
                    'valid': False,
                    'error': 'No field scores in validation response',
                    'score': 0.0
                }

            validation_result['field_scores'] = field_scores

            total_score = 0.0
            total_weight = 0.0

            field_weights = {
                'company.companyName': 1.5,
                'company.ticker': 1.0,
                'company.marketCap': 2.0,
                'company.sharePrice': 1.8,
                'company.sector': 1.2,
                'company.industry': 1.0,
                'company.website': 0.8,
                'company.exchange': 1.0,
                'company.volume': 1.5,
                'company.eps': 1.3,
                'company.bookValue': 1.0,
                'cryptoHoldings': 1.8,
                'totalCryptoValue': 1.8,
                'sentiment': 1.0,
                'sentimentScore': 1.0,
                'newsArticles': 0.8,
                'totalArticles': 0.8
            }

            for field, score in field_scores.items():
                if not isinstance(score, (int, float)):
                    bt.logging.warning(f"⚠️ Invalid score type for field {field}: {type(score)}")
                    continue

                normalized_score = max(0.0, min(1.0, float(score)))
                weight = field_weights.get(field, 1.0)

                total_score += normalized_score * weight
                total_weight += weight

            if total_weight > 0:
                validation_result['score'] = total_score / total_weight
            else:
                validation_result['score'] = 0.0

            bt.logging.debug(f"📊 Validation result for {ticker}: score={validation_result['score']:.3f}, fields={len(field_scores)}")

            return validation_result

        except Exception as e:
            bt.logging.error(f"💥 Error processing validation scores for {ticker}: {e}")
            return {
                'valid': False,
                'error': f'Error processing validation scores: {str(e)}',
                'score': 0.0
            }
