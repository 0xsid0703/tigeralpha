import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Dict, Tuple

import bittensor as bt

from config.config import appConfig as config
from miners.api_manager import APIManager
from neurons.protocol import AnalysisType, IntelligenceResponse


class CompanyIntelligenceProvider:
    """Handles external API calls for company intelligence."""

    def __init__(self, api_manager: APIManager):
        self.api_manager = api_manager
        self.cache = {}  # Simple in-memory cache
        self.cache_ttl = config.CACHE_TTL

    async def get_intelligence(self, ticker: str, analysis_type: AnalysisType, additional_params: dict) -> IntelligenceResponse:
        try:
            cache_key = self._get_cache_key(ticker, analysis_type.value)
            if cache_key in self.cache:
                cached_data, timestamp = self.cache[cache_key]
                if self._is_cache_valid(timestamp):
                    return cached_data

            company_data, error_message = await self._get_company_data(ticker, analysis_type, additional_params)
            if error_message or not company_data:
                return IntelligenceResponse(
                    success=False,
                    data=company_data,
                    errorMessage=error_message or "No data returned from API"
                )

            response = IntelligenceResponse(
                success=True,
                data=company_data,
                errorMessage=''
            )

            self.cache[cache_key] = (response, datetime.now(timezone.utc))

            return response
        except Exception as e:
            bt.logging.error(f"Error getting intelligence for {ticker} / {analysis_type}: {e}")

            return IntelligenceResponse(
                success=False,
                data={'company': {'ticker': ticker}},
                errorMessage=str(e)
            )

    def _get_cache_key(self, ticker: str, analysis_type: str) -> str:
        data = f"{ticker}:{analysis_type}"
        return hashlib.md5(data.encode()).hexdigest()

    def _is_cache_valid(self, timestamp: datetime) -> bool:
        return (datetime.now(timezone.utc) - timestamp).total_seconds() < self.cache_ttl

    async def _get_company_data(self, ticker: str, analysis_type: AnalysisType, additional_params: dict, max_retries: int = 2) -> Tuple[Dict | None, str | None]:
        session = await self.api_manager.get_session()

        params = {}

        headers = {
            'User-Agent': 'Bittensor-CompanyIntel/1.0',
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {config.CRYPTO_HOLDINGS_API_KEY}"
        }

        url = f"{config.CRYPTO_HOLDINGS_BASE_URL}/analysis/{ticker.lower()}/types/{analysis_type.value.lower()}"

        bt.logging.info(f"Fetching data for {ticker} from {url} with params: {params}")

        for attempt in range(max_retries):
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()

                        if not isinstance(data, dict):
                            bt.logging.warning(f"Invalid response format for {ticker}")
                            return {'company': {'ticker': ticker}}, 'Invalid downstream response format'

                        if 'error' in data or ('status' not in data or data['status'] != 'ok') or not 'result' in data:
                            bt.logging.warning(f"API error for {ticker}: {data}")
                            return None, data['message'] if 'message' in data else 'Unknown error from downstream API'

                        result: dict = data['result']
                        if not isinstance(result, dict):
                            bt.logging.warning(f"Invalid result format for {ticker}: {result}")
                            return None, 'Invalid result format from downstream API'

                        return result, None

                    elif response.status == 429:
                        wait_time = 2 ** attempt

                        bt.logging.info(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}")

                        await asyncio.sleep(wait_time)

                    else:
                        bt.logging.warning(f"HTTP {response.status} for {ticker}")
                        if attempt == max_retries - 1:
                            return None, f"Downstream API responded with HTTP {response.status} for {ticker}"

            except asyncio.TimeoutError:
                bt.logging.warning(f"Timeout for {ticker}, attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    return None, 'Timeout while fetching data from downstream API'

            except Exception as e:
                if attempt == max_retries - 1:
                    return None, f"Error fetching data from downstream API: {str(e)}"

            if attempt < max_retries - 1:
                await asyncio.sleep(1)

        return None, f"Max retries exceeded while fetching data from downstream API for {ticker}"
