import asyncio
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import bittensor as bt

from analysis.external_api_client import ExternalAPIClient


class CompanyDatabase:
    """In-memory database of companies with external API integration."""

    def __init__(self, cache_duration_hours: int):
        self.cache_duration = timedelta(hours=cache_duration_hours)

        # In-memory storage
        self.companies_cache: Dict[str, Dict] = {}
        self.sectors_cache: Dict[str, List[str]] = {}
        self.last_refresh: Optional[datetime] = None

        # Fallback companies for when API is unavailable
        self.fallback_companies = {
            'Technology': ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'TSLA', 'NFLX', 'AMZN'],
            'Healthcare': ['JNJ', 'PFE', 'UNH', 'ABBV', 'TMO', 'DHR', 'CVS', 'MRK'],
            'Financial': ['JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'COF', 'AXP'],
            'Energy': ['XOM', 'CVX', 'COP', 'EOG', 'PXD', 'SLB', 'MRO', 'VLO'],
            'Consumer': ['PG', 'KO', 'PEP', 'WMT', 'HD', 'MCD', 'NKE', 'SBUX'],
            'Industrial': ['BA', 'CAT', 'GE', 'HON', 'UPS', 'LMT', 'MMM', 'FDX'],
            'Materials': ['LIN', 'APD', 'SHW', 'ECL', 'FCX', 'NEM', 'DOW', 'DD'],
            'Utilities': ['NEE', 'SO', 'DUK', 'AEP', 'EXC', 'XEL', 'PEG', 'SRE']
        }

        self.external_api_client = ExternalAPIClient()
        self._initialization_task = None

        # Load fallback data immediately for synchronous access
        self._load_fallback_data_sync()

    def _load_fallback_data_sync(self):
        """Load fallback company data synchronously for immediate availability."""
        bt.logging.info("📋 Loading fallback company data")

        self.companies_cache.clear()
        self.sectors_cache = {}

        for sector, tickers in self.fallback_companies.items():
            self.sectors_cache[sector] = tickers.copy()

            for ticker in tickers:
                self.companies_cache[ticker] = {
                    'ticker': ticker,
                    'companyName': f'{ticker} Corporation',
                    'sector': sector,
                    'country': 'USA',
                    'countryCode': 'US',
                    'exchange': 'NASDAQ',
                    'marketCap': 0,
                    'website': '',
                    'description': f'{ticker} is a major {sector.lower()} company',
                    'last_updated': datetime.now(timezone.utc).isoformat(),
                    'data_source': 'fallback'
                }

        bt.logging.info(f"🔄 Loaded {len(self.companies_cache)} fallback companies")

    async def initialize(self):
        """Initialize the company database with API data."""
        if self._initialization_task is None:
            self._initialization_task = asyncio.create_task(self._load_initial_data())

        try:
            await self._initialization_task
        except Exception as e:
            bt.logging.error(f"💥 Error during database initialization: {e}")
            # Fallback data is already loaded synchronously

    async def _load_initial_data(self):
        """Load initial data from external API or fallback."""
        try:
            # Try to refresh from API first
            success = await self.refresh_from_api()

            if not success:
                bt.logging.info("🔄 API refresh failed, loading fallback data")
                await self._load_fallback_data()
            else:
                bt.logging.info(f"📋 Loaded {len(self.companies_cache)} companies from external API")

        except Exception as e:
            bt.logging.error(f"💥 Error loading initial data: {e}")
            await self._load_fallback_data()

    async def _load_fallback_data(self):
        """Load fallback company data when API is unavailable."""
        bt.logging.warning("⚠️  Loading fallback company data")

        self.companies_cache.clear()
        self.sectors_cache = {}

        for sector, tickers in self.fallback_companies.items():
            self.sectors_cache[sector] = tickers.copy()

            for ticker in tickers:
                self.companies_cache[ticker] = {
                    'ticker': ticker,
                    'companyName': f'{ticker} Corporation',
                    'sector': sector,
                    'country': 'USA',
                    'countryCode': 'US',
                    'exchange': 'NASDAQ',
                    'marketCap': 0,
                    'website': '',
                    'description': f'{ticker} is a major {sector.lower()} company',
                    'last_updated': datetime.now(timezone.utc).isoformat(),
                    'data_source': 'fallback'
                }

        bt.logging.info(f"🔄 Loaded {len(self.companies_cache)} fallback companies")

    def _needs_refresh(self) -> bool:
        """Check if company data needs to be refreshed."""
        if not self.last_refresh:
            return True

        return datetime.now(timezone.utc) - self.last_refresh > self.cache_duration

    async def refresh_from_api(self, force: bool = False) -> bool:
        """Refresh company data from external API."""
        if not force and not self._needs_refresh():
            bt.logging.info("📋 Company data is still fresh, skipping refresh")
            return True

        try:
            bt.logging.info("🔄 Refreshing company data from external API...")

            async with self.external_api_client:
                companies_data = await self.external_api_client.get_companies_list()

            if not companies_data:
                bt.logging.error("❌ Failed to fetch companies from external API")
                return False

            self.companies_cache.clear()
            self.sectors_cache.clear()

            processed_count = 0
            for company_data in companies_data:
                try:
                    ticker = company_data.get('ticker', '').upper()
                    if not ticker:
                        continue

                    normalized_company = {
                        'ticker': ticker,
                        'companyName': company_data.get('companyName', company_data.get('companyName', '')),
                        'sector': company_data.get('sector') if company_data.get('sector') is not None else 'Unknown',
                        'exchange': company_data.get('exchange') if company_data.get('exchange') is not None else 'Unknown',
                        'marketCap': company_data.get('marketCap') if company_data.get('marketCap') is not None else 0,
                        'country': company_data.get('country', company_data.get('country', '')),
                        'countryCode': company_data.get('countryCode', company_data.get('countryCode', '')),
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                        'data_source': 'external_api'
                    }

                    self.companies_cache[ticker] = normalized_company

                    sector = normalized_company['sector']
                    if sector not in self.sectors_cache:
                        self.sectors_cache[sector] = []

                    self.sectors_cache[sector].append(ticker)

                    processed_count += 1
                except Exception as e:
                    bt.logging.warning(f"⚠️  Error processing company data: {e}")
                    continue

            self.last_refresh = datetime.now(timezone.utc)

            bt.logging.success(f"✅ Refreshed {processed_count} companies from external API")
            return True

        except Exception as e:
            bt.logging.error(f"💥 Error refreshing from API: {e}")
            return False

    def get_random_ticker(self, sector: Optional[str] = None) -> str:
        """Get a random ticker, optionally filtered by sector."""
        try:
            if sector and sector in self.sectors_cache:
                return random.choice(self.sectors_cache[sector])

            if self.companies_cache:
                return random.choice(list(self.companies_cache.keys()))

            # Fallback to hardcoded list
            all_fallback = []
            for tickers in self.fallback_companies.values():
                all_fallback.extend(tickers)
            return random.choice(all_fallback)

        except Exception as e:
            bt.logging.error(f"💥 Error getting random ticker: {e}")
            return 'AAPL'  # Ultimate fallback

    def get_company_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get company information by ticker."""
        return self.companies_cache.get(ticker.upper())

    def get_companies_by_sector(self, sector: str) -> List[str]:
        """Get all companies in a specific sector."""
        return self.sectors_cache.get(sector, [])

    def get_all_sectors(self) -> List[str]:
        """Get list of all available sectors."""
        return list(self.sectors_cache.keys())

    def get_all_tickers(self) -> List[str]:
        """Get list of all available tickers."""
        return list(self.companies_cache.keys())

    def get_popular_companies(self, limit: int = 20) -> List[str]:
        """Get popular/high market cap companies."""
        try:
            # Sort by market cap if available
            companies_with_cap = [
                (ticker, company.get('marketCap', 0))
                for ticker, company in self.companies_cache.items()
                if company.get('marketCap', 0) > 0
            ]

            if companies_with_cap:
                companies_with_cap.sort(key=lambda x: x[1], reverse=True)
                return [ticker for ticker, _ in companies_with_cap[:limit]]

            # Fallback to tech companies
            tech_companies = self.get_companies_by_sector('Technology')
            return tech_companies[:limit] if tech_companies else ['AAPL', 'MSFT', 'GOOGL']

        except Exception as e:
            bt.logging.error(f"💥 Error getting popular companies: {e}")
            return ['AAPL', 'MSFT', 'GOOGL']

    def get_emerging_companies(self, limit: int = 10) -> List[str]:
        """Get smaller/emerging companies."""
        try:
            # Companies with smaller market caps
            companies_with_cap = [
                (ticker, company.get('marketCap', 0))
                for ticker, company in self.companies_cache.items()
                if 0 < company.get('marketCap', 0) < 10_000_000_000  # Under 10B market cap
            ]

            if companies_with_cap:
                companies_with_cap.sort(key=lambda x: x[1])  # Ascending order
                return [ticker for ticker, _ in companies_with_cap[:limit]]

            # Fallback to random selection
            all_tickers = self.get_all_tickers()
            if len(all_tickers) > limit:
                return random.sample(all_tickers, limit)
            else:
                return all_tickers

        except Exception as e:
            bt.logging.error(f"💥 Error getting emerging companies: {e}")
            available_tickers = self.get_all_tickers()
            return available_tickers[:limit] if available_tickers else ['AAPL']

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        return {
            'total_companies': len(self.companies_cache),
            'sectors': len(self.sectors_cache),
            'sector_breakdown': {sector: len(tickers) for sector, tickers in self.sectors_cache.items()},
            'last_refresh': self.last_refresh.isoformat() if self.last_refresh else None,
            'cache_age_hours': (datetime.now(timezone.utc) - self.last_refresh).total_seconds() / 3600 if self.last_refresh else None,
            'needs_refresh': self._needs_refresh(),
            'data_sources': {
                'external_api': len([c for c in self.companies_cache.values() if c.get('data_source') == 'external_api']),
                'fallback': len([c for c in self.companies_cache.values() if c.get('data_source') == 'fallback'])
            }
        }

    async def validate_ticker(self, ticker: str) -> bool:
        """Validate if a ticker exists in our database."""
        ticker = ticker.upper()

        # Check cache first
        if ticker in self.companies_cache:
            return True

        # If not in cache and cache is old, try refreshing
        if self._needs_refresh():
            await self.refresh_from_api()
            return ticker in self.companies_cache

        return False

    def __len__(self) -> int:
        """Return number of companies in database."""
        return len(self.companies_cache)

    def __contains__(self, ticker: str) -> bool:
        """Check if ticker exists in database."""
        return ticker.upper() in self.companies_cache
