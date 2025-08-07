import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import bittensor as bt

from analysis.company_database import CompanyDatabase
from neurons.protocol import AnalysisType, CompanyIntelligenceSynapse


class EnhancedQueryGenerator:
    """Advanced query generator using real company data."""

    def __init__(self, company_db: CompanyDatabase):
        self.company_db = company_db
        self.query_history: List[Dict[str, Any]] = []
        self.max_history = 5000

        self.strategy_weights = {
            'popular_companies': 0.4,  # Large, well-known companies
            'emerging_companies': 0.2,  # Smaller companies
            'sector_focused': 0.15,    # Sector-specific queries
            'crypto_focused': 0.15,    # Companies with crypto exposure
            'random_selection': 0.1    # Random picks
        }

        # Analysis type distribution
        self.analysis_weights = {
            AnalysisType.CRYPTO: 0.35,      # Primary focus
            AnalysisType.FINANCIAL: 0.35,   # Core financial data
            AnalysisType.SENTIMENT: 0.15,   # Market sentiment
            AnalysisType.NEWS: 0.15         # News analysis
        }

        self.recent_tickers: Set[str] = set()
        self.sector_rotation = {}
        self.last_strategy_used = None

    async def generate_query(self, organic: bool = False,
                             preferred_analysis: Optional[AnalysisType] = None,
                             preferred_sector: Optional[str] = None) -> CompanyIntelligenceSynapse:
        """
        Generate a sophisticated query based on various strategies.

        Args:
            organic: If True, focus on high-value / popular companies
            preferred_analysis: Force specific analysis type
            preferred_sector: Force specific sector
        """
        try:
            if len(self.company_db) == 0:
                bt.logging.warning("⚠️ Company database is empty, refreshing ...")
                await self.company_db.refresh_from_api()

            analysis_type = preferred_analysis or self._choose_analysis_type()
            strategy = self._choose_query_strategy(organic)

            ticker = await self._generate_ticker_by_strategy(strategy, preferred_sector)
            additional_params = self._generate_additional_params(analysis_type, strategy)

            query = CompanyIntelligenceSynapse(
                ticker=ticker,
                analysis_type=analysis_type,
                additional_params=additional_params
            )

            self._record_query(ticker, analysis_type, strategy, organic)

            bt.logging.info(f"📝 Generated {strategy} query: {ticker} - {analysis_type.value}")

            return query

        except Exception as e:
            bt.logging.error(f"💥 Error generating query: {e}")
            return self._generate_fallback_query()

    def _choose_analysis_type(self) -> AnalysisType:
        """Choose analysis type based on weights and recent history."""

        adjusted_weights = self.analysis_weights.copy()

        # Get recent analysis types (last 10 queries)
        recent_analyses = [
            entry['analysis_type'] for entry in self.query_history[-10:]
            if 'analysis_type' in entry
        ]

        # Reduce weight for overused analysis types
        if recent_analyses:
            for analysis_type in recent_analyses:
                if analysis_type in adjusted_weights:
                    adjusted_weights[analysis_type] *= 0.8

        # Normalize weights
        total_weight = sum(adjusted_weights.values())
        if total_weight > 0:
            for key in adjusted_weights:
                adjusted_weights[key] /= total_weight

        return random.choices(
            list(adjusted_weights.keys()),
            weights=list(adjusted_weights.values())
        )[0]

    def _choose_query_strategy(self, organic: bool) -> str:
        """Choose query generation strategy."""
        if organic:
            # For organic queries, favor popular companies
            organic_weights = {
                'popular_companies': 0.6,
                'crypto_focused': 0.3,
                'sector_focused': 0.1
            }
            strategies = list(organic_weights.keys())
            weights = list(organic_weights.values())
        else:
            # Use full strategy distribution for synthetic queries
            strategies = list(self.strategy_weights.keys())
            weights = list(self.strategy_weights.values())

            # Avoid repeating the same strategy too often
            if self.last_strategy_used in strategies:
                idx = strategies.index(self.last_strategy_used)
                weights[idx] *= 0.5  # Reduce weight for last used strategy

                total_weight = sum(weights)
                weights = [w / total_weight for w in weights]

        strategy = random.choices(strategies, weights=weights)[0]
        self.last_strategy_used = strategy

        return strategy

    async def _generate_ticker_by_strategy(self, strategy: str, preferred_sector: Optional[str] = None) -> str:
        """Generate ticker based on chosen strategy."""
        try:
            if strategy == 'popular_companies':
                return await self._get_popular_company_ticker()

            elif strategy == 'emerging_companies':
                return await self._get_emerging_company_ticker()

            elif strategy == 'sector_focused':
                return await self._get_sector_focused_ticker(preferred_sector)

            elif strategy == 'crypto_focused':
                return await self._get_crypto_focused_ticker()

            elif strategy == 'random_selection':
                return await self._get_random_ticker()

            else:
                bt.logging.warning(f"⚠️ Unknown strategy: {strategy}, using random selection")
                return await self._get_random_ticker()

        except Exception as e:
            bt.logging.error(f"💥 Error in strategy {strategy}: {e}")
            return await self._get_random_ticker()

    async def _get_popular_company_ticker(self) -> str:
        """Get ticker from popular / high market cap companies."""
        popular_tickers = self.company_db.get_popular_companies(limit=50)

        if not popular_tickers:
            return self.company_db.get_random_ticker()

        # Avoid recently used tickers if possible
        available_tickers = [t for t in popular_tickers if t not in self.recent_tickers]

        if available_tickers:
            return random.choice(available_tickers)
        else:
            return random.choice(popular_tickers)

    async def _get_emerging_company_ticker(self) -> str:
        """Get ticker from smaller / emerging companies."""
        emerging_tickers = self.company_db.get_emerging_companies(limit=30)

        if not emerging_tickers:
            return self.company_db.get_random_ticker()

        available_tickers = [t for t in emerging_tickers if t not in self.recent_tickers]

        if available_tickers:
            return random.choice(available_tickers)
        else:
            return random.choice(emerging_tickers)

    async def _get_sector_focused_ticker(self, preferred_sector: Optional[str] = None) -> str:
        """Get ticker from a specific sector with rotation."""
        sectors = self.company_db.get_all_sectors()

        if not sectors:
            return self.company_db.get_random_ticker()

        if preferred_sector and preferred_sector in sectors:
            chosen_sector = preferred_sector
        else:
            underused_sectors = [
                sector for sector in sectors
                if self.sector_rotation.get(sector, 0) < 3
            ]

            if underused_sectors:
                chosen_sector = random.choice(underused_sectors)
            else:
                chosen_sector = random.choice(sectors)
                self.sector_rotation = {}

        self.sector_rotation[chosen_sector] = self.sector_rotation.get(chosen_sector, 0) + 1

        sector_companies = self.company_db.get_companies_by_sector(chosen_sector)

        if sector_companies:
            return random.choice(sector_companies)
        else:
            return self.company_db.get_random_ticker()

    async def _get_crypto_focused_ticker(self) -> str:
        """Get ticker from companies with known crypto exposure."""
        # Known crypto-exposed companies
        crypto_tickers = ['MSTR', 'TSLA', 'COIN', 'RIOT', 'MARA', 'CLSK', 'HUT', 'BITF', 'SQ', 'PYPL', 'NVDA', 'AMD']

        available_crypto_tickers = [
            ticker for ticker in crypto_tickers
            if ticker in self.company_db
        ]

        if available_crypto_tickers:
            unused_tickers = [t for t in available_crypto_tickers if t not in self.recent_tickers]
            if unused_tickers:
                return random.choice(unused_tickers)
            else:
                return random.choice(available_crypto_tickers)
        else:
            tech_companies = self.company_db.get_companies_by_sector('Technology')
            if tech_companies:
                return random.choice(tech_companies)
            else:
                return self.company_db.get_random_ticker()

    async def _get_random_ticker(self) -> str:
        return self.company_db.get_random_ticker()

    def _generate_additional_params(self, analysis_type: AnalysisType, strategy: str) -> Dict[str, Any]:
        """Generate additional parameters based on analysis type and strategy."""
        params = {}

        if analysis_type == AnalysisType.CRYPTO:
            params.update({
                'currentHoldings': True,
                'historicalHoldings': random.choice([True, False])
            })

        elif analysis_type == AnalysisType.FINANCIAL:
            params.update({
                'fields': random.sample([
                    'address', 'country', 'countryCode', 'currency', 'description', 'exchange',
                    'industry', 'marketCap', 'companyName', 'website', 'sector', 'symbol',
                    'sharesFloat', 'sharesOutstanding'
                ], k=random.randint(3, 5))
            })

        elif analysis_type == AnalysisType.SENTIMENT:
            params.update({
                'timeframe': random.choice(['1D', '7D', '30D']),
                'sources': random.choice([
                    ['social', 'news'],
                    ['news', 'analyst'],
                    ['social', 'news', 'analyst']
                ])
            })

        elif analysis_type == AnalysisType.NEWS:
            params.update({
                'max_articles': random.randint(5, 20),
                'timeframe': random.choice(['1D', '3D', '7D', '14D']),
                'include_sentiment': True
            })

        return params

    def _record_query(self, ticker: str, analysis_type: AnalysisType, strategy: str, organic: bool):
        """Record query in history for pattern tracking."""
        query_record = {
            'ticker': ticker,
            'analysis_type': analysis_type.value,
            'strategy': strategy,
            'organic': organic,
            'timestamp': datetime.now(timezone.utc),
            'company_info': self.company_db.get_company_info(ticker)
        }

        self.query_history.append(query_record)

        self.recent_tickers.add(ticker)
        if len(self.recent_tickers) > 20:
            oldest_tickers = [
                entry['ticker'] for entry in self.query_history[-50:-20]
            ]

            for old_ticker in oldest_tickers:
                self.recent_tickers.discard(old_ticker)

        if len(self.query_history) > self.max_history:
            self.query_history = self.query_history[-self.max_history:]

    def _generate_fallback_query(self) -> CompanyIntelligenceSynapse:
        """Generate a simple fallback query when main generation fails."""
        fallback_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        ticker = random.choice(fallback_tickers)
        analysis_type = AnalysisType.FINANCIAL

        return CompanyIntelligenceSynapse(
            ticker=ticker,
            analysis_type=analysis_type,
            additional_params={'fallback': True}
        )

    def get_query_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive query generation statistics."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        recent_queries = [
            q for q in self.query_history
            if q['timestamp'] > cutoff_time
        ]

        if not recent_queries:
            return {'error': f'No queries in the last {hours} hours'}

        analysis_dist = {}
        strategy_dist = {}
        ticker_counts = {}
        sector_dist = {}

        for query in recent_queries:
            analysis_type = query['analysis_type']
            analysis_dist[analysis_type] = analysis_dist.get(analysis_type, 0) + 1

            strategy = query['strategy']
            strategy_dist[strategy] = strategy_dist.get(strategy, 0) + 1

            ticker = query['ticker']
            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1

            company_info = query.get('company_info', {})
            sector = company_info.get('sector', 'Unknown')
            sector_dist[sector] = sector_dist.get(sector, 0) + 1

        unique_tickers = len(ticker_counts)
        total_queries = len(recent_queries)
        ticker_diversity = unique_tickers / total_queries if total_queries > 0 else 0

        return {
            'time_period_hours': hours,
            'total_queries': total_queries,
            'unique_tickers': unique_tickers,
            'ticker_diversity_ratio': ticker_diversity,
            'distributions': {
                'analysis_types': analysis_dist,
                'strategies': strategy_dist,
                'sectors': sector_dist
            },
            'top_tickers': sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            'organic_queries': sum(1 for q in recent_queries if q.get('organic', False)),
            'synthetic_queries': sum(1 for q in recent_queries if not q.get('organic', False))
        }

    def adjust_strategy_weights(self, new_weights: Dict[str, float]):
        """Adjust strategy weights for query generation."""

        if abs(sum(new_weights.values()) - 1.0) > 0.01:
            raise ValueError("Strategy weights must sum to 1.0")

        for strategy in new_weights:
            if strategy not in self.strategy_weights:
                raise ValueError(f"Unknown strategy: {strategy}")

        self.strategy_weights.update(new_weights)

        bt.logging.info(f"🎛️ Updated strategy weights: {new_weights}")

    def adjust_analysis_weights(self, new_weights: Dict[AnalysisType, float]):
        """Adjust analysis type weights."""

        if abs(sum(new_weights.values()) - 1.0) > 0.01:
            raise ValueError("Analysis weights must sum to 1.0")

        self.analysis_weights.update(new_weights)

        bt.logging.info(f"🎛️ Updated analysis weights: {new_weights}")

    def clear_query_history(self, older_than_days: int = 7):
        """Clear old query history to manage memory."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=older_than_days)

        original_count = len(self.query_history)
        self.query_history = [
            query for query in self.query_history
            if query['timestamp'] > cutoff_time
        ]

        removed_count = original_count - len(self.query_history)
        bt.logging.info(f"🗑️ Cleared {removed_count} old query records, kept {len(self.query_history)}")

        self.recent_tickers.clear()
        self.sector_rotation = {}
