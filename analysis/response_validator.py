import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import bittensor as bt

from analysis.external_api_client import ExternalAPIClient
from analysis.validation_schemas import ValidationSchemas
from neurons.protocol import AnalysisType, CompanyIntelligenceSynapse, IntelligenceResponse


class ResponseValidator:
    """Enhanced validator with structure and API validation tiers."""

    def __init__(self):
        self.validation_history: Dict[str, List[Dict]] = {}
        self.external_api_client = ExternalAPIClient()
        self.validation_schemas = ValidationSchemas()

        # Validation weights (API validation gets more weight)
        self.structure_weight = 0.3
        self.api_validation_weight = 0.7

        # Performance tracking
        self.validation_stats = {
            'total_validations': 0,
            'structure_failures': 0,
            'api_failures': 0,
            'avg_validation_time': 0.0
        }

    async def validate_response(self, query: CompanyIntelligenceSynapse,
                                response: IntelligenceResponse,
                                response_time: float) -> float:
        """
        Main validation method with two-tier approach.

        Returns:
            Float score between 0.0 and 1.0
        """
        validation_start = datetime.now(timezone.utc)

        try:
            self.validation_stats['total_validations'] += 1

            response_dict = response.model_dump()

            structure_score = await self._validate_structure(response_dict, query.analysis_type)
            if structure_score < 0.3:
                return structure_score

            api_score = 0.0
            if structure_score > 0.3:  # Only do API validation if structure is reasonable
                api_score = await self._validate_against_api(query.ticker, query.analysis_type, response_dict)

            if api_score < 0.5:
                return min(structure_score, api_score)

            time_score = self._score_response_time(response_time)

            confidence_score = self._score_confidence(response)

            bt.logging.info(f"Scores: structure={structure_score:.3f}, api={api_score:.3f}, time={time_score:.3f}, confidence={confidence_score:.3f}")

            final_score = (
                structure_score * self.structure_weight +
                api_score * self.api_validation_weight +
                time_score * 0.15 +
                confidence_score * 0.15
            )

            self._update_validation_history(query.ticker, {
                'timestamp': validation_start,
                'structure_score': structure_score,
                'api_score': api_score,
                'time_score': time_score,
                'confidenceScore': confidence_score,
                'final_score': final_score,
                'response_time': response_time,
                'analysis_type': query.analysis_type.value
            })

            validation_duration = (datetime.now(timezone.utc) - validation_start).total_seconds()
            self._update_performance_stats(validation_duration)

            return max(0.0, min(1.0, final_score))
        except Exception as e:
            bt.logging.error(f"💥 Error validating response for {query.ticker}: {e}")
            return 0.0

    async def _validate_structure(self, response_data: Dict[str, Any],
                                  analysis_type: AnalysisType) -> float:
        """
        Tier 1: Validate response structure against JSON schemas.

        Returns:
            Float score between 0.0 and 1.0
        """
        try:
            is_valid, errors, validation_details = self.validation_schemas.validate_intelligence_response(response_data)

            if not is_valid:
                self.validation_stats['structure_failures'] += 1

                bt.logging.warning(f"⚠️ Structure validation failed: {errors[:3]}")  # Log first 3 errors

                return 0.0

            base_score = validation_details['completenessScore']

            # Additional validation for analysis-specific data
            if response_data.get('success') and response_data.get('data', {}).get('data'):
                data_valid, data_errors, data_completeness = self.validation_schemas.validate_company_data_schema(
                    response_data['data'], analysis_type.value
                )

                if data_valid:
                    base_score = (base_score + data_completeness) / 2
                else:
                    bt.logging.debug(f"⚠️ Data schema validation failed: {data_errors[:2]}")
                    base_score *= 0.8  # Reduce score for data validation issues

            # Bonus points for high-quality responses
            if response_data.get('success') and base_score > 0.8:
                base_score = min(1.0, base_score + 0.1)

            return base_score

        except Exception as e:
            bt.logging.error(f"💥 Error in structure validation: {e}")
            self.validation_stats['structure_failures'] += 1
            return 0.0

    async def _validate_against_api(self, ticker: str, analysis_type: AnalysisType, response_data: Dict[str, Any]) -> float:
        """
        Tier 2: Validate response against external API data.

        Returns:
            Float score between 0.0 and 1.0
        """
        try:
            if not response_data.get('success'):
                return 0.0

            miner_data = response_data.get('data', {})
            if not miner_data:
                return 0.0

            # Get validation data from external API using POST method
            async with self.external_api_client:
                api_result = await self.external_api_client.validate_company_data(ticker, analysis_type.value, miner_data)

            if not api_result or not api_result.get('valid'):
                self.validation_stats['api_failures'] += 1
                bt.logging.warning(f"⚠️ API validation failed for {ticker}: {api_result.get('error', 'Unknown error')}")
                return 0.0

            field_scores = api_result.get('field_scores', {})
            overall_score = api_result.get('score', 0.0)

            final_score = self._calculate_enhanced_api_score(overall_score, field_scores, analysis_type, api_result)

            bt.logging.debug(f"🌐 API validation for {ticker}: {final_score:.3f} (fields: {len(field_scores)}, overall: {overall_score:.3f})")

            return final_score

        except Exception as e:
            bt.logging.error(f"💥 Error in API validation for {ticker}: {e}")

            self.validation_stats['api_failures'] += 1

            return 0.0

    def _calculate_enhanced_api_score(self, overall_score: float, field_scores: Dict[str, float],
                                      analysis_type: AnalysisType, api_result: Dict[str, Any]) -> float:
        """Calculate enhanced API score based on field-specific scores and analysis type."""
        base_score = overall_score

        if analysis_type == AnalysisType.CRYPTO:
            base_score = self._adjust_crypto_score(base_score, field_scores)
        elif analysis_type == AnalysisType.FINANCIAL:
            base_score = self._adjust_financial_score(base_score, field_scores)
        elif analysis_type == AnalysisType.SENTIMENT:
            base_score = self._adjust_sentiment_score(base_score, field_scores)
        elif analysis_type == AnalysisType.NEWS:
            base_score = self._adjust_news_score(base_score, field_scores)

        base_score = self._apply_quality_adjustments(base_score, field_scores, api_result)

        return max(0.0, min(1.0, base_score))

    def _adjust_crypto_score(self, base_score: float, field_scores: Dict[str, float]) -> float:
        """Adjust score for crypto analysis type."""
        adjusted_score = base_score

        # Crypto-specific field importance
        crypto_fields = ['cryptoHoldings', 'totalCryptoValue', 'marketCap']
        crypto_scores = [field_scores.get(field, 0.0) for field in crypto_fields if field in field_scores]

        if crypto_scores:
            crypto_avg = sum(crypto_scores) / len(crypto_scores)
            adjusted_score = (base_score * 0.6) + (crypto_avg * 0.4)

        if field_scores.get('cryptoHoldings', 0.0) >= 0.9:
            adjusted_score += 0.1

        if field_scores.get('totalCryptoValue', 0.0) >= 0.9:
            adjusted_score += 0.1

        return adjusted_score

    def _adjust_financial_score(self, base_score: float, field_scores: Dict[str, float]) -> float:
        """Adjust score for financial analysis type."""
        adjusted_score = base_score

        financial_fields = ['marketCap', 'sharePrice']
        financial_scores = [field_scores.get(field, 0.0) for field in financial_fields if field in field_scores]

        if financial_scores:
            financial_avg = sum(financial_scores) / len(financial_scores)
            adjusted_score = (base_score * 0.5) + (financial_avg * 0.5)

        key_financial_accuracy = [
            field_scores.get('marketCap', 0.0),
            field_scores.get('sharePrice', 0.0)
        ]

        high_accuracy_count = sum(1 for score in key_financial_accuracy if score >= 0.8)
        if high_accuracy_count >= 2:
            adjusted_score += 0.05 * high_accuracy_count

        return adjusted_score

    def _adjust_sentiment_score(self, base_score: float, field_scores: Dict[str, float]) -> float:
        """Adjust score for sentiment analysis type."""
        adjusted_score = base_score

        sentiment_fields = ['sentiment', 'sentimentScore']
        sentiment_scores = [field_scores.get(field, 0.0) for field in sentiment_fields if field in field_scores]

        if sentiment_scores:
            sentiment_avg = sum(sentiment_scores) / len(sentiment_scores)
            adjusted_score = (base_score * 0.3) + (sentiment_avg * 0.7)

        if 'sentiment' in field_scores or 'sentimentScore' in field_scores:
            adjusted_score += 0.1

        return adjusted_score

    def _adjust_news_score(self, base_score: float, field_scores: Dict[str, float]) -> float:
        """Adjust score for news analysis type."""
        adjusted_score = base_score

        news_fields = ['newsArticles', 'totalArticles']
        news_scores = [field_scores.get(field, 0.0) for field in news_fields if field in field_scores]

        if news_scores:
            news_avg = sum(news_scores) / len(news_scores)
            adjusted_score = (base_score * 0.7) + (news_avg * 0.3)

        if any(field in field_scores for field in news_fields):
            adjusted_score += 0.05

        return adjusted_score

    def _apply_quality_adjustments(self, base_score: float, field_scores: Dict[str, float],
                                   api_result: Dict[str, Any]) -> float:
        """Apply general quality adjustments based on API validation results."""
        adjusted_score = base_score

        freshness_score = api_result.get('freshnessScore', 0.5)
        if freshness_score >= 0.9:
            adjusted_score += 0.05
        elif freshness_score <= 0.3:
            adjusted_score -= 0.05

        completeness_score = api_result.get('completenessScore', 0.5)
        if completeness_score >= 0.9:
            adjusted_score += 0.05
        elif completeness_score <= 0.3:
            adjusted_score -= 0.05

        field_count = len(field_scores)
        if field_count >= 8:
            adjusted_score += 0.1
        elif field_count >= 5:
            adjusted_score += 0.05
        elif field_count < 3:
            adjusted_score -= 0.1

        high_accuracy_fields = sum(1 for score in field_scores.values() if score >= 0.8)
        if high_accuracy_fields >= len(field_scores) * 0.8:
            adjusted_score += 0.1

        low_accuracy_fields = sum(1 for score in field_scores.values() if score < 0.3)
        if low_accuracy_fields >= len(field_scores) * 0.5:
            adjusted_score -= 0.1

        summary = api_result.get('summary', {})
        validation_confidence = summary.get('validationConfidence', 0.5)
        confidence_adjustment = (validation_confidence - 0.5) * 0.1
        adjusted_score += confidence_adjustment

        return adjusted_score

    def _score_response_time(self, response_time: float) -> float:
        if response_time <= 2.0:
            return 1.0
        elif response_time <= 5.0:
            return 0.9 - (response_time - 2.0) * 0.1
        elif response_time <= 10.0:
            return 0.6 - (response_time - 5.0) * 0.08
        elif response_time <= 20.0:
            return 0.2 - (response_time - 10.0) * 0.02
        else:
            return max(0.0, 0.05)

    def _score_confidence(self, response: IntelligenceResponse) -> float:
        """Score based on confidence alignment with success."""
        try:
            confidence = response.data['confidenceScore'] if 'confidenceScore' in response.data else 0.0
            success = response.success

            if not isinstance(confidence, (int, float)):
                return 0.3

            if not 0 <= confidence <= 1:
                return 0.2

            if success and confidence > 0.6:
                return 1.0
            elif success and confidence > 0.4:
                return 0.8
            elif not success and confidence < 0.4:
                return 0.9
            elif not success and confidence < 0.6:
                return 0.7
            else:
                return 0.5

        except Exception as e:
            bt.logging.warning(f"⚠️ Error scoring confidence: {e}")
            return 0.3

    def _update_validation_history(self, ticker: str, validation_data: Dict[str, Any]):
        """Update validation history for consistency tracking."""
        if ticker not in self.validation_history:
            self.validation_history[ticker] = []

        self.validation_history[ticker].append(validation_data)

        if len(self.validation_history[ticker]) > 50:
            self.validation_history[ticker] = self.validation_history[ticker][-50:]

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)
        self.validation_history[ticker] = [
            entry for entry in self.validation_history[ticker]
            if entry['timestamp'] > cutoff_time
        ]

    def _update_performance_stats(self, validation_duration: float):
        """Update performance statistics."""
        current_avg = self.validation_stats['avg_validation_time']
        total_validations = self.validation_stats['total_validations']

        self.validation_stats['avg_validation_time'] = (
            (current_avg * (total_validations - 1) + validation_duration) / total_validations
        )

    async def validate_batch_responses(self, queries_and_responses: List[Tuple[CompanyIntelligenceSynapse, IntelligenceResponse, float]]) -> List[float]:
        """Validate multiple responses in batch for efficiency."""
        tasks = []

        for query, response, response_time in queries_and_responses:
            task = self.validate_response(query, response, response_time)
            tasks.append(task)

        try:
            scores = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle any exceptions
            final_scores = []
            for i, score in enumerate(scores):
                if isinstance(score, Exception):
                    bt.logging.error(f"💥 Batch validation error for query {i}: {score}")
                    final_scores.append(0.0)
                else:
                    final_scores.append(score)

            return final_scores

        except Exception as e:
            bt.logging.error(f"💥 Error in batch validation: {e}")
            return [0.0] * len(queries_and_responses)

    def clear_validation_history(self, older_than_days: int = 30):
        """Clear old validation history to manage memory usage."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=older_than_days)

        tickers_to_remove = []
        for ticker, entries in self.validation_history.items():
            # Filter out old entries
            self.validation_history[ticker] = [
                entry for entry in entries
                if entry['timestamp'] > cutoff_time
            ]

            # Remove ticker if no entries remain
            if not self.validation_history[ticker]:
                tickers_to_remove.append(ticker)

        for ticker in tickers_to_remove:
            del self.validation_history[ticker]

        bt.logging.info(f"🗑️ Cleared validation history: removed {len(tickers_to_remove)} empty tickers, "
                        f"kept history for {len(self.validation_history)} tickers")

    def set_validation_weights(self, structure_weight: float, api_weight: float):
        """Update validation weights (must sum to 1.0)."""
        if abs(structure_weight + api_weight - 1.0) > 0.01:
            raise ValueError("Structure and API weights must sum to 1.0")

        self.structure_weight = structure_weight
        self.api_validation_weight = api_weight

        bt.logging.info(f"🎛️ Updated validation weights: structure={structure_weight:.2f}, api={api_weight:.2f}")
