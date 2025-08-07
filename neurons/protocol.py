import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict

import bittensor as bt
from pydantic import BaseModel, Field

TICKER_PATTERN = pattern = r'^[A-Za-z0-9.-]{1,8}$'


class AnalysisType(str, Enum):
    CRYPTO = "crypto"
    FINANCIAL = "financial"
    SENTIMENT = "sentiment"
    NEWS = "news"


class IntelligenceResponse(BaseModel):
    """Structure for intelligence response."""
    success: bool = Field(..., description="Whether the request was successful")
    data: Dict = Field(default_factory=dict, description="Company intelligence data")
    errorMessage: str = Field("", description="Error message if any")


class CompanyIntelligenceSynapse(bt.Synapse):
    ticker: str = Field(..., description="Company ticker symbol (e.g., AAPL, MSFT)")
    analysis_type: AnalysisType = Field(AnalysisType.CRYPTO, description="Type of analysis requested")
    additional_params: dict = Field(default_factory=dict, description="Additional analysis parameters")

    intelligence_response: IntelligenceResponse = Field(
        default_factory=lambda: IntelligenceResponse(
            success=False,
            data={'company': {'ticker': ""}}
        ),
        description="Complete intelligence response"
    )

    def deserialize(self) -> "CompanyIntelligenceSynapse":
        return self

    def serialize(self) -> dict:
        return {
            "ticker": self.ticker,
            "analysis_type": self.analysis_type.value if isinstance(self.analysis_type, AnalysisType) else self.analysis_type,
            "additional_params": self.additional_params,
            "intelligence_response": self.intelligence_response.dict() if hasattr(self.intelligence_response, 'dict') else self.intelligence_response
        }


class CompanyIntelligenceProtocol:

    @staticmethod
    def validate_ticker(ticker: str) -> bool:
        if not ticker:
            return False

        if not re.match(TICKER_PATTERN, ticker):
            return False

        if ticker.startswith('.') or ticker.endswith('.'):
            return False

        if '..' in ticker or '--' in ticker:
            return False

        return True

    @staticmethod
    def calculate_complexity_score(analysis_type: AnalysisType) -> float:
        complexity_weights = {
            AnalysisType.CRYPTO: 2.0,
            AnalysisType.FINANCIAL: 1.0,
            AnalysisType.SENTIMENT: 1.2,
            AnalysisType.NEWS: 1.6
        }

        return complexity_weights.get(analysis_type, 1.0)


@dataclass
class ValidationResult:
    """Result of validating a miner response."""
    uid: int
    score: float
    response_time: float
    success: bool
    confidence: float
    error_message: str = ""
    details: Dict[str, Any] = None
