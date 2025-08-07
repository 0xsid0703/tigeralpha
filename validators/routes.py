import time
import traceback
from typing import Callable, List, Optional

import bittensor as bt
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from config.config import appConfig as config
from neurons.protocol import AnalysisType, CompanyIntelligenceSynapse, IntelligenceResponse, ValidationResult
from validators.validator import CompanyIntelligenceValidator


class QueryRequest(BaseModel):
    """HTTP request model for company intelligence queries."""
    ticker: str = Field(..., description="Company ticker symbol", min_length=1, max_length=10)
    analysis_type: str = Field(..., description="Type of analysis: crypto, financial, sentiment, news")
    timeframe: Optional[str] = Field(None, description="Analysis timeframe")
    specific_metrics: Optional[List[str]] = Field(None, description="Specific metrics to analyze")
    custom_parameters: Optional[dict] = Field(None, description="Custom analysis parameters")

    class Config:
        schema_extra = {
            "example": {
                "ticker": "BTC",
                "analysis_type": "crypto",
                "timeframe": "1y",
                "specific_metrics": ["price", "volume"],
                "custom_parameters": {"include_sentiment": True}
            }
        }


class MinerResponse(BaseModel):
    """Response from a single miner."""
    uid: int
    score: float
    response_time: float
    success: bool
    confidence: float
    data: Optional[dict] = None
    error_message: Optional[str] = None


class QueryResponse(BaseModel):
    """HTTP response model for company intelligence queries."""
    query_id: str
    ticker: str
    analysis_type: str
    total_miners_queried: int
    successful_responses: int
    responses: List[MinerResponse]
    average_response_time: float
    best_response: Optional[MinerResponse] = None


async def verify_bearer_token(authorization: str = Header(None)):
    """Verify Bearer token authentication."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format. Use 'Bearer <token>'")

    token = authorization.split(" ", 1)[1]

    if not config.API_TOKEN:
        raise HTTPException(status_code=500, detail="API token not configured")

    if token != config.API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

    return token


def create_validator_routes(get_validator: Callable):
    """Create FastAPI router with validator routes."""
    router = APIRouter()

    @router.post("/query", response_model=QueryResponse)
    async def query_miners(
        request: QueryRequest,
        token: str = Depends(verify_bearer_token)
    ):
        """Send a query to miners and return aggregated results."""
        validator: CompanyIntelligenceValidator = get_validator()

        try:
            bt.logging.info(f"📝 Received HTTP query: {request.ticker} - {request.analysis_type}")

            # Validate analysis type
            try:
                analysis_type_enum = AnalysisType(request.analysis_type.lower())
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid analysis_type. Must be one of: {[e.value for e in AnalysisType]}"
                )

            # Convert HTTP request to internal query format
            query = CompanyIntelligenceSynapse(
                ticker=request.ticker.upper(),
                analysis_type=analysis_type_enum,
                additional_params={
                    "timeframe": request.timeframe if request.timeframe else {},
                    "specific_metrics": request.specific_metrics if request.specific_metrics else {},
                    "custom_parameters": request.custom_parameters if request.custom_parameters else {}
                }
            )

            # Get available miners
            miner_uids = validator.get_available_miners()
            if not miner_uids:
                raise HTTPException(status_code=503, detail="No miners available")

            # Limit concurrent queries for HTTP requests
            max_miners = min(len(miner_uids), 20)  # Configurable limit
            selected_uids = miner_uids[:max_miners]

            bt.logging.info(f"📤 Querying {len(selected_uids)} miners...")

            # Send queries to miners
            axons = [validator.metagraph.axons[uid] for uid in selected_uids]
            batch_start_time = time.time()

            responses = await validator.dendrite(
                axons=axons,
                synapse=query,
                deserialize=True,
                timeout=validator.config.neuron.timeout
            )

            batch_end_time = time.time()
            avg_response_time = (batch_end_time - batch_start_time) / len(selected_uids)

            bt.logging.info(f"📥 Received {len(responses)} responses in {avg_response_time:.2f}s avg")

            # Process and validate responses
            miner_responses = []
            successful_responses = 0

            for uid, response in zip(selected_uids, responses):
                bt.logging.info(f"🔍 Processing response from miner {uid}: {response}")

                try:
                    if hasattr(response, 'intelligence_response'):
                        intel_response: IntelligenceResponse = response.intelligence_response

                        # Validate response
                        score = await validator.response_validator.validate_response(
                            query, intel_response, avg_response_time
                        )

                        miner_response = MinerResponse(
                            uid=uid,
                            score=score,
                            response_time=avg_response_time,
                            success=intel_response.success,
                            confidence=intel_response.data['confidenceScore'] if 'confidenceScore' in intel_response.data else 0.0,
                            data=intel_response.data
                        )

                        if intel_response.success:
                            successful_responses += 1

                    else:
                        miner_response = MinerResponse(
                            uid=uid,
                            score=0.0,
                            response_time=avg_response_time,
                            success=False,
                            confidence=0.0,
                            error_message="No intelligence response received"
                        )

                    miner_responses.append(miner_response)

                except Exception as e:
                    bt.logging.error(f"💥 Error processing response from miner {uid}: {e}")
                    miner_responses.append(MinerResponse(
                        uid=uid,
                        score=0.0,
                        response_time=avg_response_time,
                        success=False,
                        confidence=0.0,
                        error_message=str(e)
                    ))

            # Find best response
            best_response = None
            if miner_responses:
                successful_responses_list = [r for r in miner_responses if r.success]
                if successful_responses_list:
                    best_response = max(successful_responses_list, key=lambda x: x.score)

            # Create response
            query_response = QueryResponse(
                query_id=f"query_{int(time.time())}_{request.ticker}",
                ticker=request.ticker,
                analysis_type=request.analysis_type,
                total_miners_queried=len(selected_uids),
                successful_responses=successful_responses,
                responses=miner_responses,
                average_response_time=avg_response_time,
                best_response=best_response
            )

            # Update validator scores (optional for HTTP requests)
            validation_results = [
                ValidationResult(
                    uid=r.uid,
                    score=r.score,
                    response_time=r.response_time,
                    success=r.success,
                    confidence=r.confidence,
                    error_message=r.error_message
                )
                for r in miner_responses
            ]

            # Update incentive mechanism
            validator.incentive_mechanism.update_scores(validation_results)

            bt.logging.info(f"✅ HTTP query completed: {successful_responses}/{len(selected_uids)} successful")

            return query_response

        except HTTPException:
            raise
        except Exception as e:
            bt.logging.error(f"💥 Error processing HTTP query: {e}")
            bt.logging.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    @router.get("/miners")
    async def get_miners(token: str = Depends(verify_bearer_token)):
        """Get list of available miners."""
        validator = get_validator()

        try:
            miner_uids = validator.get_available_miners()

            miners_info = []
            for uid in miner_uids:
                axon = validator.metagraph.axons[uid]
                stake = validator.metagraph.total_stake[uid]

                miners_info.append({
                    "uid": uid,
                    "ip": axon.ip,
                    "port": axon.port,
                    "stake": float(stake),
                    "hotkey": validator.metagraph.hotkeys[uid]
                })

            return {
                "total_miners": len(miners_info),
                "miners": miners_info
            }

        except Exception as e:
            bt.logging.error(f"💥 Error getting miners: {e}")
            raise HTTPException(status_code=500, detail=f"Error getting miners: {str(e)}")

    @router.get("/scores")
    async def get_scores(token: str = Depends(verify_bearer_token)):
        """Get current miner scores."""
        validator = get_validator()

        try:
            scores = validator.incentive_mechanism.get_scores()

            return {
                "scores": scores,
                "last_updated": validator.last_update.isoformat()
            }

        except Exception as e:
            bt.logging.error(f"💥 Error getting scores: {e}")
            raise HTTPException(status_code=500, detail=f"Error getting scores: {str(e)}")

    return router
