import asyncio
from contextlib import asynccontextmanager

import bittensor as bt
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from routes import create_validator_routes

from config.config import appConfig as config
from validators.validator import CompanyIntelligenceValidator

validator_instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global validator_instance

    try:
        validator_instance = CompanyIntelligenceValidator()
        validator_instance.load_state()

        validator_task = asyncio.create_task(validator_instance.run_background())

        bt.logging.info("🌐 Starting HTTP server...")

        yield

    except Exception as e:
        bt.logging.error(f"💥 Error during startup: {e}")
        raise
    finally:
        if validator_instance:
            validator_instance.save_state()

            if 'validator_task' in locals():
                validator_task.cancel()
                try:
                    await validator_task
                except asyncio.CancelledError:
                    pass

        bt.logging.info("✅ Server shutdown complete")


app = FastAPI(
    title="Company Intelligence Validator API",
    description="HTTP API for Bittensor Company Intelligence Subnet Validator",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/status")
async def get_status():
    return {"status": "ok"}


@app.get("/info")
async def get_info():
    global validator_instance

    if not validator_instance:
        raise HTTPException(status_code=503, detail="Validator not initialized")

    try:
        available_miners = validator_instance.get_available_miners()

        status = {
            "status": "healthy",
            "validator_address": validator_instance.wallet.hotkey.ss58_address,
            "netuid": validator_instance.config.netuid,
            "current_step": validator_instance.step,
            "available_miners": len(available_miners),
            "miner_uids": available_miners,
            "last_update": validator_instance.last_update.isoformat(),
            "metagraph_block": validator_instance.metagraph.block.item() if hasattr(validator_instance.metagraph.block, 'item') else int(validator_instance.metagraph.block),
        }

        return status

    except Exception as e:
        bt.logging.error(f"💥 Error getting validator status: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting validator status: {str(e)}")


def get_validator():
    """Get the global validator instance."""
    global validator_instance

    if not validator_instance:
        raise HTTPException(status_code=503, detail="Validator not initialized")

    return validator_instance


app.include_router(create_validator_routes(get_validator), prefix="/validator")


def main():
    bt.logging.info(f"🌐 Starting server on {config.VALIDATOR_HOST}:{config.VALIDATOR_PORT}")

    try:
        uvicorn.run(
            "server:app",
            host=config.VALIDATOR_HOST,
            port=config.VALIDATOR_PORT,
            log_level=config.VALIDATOR_LOG_LEVEL,
            reload=False,  # Set to True for development
            access_log=True
        )
    except KeyboardInterrupt:
        bt.logging.info("⏹️ Server interrupted by user")
    except Exception as e:
        bt.logging.error(f"💥 Server error: {e}")


if __name__ == "__main__":
    main()
