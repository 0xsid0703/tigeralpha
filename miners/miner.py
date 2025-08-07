import argparse
import asyncio
import traceback

import bittensor as bt

from miners.api_manager import APIManager
from miners.intelligence_provider import CompanyIntelligenceProvider
from neurons.protocol import CompanyIntelligenceProtocol, CompanyIntelligenceSynapse, IntelligenceResponse


class CompanyIntelligenceMiner:
    """Main miner class for company intelligence subnet."""

    def __init__(self):
        self.config = self.get_config()
        bt.logging(config=self.config, logging_dir=self.config.neuron.full_path)

        self.wallet = bt.wallet(config=self.config)
        self.subtensor = bt.subtensor(config=self.config)
        self.metagraph = self.subtensor.metagraph(self.config.netuid)

        self.api_manager = APIManager()
        self.intelligence_provider = CompanyIntelligenceProvider(self.api_manager)

        self.axon = bt.axon(wallet=self.wallet, config=self.config)

        self.axon.attach(
            forward_fn=self.forward,
            priority_fn=self.priority
        )

        bt.logging.info(f"Miner initialized. Wallet: {self.wallet.hotkey.ss58_address}; IP: {self.axon.ip}; Port: {self.axon.port}")

    def get_config(self) -> bt.config:
        parser = argparse.ArgumentParser()

        # Add subnet template arguments
        parser.add_argument('--netuid', type=int, default=1, help='Subnet netuid')
        parser.add_argument('--neuron.device', type=str, default='cpu', help='Device to run on')
        parser.add_argument('--neuron.epoch_length', type=int, default=100, help='Blocks until next epoch')
        parser.add_argument('--logging.debug', action='store_true', help='Enable debug logging')
        parser.add_argument('--logging.trace', action='store_true', help='Enable trace logging')

        bt.wallet.add_args(parser)
        bt.subtensor.add_args(parser)
        bt.logging.add_args(parser)
        bt.axon.add_args(parser)

        return bt.config(parser)

    async def forward(self, synapse: CompanyIntelligenceSynapse) -> CompanyIntelligenceSynapse:
        bt.logging.info("=" * 50)
        bt.logging.info(f"🔥 RECEIVED REQUEST: {synapse.ticker} - {synapse.analysis_type}")
        bt.logging.info(f"Request details: {synapse.serialize()}")

        try:
            if not CompanyIntelligenceProtocol.validate_ticker(synapse.ticker):
                bt.logging.warning(f"❌ Invalid ticker: {synapse.ticker}")

                synapse.intelligence_response = IntelligenceResponse(
                    success=False,
                    data={'company': {'ticker': synapse.ticker}},
                    errorMessage="Invalid ticker symbol"
                )

                return synapse

            intelligence_response = await self.intelligence_provider.get_intelligence(
                synapse.ticker,
                synapse.analysis_type,
                synapse.additional_params
            )

            bt.logging.info(f"🔄 Intelligence response: success={intelligence_response.success}")

            synapse.intelligence_response = intelligence_response
        except Exception as e:
            bt.logging.error(f"💥 ERROR in forward pass: {e}")
            bt.logging.error(f"💥 Traceback: {traceback.format_exc()}")

            synapse.intelligence_response = IntelligenceResponse(
                success=False,
                data={'company': {'ticker': synapse.ticker}},
                errorMessage=f"Internal error: {str(e)}"
            )

        bt.logging.info("=" * 50)

        return synapse

    def priority(self, synapse: CompanyIntelligenceSynapse) -> float:
        """Priority function to rank requests."""
        try:
            return CompanyIntelligenceProtocol.calculate_complexity_score(synapse.analysis_type)
        except Exception as e:
            bt.logging.error(f"💥 Error in priority calculation: {e}")
            return 0.0

    async def run(self):
        try:
            bt.logging.info("🚀 STARTING MINER...")

            self.axon.start()

            bt.logging.success(f"✅ Axon started successfully!")
            bt.logging.success(f"🌐 Axon listening on {self.axon.ip}:{self.axon.port}")

            bt.logging.info("📡 Serving axon to network...")
            self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)
            bt.logging.success("✅ Axon served to network!")

            step = 0

            while True:
                try:
                    # Resync metagraph
                    if step % 5 == 0:
                        self.metagraph.sync(subtensor=self.subtensor)

                        our_uid = None
                        for uid, hotkey in enumerate(self.metagraph.hotkeys):
                            if hotkey == self.wallet.hotkey.ss58_address:
                                our_uid = uid
                                break

                        if our_uid is not None:
                            bt.logging.info(f"🎯 Our UID: {our_uid}")
                            bt.logging.info(f"💰 Our stake: {self.metagraph.total_stake[our_uid]}")
                            bt.logging.info(f"🌐 Our axon: {self.metagraph.axons[our_uid].ip}:{self.metagraph.axons[our_uid].port}")

                            # Check if our axon is properly registered
                            if (self.metagraph.axons[our_uid].ip == '0.0.0.0' or self.metagraph.axons[our_uid].port == 0):
                                bt.logging.warning("⚠️ Axon not properly registered! Re-serving...")
                                self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)
                        else:
                            bt.logging.warning("⚠️ Could not find our UID in metagraph")

                    await asyncio.sleep(12)

                    step += 1

                except KeyboardInterrupt:
                    bt.logging.info("⏹️  Received interrupt signal")
                    break

                except Exception as e:
                    bt.logging.error(f"💥 Error in run loop: {e}")
                    bt.logging.error(traceback.format_exc())
                    await asyncio.sleep(60)

        finally:
            bt.logging.info("⏹️  Stopping axon...")
            self.axon.stop()

            await self.api_manager.close()
            bt.logging.info("✅ Miner stopped")


def main():
    miner = CompanyIntelligenceMiner()

    try:
        asyncio.run(miner.run())
    except KeyboardInterrupt:
        bt.logging.info("Miner interrupted by user")
    except Exception as e:
        bt.logging.error(f"Miner failed: {e}")
        bt.logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
