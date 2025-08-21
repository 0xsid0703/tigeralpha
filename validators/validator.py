import argparse
import asyncio
import json
import os
import random
import time
import traceback
from datetime import datetime, timezone
from typing import List

import bittensor as bt

from analysis.company_database import CompanyDatabase
from analysis.incentive_mechanism import IncentiveMechanism
from analysis.query_generator import EnhancedQueryGenerator
from analysis.response_validator import ResponseValidator
from config.config import appConfig as config
from neurons.protocol import AnalysisType, CompanyIntelligenceSynapse, IntelligenceResponse, ValidationResult


class CompanyIntelligenceValidator:
    """Validator class for company intelligence subnet."""

    def __init__(self):
        self.config = self.get_config()
        bt.logging(config=self.config, logging_dir=self.config.neuron.full_path)

        os.makedirs(config.DATA_DIRECTORY, exist_ok=True)

        self.wallet = bt.wallet(config=self.config)
        self.subtensor = bt.subtensor(config=self.config)
        self.dendrite = bt.dendrite(wallet=self.wallet)
        self.metagraph = self.subtensor.metagraph(self.config.netuid)

        self.company_db = CompanyDatabase(config.COMPANY_CACHE_DURATION_HOURS)

        self.query_generator = EnhancedQueryGenerator(self.company_db)
        self.response_validator = ResponseValidator()
        self.incentive_mechanism = IncentiveMechanism()

        self.response_validator.set_validation_weights(
            config.STRUCTURE_VALIDATION_WEIGHT,
            config.API_VALIDATION_WEIGHT
        )

        self.query_generator.adjust_strategy_weights(config.get_strategy_weights())
        self.query_generator.adjust_analysis_weights(config.get_analysis_weights())

        self.step = 0
        self.last_update = datetime.now(timezone.utc)
        self._database_initialized = False

        bt.logging.info(f"🚀 Enhanced Validator initialized. Wallet: {self.wallet.hotkey.ss58_address}")

    def get_config(self) -> bt.config:
        parser = argparse.ArgumentParser()

        parser.add_argument('--netuid', type=int, default=1, help='Subnet netuid')
        parser.add_argument('--neuron.device', type=str, default='cpu', help='Device to run on')
        parser.add_argument('--neuron.epoch_length', type=int, default=100, help='Blocks until next epoch')
        parser.add_argument('--neuron.num_concurrent_forwards', type=int, default=1, help='Concurrent forwards')
        parser.add_argument('--neuron.sample_size', type=int, default=12, help='Sample size for validation')
        parser.add_argument('--neuron.timeout', type=int, default=config.MINER_TIMEOUT, help='Request timeout')
        parser.add_argument('--logging.debug', action='store_true', help='Enable debug logging')

        parser.add_argument('--validator.max_concurrent_miners', type=int, default=config.MAX_CONCURRENT_MINERS, help='Max concurrent miner queries')

        bt.wallet.add_args(parser)
        bt.subtensor.add_args(parser)
        bt.logging.add_args(parser)

        return bt.config(parser)

    async def forward(self) -> List[ValidationResult]:
        """Enhanced forward pass with improved query generation and validation."""
        bt.logging.info("=" * 60)
        bt.logging.info("🚀 STARTING VALIDATION ROUND")

        await self._ensure_database_initialized()

        miner_uids = self.get_available_miners()
        if not miner_uids:
            bt.logging.warning("❌ No miners available for validation")
            return []

        # Sample miners
        max_miners = min(self.config.validator.max_concurrent_miners, len(miner_uids))
        sample_size = min(self.config.neuron.sample_size, max_miners)
        sampled_uids = random.sample(miner_uids, sample_size)

        if self.step == 0:
            # Send initial query to all miners
            sampled_uids = miner_uids

        # Generate intelligent query
        organic = self.step % 10 == 0
        try:
            query = await self.query_generator.generate_query(organic=organic)

            bt.logging.info(f"📝 Generated {('organic' if organic else 'synthetic')} query: {query.ticker} - {query.analysis_type.value}")
        except Exception as e:
            bt.logging.error(f"💥 Error generating query: {e}")
            query = CompanyIntelligenceSynapse(
                ticker="TSLA",
                analysis_type=AnalysisType.CRYPTO,
                additional_params={"fallback": True}
            )

        # Send query to miners
        axons = [self.metagraph.axons[uid] for uid in sampled_uids]
        bt.logging.info(f"📤 Sending queries to {len(sampled_uids)} miners...")

        batch_start_time = time.time()

        try:
            responses = await self.dendrite(
                axons=axons,
                synapse=query,
                deserialize=True,
                timeout=self.config.neuron.timeout
            )
            bt.logging.info(f"📥 Received {len(responses)} responses")

        except Exception as e:
            bt.logging.error(f"💥 Error sending queries: {e}")
            bt.logging.error(f"💥 Traceback: {traceback.format_exc()}")
            return []

        batch_end_time = time.time()
        avg_response_time = (batch_end_time - batch_start_time) / len(sampled_uids) if sampled_uids else 0
        bt.logging.info(f"⏱️ Average response time: {avg_response_time:.2f}s")

        validation_results = []

        queries_and_responses = []
        for uid, response in zip(sampled_uids, responses):
            if hasattr(response, 'intelligence_response'):
                queries_and_responses.append((query, response.intelligence_response, avg_response_time))
            else:
                # Handle invalid responses
                dummy_response = IntelligenceResponse(
                    success=False,
                    data={'company': {'ticker': query.ticker}},
                    errorMessage="No intelligence response received"
                )
                queries_and_responses.append((query, dummy_response, avg_response_time))

        try:
            scores = await self.response_validator.validate_batch_responses(queries_and_responses)

            for i, (uid, score) in enumerate(zip(sampled_uids, scores)):
                query_item, response_item, response_time = queries_and_responses[i]

                validation_result = ValidationResult(
                    uid=uid,
                    score=score,
                    response_time=response_time,
                    success=response_item.success,
                    confidence=response_item.data['confidenceScore'] if 'confidenceScore' in response_item.data else 0.0
                )

                validation_results.append(validation_result)

                bt.logging.info(f"✅ Miner {uid}: score={score:.3f}, time={response_time:.2f}s, success={response_item.success}")
        except Exception as e:
            bt.logging.error(f"💥 Error in enhanced validation: {e}")
            bt.logging.error(traceback.format_exc())

            for uid in sampled_uids:
                validation_results.append(ValidationResult(
                    uid=uid,
                    score=0.0,
                    response_time=avg_response_time,
                    success=False,
                    confidence=0.0,
                    error_message="Validation error"
                ))

        bt.logging.info(f"📊 Validation complete: {len(validation_results)} results")
        bt.logging.info("=" * 60)

        return validation_results

    def get_available_miners(self) -> List[int]:
        ip_port_map = {}

        for uid in range(len(self.metagraph.hotkeys)):
            try:
                axon = self.metagraph.axons[uid]
                stake = self.metagraph.total_stake[uid]
                is_available = (
                    axon.ip != '0.0.0.0' and
                    axon.port > 0 and
                    stake > 0 and
                    uid != self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
                )

                if is_available:
                    # Deduplicate by IP, keep lowest port
                    if axon.ip not in ip_port_map or axon.port < self.metagraph.axons[ip_port_map[axon.ip]].port:
                        ip_port_map[axon.ip] = uid
            except Exception as e:
                bt.logging.debug(f"⚠️ Error checking miner {uid}: {e}")
                continue

        available_uids = list(ip_port_map.values())

        bt.logging.info(f"📊 Found {len(available_uids)} valid available miners")

        return available_uids

    def set_weights(self, validation_results: List[ValidationResult]):
        if not validation_results:
            bt.logging.warning("❌ No validation results to set weights")
            return

        self.incentive_mechanism.update_scores(validation_results)

        all_miner_uids = self.get_available_miners()
        if not all_miner_uids:
            bt.logging.warning("❌ No miners available for weight setting")
            return

        weights = self.incentive_mechanism.calculate_weights(all_miner_uids)

        try:
            bt.logging.info(f"⚖️ Setting weights for {len(all_miner_uids)} UIDs...")

            result = self.subtensor.set_weights(
                wallet=self.wallet,
                netuid=self.config.netuid,
                uids=all_miner_uids,
                weights=weights,
                wait_for_inclusion=True
            )

            if result:
                bt.logging.success(f"✅ Successfully set weights for {len(all_miner_uids)} UIDs")

                miner_weight_pairs = list(zip(all_miner_uids, weights))
                top_performers = sorted(miner_weight_pairs, key=lambda x: x[1], reverse=True)[:15]
                bt.logging.info(f"🏆 Top performers: {top_performers}")
            else:
                bt.logging.error("❌ Failed to set weights")

        except Exception as e:
            bt.logging.error(f"💥 Error setting weights: {e}")
            bt.logging.error(traceback.format_exc())

    async def _ensure_database_initialized(self):
        if not self._database_initialized:
            try:
                await self.company_db.initialize()
                self._database_initialized = True

                bt.logging.info(f"📊 Company database initialized with {len(self.company_db)} companies")
            except Exception as e:
                bt.logging.warning(f"⚠️ Database initialization failed: {e}, using fallback data")
                self._database_initialized = True

        bt.logging.info(f"Enable refresh: {config.ENABLE_COMPANY_REFRESH}")
        bt.logging.info(f"Needs refresh: {self.company_db._needs_refresh()}")

        """Perform periodic maintenance tasks."""
        try:
            if config.ENABLE_COMPANY_REFRESH:
                needs_refresh = self.company_db._needs_refresh()
                if needs_refresh:
                    bt.logging.info("🔄 Performing scheduled company database refresh ...")

                    success = await self.company_db.refresh_from_api()
                    if success:
                        bt.logging.success("✅ Company database refreshed successfully")
                    else:
                        bt.logging.warning("⚠️ Company database refresh failed")

            if self.step % 100 == 0:
                bt.logging.info("🗑️ Performing maintenance cleanup ...")

                self.response_validator.clear_validation_history(
                    older_than_days=config.VALIDATION_HISTORY_RETENTION_DAYS
                )
                self.query_generator.clear_query_history(
                    older_than_days=config.QUERY_HISTORY_RETENTION_DAYS
                )
        except Exception as e:
            bt.logging.error(f"💥 Error in periodic maintenance: {e}")

    def save_state(self):
        """Save validator state for persistence (in-memory only)."""
        try:
            if not config.SAVE_VALIDATION_DETAILS:
                return

            stats = {
                'step': self.step,
                'last_update': self.last_update.isoformat(),
                'query_stats': self.query_generator.get_query_statistics(),
                'company_db_stats': self.company_db.get_database_stats(),
            }

            state_file = os.path.join(config.DATA_DIRECTORY, 'validator_state.json')
            with open(state_file, 'w') as f:
                json.dump(stats, f, indent=2, default=str)

            bt.logging.info("💾 Validator state saved to JSON")

        except Exception as e:
            bt.logging.warning(f"⚠️ Error saving state: {e}")

    def load_state(self):
        """Load validator state from persistence (in-memory only)."""
        try:
            state_file = os.path.join(config.DATA_DIRECTORY, 'validator_state.json')
            if not os.path.exists(state_file):
                bt.logging.info("📋 No previous state found, starting fresh")
                return

            with open(state_file, 'r') as f:
                state = json.load(f)

            self.step = state.get('step', 0)
            if 'last_update' in state:
                self.last_update = datetime.fromisoformat(state['last_update'])

            bt.logging.info(f"📋 Validator state loaded: step={self.step}")
        except Exception as e:
            bt.logging.warning(f"⚠️ Error loading state: {e}")

    async def run(self):
        """Validator loop."""
        try:
            bt.logging.info("🚀 STARTING VALIDATOR...")
            bt.logging.info(f"🔑 Wallet: {self.wallet.hotkey.ss58_address}")
            bt.logging.info(f"🌐 Netuid: {self.config.netuid}")
            bt.logging.info(f"⏱️ Timeout: {self.config.neuron.timeout}s")
            bt.logging.info(f"📊 Sample size: {self.config.neuron.sample_size}")

            await self._ensure_database_initialized()
            bt.logging.info(f"🏢 Company database: {len(self.company_db)} companies")

            self.metagraph.sync(subtensor=self.subtensor)
            bt.logging.info(f"📊 Metagraph synced. Block: {self.metagraph.block}")

            while True:
                try:
                    epoch_start = time.time()

                    if self.step % 5 == 0:
                        self.metagraph.sync(subtensor=self.subtensor)

                    validation_results = await self.forward()

                    if validation_results:
                        self.set_weights(validation_results)

                    epoch_duration = time.time() - epoch_start
                    target_epoch_time = self.config.neuron.epoch_length * 12
                    sleep_time = max(10, target_epoch_time - epoch_duration)

                    bt.logging.info(f"✅ Epoch {self.step} completed in {epoch_duration:.2f}s, sleeping {sleep_time:.2f}s")

                    self.last_update = datetime.now(timezone.utc)
                    self.step += 1

                    if self.step % 10 == 0:
                        self.save_state()

                    await asyncio.sleep(sleep_time)
                except KeyboardInterrupt:
                    bt.logging.info("⏹️ Received interrupt signal")
                    break
                except Exception as e:
                    bt.logging.error(f"💥 Error in validator loop: {e}")
                    bt.logging.error(traceback.format_exc())

                    await asyncio.sleep(60)
        finally:
            bt.logging.info("🔚 Validator stopping ...")
            self.save_state()
            bt.logging.info("✅ Validator stopped gracefully")

    async def run_background(self):
        """Background mode for HTTP server."""
        try:
            bt.logging.info("🚀 STARTING VALIDATOR...")
            bt.logging.info(f"🔑 Wallet: {self.wallet.hotkey.ss58_address}")
            bt.logging.info(f"🌐 Netuid: {self.config.netuid}")

            await self._ensure_database_initialized()
            bt.logging.info(f"🏢 Company database: {len(self.company_db)} companies")

            self.metagraph.sync(subtensor=self.subtensor)
            bt.logging.info(f"📊 Metagraph synced. Block: {self.metagraph.block}")

            while True:
                try:
                    if self.step % 20 == 0:
                        self.metagraph.sync(subtensor=self.subtensor)

                    # Occasional organic validation
                    validation_results = await self.forward()
                    if validation_results:
                        self.set_weights(validation_results)

                    # Sleep between maintenance cycles
                    await asyncio.sleep(180)

                    self.step += 1
                except KeyboardInterrupt:
                    bt.logging.info("⏹️ Background validator interrupted")
                    break
                except Exception as e:
                    bt.logging.error(f"💥 Error in validator: {e}")
                    bt.logging.error(traceback.format_exc())
                    await asyncio.sleep(120)
        except Exception as e:
            bt.logging.error(f"💥 Fatal error in validator: {e}")
            bt.logging.error(traceback.format_exc())
        finally:
            bt.logging.info("✅ Validator stopped")


def main():
    """Main entry point for the validator."""
    validator = CompanyIntelligenceValidator()

    try:
        validator.load_state()
        asyncio.run(validator.run())

    except KeyboardInterrupt:
        bt.logging.info("⏹️ Validator interrupted by user")

    except Exception as e:
        bt.logging.error(f"💥 Validator failed: {e}")
        bt.logging.error(traceback.format_exc())

    finally:
        validator.save_state()


if __name__ == "__main__":
    main()
