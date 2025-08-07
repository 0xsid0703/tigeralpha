from typing import Optional

import aiohttp

from config.config import appConfig as config


class APIManager:
    """Manages external API connections and credentials."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

        if config.CRYPTO_HOLDINGS_BASE_URL is None or config.CRYPTO_HOLDINGS_API_KEY is None:
            raise ValueError("CRYPTO_HOLDINGS_URL and CRYPTO_HOLDINGS_API_KEY must be set in environment variables")

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit=config.API_MANAGER_CONNECTION_LIMIT, ttl_dns_cache=config.API_MANAGER_DNS_CACHE)
            timeout = aiohttp.ClientTimeout(total=config.API_MANAGER_CLIENT_TIMEOUT)

            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'Bittensor-CompanyIntel/1.0'
                }
            )

        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
