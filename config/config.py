import os
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Enhanced configuration management for the validator."""

    # External API Configuration
    CRYPTO_HOLDINGS_BASE_URL: str = os.getenv('CRYPTO_HOLDINGS_URL', 'https://api.example.com')
    CRYPTO_HOLDINGS_API_KEY: str = os.getenv('CRYPTO_HOLDINGS_API_KEY', '')

    # API Endpoints
    COMPANIES_ENDPOINT: str = os.getenv('COMPANIES_ENDPOINT', '/validator/companies')
    VALIDATION_ENDPOINT: str = os.getenv('VALIDATION_ENDPOINT', '/validator/<ticker>/types/<analysis_type>')

    # API Client Configuration
    API_MANAGER_CONNECTION_LIMIT: int = int(os.getenv('API_MANAGER_CONNECTION_LIMIT', '100'))
    API_MANAGER_DNS_CACHE: int = int(os.getenv('API_MANAGER_DNS_CACHE', '300'))
    API_MANAGER_CLIENT_TIMEOUT: int = int(os.getenv('API_MANAGER_CLIENT_TIMEOUT', '30'))

    # Cache Configuration
    CACHE_TTL: int = int(os.getenv('CACHE_TTL', '300'))  # 5 minutes default
    COMPANY_CACHE_DURATION_HOURS: int = int(os.getenv('COMPANY_CACHE_DURATION_HOURS', '1'))

    # Validator HTTP Server Configuration
    API_TOKEN: str = os.getenv("API_TOKEN", "")
    VALIDATOR_HOST: str = os.getenv("VALIDATOR_HOST", "0.0.0.0")
    VALIDATOR_PORT: int = int(os.getenv("VALIDATOR_PORT", "8000"))
    VALIDATOR_LOG_LEVEL: str = os.getenv("VALIDATOR_LOG_LEVEL", "info")

    # Data Storage Configuration (In-Memory)
    DATA_DIRECTORY: str = os.getenv("DATA_DIRECTORY", "data")

    # Validation Configuration
    STRUCTURE_VALIDATION_WEIGHT: float = float(os.getenv("STRUCTURE_VALIDATION_WEIGHT", "0.3"))
    API_VALIDATION_WEIGHT: float = float(os.getenv("API_VALIDATION_WEIGHT", "0.7"))

    # Query Generation Configuration
    POPULAR_COMPANIES_WEIGHT: float = float(os.getenv("POPULAR_COMPANIES_WEIGHT", "0.4"))
    EMERGING_COMPANIES_WEIGHT: float = float(os.getenv("EMERGING_COMPANIES_WEIGHT", "0.2"))
    SECTOR_FOCUSED_WEIGHT: float = float(os.getenv("SECTOR_FOCUSED_WEIGHT", "0.15"))
    CRYPTO_FOCUSED_WEIGHT: float = float(os.getenv("CRYPTO_FOCUSED_WEIGHT", "0.15"))
    RANDOM_SELECTION_WEIGHT: float = float(os.getenv("RANDOM_SELECTION_WEIGHT", "0.1"))

    # Analysis Type Weights
    CRYPTO_ANALYSIS_WEIGHT: float = float(os.getenv("CRYPTO_ANALYSIS_WEIGHT", "0.35"))
    FINANCIAL_ANALYSIS_WEIGHT: float = float(os.getenv("FINANCIAL_ANALYSIS_WEIGHT", "0.35"))
    SENTIMENT_ANALYSIS_WEIGHT: float = float(os.getenv("SENTIMENT_ANALYSIS_WEIGHT", "0.15"))
    NEWS_ANALYSIS_WEIGHT: float = float(os.getenv("NEWS_ANALYSIS_WEIGHT", "0.15"))

    # Rate Limiting
    API_RATE_LIMIT_REQUESTS: int = int(os.getenv("API_RATE_LIMIT_REQUESTS", "100"))
    API_RATE_LIMIT_TIME_WINDOW: int = int(os.getenv("API_RATE_LIMIT_TIME_WINDOW", "60"))

    # Miner Configuration
    MAX_CONCURRENT_MINERS: int = int(os.getenv("MAX_CONCURRENT_MINERS", "20"))
    MINER_TIMEOUT: int = int(os.getenv("MINER_TIMEOUT", "15"))

    # Cleanup Configuration
    VALIDATION_HISTORY_RETENTION_DAYS: int = int(os.getenv("VALIDATION_HISTORY_RETENTION_DAYS", "7"))
    QUERY_HISTORY_RETENTION_DAYS: int = int(os.getenv("QUERY_HISTORY_RETENTION_DAYS", "7"))
    COMPANY_DATA_CLEANUP_DAYS: int = int(os.getenv("COMPANY_DATA_CLEANUP_DAYS", "30"))

    # Monitoring Configuration
    ENABLE_METRICS: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"
    METRICS_RETENTION_HOURS: int = int(os.getenv("METRICS_RETENTION_HOURS", "168"))  # 1 week

    # Feature Flags
    ENABLE_QUERY_CACHING: bool = os.getenv("ENABLE_QUERY_CACHING", "true").lower() == "true"
    ENABLE_COMPANY_REFRESH: bool = os.getenv("ENABLE_COMPANY_REFRESH", "true").lower() == "true"

    # Development/Debug Configuration
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"
    VERBOSE_LOGGING: bool = os.getenv("VERBOSE_LOGGING", "false").lower() == "true"
    SAVE_VALIDATION_DETAILS: bool = os.getenv("SAVE_VALIDATION_DETAILS", "false").lower() == "true"

    @classmethod
    def validate_config(cls) -> bool:
        """Validate configuration settings."""
        errors = []

        # Required settings
        if not cls.CRYPTO_HOLDINGS_BASE_URL:
            errors.append("CRYPTO_HOLDINGS_URL is required")

        if not cls.CRYPTO_HOLDINGS_API_KEY and not cls.DEBUG_MODE:
            errors.append("CRYPTO_HOLDINGS_API_KEY is required (unless in DEBUG_MODE)")

        if not cls.API_TOKEN:
            errors.append("API_TOKEN is required for HTTP authentication")

        # Validation weights must sum to 1.0
        total_validation_weight = cls.STRUCTURE_VALIDATION_WEIGHT + cls.API_VALIDATION_WEIGHT
        if abs(total_validation_weight - 1.0) > 0.01:
            errors.append(f"Validation weights must sum to 1.0, got {total_validation_weight}")

        # Query strategy weights validation
        total_strategy_weight = (
            cls.POPULAR_COMPANIES_WEIGHT + cls.EMERGING_COMPANIES_WEIGHT +
            cls.SECTOR_FOCUSED_WEIGHT + cls.CRYPTO_FOCUSED_WEIGHT + cls.RANDOM_SELECTION_WEIGHT
        )
        if abs(total_strategy_weight - 1.0) > 0.01:
            errors.append(f"Query strategy weights must sum to 1.0, got {total_strategy_weight}")

        # Analysis type weights validation
        total_analysis_weight = (
            cls.CRYPTO_ANALYSIS_WEIGHT + cls.FINANCIAL_ANALYSIS_WEIGHT +
            cls.SENTIMENT_ANALYSIS_WEIGHT + cls.NEWS_ANALYSIS_WEIGHT
        )
        if abs(total_analysis_weight - 1.0) > 0.01:
            errors.append(f"Analysis type weights must sum to 1.0, got {total_analysis_weight}")

        # Port validation
        if not (1 <= cls.VALIDATOR_PORT <= 65535):
            errors.append(f"VALIDATOR_PORT must be between 1 and 65535, got {cls.VALIDATOR_PORT}")

        # Positive value validations
        if cls.CACHE_TTL <= 0:
            errors.append("CACHE_TTL must be positive")

        if cls.API_MANAGER_CLIENT_TIMEOUT <= 0:
            errors.append("API_MANAGER_CLIENT_TIMEOUT must be positive")

        if cls.MINER_TIMEOUT <= 0:
            errors.append("MINER_TIMEOUT must be positive")

        if errors:
            print("Configuration Validation Errors:")
            for error in errors:
                print(f"  - {error}")
            return False

        return True

    @classmethod
    def get_strategy_weights(cls) -> dict:
        """Get query strategy weights as a dictionary."""
        return {
            'popular_companies': cls.POPULAR_COMPANIES_WEIGHT,
            'emerging_companies': cls.EMERGING_COMPANIES_WEIGHT,
            'sector_focused': cls.SECTOR_FOCUSED_WEIGHT,
            'crypto_focused': cls.CRYPTO_FOCUSED_WEIGHT,
            'random_selection': cls.RANDOM_SELECTION_WEIGHT
        }

    @classmethod
    def get_analysis_weights(cls) -> dict:
        """Get analysis type weights as a dictionary."""
        from neurons.protocol import AnalysisType
        return {
            AnalysisType.CRYPTO: cls.CRYPTO_ANALYSIS_WEIGHT,
            AnalysisType.FINANCIAL: cls.FINANCIAL_ANALYSIS_WEIGHT,
            AnalysisType.SENTIMENT: cls.SENTIMENT_ANALYSIS_WEIGHT,
            AnalysisType.NEWS: cls.NEWS_ANALYSIS_WEIGHT
        }


# Create global config instance
appConfig = Config()

# Environment-specific configurations


def load_environment_config(env: str = None):
    """Load environment-specific configuration."""
    if env is None:
        env = os.getenv('ENVIRONMENT', 'development')

    if env == 'production':
        # Production-specific settings
        appConfig.VERBOSE_LOGGING = False
        appConfig.DEBUG_MODE = False
        appConfig.CACHE_TTL = 300  # 5 minutes
        appConfig.VALIDATOR_LOG_LEVEL = 'info'

    elif env == 'staging':
        # Staging-specific settings
        appConfig.VERBOSE_LOGGING = True
        appConfig.DEBUG_MODE = False
        appConfig.CACHE_TTL = 180  # 3 minutes
        appConfig.VALIDATOR_LOG_LEVEL = 'debug'

    elif env == 'development':
        # Development-specific settings
        appConfig.VERBOSE_LOGGING = True
        appConfig.DEBUG_MODE = True
        appConfig.CACHE_TTL = 60  # 1 minute
        appConfig.VALIDATOR_LOG_LEVEL = 'debug'
        appConfig.SAVE_VALIDATION_DETAILS = True

    print(f"🌍 Loaded {env} environment configuration")


# Auto-load environment configuration
load_environment_config()

# Validate configuration on import
if not appConfig.validate_config():
    raise ValueError("Invalid configuration detected. Please check your environment variables.")
