from typing import Any, Dict, List

from jsonschema import Draft7Validator


class ValidationSchemas:
    """JSON schemas for validating miner responses."""

    COMPANY_DATA_SCHEMA = {
        "type": "object",
        "properties": {
            "ticker": {
                "type": "string",
                "pattern": "^[A-Z0-9.-]{1,8}$",
                "description": "Company ticker symbol"
            },
            "companyName": {
                "type": "string",
                "minLength": 1,
                "maxLength": 200,
                "description": "Full company name"
            },
            "website": {
                "type": "string",
                "format": "uri",
                "description": "Company website URL"
            },
            "exchange": {
                "type": ["string", "null"],
                "description": "Stock exchange"
            },
            "marketCap": {
                "type": ["number", "null"],
                "minimum": 0,
                "description": "Market capitalization in USD"
            },
            "sharePrice": {
                "type": ["number", "null"],
                "minimum": 0,
                "description": "Current share price"
            },
            "sector": {
                "type": ["string", "null"],
                "description": "Company sector"
            },
            "data": {
                "type": "object",
                "description": "Analysis-specific data"
            }
        },
        "required": ["ticker", "companyName", "website"],
        "additionalProperties": True
    }

    FINANCIAL_DATA_SCHEMA = {
        "type": "object",
        "properties": {
            "volume": {
                "type": ["number", "null"],
                "minimum": 0,
                "description": "Trading volume"
            },
            "eps": {
                "type": ["number", "null"],
                "description": "Earnings per share"
            },
            "bookValue": {
                "type": ["number", "null"],
                "description": "Book value per share"
            },
            "industry": {
                "type": ["string", "null"],
                "description": "Company industry"
            }
        },
        "additionalProperties": True
    }

    CRYPTO_DATA_SCHEMA = {
        "type": "object",
        "properties": {
            "currentHoldings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "currency": {
                            "type": "string",
                            "pattern": "^[A-Z]{3,10}$",
                            "description": "Cryptocurrency symbol"
                        },
                        "amount": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Amount held"
                        },
                        "usdValue": {
                            "type": "number",
                            "minimum": 0,
                            "description": "USD value of holdings"
                        },
                        "lastUpdated": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Last update timestamp"
                        }
                    },
                    "required": ["currency", "amount", "usdValue"],
                    "additionalProperties": True
                },
                "description": "Current cryptocurrency holdings"
            },
            "currentTotalUsd": {
                "type": "number",
                "minimum": 0,
                "description": "Total USD value of all holdings"
            },
            "historicalHoldings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "recordedAt": {
                            "type": "string",
                            "format": "date-time"
                        },
                        "totalUsdValue": {
                            "type": "number",
                            "minimum": 0
                        },
                        "holdings": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "currency": {"type": "string"},
                                    "amount": {"type": "number", "minimum": 0},
                                    "usdValue": {"type": "number", "minimum": 0}
                                },
                                "required": ["currency", "amount", "usdValue"]
                            }
                        }
                    },
                    "required": ["recordedAt", "totalUsdValue"]
                }
            }
        },
        "additionalProperties": True
    }

    SENTIMENT_DATA_SCHEMA = {
        "type": "object",
        "properties": {
            "overallSentiment": {
                "type": "string",
                "enum": ["positive", "negative", "neutral"],
                "description": "Overall sentiment classification"
            },
            "sentimentScore": {
                "type": "number",
                "minimum": -1.0,
                "maximum": 1.0,
                "description": "Numerical sentiment score"
            },
            "confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "description": "Confidence in sentiment analysis"
            },
            "sources": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "sentiment": {"type": "string"},
                        "score": {"type": "number"},
                        "timestamp": {"type": "string", "format": "date-time"}
                    },
                    "required": ["source", "sentiment"]
                },
                "description": "Individual sentiment sources"
            },
            "keywords": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Key sentiment-driving terms"
            },
            "timePeriod": {
                "type": "string",
                "description": "Time period analyzed"
            }
        },
        "required": ["overallSentiment", "sentimentScore"],
        "additionalProperties": True
    }

    NEWS_DATA_SCHEMA = {
        "type": "object",
        "properties": {
            "articles": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "minLength": 1},
                        "summary": {"type": "string"},
                        "url": {"type": "string", "format": "uri"},
                        "source": {"type": "string"},
                        "published_date": {"type": "string", "format": "date-time"},
                        "relevance_score": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0
                        },
                        "sentiment": {
                            "type": "string",
                            "enum": ["positive", "negative", "neutral"]
                        }
                    },
                    "required": ["title", "source", "published_date"],
                    "additionalProperties": True
                },
                "minItems": 1,
                "description": "News articles"
            },
            "summary": {
                "type": "object",
                "properties": {
                    "total_articles": {"type": "integer", "minimum": 0},
                    "date_range": {
                        "type": "object",
                        "properties": {
                            "start": {"type": "string", "format": "date-time"},
                            "end": {"type": "string", "format": "date-time"}
                        },
                        "required": ["start", "end"]
                    },
                    "sentiment_breakdown": {
                        "type": "object",
                        "properties": {
                            "positive": {"type": "integer", "minimum": 0},
                            "negative": {"type": "integer", "minimum": 0},
                            "neutral": {"type": "integer", "minimum": 0}
                        }
                    },
                    "top_sources": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["total_articles"]
            }
        },
        "required": ["articles", "summary"],
        "additionalProperties": True
    }

    INTELLIGENCE_RESPONSE_SCHEMA = {
        "type": "object",
        "properties": {
            "success": {
                "type": "boolean",
                "description": "Whether the request was successful"
            },
            "data": {
                "type": "object",
                "properties": {
                    "company": COMPANY_DATA_SCHEMA
                }
            },
            "errorMessage": {
                "type": "string",
                "description": "Error message if any"
            }
        },
        "required": ["success", "data"],
        "additionalProperties": True
    }

    @classmethod
    def get_schema_for_analysis_type(cls, analysis_type: str) -> Dict[str, Any]:
        """Get the appropriate data schema for an analysis type."""
        schema_map = {
            'crypto': cls.CRYPTO_DATA_SCHEMA,
            'financial': cls.FINANCIAL_DATA_SCHEMA,
            'sentiment': cls.SENTIMENT_DATA_SCHEMA,
            'news': cls.NEWS_DATA_SCHEMA
        }

        return schema_map.get(analysis_type.lower(), {})

    @classmethod
    def validate_structure(cls, data: Any, schema: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate data against schema.

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        try:
            validator = Draft7Validator(schema)
            errors = list(validator.iter_errors(data))

            if not errors:
                return True, []

            error_messages = []
            for error in errors:
                path = ".".join(str(p) for p in error.path) if error.path else "root"
                error_messages.append(f"{path}: {error.message}")

            return False, error_messages

        except Exception as e:
            return False, [f"Schema validation error: {str(e)}"]

    @classmethod
    def validate_intelligence_response(cls, response_data: Any) -> tuple[bool, List[str], Dict[str, Any]]:
        """
        Validate a complete intelligence response.

        Returns:
            Tuple of (is_valid, errors, validation_details)
        """
        validation_details = {
            'structure_valid': False,
            'data_valid': False,
            'completenessScore': 0.0,
            'errors': []
        }

        # First validate overall structure
        is_valid, errors = cls.validate_structure(response_data, cls.INTELLIGENCE_RESPONSE_SCHEMA)
        validation_details['structure_valid'] = is_valid
        validation_details['errors'].extend(errors)

        if not is_valid:
            return False, errors, validation_details

        # Calculate completeness score
        completeness_score = 0.0
        max_score = 100.0

        # Check required fields presence (40 points)
        if response_data.get('success') is not None:
            completeness_score += 10
        if response_data.get('data') is not None:
            completeness_score += 20
        if response_data.get('confidenceScore') is not None:
            completeness_score += 10

        # Check data object completeness (40 points)
        company_data = response_data.get('data', {}).get('company', {})
        if company_data.get('ticker'):
            completeness_score += 10
        if company_data.get('companyName'):
            completeness_score += 10
        if company_data.get('website'):
            completeness_score += 10
        if company_data.get('exchange'):
            completeness_score += 10
        if company_data.get('sector'):
            completeness_score += 10
        if company_data.get('marketCap'):
            completeness_score += 10

        # Check confidence score validity (20 points)
        confidence = response_data.get('confidenceScore')
        if isinstance(confidence, (int, float)) and 0 <= confidence <= 1:
            completeness_score += 20

        validation_details['completenessScore'] = completeness_score / max_score
        validation_details['data_valid'] = True

        return True, [], validation_details

    @classmethod
    def validate_company_data_schema(cls, data: Dict[str, Any], analysis_type: str) -> tuple[bool, List[str], float]:
        """
        Validate company data against analysis-specific schema.

        Returns:
            Tuple of (is_valid, errors, completeness_score)
        """
        # Get analysis-specific schema
        data_schema = cls.get_schema_for_analysis_type(analysis_type)

        if not data_schema:
            # No specific schema, just check basic structure
            return True, [], 0.7

        # Validate the data field against analysis-specific schema
        company_data = data.get('data', {})
        is_valid, errors = cls.validate_structure(company_data, data_schema)

        # Calculate completeness score based on schema requirements
        completeness_score = 0.0

        if analysis_type.lower() == 'crypto':
            completeness_score = cls._calculate_crypto_completeness(company_data)
        elif analysis_type.lower() == 'financial':
            completeness_score = cls._calculate_financial_completeness(company_data)
        elif analysis_type.lower() == 'sentiment':
            completeness_score = cls._calculate_sentiment_completeness(company_data)
        elif analysis_type.lower() == 'news':
            completeness_score = cls._calculate_news_completeness(company_data)
        else:
            completeness_score = 0.5  # Default for unknown types

        return is_valid, errors, completeness_score

    @classmethod
    def _calculate_crypto_completeness(cls, data: Dict[str, Any]) -> float:
        """Calculate completeness score for crypto data."""
        score = 0.0

        if 'currentHoldings' in data:
            score += 0.4
            holdings = data['currentHoldings']
            if isinstance(holdings, list) and len(holdings) > 0:
                score += 0.2
                # Check if holdings have required fields
                for holding in holdings:
                    if all(key in holding for key in ['currency', 'amount', 'usdValue']):
                        score += 0.1
                        break

        if 'currentTotalUsd' in data:
            score += 0.2

        if 'historicalHoldings' in data:
            score += 0.1

        return min(score, 1.0)

    @classmethod
    def _calculate_financial_completeness(cls, data: Dict[str, Any]) -> float:
        """Calculate completeness score for financial data."""
        important_fields = ['marketCap', 'sharePrice', 'volume', 'eps', 'sector']
        present_fields = sum(1 for field in important_fields if field in data)

        return present_fields / len(important_fields)

    @classmethod
    def _calculate_sentiment_completeness(cls, data: Dict[str, Any]) -> float:
        """Calculate completeness score for sentiment data."""
        score = 0.0

        if 'overall_sentiment' in data:
            score += 0.3
        if 'sentiment_score' in data:
            score += 0.3
        if 'confidence' in data:
            score += 0.2
        if 'sources' in data and isinstance(data['sources'], list):
            score += 0.2

        return min(score, 1.0)

    @classmethod
    def _calculate_news_completeness(cls, data: Dict[str, Any]) -> float:
        """Calculate completeness score for news data."""
        score = 0.0

        if 'articles' in data and isinstance(data['articles'], list):
            score += 0.5
            if len(data['articles']) > 0:
                score += 0.2
                # Check first article structure
                first_article = data['articles'][0]
                if all(key in first_article for key in ['title', 'source', 'published_date']):
                    score += 0.2

        if 'summary' in data:
            score += 0.1

        return min(score, 1.0)
