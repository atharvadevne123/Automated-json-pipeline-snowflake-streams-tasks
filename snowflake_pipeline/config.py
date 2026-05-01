"""Environment-based configuration for the Snowflake JSON pipeline."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from snowflake_pipeline.constants import DEFAULT_BATCH_SIZE, DEFAULT_RETRY_ATTEMPTS, DEFAULT_RETRY_BASE_DELAY
from snowflake_pipeline.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConfig:
    """Snowflake connection parameters loaded from environment variables."""

    account: str
    user: str
    password: str
    database: str
    schema: str
    warehouse: str
    role: str = ""

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Load Snowflake config from environment variables.

        Raises:
            ConfigurationError: if any required env var is missing.
        """
        required = {
            "SNOWFLAKE_ACCOUNT": "account",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "password",
            "SNOWFLAKE_DATABASE": "database",
            "SNOWFLAKE_SCHEMA": "schema",
            "SNOWFLAKE_WAREHOUSE": "warehouse",
        }
        values: dict[str, str] = {}
        missing = []
        for env_key, attr in required.items():
            val = os.environ.get(env_key, "")
            if not val:
                missing.append(env_key)
            else:
                values[attr] = val
        if missing:
            raise ConfigurationError(f"Missing required environment variables: {missing}")
        values["role"] = os.environ.get("SNOWFLAKE_ROLE", "")
        logger.debug("SnowflakeConfig loaded from environment")
        return cls(**values)


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration."""

    batch_size: int = DEFAULT_BATCH_SIZE
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY
    log_level: str = "INFO"
    snowflake: SnowflakeConfig | None = None
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls, load_snowflake: bool = False) -> "PipelineConfig":
        """Build PipelineConfig from environment variables.

        Args:
            load_snowflake: if True, also loads SnowflakeConfig.

        Returns:
            Populated PipelineConfig instance.
        """
        cfg = cls(
            batch_size=int(os.environ.get("PIPELINE_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
            retry_attempts=int(os.environ.get("PIPELINE_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)),
            retry_base_delay=float(os.environ.get("PIPELINE_RETRY_BASE_DELAY", DEFAULT_RETRY_BASE_DELAY)),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
        if load_snowflake:
            cfg.snowflake = SnowflakeConfig.from_env()
        logger.debug("PipelineConfig loaded (batch_size=%d)", cfg.batch_size)
        return cfg
