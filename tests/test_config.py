"""Tests for snowflake_pipeline.config."""
from __future__ import annotations

import pytest

from snowflake_pipeline.config import PipelineConfig, SnowflakeConfig
from snowflake_pipeline.exceptions import ConfigurationError


def test_pipeline_config_defaults():
    cfg = PipelineConfig()
    assert cfg.batch_size == 100
    assert cfg.retry_attempts == 3
    assert cfg.log_level == "INFO"


def test_pipeline_config_from_env_defaults(monkeypatch):
    monkeypatch.delenv("PIPELINE_BATCH_SIZE", raising=False)
    monkeypatch.delenv("PIPELINE_RETRY_ATTEMPTS", raising=False)
    cfg = PipelineConfig.from_env()
    assert cfg.batch_size == 100
    assert cfg.retry_attempts == 3


def test_pipeline_config_from_env_custom(monkeypatch):
    monkeypatch.setenv("PIPELINE_BATCH_SIZE", "250")
    monkeypatch.setenv("PIPELINE_RETRY_ATTEMPTS", "5")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    cfg = PipelineConfig.from_env()
    assert cfg.batch_size == 250
    assert cfg.retry_attempts == 5
    assert cfg.log_level == "DEBUG"


def test_snowflake_config_from_env_missing_raises(monkeypatch):
    for key in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_WAREHOUSE"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ConfigurationError):
        SnowflakeConfig.from_env()


def test_snowflake_config_from_env_success(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myaccount")
    monkeypatch.setenv("SNOWFLAKE_USER", "user1")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "secret")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "mydb")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "public")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "compute_wh")
    cfg = SnowflakeConfig.from_env()
    assert cfg.account == "myaccount"
    assert cfg.warehouse == "compute_wh"


def test_pipeline_config_from_env_no_snowflake():
    cfg = PipelineConfig.from_env(load_snowflake=False)
    assert cfg.snowflake is None


def test_pipeline_config_extra_dict():
    cfg = PipelineConfig(extra={"key": "val"})
    assert cfg.extra["key"] == "val"
