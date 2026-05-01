"""Tests for snowflake_pipeline.exceptions."""
from __future__ import annotations

import pytest

from snowflake_pipeline.exceptions import (
    BatchProcessingError,
    ConfigurationError,
    ExportError,
    PipelineError,
    RetryExhausted,
    SqlScriptError,
    ValidationError,
)


def test_pipeline_error_is_exception():
    assert issubclass(PipelineError, Exception)


def test_all_errors_inherit_pipeline_error():
    for cls in (ConfigurationError, ValidationError, SqlScriptError,
                BatchProcessingError, ExportError, RetryExhausted):
        assert issubclass(cls, PipelineError)


def test_validation_error_stores_errors():
    ve = ValidationError(["err1", "err2"])
    assert ve.errors == ["err1", "err2"]
    assert "err1" in str(ve)


def test_validation_error_stores_record():
    rec = {"id": "r1"}
    ve = ValidationError(["bad"], record=rec)
    assert ve.record is rec


def test_batch_processing_error_failed_count():
    exc = BatchProcessingError("msg", failed_count=5)
    assert exc.failed_count == 5


def test_retry_exhausted_attributes():
    inner = ValueError("inner")
    exc = RetryExhausted(4, inner)
    assert exc.attempts == 4
    assert exc.last_error is inner


def test_retry_exhausted_message_contains_attempts():
    exc = RetryExhausted(7, OSError("x"))
    assert "7" in str(exc)


def test_can_catch_as_pipeline_error():
    with pytest.raises(PipelineError):
        raise ConfigurationError("missing env")


def test_sql_script_error_message():
    exc = SqlScriptError("script.sql not found")
    assert "script.sql" in str(exc)
