"""Custom exception hierarchy for the Snowflake JSON pipeline."""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigurationError(PipelineError):
    """Raised when required configuration values are missing or invalid."""


class ValidationError(PipelineError):
    """Raised when a data record fails schema validation."""

    def __init__(self, errors: list[str], record: dict | None = None) -> None:
        self.errors = errors
        self.record = record
        super().__init__("; ".join(errors))


class SqlScriptError(PipelineError):
    """Raised when a SQL script cannot be loaded or parsed."""


class BatchProcessingError(PipelineError):
    """Raised when batch processing encounters an unrecoverable error."""

    def __init__(self, message: str, failed_count: int = 0) -> None:
        self.failed_count = failed_count
        super().__init__(message)


class ExportError(PipelineError):
    """Raised when exporting pipeline results fails."""


class RetryExhausted(PipelineError):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, attempts: int, last_error: Exception) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"Exhausted {attempts} retry attempts: {last_error}")
