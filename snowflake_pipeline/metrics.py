"""Pipeline execution metrics collection and reporting."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Accumulated metrics for one pipeline run."""

    run_id: str = ""
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    batches_processed: int = 0
    validation_errors: list[str] = field(default_factory=list)

    @property
    def duration_s(self) -> float:
        """Elapsed seconds between start and end (or now if not finished)."""
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time

    @property
    def throughput_rps(self) -> float:
        """Records processed per second."""
        d = self.duration_s
        return self.processed_records / d if d > 0 else 0.0

    def mark_complete(self) -> None:
        """Record the end timestamp."""
        self.end_time = time.time()
        logger.info(
            "Pipeline run %s complete: %d/%d records in %.2fs (%.1f rps)",
            self.run_id, self.processed_records, self.total_records,
            self.duration_s, self.throughput_rps,
        )

    def record_validation(self, valid: int, invalid: int, errors: list[str] | None = None) -> None:
        """Update validation counters.

        Args:
            valid: Number of valid records in this batch.
            invalid: Number of invalid records.
            errors: Optional list of error message strings.
        """
        self.valid_records += valid
        self.invalid_records += invalid
        if errors:
            self.validation_errors.extend(errors)

    def to_dict(self) -> dict:
        """Serialise metrics to a plain dict."""
        d = asdict(self)
        d["duration_s"] = self.duration_s
        d["throughput_rps"] = self.throughput_rps
        return d

    def to_json(self, indent: int = 2) -> str:
        """Serialise metrics to a JSON string.

        Args:
            indent: JSON indentation level.

        Returns:
            Formatted JSON string.
        """
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, path: Path) -> None:
        """Write metrics as JSON to *path*.

        Args:
            path: Destination file path (parent must exist).
        """
        path.write_text(self.to_json(), encoding="utf-8")
        logger.debug("Metrics saved to %s", path)
