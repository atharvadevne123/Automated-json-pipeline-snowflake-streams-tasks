"""Chunked batch processing for pipeline records."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from snowflake_pipeline.constants import DEFAULT_BATCH_SIZE
from snowflake_pipeline.exceptions import BatchProcessingError
from snowflake_pipeline.utils import chunk

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """Summary of a completed batch processing run."""

    total: int = 0
    processed: int = 0
    failed: int = 0
    duration_s: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Fraction of records processed successfully (0.0-1.0)."""
        return self.processed / self.total if self.total > 0 else 0.0


def process_batch(
    records: list[dict],
    handler: Callable[[dict], None],
    batch_size: int = DEFAULT_BATCH_SIZE,
    stop_on_error: bool = False,
) -> BatchResult:
    """Process *records* in chunks, calling *handler* for each record.

    Args:
        records: List of dicts to process.
        handler: Callable invoked for every record; must accept one dict.
        batch_size: Number of records per chunk.
        stop_on_error: If True, raises BatchProcessingError on first failure.

    Returns:
        BatchResult summarising the run.

    Raises:
        BatchProcessingError: if stop_on_error is True and any record fails.
    """
    result = BatchResult(total=len(records))
    t0 = time.monotonic()
    for batch_idx, batch in enumerate(chunk(records, batch_size)):
        logger.debug("Processing batch %d (%d records)", batch_idx, len(batch))
        for rec in batch:
            try:
                handler(rec)
                result.processed += 1
            except Exception as exc:
                result.failed += 1
                msg = f"Record processing error: {exc}"
                result.errors.append(msg)
                logger.warning(msg)
                if stop_on_error:
                    result.duration_s = time.monotonic() - t0
                    raise BatchProcessingError(msg, failed_count=result.failed)
    result.duration_s = time.monotonic() - t0
    logger.info(
        "Batch run complete: %d/%d processed in %.2fs",
        result.processed, result.total, result.duration_s,
    )
    return result


def process_stream(
    records: Iterable[dict],
    handler: Callable[[dict], None],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> BatchResult:
    """Stream-process an iterable of records in memory-efficient batches.

    Args:
        records: Any iterable of dicts (generator-safe).
        handler: Callable invoked per record.
        batch_size: Size of each accumulated batch before flushing.

    Returns:
        BatchResult for the full stream.
    """
    buffer: list[dict] = []
    aggregate = BatchResult()
    t0 = time.monotonic()

    def flush(buf: list[dict]) -> None:
        r = process_batch(buf, handler, batch_size=len(buf))
        aggregate.total += r.total
        aggregate.processed += r.processed
        aggregate.failed += r.failed
        aggregate.errors.extend(r.errors)

    for rec in records:
        buffer.append(rec)
        if len(buffer) >= batch_size:
            flush(buffer)
            buffer.clear()
    if buffer:
        flush(buffer)

    aggregate.duration_s = time.monotonic() - t0
    return aggregate
