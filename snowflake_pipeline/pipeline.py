"""Main pipeline orchestrator: validates, transforms, and processes review records."""
from __future__ import annotations

import logging
from pathlib import Path

from snowflake_pipeline.batch_processor import BatchResult, process_batch
from snowflake_pipeline.config import PipelineConfig
from snowflake_pipeline.filters import FilterFn, apply_filters
from snowflake_pipeline.io import read_ndjson, write_ndjson
from snowflake_pipeline.metrics import PipelineMetrics
from snowflake_pipeline.transformers import normalise_review
from snowflake_pipeline.utils import new_run_id
from snowflake_pipeline.validators import validate_batch

logger = logging.getLogger(__name__)


class ReviewPipeline:
    """Orchestrates the full review-record ETL pipeline.

    Attributes:
        config: Pipeline configuration.
        metrics: Accumulated run metrics (populated after run()).
    """

    def __init__(self, config: PipelineConfig | None = None) -> None:
        self.config = config or PipelineConfig()
        self.metrics = PipelineMetrics(run_id=new_run_id())

    def run(
        self,
        source: Path,
        destination: Path,
        filters: list[FilterFn] | None = None,
    ) -> BatchResult:
        """Read, validate, filter, and write review records.

        Args:
            source: Path to the input NDJSON file.
            destination: Path to the output NDJSON file.
            filters: Optional list of predicate functions for record filtering.

        Returns:
            BatchResult summarising the processing run.
        """
        logger.info("Pipeline run %s started: %s -> %s", self.metrics.run_id, source, destination)
        records = read_ndjson(source)
        self.metrics.total_records = len(records)

        records = [normalise_review(r) for r in records]

        valid, invalid = validate_batch(records)
        error_msgs = [f"{r.get('review_id','?')}: {errs}" for r, errs in invalid]
        self.metrics.record_validation(len(valid), len(invalid), error_msgs)

        if filters:
            valid = apply_filters(valid, *filters)

        processed: list[dict] = []

        def _collect(rec: dict) -> None:
            processed.append(rec)

        result = process_batch(valid, _collect, batch_size=self.config.batch_size)
        self.metrics.processed_records = result.processed
        self.metrics.failed_records = result.failed
        self.metrics.batches_processed = max(1, len(valid) // self.config.batch_size)

        write_ndjson(processed, destination)
        self.metrics.mark_complete()
        logger.info(
            "Pipeline run %s finished: %d records written to %s",
            self.metrics.run_id, len(processed), destination,
        )
        return result

    def stats(self) -> dict:
        """Return a summary dict of the last pipeline run metrics.

        Returns:
            Dict with run statistics (run_id, duration_s, throughput_rps, counts).
        """
        m = self.metrics
        return {
            "run_id": m.run_id,
            "total_records": m.total_records,
            "valid_records": m.valid_records,
            "invalid_records": m.invalid_records,
            "processed_records": m.processed_records,
            "failed_records": m.failed_records,
            "batches_processed": m.batches_processed,
            "duration_s": round(m.duration_s, 4),
            "throughput_rps": round(m.throughput_rps, 2),
        }
