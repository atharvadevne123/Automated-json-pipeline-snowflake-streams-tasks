#!/usr/bin/env python3
"""Run the pipeline on a file and export metrics to JSON."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

try:
    from snowflake_pipeline.pipeline import ReviewPipeline
    from snowflake_pipeline.config import PipelineConfig
    from snowflake_pipeline.metrics import dump_metrics
except ImportError as e:
    print(f"ERROR: snowflake_pipeline not installed. Run: pip install -e .\n{e}")
    sys.exit(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline and export metrics.")
    parser.add_argument("source", type=Path, help="Input NDJSON file")
    parser.add_argument("destination", type=Path, help="Output NDJSON file")
    parser.add_argument("--metrics-out", type=Path, default=None, help="Metrics JSON output path")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    args = parser.parse_args()

    cfg = PipelineConfig(batch_size=args.batch_size)
    pipeline = ReviewPipeline(config=cfg)
    result = pipeline.run(args.source, args.destination)

    print(dump_metrics(pipeline.metrics))

    if args.metrics_out:
        pipeline.metrics.save(args.metrics_out)
        print(f"Metrics saved to: {args.metrics_out}")

    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
