#!/usr/bin/env python3
"""Performance benchmark for the snowflake_pipeline ETL pipeline."""
from __future__ import annotations

import json
import logging
import tempfile
import time
from pathlib import Path

logging.basicConfig(level=logging.WARNING)

try:
    from snowflake_pipeline.batch_processor import process_batch
    from snowflake_pipeline.transformers import normalise_review
    from snowflake_pipeline.validators import validate_batch
    from snowflake_pipeline.io import write_ndjson, read_ndjson
except ImportError as e:
    print(f"ERROR: snowflake_pipeline not installed. Run: pip install -e .\n{e}")
    raise SystemExit(1)


def _make_record(i: int) -> dict:
    return {
        "review_id": f"R{i:06d}",
        "review_date": "2024-01-15",
        "customer_id": f"C{i % 1000:04d}",
        "product_id": f"B{i % 500:05d}",
        "product_title": "  Sample Product  ",
        "product_category": ["Electronics", "Books", "Sports"][i % 3],
        "star_rating": (i % 5) + 1,
        "review_body": "This is a sample review body with some content." * (1 + i % 3),
        "verified_purchase": "Y" if i % 2 == 0 else "N",
    }


def benchmark(n: int = 10_000) -> None:
    print(f"Benchmarking with {n:,} records...")
    records = [_make_record(i) for i in range(n)]

    # normalise
    t0 = time.perf_counter()
    normalised = [normalise_review(r) for r in records]
    dur = time.perf_counter() - t0
    print(f"  normalise_review: {dur*1000:.1f}ms  ({n/dur:,.0f} rps)")

    # validate
    t0 = time.perf_counter()
    valid, invalid = validate_batch(normalised)
    dur = time.perf_counter() - t0
    print(f"  validate_batch:   {dur*1000:.1f}ms  ({n/dur:,.0f} rps) — {len(invalid)} invalid")

    # process_batch
    t0 = time.perf_counter()
    result = process_batch(valid, lambda r: None, batch_size=500)
    dur = time.perf_counter() - t0
    print(f"  process_batch:    {dur*1000:.1f}ms  ({result.processed/dur:,.0f} rps)")

    # I/O round-trip
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "bench.ndjson"
        t0 = time.perf_counter()
        write_ndjson(valid, p)
        read_ndjson(p)
        dur = time.perf_counter() - t0
        size_mb = p.stat().st_size / 1024 / 1024
        print(f"  ndjson round-trip:{dur*1000:.1f}ms  ({size_mb:.2f} MB)")

    print("Done.")


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
    benchmark(n)
