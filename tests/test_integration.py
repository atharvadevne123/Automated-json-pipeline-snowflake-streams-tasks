"""End-to-end integration tests for the snowflake_pipeline package."""
from __future__ import annotations

import json
import pathlib

import pytest

import snowflake_pipeline as sp
from snowflake_pipeline.batch_processor import process_batch
from snowflake_pipeline.export import to_csv, to_json, to_ndjson
from snowflake_pipeline.filters import apply_filters, by_star_rating, by_verified_purchase
from snowflake_pipeline.io import read_ndjson, write_ndjson
from snowflake_pipeline.metrics import PipelineMetrics
from snowflake_pipeline.validators import validate_batch


def test_full_validate_export_cycle(tmp_path: pathlib.Path):
    """Load sample data, validate, filter, and export to all formats."""
    records = sp.load_sample_reviews()

    valid, invalid = validate_batch(records)
    assert len(valid) > 0

    high_rated = apply_filters(valid, by_star_rating(4, 5), by_verified_purchase(True))

    json_out = tmp_path / "high_rated.json"
    csv_out = tmp_path / "high_rated.csv"
    ndjson_out = tmp_path / "high_rated.ndjson"

    assert to_json(high_rated, json_out) == len(high_rated)
    assert to_csv(high_rated, csv_out) == len(high_rated)
    assert to_ndjson(high_rated, ndjson_out) == len(high_rated)

    assert json_out.exists() and json_out.stat().st_size > 0
    assert csv_out.exists() and csv_out.stat().st_size > 0
    assert ndjson_out.exists() and ndjson_out.stat().st_size > 0


def test_ndjson_roundtrip_with_sample_data(tmp_path: pathlib.Path):
    """Write sample reviews to NDJSON and read back — data must match."""
    records = sp.load_sample_reviews()
    p = tmp_path / "reviews.ndjson"
    write_ndjson(records, p)
    restored = read_ndjson(p)
    assert len(restored) == len(records)
    assert restored[0]["review_id"] == records[0]["review_id"]


def test_batch_processor_with_real_data():
    """Verify batch processor handles sample data without errors."""
    records = sp.load_sample_reviews()
    processed = []

    def collect(r: dict) -> None:
        processed.append(r)

    result = process_batch(records, collect, batch_size=10)
    assert result.processed == len(records)
    assert result.failed == 0


def test_metrics_tracks_full_run():
    """Metrics accurately reflect validation results on real data."""
    records = sp.load_sample_reviews()
    m = PipelineMetrics(run_id="integration-test")
    valid, invalid = validate_batch(records)
    m.record_validation(len(valid), len(invalid))
    m.total_records = len(records)
    m.mark_complete()
    assert m.valid_records + m.invalid_records == m.total_records
    assert m.duration_s >= 0


def test_sql_metadata_from_bundled_script():
    """Extract metadata from the bundled SQL and verify expected objects exist."""
    from snowflake_pipeline.sql_meta import extract_metadata
    sql = sp.get_sql()
    meta = extract_metadata(sql)
    assert len(meta.tables) >= 5
    assert len(meta.tasks) >= 1
    assert len(meta.streams) >= 1
