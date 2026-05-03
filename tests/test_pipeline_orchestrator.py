"""Tests for snowflake_pipeline.pipeline orchestrator."""
from __future__ import annotations

import json
import pathlib

from snowflake_pipeline.filters import by_star_rating
from snowflake_pipeline.pipeline import ReviewPipeline


def _review(idx: int = 1, **kwargs) -> dict:
    base = {
        "review_id": f"R{idx:03d}",
        "review_date": "2024-01-01",
        "customer_id": "C1",
        "product_id": "P1",
        "product_title": "Widget",
        "product_category": "Cat",
        "star_rating": 4,
        "review_body": "Nice product.",
        "verified_purchase": "Y",
    }
    base.update(kwargs)
    return base


def _write_ndjson(path: pathlib.Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def test_pipeline_run_basic(tmp_path):
    src = tmp_path / "in.ndjson"
    dst = tmp_path / "out.ndjson"
    _write_ndjson(src, [_review(1), _review(2)])
    pipeline = ReviewPipeline()
    result = pipeline.run(src, dst)
    assert result.processed == 2
    assert dst.exists()


def test_pipeline_run_invalid_records_excluded(tmp_path):
    src = tmp_path / "in.ndjson"
    dst = tmp_path / "out.ndjson"
    _write_ndjson(src, [_review(1), {"review_id": "bad"}])
    pipeline = ReviewPipeline()
    pipeline.run(src, dst)
    lines = [line for line in dst.read_text().splitlines() if line]
    assert len(lines) == 1


def test_pipeline_run_with_filter(tmp_path):
    src = tmp_path / "in.ndjson"
    dst = tmp_path / "out.ndjson"
    _write_ndjson(src, [_review(1, star_rating=5), _review(2, star_rating=2)])
    pipeline = ReviewPipeline()
    pipeline.run(src, dst, filters=[by_star_rating(4)])
    lines = [line for line in dst.read_text().splitlines() if line]
    assert len(lines) == 1
    assert json.loads(lines[0])["star_rating"] == 5


def test_pipeline_metrics_populated(tmp_path):
    src = tmp_path / "in.ndjson"
    dst = tmp_path / "out.ndjson"
    _write_ndjson(src, [_review(i) for i in range(5)])
    pipeline = ReviewPipeline()
    pipeline.run(src, dst)
    assert pipeline.metrics.total_records == 5
    assert pipeline.metrics.processed_records == 5


def test_pipeline_run_id_set(tmp_path):
    src = tmp_path / "in.ndjson"
    dst = tmp_path / "out.ndjson"
    _write_ndjson(src, [])
    pipeline = ReviewPipeline()
    pipeline.run(src, dst)
    assert len(pipeline.metrics.run_id) == 8
