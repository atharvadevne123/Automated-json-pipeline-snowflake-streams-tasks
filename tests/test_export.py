"""Tests for snowflake_pipeline.export."""
from __future__ import annotations

import csv
import json
import pathlib

import pytest

from snowflake_pipeline.exceptions import ExportError
from snowflake_pipeline.export import to_csv, to_json, to_ndjson


def _review(idx: int = 1) -> dict:
    return {
        "review_id": f"R{idx:03d}",
        "review_date": "2024-01-01",
        "customer_id": "C1",
        "product_id": "P1",
        "product_title": "Widget",
        "product_category": "Cat",
        "star_rating": 4,
        "review_body": "Good.",
        "verified_purchase": "Y",
    }


def test_to_json_writes_file(tmp_path):
    p = tmp_path / "out.json"
    count = to_json([_review()], p)
    assert count == 1
    assert p.exists()
    data = json.loads(p.read_text())
    assert isinstance(data, list)
    assert data[0]["review_id"] == "R001"


def test_to_json_empty_list(tmp_path):
    p = tmp_path / "empty.json"
    count = to_json([], p)
    assert count == 0
    assert json.loads(p.read_text()) == []


def test_to_ndjson_roundtrip(tmp_path):
    p = tmp_path / "out.ndjson"
    records = [_review(i) for i in range(3)]
    count = to_ndjson(records, p)
    assert count == 3
    lines = [line for line in p.read_text().splitlines() if line]
    assert len(lines) == 3
    assert json.loads(lines[0])["review_id"] == "R000"


def test_to_csv_creates_file(tmp_path):
    p = tmp_path / "out.csv"
    count = to_csv([_review(), _review(2)], p)
    assert count == 2
    with p.open() as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    assert len(rows) == 2


def test_to_csv_custom_fieldnames(tmp_path):
    p = tmp_path / "out.csv"
    to_csv([_review()], p, fieldnames=["review_id", "star_rating"])
    with p.open() as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    assert set(rows[0].keys()) == {"review_id", "star_rating"}


def test_to_csv_empty_records_returns_zero(tmp_path):
    p = tmp_path / "empty.csv"
    assert to_csv([], p) == 0


def test_to_json_raises_on_bad_path():
    with pytest.raises(ExportError):
        to_json([_review()], pathlib.Path("/no/such/directory/out.json"))
