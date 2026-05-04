"""Shared pytest fixtures for the snowflake_pipeline test suite."""
from __future__ import annotations

import json
import pathlib

import pytest

import snowflake_pipeline as sp


@pytest.fixture()
def valid_review() -> dict:
    """Return a fully valid review dict."""
    return {
        "review_id": "R001",
        "review_date": "2024-03-15",
        "customer_id": "C42",
        "product_id": "B00ABCDEF",
        "product_title": "Super Widget",
        "product_category": "Widgets",
        "star_rating": 5,
        "review_body": "Excellent product, highly recommended.",
        "verified_purchase": "Y",
    }


@pytest.fixture()
def invalid_review() -> dict:
    """Return a review dict missing required fields."""
    return {"review_id": "R999"}


@pytest.fixture()
def sample_reviews() -> list[dict]:
    """Return the bundled sample review records."""
    return sp.load_sample_reviews()


@pytest.fixture()
def tmp_json_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a tmp .json file path (does not exist yet)."""
    return tmp_path / "output.json"


@pytest.fixture()
def tmp_ndjson_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a tmp .ndjson file path (does not exist yet)."""
    return tmp_path / "output.ndjson"


@pytest.fixture()
def tmp_csv_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a tmp .csv file path (does not exist yet)."""
    return tmp_path / "output.csv"


@pytest.fixture()
def ndjson_path_with_records(tmp_path: pathlib.Path, valid_review: dict) -> pathlib.Path:
    """Create a temp NDJSON file containing 3 valid reviews."""
    p = tmp_path / "reviews.ndjson"
    with p.open("w") as fh:
        for i in range(3):
            rec = dict(valid_review, review_id=f"R{i:03d}")
            fh.write(json.dumps(rec) + "\n")
    return p
