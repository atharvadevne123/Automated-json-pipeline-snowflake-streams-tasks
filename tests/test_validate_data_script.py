"""Tests for scripts/validate_data.py CLI."""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys


def _review(**kwargs) -> dict:
    base = {
        "review_id": "R001", "review_date": "2024-01-01",
        "customer_id": "C1", "product_id": "P1",
        "product_title": "Widget", "product_category": "Cat",
        "star_rating": 4, "review_body": "Good.", "verified_purchase": "Y",
    }
    base.update(kwargs)
    return base


def _write_ndjson(path: pathlib.Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def test_validate_data_all_valid(tmp_path: pathlib.Path):
    src = tmp_path / "in.ndjson"
    _write_ndjson(src, [_review(), _review(review_id="R002")])
    result = subprocess.run(
        [sys.executable, "scripts/validate_data.py", str(src)],
        capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert result.returncode == 0
    assert "Valid   : 2" in result.stdout


def test_validate_data_with_invalid_strict(tmp_path: pathlib.Path):
    src = tmp_path / "in.ndjson"
    _write_ndjson(src, [_review(), {"review_id": "bad"}])
    result = subprocess.run(
        [sys.executable, "scripts/validate_data.py", str(src), "--strict"],
        capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert result.returncode == 1


def test_validate_data_limit_flag(tmp_path: pathlib.Path):
    src = tmp_path / "in.ndjson"
    _write_ndjson(src, [_review(review_id=f"R{i:03d}") for i in range(10)])
    result = subprocess.run(
        [sys.executable, "scripts/validate_data.py", str(src), "--limit", "3"],
        capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert "Total   : 3" in result.stdout
