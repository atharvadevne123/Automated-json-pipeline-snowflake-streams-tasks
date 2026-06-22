"""Tests for snowflake_pipeline.utils."""
from __future__ import annotations

import pathlib

import pytest

from snowflake_pipeline.utils import (
    chunk,
    is_valid_date,
    new_run_id,
    sanitize_identifier,
    sha256_of_file,
    today_iso,
    utcnow,
)


def test_utcnow_is_timezone_aware():
    dt = utcnow()
    assert dt.tzinfo is not None
    assert dt.utcoffset().total_seconds() == 0


def test_today_iso_format():
    import re
    assert re.match(r"\d{4}-\d{2}-\d{2}", today_iso())


@pytest.mark.parametrize("val", ["2024-01-01", "2000-12-31", "1999-06-15"])
def test_is_valid_date_valid(val):
    assert is_valid_date(val) is True


@pytest.mark.parametrize("val", ["01/01/2024", "2024.01.01", "not-a-date", "", "20240101"])
def test_is_valid_date_invalid(val):
    assert is_valid_date(val) is False


def test_new_run_id_is_8_chars():
    rid = new_run_id()
    assert len(rid) == 8
    assert rid.isalnum()


def test_new_run_id_uniqueness():
    ids = {new_run_id() for _ in range(50)}
    assert len(ids) > 40  # extremely unlikely to collide


def test_sha256_of_file(tmp_path: pathlib.Path):
    p = tmp_path / "test.txt"
    p.write_text("hello", encoding="utf-8")
    digest = sha256_of_file(p)
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


def test_chunk_basic():
    result = list(chunk([1, 2, 3, 4, 5], 2))
    assert result == [[1, 2], [3, 4], [5]]


def test_chunk_exact_division():
    result = list(chunk([1, 2, 3, 4], 2))
    assert result == [[1, 2], [3, 4]]


def test_chunk_size_larger_than_list():
    result = list(chunk([1, 2], 10))
    assert result == [[1, 2]]


def test_chunk_empty():
    assert list(chunk([], 5)) == []


def test_chunk_invalid_size():
    with pytest.raises(ValueError):
        list(chunk([1, 2], 0))


@pytest.mark.parametrize("raw,expected", [
    ("hello world", "hello_world"),
    ("my-var!", "my_var_"),
    ("valid_name123", "valid_name123"),
])
def test_sanitize_identifier(raw, expected):
    assert sanitize_identifier(raw) == expected


# ---------------------------------------------------------------------------
# sample_records
# ---------------------------------------------------------------------------

def test_sample_records_reduces_count():
    from snowflake_pipeline.utils import sample_records
    records = [{"id": i} for i in range(100)]
    result = sample_records(records, 10)
    assert len(result) == 10


def test_sample_records_reproducible():
    from snowflake_pipeline.utils import sample_records
    records = [{"id": i} for i in range(100)]
    r1 = sample_records(records, 10, seed=42)
    r2 = sample_records(records, 10, seed=42)
    assert r1 == r2


def test_sample_records_returns_all_when_small():
    from snowflake_pipeline.utils import sample_records
    records = [{"id": i} for i in range(5)]
    result = sample_records(records, 10)
    assert len(result) == 5


def test_sample_records_invalid_n():
    from snowflake_pipeline.utils import sample_records
    with pytest.raises(ValueError):
        sample_records([], 0)
