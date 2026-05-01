"""Tests for snowflake_pipeline utilities."""
from __future__ import annotations

import pathlib

import pytest

import snowflake_pipeline as sp

# ---------------------------------------------------------------------------
# list_sql
# ---------------------------------------------------------------------------

def test_list_sql_returns_list():
    result = sp.list_sql()
    assert isinstance(result, list)


def test_list_sql_contains_optimized():
    assert "snowflake_optimized.sql" in sp.list_sql()


def test_list_sql_is_sorted():
    result = sp.list_sql()
    assert result == sorted(result)


def test_list_sql_only_sql_files():
    for name in sp.list_sql():
        assert name.endswith(".sql"), f"Non-SQL entry returned: {name}"


# ---------------------------------------------------------------------------
# get_sql
# ---------------------------------------------------------------------------

def test_get_sql_default_returns_string():
    content = sp.get_sql()
    assert isinstance(content, str)
    assert len(content) > 0


def test_get_sql_default_contains_create_table():
    content = sp.get_sql()
    assert "CREATE" in content.upper()


def test_get_sql_explicit_name():
    content = sp.get_sql("snowflake_optimized.sql")
    assert isinstance(content, str)
    assert len(content) > 100


def test_get_sql_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        sp.get_sql("does_not_exist.sql")


def test_get_sql_path_traversal_raises():
    with pytest.raises(ValueError):
        sp.get_sql("../../etc/passwd")


def test_get_sql_path_traversal_absolute_raises():
    with pytest.raises(ValueError):
        sp.get_sql("/etc/passwd")


# ---------------------------------------------------------------------------
# get_sample_data_path
# ---------------------------------------------------------------------------

def test_get_sample_data_path_returns_string():
    path = sp.get_sample_data_path()
    assert isinstance(path, str)


def test_get_sample_data_path_file_exists():
    path = sp.get_sample_data_path()
    assert pathlib.Path(path).exists(), f"Sample data file missing: {path}"


def test_get_sample_data_path_is_json():
    path = sp.get_sample_data_path()
    assert path.endswith(".json")


# ---------------------------------------------------------------------------
# load_sample_reviews
# ---------------------------------------------------------------------------

def test_load_sample_reviews_returns_list():
    records = sp.load_sample_reviews()
    assert isinstance(records, list)


def test_load_sample_reviews_non_empty():
    records = sp.load_sample_reviews()
    assert len(records) > 0


def test_load_sample_reviews_all_dicts():
    for record in sp.load_sample_reviews():
        assert isinstance(record, dict)


def test_load_sample_reviews_have_review_id():
    for record in sp.load_sample_reviews():
        assert "review_id" in record


def test_load_sample_reviews_star_rating_range():
    for record in sp.load_sample_reviews():
        rating = record.get("star_rating")
        assert isinstance(rating, int), f"star_rating not int: {rating!r}"
        assert 1 <= rating <= 5, f"star_rating out of range: {rating}"


def test_load_sample_reviews_verified_purchase_values():
    for record in sp.load_sample_reviews():
        vp = record.get("verified_purchase")
        assert vp in ("Y", "N"), f"Unexpected verified_purchase: {vp!r}"


# ---------------------------------------------------------------------------
# validate_review — parametrized edge cases
# ---------------------------------------------------------------------------

def _valid_review() -> dict:
    return {
        "review_id": "abc-123",
        "review_date": "2023-01-01",
        "customer_id": "42",
        "product_id": "prod-1",
        "product_title": "Widget",
        "product_category": "Gadgets",
        "star_rating": 4,
        "review_body": "Great product!",
        "verified_purchase": "Y",
    }


def test_validate_review_valid_record():
    assert sp.validate_review(_valid_review()) == []


def test_validate_review_missing_field():
    rec = _valid_review()
    del rec["review_id"]
    errors = sp.validate_review(rec)
    assert any("review_id" in e for e in errors)


@pytest.mark.parametrize("star", [0, 6, -1, 100])
def test_validate_review_bad_star_rating(star):
    rec = {**_valid_review(), "star_rating": star}
    assert any("star_rating" in e for e in sp.validate_review(rec))


@pytest.mark.parametrize("vp", ["X", "y", "n", "", "YES", "true"])
def test_validate_review_bad_verified_purchase(vp):
    rec = {**_valid_review(), "verified_purchase": vp}
    assert any("verified_purchase" in e for e in sp.validate_review(rec))


@pytest.mark.parametrize("date_val", ["01-01-2023", "2023/01/01", "not-a-date", "20230101"])
def test_validate_review_bad_date_format(date_val):
    rec = {**_valid_review(), "review_date": date_val}
    assert any("review_date" in e for e in sp.validate_review(rec))


@pytest.mark.parametrize("field_name", ["review_id", "customer_id", "product_id"])
def test_validate_review_empty_string_required_field(field_name):
    rec = {**_valid_review(), field_name: ""}
    assert any(field_name in e for e in sp.validate_review(rec))


def test_validate_review_all_sample_records_valid():
    records = sp.load_sample_reviews()
    for i, record in enumerate(records):
        errors = sp.validate_review(record)
        assert errors == [], f"Record {i} failed validation: {errors}"


# ---------------------------------------------------------------------------
# Integration: SQL content references expected objects
# ---------------------------------------------------------------------------

def test_sql_contains_stream_definition():
    content = sp.get_sql()
    assert "CREATE OR REPLACE STREAM" in content


def test_sql_contains_task_definition():
    content = sp.get_sql()
    assert "CREATE OR REPLACE TASK" in content


def test_sql_contains_merge_statements():
    content = sp.get_sql()
    assert "MERGE INTO" in content.upper() or "MERGE\n" in content.upper()


def test_sql_contains_warehouse_config():
    content = sp.get_sql()
    assert "WAREHOUSE" in content.upper()


# ---------------------------------------------------------------------------
# Additional edge cases
# ---------------------------------------------------------------------------

def test_validate_review_returns_list_type():
    result = sp.validate_review(_valid_review())
    assert isinstance(result, list)


def test_validate_review_star_rating_string_type():
    rec = {**_valid_review(), "star_rating": "five"}
    errors = sp.validate_review(rec)
    assert any("star_rating" in e for e in errors)


def test_get_sql_name_with_null_byte_raises():
    with pytest.raises((ValueError, FileNotFoundError, OSError)):
        sp.get_sql("\x00evil.sql")


def test_load_sample_reviews_have_all_required_fields():
    required = {
        "review_id", "review_date", "customer_id", "product_id",
        "product_title", "product_category", "star_rating",
        "review_body", "verified_purchase",
    }
    for rec in sp.load_sample_reviews():
        assert required.issubset(rec.keys())


def test_list_sql_returns_at_least_one():
    assert len(sp.list_sql()) >= 1
