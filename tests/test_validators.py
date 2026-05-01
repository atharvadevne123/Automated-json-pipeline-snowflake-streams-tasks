"""Tests for snowflake_pipeline.validators."""
from __future__ import annotations

import pytest

from snowflake_pipeline.exceptions import ValidationError
from snowflake_pipeline.validators import assert_valid, validate_batch, validate_record


def _base() -> dict:
    return {
        "review_id": "R001",
        "review_date": "2024-01-01",
        "customer_id": "C1",
        "product_id": "P1",
        "product_title": "Widget",
        "product_category": "Cat",
        "star_rating": 3,
        "review_body": "Fine.",
        "verified_purchase": "Y",
    }


def test_validate_record_valid():
    assert validate_record(_base()) == []


def test_validate_record_missing_fields():
    errors = validate_record({})
    assert any("Missing fields" in e for e in errors)


@pytest.mark.parametrize("rating", [0, 6, -1, 100])
def test_validate_record_bad_rating(rating):
    rec = {**_base(), "star_rating": rating}
    errors = validate_record(rec)
    assert any("star_rating" in e for e in errors)


@pytest.mark.parametrize("vp", ["X", "y", "n", "", "YES"])
def test_validate_record_bad_verified_purchase(vp):
    rec = {**_base(), "verified_purchase": vp}
    errors = validate_record(rec)
    assert any("verified_purchase" in e for e in errors)


@pytest.mark.parametrize("date_val", ["01-01-2024", "2024/01/01", "not-a-date", "20240101"])
def test_validate_record_bad_date(date_val):
    rec = {**_base(), "review_date": date_val}
    errors = validate_record(rec)
    assert any("review_date" in e for e in errors)


@pytest.mark.parametrize("field_name", ["review_id", "customer_id", "product_id", "product_title"])
def test_validate_record_empty_string_field(field_name):
    rec = {**_base(), field_name: ""}
    errors = validate_record(rec)
    assert any(field_name in e for e in errors)


def test_assert_valid_raises_on_invalid():
    with pytest.raises(ValidationError) as exc_info:
        assert_valid({"review_id": "R1"})
    assert exc_info.value.errors


def test_assert_valid_passes_on_valid():
    assert_valid(_base())  # must not raise


def test_validate_batch_splits_correctly():
    records = [_base(), {"review_id": "bad"}]
    valid, invalid = validate_batch(records)
    assert len(valid) == 1
    assert len(invalid) == 1


def test_validate_batch_all_valid():
    records = [_base(), {**_base(), "review_id": "R002"}]
    valid, invalid = validate_batch(records)
    assert len(valid) == 2
    assert invalid == []


def test_validate_batch_all_invalid():
    records = [{}, {}]
    valid, invalid = validate_batch(records)
    assert valid == []
    assert len(invalid) == 2


def test_validate_batch_empty():
    valid, invalid = validate_batch([])
    assert valid == [] and invalid == []
