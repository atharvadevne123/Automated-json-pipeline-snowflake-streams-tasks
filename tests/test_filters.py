"""Tests for snowflake_pipeline.filters."""
from __future__ import annotations

from datetime import date

import pytest

from snowflake_pipeline.filters import (
    apply_filters,
    by_category,
    by_date_range,
    by_star_rating,
    by_verified_purchase,
)


def _rec(**kwargs) -> dict:
    base = {
        "review_id": "R1",
        "review_date": "2024-06-01",
        "star_rating": 4,
        "verified_purchase": "Y",
        "product_category": "Electronics",
    }
    base.update(kwargs)
    return base


@pytest.mark.parametrize("rating,min_s,max_s,expected", [
    (4, 3, 5, True),
    (2, 3, 5, False),
    (5, 5, 5, True),
    (1, 2, 4, False),
])
def test_by_star_rating(rating, min_s, max_s, expected):
    fn = by_star_rating(min_s, max_s)
    assert fn(_rec(star_rating=rating)) is expected


@pytest.mark.parametrize("vp,keep_verified,expected", [
    ("Y", True, True),
    ("N", True, False),
    ("Y", False, False),
    ("N", False, True),
])
def test_by_verified_purchase(vp, keep_verified, expected):
    fn = by_verified_purchase(keep_verified)
    assert fn(_rec(verified_purchase=vp)) is expected


def test_by_date_range_within():
    fn = by_date_range(date(2024, 1, 1), date(2024, 12, 31))
    assert fn(_rec(review_date="2024-06-15")) is True


def test_by_date_range_before_start():
    fn = by_date_range(date(2024, 7, 1))
    assert fn(_rec(review_date="2024-06-15")) is False


def test_by_date_range_after_end():
    fn = by_date_range(end=date(2024, 5, 31))
    assert fn(_rec(review_date="2024-06-15")) is False


def test_by_date_range_invalid_date():
    fn = by_date_range(date(2024, 1, 1))
    assert fn(_rec(review_date="not-a-date")) is False


def test_by_category_match():
    fn = by_category({"Electronics", "Books"})
    assert fn(_rec(product_category="Books")) is True


def test_by_category_no_match():
    fn = by_category({"Electronics"})
    assert fn(_rec(product_category="Clothing")) is False


def test_apply_filters_multiple():
    records = [
        _rec(star_rating=5, verified_purchase="Y"),
        _rec(star_rating=2, verified_purchase="Y"),
        _rec(star_rating=5, verified_purchase="N"),
    ]
    result = apply_filters(records, by_star_rating(4), by_verified_purchase(True))
    assert len(result) == 1
    assert result[0]["star_rating"] == 5


def test_apply_filters_no_filters():
    records = [_rec(), _rec(review_id="R2")]
    assert apply_filters(records) == records


def test_apply_filters_empty_records():
    assert apply_filters([], by_star_rating()) == []


# ---------------------------------------------------------------------------
# by_min_review_length
# ---------------------------------------------------------------------------

def test_by_min_review_length_passes_long():
    from snowflake_pipeline.filters import by_min_review_length
    f = by_min_review_length(10)
    assert f({"review_body": "A" * 10}) is True


def test_by_min_review_length_fails_short():
    from snowflake_pipeline.filters import by_min_review_length
    f = by_min_review_length(10)
    assert f({"review_body": "Short"}) is False


def test_by_min_review_length_missing_field():
    from snowflake_pipeline.filters import by_min_review_length
    f = by_min_review_length(1)
    assert f({}) is False


# ---------------------------------------------------------------------------
# by_product_ids
# ---------------------------------------------------------------------------

def test_by_product_ids_match():
    from snowflake_pipeline.filters import by_product_ids
    f = by_product_ids({"B001", "B002"})
    assert f({"product_id": "B001"}) is True


def test_by_product_ids_no_match():
    from snowflake_pipeline.filters import by_product_ids
    f = by_product_ids({"B001"})
    assert f({"product_id": "B999"}) is False


def test_by_product_ids_empty_set():
    from snowflake_pipeline.filters import by_product_ids
    f = by_product_ids(set())
    assert f({"product_id": "B001"}) is False


# ---------------------------------------------------------------------------
# by_customer_ids
# ---------------------------------------------------------------------------

def test_by_customer_ids_match():
    from snowflake_pipeline.filters import by_customer_ids
    f = by_customer_ids({"C001", "C002"})
    assert f({"customer_id": "C001"}) is True


def test_by_customer_ids_no_match():
    from snowflake_pipeline.filters import by_customer_ids
    f = by_customer_ids({"C001"})
    assert f({"customer_id": "C999"}) is False


# ---------------------------------------------------------------------------
# exclude_categories
# ---------------------------------------------------------------------------

def test_exclude_categories_excludes():
    from snowflake_pipeline.filters import exclude_categories
    f = exclude_categories({"Electronics"})
    assert f({"product_category": "Electronics"}) is False


def test_exclude_categories_keeps_others():
    from snowflake_pipeline.filters import exclude_categories
    f = exclude_categories({"Electronics"})
    assert f({"product_category": "Books"}) is True


def test_exclude_categories_empty_set():
    from snowflake_pipeline.filters import exclude_categories
    f = exclude_categories(set())
    assert f({"product_category": "Electronics"}) is True
