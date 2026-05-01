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
