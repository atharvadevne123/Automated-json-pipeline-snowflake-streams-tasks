"""Tests for snowflake_pipeline.transformers."""
from __future__ import annotations

import pytest

from snowflake_pipeline.transformers import (
    coerce_star_rating,
    coerce_verified_purchase,
    flatten_for_csv,
    normalise_review,
    normalise_text,
    parse_review_date,
)


@pytest.mark.parametrize("raw,expected", [
    ("  hello  world  ", "hello world"),
    ("line\r\nbreak", "line break"),
    ("\x01hidden\x1f", "hidden"),
    ("normal", "normal"),
])
def test_normalise_text(raw, expected):
    assert normalise_text(raw) == expected


def test_normalise_review_strips_fields():
    rec = {"review_id": "  R1 ", "review_body": "  good  ", "star_rating": 4}
    result = normalise_review(rec)
    assert result["review_id"] == "R1"
    assert result["review_body"] == "good"


def test_normalise_review_does_not_mutate_original():
    rec = {"review_id": "  R1 "}
    normalise_review(rec)
    assert rec["review_id"] == "  R1 "


def test_normalise_review_preserves_non_string():
    rec = {"star_rating": 5, "review_id": "R1"}
    result = normalise_review(rec)
    assert result["star_rating"] == 5


@pytest.mark.parametrize("val,expected", [
    (4, 4), ("3", 3), (2.9, 2), (5.0, 5),
])
def test_coerce_star_rating_valid(val, expected):
    assert coerce_star_rating(val) == expected


@pytest.mark.parametrize("val", [0, 6, "bad", None, "five"])
def test_coerce_star_rating_invalid(val):
    assert coerce_star_rating(val) is None


@pytest.mark.parametrize("val,expected", [
    ("Y", "Y"), ("N", "N"), ("yes", "Y"), ("NO", "N"),
    ("TRUE", "Y"), ("0", "N"), (True, "Y"), (False, "N"),
])
def test_coerce_verified_purchase_valid(val, expected):
    assert coerce_verified_purchase(val) == expected


@pytest.mark.parametrize("val", ["X", "maybe", 2, None])
def test_coerce_verified_purchase_invalid(val):
    assert coerce_verified_purchase(val) is None


def test_parse_review_date_valid():
    from datetime import date
    assert parse_review_date("2024-06-15") == date(2024, 6, 15)


@pytest.mark.parametrize("bad", ["01/01/2024", "not-a-date", None, 20240101])
def test_parse_review_date_invalid(bad):
    assert parse_review_date(bad) is None


def test_flatten_for_csv_converts_all_to_strings():
    rec = {"rating": 5, "vp": True, "body": None}
    flat = flatten_for_csv(rec)
    assert flat["rating"] == "5"
    assert flat["vp"] == "True"
    assert flat["body"] == ""


# ---------------------------------------------------------------------------
# Parametrized edge cases
# ---------------------------------------------------------------------------

import pytest

@pytest.mark.parametrize("value,expected", [
    (1, 1),
    (5, 5),
    (3, 3),
    (1.9, 1),
    ("4", 4),
    ("5.9", 5),
    (0, None),
    (6, None),
    ("bad", None),
    (None, None),
])
def test_coerce_star_rating_parametrized(value, expected):
    from snowflake_pipeline.transformers import coerce_star_rating
    assert coerce_star_rating(value) == expected


@pytest.mark.parametrize("value,expected", [
    (True, "Y"),
    (False, "N"),
    ("Y", "Y"),
    ("yes", "Y"),
    ("TRUE", "Y"),
    ("1", "Y"),
    ("N", "N"),
    ("no", "N"),
    ("FALSE", "N"),
    ("0", "N"),
    ("maybe", None),
    (None, None),
])
def test_coerce_verified_purchase_parametrized(value, expected):
    from snowflake_pipeline.transformers import coerce_verified_purchase
    assert coerce_verified_purchase(value) == expected


# ---------------------------------------------------------------------------
# truncate_text
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,max_chars,expected", [
    ("Hello", 10, "Hello"),
    ("Hello World", 8, "Hello W…"),
    ("Hi", 2, "Hi"),
    ("Hello", 3, "He…"),
    ("", 5, ""),
])
def test_truncate_text(text, max_chars, expected):
    from snowflake_pipeline.transformers import truncate_text
    assert truncate_text(text, max_chars) == expected


# ---------------------------------------------------------------------------
# mask_customer_id
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("customer_id,visible,expected", [
    ("C12345678", 4, "*****5678"),
    ("C123", 4, "C123"),
    ("AB", 4, "AB"),
    ("ABCDE", 2, "***DE"),
])
def test_mask_customer_id(customer_id, visible, expected):
    from snowflake_pipeline.transformers import mask_customer_id
    assert mask_customer_id(customer_id, visible) == expected
