"""Tests for snowflake_pipeline.constants."""
from __future__ import annotations

from snowflake_pipeline import constants as C


def test_star_rating_range():
    assert C.STAR_RATING_MIN == 1
    assert C.STAR_RATING_MAX == 5
    assert C.STAR_RATING_MIN < C.STAR_RATING_MAX


def test_verified_purchase_values():
    assert "Y" in C.VERIFIED_PURCHASE_VALUES
    assert "N" in C.VERIFIED_PURCHASE_VALUES
    assert len(C.VERIFIED_PURCHASE_VALUES) == 2


def test_default_batch_size_positive():
    assert C.DEFAULT_BATCH_SIZE > 0


def test_max_batch_size_gte_default():
    assert C.MAX_BATCH_SIZE >= C.DEFAULT_BATCH_SIZE


def test_default_retry_attempts():
    assert C.DEFAULT_RETRY_ATTEMPTS >= 1


def test_date_format_string():
    assert C.DATE_FORMAT == "%Y-%m-%d"


def test_csv_delimiter():
    assert len(C.CSV_DELIMITER) == 1


def test_ndjson_encoding():
    assert C.NDJSON_ENCODING == "utf-8"


def test_metrics_key_names():
    assert C.METRICS_KEY_TOTAL == "total"
    assert C.METRICS_KEY_VALID == "valid"
    assert C.METRICS_KEY_INVALID == "invalid"
