"""Standalone validators for pipeline data records."""
from __future__ import annotations

import logging
import re
from typing import Any, Callable

from snowflake_pipeline.constants import (
    DATE_PATTERN,
    STAR_RATING_MAX,
    STAR_RATING_MIN,
    VERIFIED_PURCHASE_VALUES,
)
from snowflake_pipeline.exceptions import ValidationError

logger = logging.getLogger(__name__)

_DATE_RE = re.compile(DATE_PATTERN)

ValidatorFn = Callable[[Any], list[str]]


def _check_non_empty_str(value: Any, field: str) -> list[str]:
    """Return errors if value is not a non-empty string."""
    if not isinstance(value, str) or not value.strip():
        return [f"{field} must be a non-empty string, got {value!r}"]
    return []


def _check_date_format(value: Any, field: str) -> list[str]:
    """Return errors if value does not match YYYY-MM-DD."""
    if not isinstance(value, str) or not _DATE_RE.match(value):
        return [f"{field} must be YYYY-MM-DD, got {value!r}"]
    return []


def _check_star_rating(value: Any) -> list[str]:
    """Return errors if star_rating is outside [1, 5]."""
    if not isinstance(value, int) or not (STAR_RATING_MIN <= value <= STAR_RATING_MAX):
        return [f"star_rating must be int {STAR_RATING_MIN}-{STAR_RATING_MAX}, got {value!r}"]
    return []


def _check_verified_purchase(value: Any) -> list[str]:
    """Return errors if verified_purchase is not Y or N."""
    if value not in VERIFIED_PURCHASE_VALUES:
        return [f"verified_purchase must be one of {sorted(VERIFIED_PURCHASE_VALUES)}, got {value!r}"]
    return []


_STRING_FIELDS = ("review_id", "customer_id", "product_id", "product_title", "product_category", "review_body")

REQUIRED_FIELDS = frozenset(
    {
        "review_id", "review_date", "customer_id", "product_id",
        "product_title", "product_category", "star_rating", "review_body", "verified_purchase",
    }
)


def validate_record(record: dict) -> list[str]:
    """Validate a single review record and return all error messages.

    Args:
        record: A dict representing one review.

    Returns:
        List of human-readable error strings (empty if valid).
    """
    errors: list[str] = []

    missing = REQUIRED_FIELDS - record.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    for field_name in _STRING_FIELDS:
        if field_name in record:
            errors.extend(_check_non_empty_str(record[field_name], field_name))

    if "review_date" in record:
        errors.extend(_check_date_format(record["review_date"], "review_date"))

    if "star_rating" in record:
        errors.extend(_check_star_rating(record["star_rating"]))

    if "verified_purchase" in record:
        errors.extend(_check_verified_purchase(record["verified_purchase"]))

    if errors:
        logger.debug("Record validation failed with %d error(s)", len(errors))
    return errors


def assert_valid(record: dict) -> None:
    """Validate a record and raise ValidationError if invalid.

    Args:
        record: A dict representing one review.

    Raises:
        ValidationError: containing all validation error messages.
    """
    errors = validate_record(record)
    if errors:
        raise ValidationError(errors, record)


def validate_batch(records: list[dict]) -> tuple[list[dict], list[tuple[dict, list[str]]]]:
    """Partition records into valid and invalid lists.

    Args:
        records: List of review dicts to validate.

    Returns:
        Tuple of (valid_records, [(invalid_record, errors), ...]).
    """
    valid: list[dict] = []
    invalid: list[tuple[dict, list[str]]] = []
    for rec in records:
        errs = validate_record(rec)
        if errs:
            invalid.append((rec, errs))
        else:
            valid.append(rec)
    logger.info("Batch validated: %d valid, %d invalid out of %d", len(valid), len(invalid), len(records))
    return valid, invalid
