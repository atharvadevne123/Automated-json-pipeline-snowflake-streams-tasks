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

__all__ = [
    "ValidatorFn",
    "REQUIRED_FIELDS",
    "validate_record",
    "validate_batch",
    "validation_report",
]

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
        logger.debug("Record missing fields: %s", sorted(missing))

    for field_name in _STRING_FIELDS:
        if field_name in record:
            errs = _check_non_empty_str(record[field_name], field_name)
            if errs:
                logger.debug("Field %s failed non-empty check: %s", field_name, errs)
            errors.extend(errs)

    if "review_date" in record:
        errs = _check_date_format(record["review_date"], "review_date")
        if errs:
            logger.debug("review_date format invalid: %r", record["review_date"])
        errors.extend(errs)

    if "star_rating" in record:
        errs = _check_star_rating(record["star_rating"])
        if errs:
            logger.debug("star_rating invalid: %r", record["star_rating"])
        errors.extend(errs)

    if "verified_purchase" in record:
        errs = _check_verified_purchase(record["verified_purchase"])
        if errs:
            logger.debug("verified_purchase invalid: %r", record["verified_purchase"])
        errors.extend(errs)

    if errors:
        logger.info("Record %r failed validation with %d error(s)", record.get("review_id", "?"), len(errors))
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
    logger.info(
        "Batch validated: %d valid, %d invalid out of %d total",
        len(valid), len(invalid), len(records),
    )
    return valid, invalid


def validation_report(records: list[dict]) -> dict:
    """Generate a structured validation report for a list of records.

    Args:
        records: List of review dicts to validate.

    Returns:
        Dict with keys: total, valid_count, invalid_count, error_summary.
    """
    valid, invalid = validate_batch(records)
    error_summary: dict[str, int] = {}
    for _, errs in invalid:
        for err in errs:
            key = err.split("must")[0].strip() if "must" in err else err[:40]
            error_summary[key] = error_summary.get(key, 0) + 1
    report = {
        "total": len(records),
        "valid_count": len(valid),
        "invalid_count": len(invalid),
        "pass_rate": len(valid) / len(records) if records else 1.0,
        "error_summary": error_summary,
    }
    logger.info(
        "Validation report: %d/%d passed (%.1f%%)",
        report["valid_count"], report["total"], report["pass_rate"] * 100,
    )
    return report
