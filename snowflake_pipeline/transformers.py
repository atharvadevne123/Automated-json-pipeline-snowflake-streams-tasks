"""Data transformation helpers for review records."""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

_MULTI_SPACE_RE = re.compile(r"\s+")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")


def normalise_text(value: str) -> str:
    """Normalise a text field: strip, collapse whitespace, remove control chars.

    Args:
        value: Raw string value.

    Returns:
        Cleaned string.
    """
    value = unicodedata.normalize("NFC", value)
    value = _CONTROL_CHAR_RE.sub("", value)
    value = _MULTI_SPACE_RE.sub(" ", value).strip()
    return value


def normalise_review(record: dict) -> dict:
    """Return a shallow copy of *record* with all string fields normalised.

    Args:
        record: Raw review dict.

    Returns:
        New dict with cleaned string values.
    """
    text_fields = ("review_id", "customer_id", "product_id", "product_title",
                   "product_category", "review_body", "review_date")
    result = dict(record)
    changed = 0
    for field in text_fields:
        if isinstance(result.get(field), str):
            original = result[field]
            result[field] = normalise_text(original)
            if result[field] != original:
                changed += 1
    if changed:
        logger.debug("Normalised %d field(s) in review %s", changed, result.get("review_id", "<unknown>"))
    return result


def coerce_star_rating(value: Any) -> int | None:
    """Attempt to coerce *value* to a valid star rating integer.

    Args:
        value: Raw star_rating value (int, float, or str).

    Returns:
        Integer 1-5 on success, None if coercion is impossible.
    """
    try:
        as_int = int(float(value))
        if 1 <= as_int <= 5:
            return as_int
    except (ValueError, TypeError):
        logger.debug("Could not coerce star_rating %r to int", value)
    return None


def coerce_verified_purchase(value: Any) -> str | None:
    """Normalise verified_purchase to \'Y\' or \'N\'.

    Args:
        value: Raw value (e.g. True, "yes", "1", "Y").

    Returns:
        \'Y\', \'N\', or None if unrecognisable.
    """
    if isinstance(value, bool):
        return "Y" if value else "N"
    if isinstance(value, str):
        v = value.strip().upper()
        if v in ("Y", "YES", "TRUE", "1"):
            return "Y"
        if v in ("N", "NO", "FALSE", "0"):
            return "N"
    logger.debug("Could not coerce verified_purchase %r", value)
    return None


def parse_review_date(value: Any) -> date | None:
    """Parse review_date string to a date object.

    Args:
        value: Date string in YYYY-MM-DD format.

    Returns:
        date on success, None otherwise.
    """
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        logger.debug("Could not parse review_date %r", value)
        return None


def flatten_for_csv(record: dict) -> dict:
    """Return a flat dict safe for CSV export (all values as strings).

    Args:
        record: Review dict (may contain nested types).

    Returns:
        Dict with all values converted to strings.
    """
    return {k: str(v) if v is not None else "" for k, v in record.items()}
