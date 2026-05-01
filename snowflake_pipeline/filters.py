"""Record filtering helpers for the pipeline."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Callable

logger = logging.getLogger(__name__)

FilterFn = Callable[[dict], bool]


def by_star_rating(min_stars: int = 1, max_stars: int = 5) -> FilterFn:
    """Return a filter that keeps records within a star-rating range.

    Args:
        min_stars: Minimum star rating (inclusive).
        max_stars: Maximum star rating (inclusive).

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        rating = record.get("star_rating")
        return isinstance(rating, int) and min_stars <= rating <= max_stars
    return _filter


def by_verified_purchase(verified: bool = True) -> FilterFn:
    """Return a filter that keeps only verified (or unverified) purchases.

    Args:
        verified: True keeps "Y", False keeps "N".

    Returns:
        Predicate function.
    """
    target = "Y" if verified else "N"
    def _filter(record: dict) -> bool:
        return record.get("verified_purchase") == target
    return _filter


def by_date_range(start: date | None = None, end: date | None = None) -> FilterFn:
    """Return a filter that keeps records within a date range.

    Args:
        start: Inclusive start date (None = no lower bound).
        end: Inclusive end date (None = no upper bound).

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        raw = record.get("review_date")
        if not isinstance(raw, str):
            return False
        try:
            d = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return False
        if start and d < start:
            return False
        if end and d > end:
            return False
        return True
    return _filter


def by_category(categories: set[str]) -> FilterFn:
    """Return a filter that keeps records matching one of *categories*.

    Args:
        categories: Set of allowed product_category values (case-sensitive).

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        return record.get("product_category", "") in categories
    return _filter


def apply_filters(records: list[dict], *filters: FilterFn) -> list[dict]:
    """Apply all *filters* to *records*, returning only records that pass all.

    Args:
        records: Source list of dicts.
        *filters: Zero or more predicate functions.

    Returns:
        Filtered list.
    """
    result = records
    for fn in filters:
        result = [r for r in result if fn(r)]
    logger.debug("Filtered %d -> %d records", len(records), len(result))
    return result
