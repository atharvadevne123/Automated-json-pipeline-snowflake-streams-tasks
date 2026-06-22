"""Record filtering helpers for the pipeline."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Callable

logger = logging.getLogger(__name__)

__all__ = [
    "FilterFn",
    "by_star_rating",
    "by_verified_purchase",
    "by_date_range",
    "by_category",
    "apply_filters",
    "by_min_review_length",
    "by_product_ids",
    "by_customer_ids",
    "exclude_categories",
]

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
    if not records or not filters:
        return list(records)
    result = records
    for fn in filters:
        if not result:
            break
        result = [r for r in result if fn(r)]
    logger.debug("Filtered %d -> %d records", len(records), len(result))
    return result


def by_min_review_length(min_chars: int = 10) -> FilterFn:
    """Return a filter that keeps records with review_body longer than min_chars.

    Args:
        min_chars: Minimum character count for review_body (inclusive).

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        body = record.get("review_body", "")
        return isinstance(body, str) and len(body) >= min_chars
    return _filter


def by_product_ids(product_ids: set[str]) -> FilterFn:
    """Return a filter that keeps records matching one of *product_ids*.

    Args:
        product_ids: Set of allowed product_id values.

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        return record.get("product_id", "") in product_ids
    return _filter


def by_customer_ids(customer_ids: set[str]) -> FilterFn:
    """Return a filter that keeps records matching one of *customer_ids*.

    Args:
        customer_ids: Set of allowed customer_id values.

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        return record.get("customer_id", "") in customer_ids
    return _filter


def exclude_categories(categories: set[str]) -> FilterFn:
    """Return a filter that excludes records in the given categories.

    Args:
        categories: Set of product_category values to exclude.

    Returns:
        Predicate function.
    """
    def _filter(record: dict) -> bool:
        return record.get("product_category", "") not in categories
    return _filter
