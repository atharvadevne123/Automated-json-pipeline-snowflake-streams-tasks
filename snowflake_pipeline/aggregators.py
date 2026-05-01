"""Aggregation helpers for review record collections."""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class ReviewSummary:
    """Aggregated statistics over a set of review records."""

    total: int = 0
    avg_star_rating: float = 0.0
    star_distribution: dict[int, int] = field(default_factory=dict)
    category_counts: dict[str, int] = field(default_factory=dict)
    verified_count: int = 0
    unverified_count: int = 0

    @property
    def verified_rate(self) -> float:
        """Fraction of verified purchases."""
        total = self.verified_count + self.unverified_count
        return self.verified_count / total if total > 0 else 0.0


def summarise_reviews(records: list[dict]) -> ReviewSummary:
    """Compute aggregate statistics from a list of review records.

    Args:
        records: List of validated review dicts.

    Returns:
        ReviewSummary with counts, averages, and distributions.
    """
    summary = ReviewSummary(total=len(records))
    if not records:
        return summary

    ratings = [r.get("star_rating") for r in records if isinstance(r.get("star_rating"), int)]
    summary.avg_star_rating = sum(ratings) / len(ratings) if ratings else 0.0
    summary.star_distribution = dict(Counter(ratings))

    categories = [r.get("product_category", "") for r in records if r.get("product_category")]
    summary.category_counts = dict(Counter(categories))

    summary.verified_count = sum(1 for r in records if r.get("verified_purchase") == "Y")
    summary.unverified_count = sum(1 for r in records if r.get("verified_purchase") == "N")

    logger.info(
        "Summarised %d records: avg_rating=%.2f, %d categories, verified_rate=%.1f%%",
        summary.total, summary.avg_star_rating,
        len(summary.category_counts), summary.verified_rate * 100,
    )
    return summary


def top_categories(records: list[dict], n: int = 5) -> list[tuple[str, int]]:
    """Return the top-N product categories by review count.

    Args:
        records: List of review dicts.
        n: Number of top categories to return.

    Returns:
        List of (category, count) tuples, descending by count.
    """
    counts: Counter = Counter(r.get("product_category", "") for r in records if r.get("product_category"))
    return counts.most_common(n)


def group_by(records: list[dict], key: str) -> dict[str, list[dict]]:
    """Group records by the value of a specific field.

    Args:
        records: List of review dicts.
        key: Field name to group by.

    Returns:
        Dict mapping field values to lists of matching records.
    """
    groups: dict[str, list[dict]] = {}
    for rec in records:
        val = str(rec.get(key, ""))
        groups.setdefault(val, []).append(rec)
    logger.debug("Grouped %d records by %r into %d groups", len(records), key, len(groups))
    return groups
