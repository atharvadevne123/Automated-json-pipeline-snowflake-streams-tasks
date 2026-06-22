"""Tests for snowflake_pipeline.aggregators."""
from __future__ import annotations

import pytest

from snowflake_pipeline.aggregators import ReviewSummary, group_by, summarise_reviews, top_categories


def _review(rating: int = 4, category: str = "Electronics", vp: str = "Y") -> dict:
    return {
        "review_id": "R1", "review_date": "2024-01-01",
        "customer_id": "C1", "product_id": "P1",
        "product_title": "Widget", "product_category": category,
        "star_rating": rating, "review_body": "Good.", "verified_purchase": vp,
    }


def test_summarise_reviews_empty():
    s = summarise_reviews([])
    assert s.total == 0
    assert s.avg_star_rating == 0.0


def test_summarise_reviews_single():
    s = summarise_reviews([_review(rating=5)])
    assert s.total == 1
    assert s.avg_star_rating == 5.0
    assert s.verified_count == 1


def test_summarise_reviews_avg_rating():
    records = [_review(r) for r in [3, 4, 5]]
    s = summarise_reviews(records)
    assert s.avg_star_rating == pytest.approx(4.0)


def test_summarise_reviews_star_distribution():
    records = [_review(4), _review(4), _review(5)]
    s = summarise_reviews(records)
    assert s.star_distribution[4] == 2
    assert s.star_distribution[5] == 1


def test_summarise_reviews_category_counts():
    records = [_review(category="Books"), _review(category="Books"), _review(category="Tech")]
    s = summarise_reviews(records)
    assert s.category_counts["Books"] == 2
    assert s.category_counts["Tech"] == 1


def test_verified_rate():
    records = [_review(vp="Y"), _review(vp="Y"), _review(vp="N")]
    s = summarise_reviews(records)
    assert s.verified_rate == pytest.approx(2 / 3)


def test_verified_rate_all_unverified():
    s = ReviewSummary(verified_count=0, unverified_count=0)
    assert s.verified_rate == 0.0


def test_top_categories():
    records = [_review(category=c) for c in ["A", "A", "B", "C", "A"]]
    top = top_categories(records, n=2)
    assert top[0] == ("A", 3)
    assert len(top) == 2


def test_top_categories_empty():
    assert top_categories([]) == []


def test_group_by_field():
    records = [_review(category="Books"), _review(category="Tech"), _review(category="Books")]
    groups = group_by(records, "product_category")
    assert len(groups["Books"]) == 2
    assert len(groups["Tech"]) == 1


def test_group_by_missing_field():
    records = [{"star_rating": 4}, {"star_rating": 5}]
    groups = group_by(records, "missing_field")
    assert "" in groups
    assert len(groups[""]) == 2


# ---------------------------------------------------------------------------
# count_by
# ---------------------------------------------------------------------------

def test_count_by_returns_dict():
    from snowflake_pipeline.aggregators import count_by
    records = [
        {"product_category": "Books"},
        {"product_category": "Books"},
        {"product_category": "Electronics"},
    ]
    result = count_by(records, "product_category")
    assert result["Books"] == 2
    assert result["Electronics"] == 1


def test_count_by_empty():
    from snowflake_pipeline.aggregators import count_by
    assert count_by([], "star_rating") == {}


def test_count_by_missing_key():
    from snowflake_pipeline.aggregators import count_by
    records = [{"star_rating": 5}, {"other": 1}]
    result = count_by(records, "star_rating")
    assert result.get("5") == 1


# ---------------------------------------------------------------------------
# rating_histogram
# ---------------------------------------------------------------------------

def test_rating_histogram_all_stars():
    from snowflake_pipeline.aggregators import rating_histogram
    records = [{"star_rating": i} for i in range(1, 6)]
    hist = rating_histogram(records)
    assert all(hist[i] == 1 for i in range(1, 6))


def test_rating_histogram_empty():
    from snowflake_pipeline.aggregators import rating_histogram
    hist = rating_histogram([])
    assert hist == {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}


def test_rating_histogram_skips_invalid():
    from snowflake_pipeline.aggregators import rating_histogram
    records = [{"star_rating": 6}, {"star_rating": 0}, {"star_rating": 3}]
    hist = rating_histogram(records)
    assert hist[3] == 1
    assert hist[1] == 0


# ---------------------------------------------------------------------------
# field_stats
# ---------------------------------------------------------------------------

def test_field_stats_computes_mean(sample_reviews):
    from snowflake_pipeline.aggregators import field_stats
    stats = field_stats(sample_reviews, "star_rating")
    assert stats["non_null"] > 0
    assert 1.0 <= stats["mean"] <= 5.0


def test_field_stats_empty_records():
    from snowflake_pipeline.aggregators import field_stats
    stats = field_stats([], "star_rating")
    assert stats["count"] == 0
    assert stats["mean"] is None


def test_field_stats_missing_field(sample_reviews):
    from snowflake_pipeline.aggregators import field_stats
    stats = field_stats(sample_reviews, "nonexistent_field")
    assert stats["non_null"] == 0
