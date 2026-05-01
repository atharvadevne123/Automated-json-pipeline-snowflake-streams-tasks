"""Snowflake JSON ETL Pipeline — utilities to load SQL scripts and sample data."""
from __future__ import annotations

import json
import logging
import pathlib as _pl
import re

_PKG = _pl.Path(__file__).parent
_SQL_DIR = _PKG / "sql"
_DATA_DIR = _PKG / "data"

logger = logging.getLogger(__name__)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

__all__ = [
    "get_sql",
    "list_sql",
    "get_sample_data_path",
    "load_sample_reviews",
    "validate_review",
    "REQUIRED_REVIEW_FIELDS",
]


def get_sql(name: str = "snowflake_optimized.sql") -> str:
    """Return the text of a bundled SQL script by filename.

    Raises ValueError for names that escape the sql/ directory.
    Raises FileNotFoundError when the script does not exist.
    """
    resolved = (_SQL_DIR / name).resolve()
    if not resolved.is_relative_to(_SQL_DIR.resolve()):
        raise ValueError(f"Invalid SQL script name: {name!r}")
    if not resolved.exists():
        raise FileNotFoundError(f"SQL script not found: {name!r}")
    logger.debug("Loading SQL script: %s", name)
    return resolved.read_text(encoding="utf-8")


def list_sql() -> list[str]:
    """Return sorted names of all bundled SQL scripts."""
    return sorted(f.name for f in _SQL_DIR.iterdir() if f.suffix == ".sql")


def get_sample_data_path() -> str:
    """Return the filesystem path to the bundled sample JSON."""
    return str(_DATA_DIR / "sample_amazon_reviews.json")


def load_sample_reviews() -> list[dict]:
    """Parse and return all records from the bundled NDJSON sample file."""
    path = _DATA_DIR / "sample_amazon_reviews.json"
    records: list[dict] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    logger.debug("Loaded %d sample review records", len(records))
    return records


REQUIRED_REVIEW_FIELDS: frozenset[str] = frozenset(
    {
        "review_id",
        "review_date",
        "customer_id",
        "product_id",
        "product_title",
        "product_category",
        "star_rating",
        "review_body",
        "verified_purchase",
    }
)

_REQUIRED_STR_FIELDS = frozenset(
    {"review_id", "customer_id", "product_id", "product_title", "product_category", "review_body"}
)


def validate_review(record: dict) -> list[str]:
    """Return a list of validation error strings for a single review record.

    Returns an empty list when the record is valid.
    """
    errors: list[str] = []

    missing = REQUIRED_REVIEW_FIELDS - record.keys()
    if missing:
        errors.append(f"Missing fields: {sorted(missing)}")

    for field in _REQUIRED_STR_FIELDS:
        val = record.get(field)
        if val is not None and (not isinstance(val, str) or not val.strip()):
            errors.append(f"{field} must be a non-empty string, got {val!r}")

    date_val = record.get("review_date")
    if date_val is not None:
        if not isinstance(date_val, str) or not _DATE_RE.match(date_val):
            errors.append(f"review_date must be YYYY-MM-DD, got {date_val!r}")

    rating = record.get("star_rating")
    if rating is not None and (not isinstance(rating, int) or not 1 <= rating <= 5):
        errors.append(f"star_rating must be an int between 1 and 5, got {rating!r}")

    vp = record.get("verified_purchase")
    if vp is not None and vp not in ("Y", "N"):
        errors.append(f"verified_purchase must be 'Y\'or \'N', got {vp!r}")

    return errors
