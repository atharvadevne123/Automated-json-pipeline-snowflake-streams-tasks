"""Shared constants for the Snowflake JSON pipeline."""
from __future__ import annotations

# Review field names
FIELD_REVIEW_ID = "review_id"
FIELD_REVIEW_DATE = "review_date"
FIELD_CUSTOMER_ID = "customer_id"
FIELD_PRODUCT_ID = "product_id"
FIELD_PRODUCT_TITLE = "product_title"
FIELD_PRODUCT_CATEGORY = "product_category"
FIELD_STAR_RATING = "star_rating"
FIELD_REVIEW_BODY = "review_body"
FIELD_VERIFIED_PURCHASE = "verified_purchase"

# Valid values
VERIFIED_PURCHASE_VALUES: frozenset[str] = frozenset({"Y", "N"})
STAR_RATING_MIN: int = 1
STAR_RATING_MAX: int = 5
DATE_FORMAT: str = "%Y-%m-%d"
DATE_PATTERN: str = r"^\d{4}-\d{2}-\d{2}$"

# Batch processing
DEFAULT_BATCH_SIZE: int = 100
MAX_BATCH_SIZE: int = 10_000

# Retry
DEFAULT_RETRY_ATTEMPTS: int = 3
DEFAULT_RETRY_BASE_DELAY: float = 1.0
DEFAULT_RETRY_MAX_DELAY: float = 30.0

# Metrics
METRICS_KEY_TOTAL = "total"
METRICS_KEY_VALID = "valid"
METRICS_KEY_INVALID = "invalid"
METRICS_KEY_DURATION_S = "duration_s"

# Export
CSV_DELIMITER: str = ","
NDJSON_ENCODING: str = "utf-8"
