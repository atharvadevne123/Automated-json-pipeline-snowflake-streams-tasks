"""CSV and JSON export utilities for pipeline output."""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Sequence

from snowflake_pipeline.exceptions import ExportError
from snowflake_pipeline.transformers import flatten_for_csv

logger = logging.getLogger(__name__)

_REVIEW_CSV_FIELDS = (
    "review_id", "review_date", "customer_id", "product_id",
    "product_title", "product_category", "star_rating",
    "review_body", "verified_purchase",
)


def to_csv(records: list[dict], path: Path, fieldnames: Sequence[str] | None = None) -> int:
    """Write *records* to a CSV file.

    Args:
        records: List of dicts to export.
        path: Destination .csv file path.
        fieldnames: Column order; defaults to standard review fields.

    Returns:
        Number of rows written.

    Raises:
        ExportError: on I/O errors.
    """
    if not records:
        logger.warning("to_csv called with empty records list")
        return 0
    cols = list(fieldnames) if fieldnames else list(_REVIEW_CSV_FIELDS)
    flat = [flatten_for_csv(r) for r in records]
    try:
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(flat)
    except OSError as exc:
        raise ExportError(f"Cannot write CSV to {path}: {exc}") from exc
    logger.info("Exported %d records to %s", len(records), path)
    return len(records)


def to_json(records: list[dict], path: Path, indent: int = 2) -> int:
    """Write *records* as a JSON array to *path*.

    Args:
        records: List of dicts to export.
        path: Destination .json file path.
        indent: JSON indentation level.

    Returns:
        Number of records written.

    Raises:
        ExportError: on I/O errors.
    """
    try:
        path.write_text(json.dumps(records, indent=indent, ensure_ascii=False), encoding="utf-8")
    except OSError as exc:
        raise ExportError(f"Cannot write JSON to {path}: {exc}") from exc
    logger.info("Exported %d records to %s", len(records), path)
    return len(records)


def to_ndjson(records: list[dict], path: Path) -> int:
    """Write *records* as newline-delimited JSON to *path*.

    Args:
        records: List of dicts to export.
        path: Destination file path.

    Returns:
        Number of records written.

    Raises:
        ExportError: on I/O errors.
    """
    try:
        with path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as exc:
        raise ExportError(f"Cannot write NDJSON to {path}: {exc}") from exc
    logger.info("Exported %d records (NDJSON) to %s", len(records), path)
    return len(records)
