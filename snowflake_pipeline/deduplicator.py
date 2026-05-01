"""Record deduplication utilities for the pipeline."""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Callable

logger = logging.getLogger(__name__)

KeyFn = Callable[[dict], str]


def _default_key(record: dict) -> str:
    """Return the review_id as the deduplication key."""
    return str(record.get("review_id", ""))


def _content_hash_key(record: dict) -> str:
    """Return a SHA-256 hash of the full record content."""
    serialised = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialised.encode()).hexdigest()


def deduplicate(
    records: list[dict],
    key_fn: KeyFn | None = None,
    keep: str = "first",
) -> list[dict]:
    """Remove duplicate records, keeping either the first or last occurrence.

    Args:
        records: Source list of review dicts.
        key_fn: Function that returns a string deduplication key from a record.
                Defaults to using the ``review_id`` field.
        keep: ``"first"`` keeps the first occurrence; ``"last"`` keeps the last.

    Returns:
        Deduplicated list preserving relative order.

    Raises:
        ValueError: if *keep* is not "first" or "last".
    """
    if keep not in ("first", "last"):
        raise ValueError(f"keep must be 'first' or 'last', got {keep!r}")

    fn = key_fn or _default_key
    if keep == "last":
        records = list(reversed(records))

    seen: set[str] = set()
    result: list[dict] = []
    for rec in records:
        key = fn(rec)
        if key and key not in seen:
            seen.add(key)
            result.append(rec)

    if keep == "last":
        result = list(reversed(result))

    removed = len(records) - len(result)
    if removed:
        logger.info("Deduplicated %d records: removed %d duplicate(s)", len(records), removed)
    return result


def find_duplicates(records: list[dict], key_fn: KeyFn | None = None) -> dict[str, list[dict]]:
    """Return a dict of keys that appear more than once, mapped to their records.

    Args:
        records: Source list of review dicts.
        key_fn: Deduplication key function (defaults to review_id).

    Returns:
        Dict mapping duplicate keys to all records sharing that key.
    """
    fn = key_fn or _default_key
    groups: dict[str, list[dict]] = {}
    for rec in records:
        key = fn(rec)
        groups.setdefault(key, []).append(rec)
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    logger.debug("Found %d duplicate key(s) in %d records", len(duplicates), len(records))
    return duplicates
