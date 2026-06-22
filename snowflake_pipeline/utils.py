"""General-purpose utility helpers for the pipeline."""
from __future__ import annotations

import functools
import hashlib
import logging
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

__all__ = [
    "utcnow",
    "today_iso",
    "is_valid_date",
    "new_run_id",
    "sha256_of_file",
    "chunk",
    "sanitize_identifier",
    "sample_records",
    "format_duration",
]

_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


def utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(tz=timezone.utc)


def today_iso() -> str:
    """Return today\'s date as an ISO-8601 string (YYYY-MM-DD)."""
    return date.today().isoformat()


@functools.lru_cache(maxsize=1024)
def is_valid_date(value: str) -> bool:
    """Return True when *value* matches YYYY-MM-DD format.

    Args:
        value: String to check.

    Returns:
        True if the format is valid, False otherwise.
    """
    return bool(_DATE_RE.match(value))


def new_run_id() -> str:
    """Generate a short random run identifier.

    Returns:
        8-character hex string.
    """
    return uuid.uuid4().hex[:8]


def sha256_of_file(path: Path) -> str:
    """Return the SHA-256 hex digest of a file\'s contents.

    Args:
        path: Filesystem path to the file.

    Returns:
        Lowercase hex string.
    """
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def chunk(items: list, size: int) -> Iterator[list]:
    """Yield successive non-overlapping chunks from *items*.

    Args:
        items: Source list.
        size: Maximum chunk size (must be > 0).

    Yields:
        Sublists of at most *size* elements.

    Raises:
        ValueError: if size <= 0.
    """
    if size <= 0:
        raise ValueError(f"chunk size must be > 0, got {size}")
    for i in range(0, len(items), size):
        yield items[i : i + size]


def sanitize_identifier(name: str) -> str:
    """Strip non-alphanumeric characters from an identifier.

    Args:
        name: Raw identifier string.

    Returns:
        Cleaned string (only letters, digits, underscores).
    """
    return re.sub(r"[^\w]", "_", name, flags=re.ASCII)


def sample_records(records: list[dict], n: int, seed: int | None = None) -> list[dict]:
    """Return up to *n* randomly sampled records.

    Args:
        records: Source list of dicts.
        n: Maximum number of records to return.
        seed: Optional random seed for reproducibility.

    Returns:
        Sampled list (or the full list if len(records) <= n).
    """
    import random as _random
    if n <= 0:
        raise ValueError(f"n must be > 0, got {n}")
    if len(records) <= n:
        return list(records)
    rng = _random.Random(seed)
    result = rng.sample(records, n)
    logger.debug("Sampled %d records from %d total (seed=%s)", n, len(records), seed)
    return result


def format_duration(seconds: float) -> str:
    """Format *seconds* as a human-readable duration string.

    Args:
        seconds: Duration in seconds.

    Returns:
        String like "1h 23m 45s" or "2m 30s" or "45.2s".
    """
    if seconds < 0:
        return "0s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    if hours > 0:
        return f"{hours}h {minutes}m {secs:.0f}s"
    if minutes > 0:
        return f"{minutes}m {secs:.0f}s"
    return f"{secs:.1f}s"
