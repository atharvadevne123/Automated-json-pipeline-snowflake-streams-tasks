"""General-purpose utility helpers for the pipeline."""
from __future__ import annotations

import hashlib
import logging
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


def utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(tz=timezone.utc)


def today_iso() -> str:
    """Return today\'s date as an ISO-8601 string (YYYY-MM-DD)."""
    return date.today().isoformat()


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
