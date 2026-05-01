"""File I/O helpers for NDJSON and JSON data files."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator

from snowflake_pipeline.exceptions import PipelineError

logger = logging.getLogger(__name__)


def read_ndjson(path: Path) -> list[dict]:
    """Read all records from a newline-delimited JSON file.

    Args:
        path: Path to the .ndjson / .jsonl file.

    Returns:
        List of parsed dicts (blank lines are skipped).

    Raises:
        PipelineError: if the file cannot be read or a line fails to parse.
    """
    records: list[dict] = []
    try:
        with path.open(encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise PipelineError(f"JSON parse error at {path}:{lineno}: {exc}") from exc
    except OSError as exc:
        raise PipelineError(f"Cannot read file {path}: {exc}") from exc
    logger.debug("Read %d records from %s", len(records), path)
    return records


def stream_ndjson(path: Path) -> Iterator[dict]:
    """Yield records one at a time from a newline-delimited JSON file.

    Args:
        path: Path to the source file.

    Yields:
        Parsed dict for each non-blank line.

    Raises:
        PipelineError: on I/O or parse errors.
    """
    try:
        with path.open(encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise PipelineError(f"JSON parse error at {path}:{lineno}: {exc}") from exc
    except OSError as exc:
        raise PipelineError(f"Cannot read file {path}: {exc}") from exc


def write_ndjson(records: list[dict], path: Path) -> int:
    """Write records to *path* in newline-delimited JSON format.

    Args:
        records: Dicts to serialise.
        path: Destination file (will be created or overwritten).

    Returns:
        Number of records written.

    Raises:
        PipelineError: on I/O errors.
    """
    try:
        with path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as exc:
        raise PipelineError(f"Cannot write file {path}: {exc}") from exc
    logger.debug("Wrote %d records to %s", len(records), path)
    return len(records)


def read_json(path: Path) -> dict | list:
    """Read a standard (non-newline-delimited) JSON file.

    Args:
        path: Path to the .json file.

    Returns:
        Parsed Python object.

    Raises:
        PipelineError: on I/O or parse errors.
    """
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Cannot read JSON from {path}: {exc}") from exc
