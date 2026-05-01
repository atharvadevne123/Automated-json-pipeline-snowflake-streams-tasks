"""Lightweight SQL metadata extractor for Snowflake DDL scripts."""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class SqlMetadata:
    """Container for object names parsed from a Snowflake DDL script."""

    tables: list[str] = field(default_factory=list)
    streams: list[str] = field(default_factory=list)
    tasks: list[str] = field(default_factory=list)
    procedures: list[str] = field(default_factory=list)
    warehouses: list[str] = field(default_factory=list)
    sequences: list[str] = field(default_factory=list)


_OBJECT_NAME = r"([A-Z0-9_.\"]+)"

_COMPILED: dict[str, re.Pattern] = {
    "tables": re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?" + _OBJECT_NAME,
        re.IGNORECASE | re.MULTILINE,
    ),
    "streams": re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?STREAM\s+" + _OBJECT_NAME,
        re.IGNORECASE | re.MULTILINE,
    ),
    "tasks": re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+" + _OBJECT_NAME,
        re.IGNORECASE | re.MULTILINE,
    ),
    "procedures": re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+" + _OBJECT_NAME,
        re.IGNORECASE | re.MULTILINE,
    ),
    "warehouses": re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?WAREHOUSE\s+" + _OBJECT_NAME,
        re.IGNORECASE | re.MULTILINE,
    ),
    "sequences": re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\s+" + _OBJECT_NAME,
        re.IGNORECASE | re.MULTILINE,
    ),
}


def extract_metadata(sql: str) -> SqlMetadata:
    """Parse a Snowflake SQL script and return object names grouped by type.

    Args:
        sql: Raw SQL DDL string.

    Returns:
        SqlMetadata with names in UPPER_CASE (quotes stripped).
    """
    upper = sql.upper()
    meta = SqlMetadata()
    for attr, pattern in _COMPILED.items():
        matches = pattern.findall(upper)
        getattr(meta, attr).extend(m.strip('"') for m in matches)
    return meta


def summarize(sql: str) -> str:
    """Return a human-readable summary of objects defined in a SQL script.

    Args:
        sql: Raw SQL DDL string.

    Returns:
        Multi-line string with counts and names per object type.
    """
    m = extract_metadata(sql)
    lines = [
        f"Tables     : {len(m.tables)}  -> {\', \'.join(m.tables) or \'none\'}",
        f"Streams    : {len(m.streams)}  -> {\', \'.join(m.streams) or \'none\'}",
        f"Tasks      : {len(m.tasks)}  -> {\', \'.join(m.tasks) or \'none\'}",
        f"Procedures : {len(m.procedures)}  -> {\', \'.join(m.procedures) or \'none\'}",
        f"Warehouses : {len(m.warehouses)}  -> {\', \'.join(m.warehouses) or \'none\'}",
        f"Sequences  : {len(m.sequences)}  -> {\', \'.join(m.sequences) or \'none\'}",
    ]
    return "\n".join(lines)
