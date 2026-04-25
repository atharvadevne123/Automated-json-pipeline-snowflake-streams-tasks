"""Lightweight SQL metadata extractor for Snowflake DDL scripts."""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class SqlMetadata:
    tables: list[str] = field(default_factory=list)
    streams: list[str] = field(default_factory=list)
    tasks: list[str] = field(default_factory=list)
    procedures: list[str] = field(default_factory=list)
    warehouses: list[str] = field(default_factory=list)
    sequences: list[str] = field(default_factory=list)


_PATTERNS: dict[str, str] = {
    "tables": r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Z0-9_.\"]+)",
    "streams": r"CREATE\s+(?:OR\s+REPLACE\s+)?STREAM\s+([A-Z0-9_.\"]+)",
    "tasks": r"CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+([A-Z0-9_.\"]+)",
    "procedures": r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([A-Z0-9_.\"]+)",
    "warehouses": r"CREATE\s+(?:OR\s+REPLACE\s+)?WAREHOUSE\s+([A-Z0-9_.\"]+)",
    "sequences": r"CREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\s+([A-Z0-9_.\"]+)",
}


def extract_metadata(sql: str) -> SqlMetadata:
    """Parse a Snowflake SQL script and return object names by type."""
    upper = sql.upper()
    meta = SqlMetadata()
    for attr, pattern in _PATTERNS.items():
        matches = re.findall(pattern, upper, re.IGNORECASE | re.MULTILINE)
        getattr(meta, attr).extend(m.strip('"') for m in matches)
    return meta


def summarize(sql: str) -> str:
    """Return a human-readable summary of objects defined in a SQL script."""
    m = extract_metadata(sql)
    lines = [
        f"Tables     : {len(m.tables)}  -> {', '.join(m.tables) or 'none'}",
        f"Streams    : {len(m.streams)}  -> {', '.join(m.streams) or 'none'}",
        f"Tasks      : {len(m.tasks)}  -> {', '.join(m.tasks) or 'none'}",
        f"Procedures : {len(m.procedures)}  -> {', '.join(m.procedures) or 'none'}",
        f"Warehouses : {len(m.warehouses)}  -> {', '.join(m.warehouses) or 'none'}",
        f"Sequences  : {len(m.sequences)}  -> {', '.join(m.sequences) or 'none'}",
    ]
    return "\n".join(lines)
