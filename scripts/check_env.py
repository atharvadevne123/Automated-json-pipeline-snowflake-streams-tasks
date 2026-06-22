#!/usr/bin/env python3
"""Validate that all required environment variables are set before running the pipeline."""
from __future__ import annotations

import os
import sys

REQUIRED = {
    "SNOWFLAKE_ACCOUNT": "Snowflake account identifier (e.g. xy12345.us-east-1)",
    "SNOWFLAKE_USER": "Snowflake username",
    "SNOWFLAKE_PASSWORD": "Snowflake password",
    "SNOWFLAKE_DATABASE": "Target database name",
    "SNOWFLAKE_SCHEMA": "Target schema name",
    "SNOWFLAKE_WAREHOUSE": "Compute warehouse name",
}

OPTIONAL = {
    "SNOWFLAKE_ROLE": "Snowflake role (optional, defaults to user default)",
    "PIPELINE_BATCH_SIZE": "Records per batch (optional, default 100)",
    "PIPELINE_RETRY_ATTEMPTS": "Max retry attempts (optional, default 3)",
    "LOG_LEVEL": "Log verbosity: DEBUG/INFO/WARNING (optional, default INFO)",
}


def check() -> int:
    missing = [k for k in REQUIRED if not os.environ.get(k)]
    present = [k for k in REQUIRED if os.environ.get(k)]

    print("=== Snowflake Pipeline Environment Check ===\n")
    print(f"Required ({len(REQUIRED)} total):")
    for k in REQUIRED:
        status = "OK " if k in present else "MISSING"
        marker = "✓" if k in present else "✗"
        print(f"  [{marker}] {k:30s} — {status}")

    print(f"\nOptional ({len(OPTIONAL)} total):")
    for k, desc in OPTIONAL.items():
        val = os.environ.get(k, "")
        print(f"  [{'✓' if val else '-'}] {k:30s} = {val or '(not set)'}")

    if missing:
        print(f"\n✗ {len(missing)} required variable(s) missing: {missing}")
        print("  Copy .env.example to .env and fill in the values.")
        return 1
    print("\n✓ All required variables are set.")
    return 0


if __name__ == "__main__":
    sys.exit(check())
