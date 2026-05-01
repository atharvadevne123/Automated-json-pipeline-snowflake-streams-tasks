#!/usr/bin/env python3
"""CLI tool to validate an NDJSON review file and report errors."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate NDJSON review records")
    parser.add_argument("input_file", type=pathlib.Path, help="NDJSON file to validate")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any record is invalid")
    parser.add_argument("--limit", type=int, default=0, help="Only validate first N records (0=all)")
    args = parser.parse_args()

    try:
        from snowflake_pipeline.validators import validate_batch
        from snowflake_pipeline.io import read_ndjson
    except ImportError:
        print("ERROR: snowflake_pipeline package not installed.", file=sys.stderr)
        sys.exit(2)

    records = read_ndjson(args.input_file)
    if args.limit > 0:
        records = records[: args.limit]

    valid, invalid = validate_batch(records)
    print(f"Total   : {len(records)}")
    print(f"Valid   : {len(valid)}")
    print(f"Invalid : {len(invalid)}")

    if invalid:
        print("\nInvalid records:")
        for rec, errors in invalid[:20]:
            print(f"  [{rec.get('review_id', '?')}] {'; '.join(errors)}")
        if len(invalid) > 20:
            print(f"  ... and {len(invalid) - 20} more")

    if args.strict and invalid:
        sys.exit(1)


if __name__ == "__main__":
    main()
