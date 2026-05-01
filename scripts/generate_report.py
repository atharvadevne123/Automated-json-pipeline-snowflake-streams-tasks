#!/usr/bin/env python3
"""Generate a summary report of the pipeline run metrics."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys


def _load_metrics(path: pathlib.Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: Cannot load metrics from {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def render_report(metrics: dict) -> str:
    """Format a metrics dict as a human-readable report."""
    lines = [
        "=" * 60,
        "  Snowflake JSON Pipeline — Run Report",
        "=" * 60,
        f"  Run ID        : {metrics.get('run_id', 'n/a')}",
        f"  Total records : {metrics.get('total_records', 0)}",
        f"  Valid         : {metrics.get('valid_records', 0)}",
        f"  Invalid       : {metrics.get('invalid_records', 0)}",
        f"  Processed     : {metrics.get('processed_records', 0)}",
        f"  Failed        : {metrics.get('failed_records', 0)}",
        f"  Duration      : {metrics.get('duration_s', 0):.2f}s",
        f"  Throughput    : {metrics.get('throughput_rps', 0):.1f} rec/s",
        "=" * 60,
    ]
    errors = metrics.get("validation_errors", [])
    if errors:
        lines.append(f"  Validation errors ({len(errors)}):")
        for e in errors[:10]:
            lines.append(f"    - {e}")
        if len(errors) > 10:
            lines.append(f"    ... and {len(errors) - 10} more")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate pipeline run report")
    parser.add_argument("metrics_file", type=pathlib.Path, help="Path to metrics JSON file")
    parser.add_argument("--output", "-o", type=pathlib.Path, default=None, help="Write report to file")
    args = parser.parse_args()

    metrics = _load_metrics(args.metrics_file)
    report = render_report(metrics)
    print(report)
    if args.output:
        args.output.write_text(report, encoding="utf-8")
        print(f"\nReport saved to {args.output}")


if __name__ == "__main__":
    main()
