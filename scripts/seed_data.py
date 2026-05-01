#!/usr/bin/env python3
"""Generate synthetic NDJSON review data for testing and development."""
from __future__ import annotations

import argparse
import json
import pathlib
import random
import string
import sys
from datetime import date, timedelta


_CATEGORIES = ["Electronics", "Books", "Sports", "Home & Garden", "Clothing", "Toys", "Food"]
_TITLES = [
    "Super Widget Pro", "Basic Gizmo", "Deluxe Gadget", "Premium Device",
    "Economy Tool", "Advanced System", "Classic Item", "Modern Solution",
]
_BODIES = [
    "Great product, highly recommend!",
    "Works as described, very satisfied.",
    "Could be better but gets the job done.",
    "Disappointed with the quality.",
    "Absolutely love this, five stars!",
    "Returned it immediately, waste of money.",
    "Decent value for the price.",
    "Best purchase I have made this year.",
]


def _random_id(prefix: str, length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return prefix + "".join(random.choices(chars, k=length))


def _random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def generate_review(idx: int) -> dict:
    """Generate one synthetic review record."""
    rating = random.randint(1, 5)
    return {
        "review_id": _random_id("R"),
        "review_date": _random_date(date(2020, 1, 1), date(2025, 12, 31)),
        "customer_id": _random_id("C"),
        "product_id": _random_id("B00"),
        "product_title": random.choice(_TITLES),
        "product_category": random.choice(_CATEGORIES),
        "star_rating": rating,
        "review_body": random.choice(_BODIES),
        "verified_purchase": random.choice(["Y", "N"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic review NDJSON data")
    parser.add_argument("output", type=pathlib.Path, help="Output .ndjson file path")
    parser.add_argument("-n", "--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    try:
        with args.output.open("w", encoding="utf-8") as fh:
            for i in range(args.count):
                fh.write(json.dumps(generate_review(i)) + "\n")
        print(f"Generated {args.count} records -> {args.output}")
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
