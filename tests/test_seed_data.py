"""Tests for scripts/seed_data.py."""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys


def test_seed_data_generates_records(tmp_path: pathlib.Path):
    out = tmp_path / "synthetic.ndjson"
    result = subprocess.run(
        [sys.executable, "scripts/seed_data.py", str(out), "-n", "10", "--seed", "42"],
        capture_output=True, text=True, cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert result.returncode == 0, result.stderr
    assert out.exists()
    lines = [l for l in out.read_text().splitlines() if l]
    assert len(lines) == 10


def test_seed_data_records_are_valid_json(tmp_path: pathlib.Path):
    out = tmp_path / "synthetic.ndjson"
    subprocess.run(
        [sys.executable, "scripts/seed_data.py", str(out), "-n", "5"],
        capture_output=True, cwd=str(pathlib.Path(__file__).parents[1]),
    )
    for line in out.read_text().splitlines():
        if line:
            rec = json.loads(line)
            assert "review_id" in rec
            assert "star_rating" in rec
            assert 1 <= rec["star_rating"] <= 5


def test_seed_data_reproducible_with_seed(tmp_path: pathlib.Path):
    out1 = tmp_path / "r1.ndjson"
    out2 = tmp_path / "r2.ndjson"
    base = str(pathlib.Path(__file__).parents[1])
    for out in (out1, out2):
        subprocess.run(
            [sys.executable, "scripts/seed_data.py", str(out), "-n", "5", "--seed", "99"],
            capture_output=True, cwd=base,
        )
    assert out1.read_text() == out2.read_text()
