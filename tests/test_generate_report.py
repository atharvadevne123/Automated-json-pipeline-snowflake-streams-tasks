"""Tests for scripts/generate_report.py."""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys


def _write_metrics(path: pathlib.Path, **kwargs) -> None:
    defaults = {
        "run_id": "test-run",
        "total_records": 100,
        "valid_records": 90,
        "invalid_records": 10,
        "processed_records": 88,
        "failed_records": 2,
        "duration_s": 1.23,
        "throughput_rps": 71.5,
        "validation_errors": [],
    }
    defaults.update(kwargs)
    path.write_text(json.dumps(defaults), encoding="utf-8")


def test_generate_report_runs(tmp_path: pathlib.Path):
    m = tmp_path / "metrics.json"
    _write_metrics(m)
    result = subprocess.run(
        [sys.executable, "scripts/generate_report.py", str(m)],
        capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert result.returncode == 0
    assert "test-run" in result.stdout
    assert "100" in result.stdout


def test_generate_report_shows_throughput(tmp_path: pathlib.Path):
    m = tmp_path / "metrics.json"
    _write_metrics(m, throughput_rps=55.0)
    result = subprocess.run(
        [sys.executable, "scripts/generate_report.py", str(m)],
        capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert "55" in result.stdout


def test_generate_report_save_to_file(tmp_path: pathlib.Path):
    m = tmp_path / "metrics.json"
    out = tmp_path / "report.txt"
    _write_metrics(m)
    subprocess.run(
        [sys.executable, "scripts/generate_report.py", str(m), "-o", str(out)],
        capture_output=True,
        cwd=str(pathlib.Path(__file__).parents[1]),
    )
    assert out.exists()
    assert "test-run" in out.read_text()
