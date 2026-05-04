"""Tests for snowflake_pipeline.metrics."""
from __future__ import annotations

import json
import pathlib
import time

from snowflake_pipeline.metrics import PipelineMetrics


def test_metrics_defaults():
    m = PipelineMetrics(run_id="test-run")
    assert m.run_id == "test-run"
    assert m.total_records == 0
    assert m.processed_records == 0


def test_metrics_duration_before_end():
    m = PipelineMetrics(run_id="r1")
    time.sleep(0.01)
    assert m.duration_s > 0


def test_metrics_duration_after_mark_complete():
    m = PipelineMetrics(run_id="r1")
    m.mark_complete()
    d = m.duration_s
    assert d >= 0


def test_metrics_throughput_rps():
    m = PipelineMetrics(run_id="r1")
    m.processed_records = 100
    m.mark_complete()
    assert m.throughput_rps > 0


def test_metrics_throughput_zero_duration():
    m = PipelineMetrics(run_id="r1", start_time=1.0)
    m.end_time = 1.0
    m.processed_records = 10
    assert m.throughput_rps == 0.0


def test_record_validation():
    m = PipelineMetrics(run_id="r1")
    m.record_validation(8, 2, errors=["err1"])
    assert m.valid_records == 8
    assert m.invalid_records == 2
    assert "err1" in m.validation_errors


def test_to_dict_includes_computed_fields():
    m = PipelineMetrics(run_id="r1")
    m.mark_complete()
    d = m.to_dict()
    assert "duration_s" in d
    assert "throughput_rps" in d


def test_to_json_valid():
    m = PipelineMetrics(run_id="r1")
    m.mark_complete()
    s = m.to_json()
    parsed = json.loads(s)
    assert parsed["run_id"] == "r1"


def test_save_to_file(tmp_path: pathlib.Path):
    m = PipelineMetrics(run_id="r2")
    m.total_records = 5
    m.mark_complete()
    p = tmp_path / "metrics.json"
    m.save(p)
    assert p.exists()
    data = json.loads(p.read_text())
    assert data["run_id"] == "r2"
    assert data["total_records"] == 5
