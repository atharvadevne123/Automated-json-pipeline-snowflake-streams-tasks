"""Tests for snowflake_pipeline.batch_processor."""
from __future__ import annotations

import pytest

from snowflake_pipeline.batch_processor import BatchResult, process_batch, process_stream
from snowflake_pipeline.exceptions import BatchProcessingError


def _noop(record: dict) -> None:
    pass


def _fail_all(record: dict) -> None:
    raise ValueError("deliberate failure")


def _fail_on_id(fail_id: str):
    def handler(record: dict) -> None:
        if record.get("id") == fail_id:
            raise ValueError(f"failing on {fail_id}")
    return handler


def test_process_batch_all_succeed():
    records = [{"id": i} for i in range(5)]
    result = process_batch(records, _noop)
    assert result.total == 5
    assert result.processed == 5
    assert result.failed == 0


def test_process_batch_all_fail():
    records = [{"id": i} for i in range(3)]
    result = process_batch(records, _fail_all)
    assert result.failed == 3
    assert result.processed == 0


def test_process_batch_partial_fail():
    records = [{"id": "a"}, {"id": "fail"}, {"id": "b"}]
    result = process_batch(records, _fail_on_id("fail"))
    assert result.processed == 2
    assert result.failed == 1


def test_process_batch_stop_on_error_raises():
    records = [{"id": "fail"}]
    with pytest.raises(BatchProcessingError):
        process_batch(records, _fail_all, stop_on_error=True)


def test_process_batch_custom_batch_size():
    records = list(range(10))
    records = [{"id": i} for i in range(10)]
    result = process_batch(records, _noop, batch_size=3)
    assert result.processed == 10


def test_batch_result_success_rate():
    r = BatchResult(total=10, processed=8, failed=2)
    assert r.success_rate == pytest.approx(0.8)


def test_batch_result_success_rate_zero_total():
    r = BatchResult(total=0)
    assert r.success_rate == 0.0


def test_process_stream_generator():
    def gen():
        for i in range(7):
            yield {"id": i}
    result = process_stream(gen(), _noop, batch_size=3)
    assert result.processed == 7


def test_process_batch_empty():
    result = process_batch([], _noop)
    assert result.total == 0
    assert result.processed == 0


def test_process_batch_duration_positive():
    result = process_batch([{"id": 1}], _noop)
    assert result.duration_s >= 0
