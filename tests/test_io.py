"""Tests for snowflake_pipeline.io."""
from __future__ import annotations

import json
import pathlib

import pytest

from snowflake_pipeline.exceptions import PipelineError
from snowflake_pipeline.io import read_json, read_ndjson, stream_ndjson, write_ndjson


def _write_ndjson(path: pathlib.Path, records: list[dict]) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def test_read_ndjson_basic(tmp_path):
    p = tmp_path / "data.ndjson"
    records = [{"id": 1}, {"id": 2}]
    _write_ndjson(p, records)
    result = read_ndjson(p)
    assert result == records


def test_read_ndjson_skips_blank_lines(tmp_path):
    p = tmp_path / "data.ndjson"
    p.write_text('{"id":1}\n\n{"id":2}\n', encoding="utf-8")
    result = read_ndjson(p)
    assert len(result) == 2


def test_read_ndjson_missing_file_raises(tmp_path):
    with pytest.raises(PipelineError):
        read_ndjson(tmp_path / "missing.ndjson")


def test_read_ndjson_invalid_json_raises(tmp_path):
    p = tmp_path / "bad.ndjson"
    p.write_text("not json\n", encoding="utf-8")
    with pytest.raises(PipelineError):
        read_ndjson(p)


def test_stream_ndjson_yields_records(tmp_path):
    p = tmp_path / "data.ndjson"
    records = [{"id": i} for i in range(5)]
    _write_ndjson(p, records)
    result = list(stream_ndjson(p))
    assert len(result) == 5


def test_stream_ndjson_invalid_json_raises(tmp_path):
    p = tmp_path / "bad.ndjson"
    p.write_text("bad line\n", encoding="utf-8")
    with pytest.raises(PipelineError):
        list(stream_ndjson(p))


def test_write_ndjson_roundtrip(tmp_path):
    p = tmp_path / "out.ndjson"
    records = [{"a": 1}, {"b": 2}]
    count = write_ndjson(records, p)
    assert count == 2
    assert read_ndjson(p) == records


def test_write_ndjson_empty(tmp_path):
    p = tmp_path / "empty.ndjson"
    assert write_ndjson([], p) == 0
    assert p.read_text() == ""


def test_read_json_basic(tmp_path):
    p = tmp_path / "data.json"
    data = {"key": "value", "num": 42}
    p.write_text(json.dumps(data), encoding="utf-8")
    assert read_json(p) == data


def test_read_json_list(tmp_path):
    p = tmp_path / "list.json"
    p.write_text("[1,2,3]", encoding="utf-8")
    assert read_json(p) == [1, 2, 3]


def test_read_json_missing_raises(tmp_path):
    with pytest.raises(PipelineError):
        read_json(tmp_path / "no.json")
