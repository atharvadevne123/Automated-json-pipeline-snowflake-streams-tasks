"""Tests for snowflake_pipeline.deduplicator."""
from __future__ import annotations

import pytest

from snowflake_pipeline.deduplicator import deduplicate, find_duplicates


def _rec(rid: str, body: str = "text") -> dict:
    return {"review_id": rid, "review_body": body}


def test_deduplicate_no_duplicates():
    records = [_rec("R1"), _rec("R2"), _rec("R3")]
    result = deduplicate(records)
    assert len(result) == 3


def test_deduplicate_removes_duplicate():
    records = [_rec("R1"), _rec("R2"), _rec("R1", body="again")]
    result = deduplicate(records)
    assert len(result) == 2
    assert result[0]["review_id"] == "R1"
    assert result[0]["review_body"] == "text"  # first kept


def test_deduplicate_keep_last():
    records = [_rec("R1", body="first"), _rec("R2"), _rec("R1", body="last")]
    result = deduplicate(records, keep="last")
    r1 = next(r for r in result if r["review_id"] == "R1")
    assert r1["review_body"] == "last"


def test_deduplicate_invalid_keep():
    with pytest.raises(ValueError):
        deduplicate([_rec("R1")], keep="middle")


def test_deduplicate_empty():
    assert deduplicate([]) == []


def test_deduplicate_preserves_order():
    records = [_rec("R3"), _rec("R1"), _rec("R2")]
    result = deduplicate(records)
    assert [r["review_id"] for r in result] == ["R3", "R1", "R2"]


def test_deduplicate_custom_key():
    records = [{"body": "hello"}, {"body": "world"}, {"body": "hello"}]
    result = deduplicate(records, key_fn=lambda r: r.get("body", ""))
    assert len(result) == 2


def test_find_duplicates_basic():
    records = [_rec("R1"), _rec("R2"), _rec("R1", body="again")]
    dups = find_duplicates(records)
    assert "R1" in dups
    assert len(dups["R1"]) == 2
    assert "R2" not in dups


def test_find_duplicates_no_duplicates():
    records = [_rec("R1"), _rec("R2")]
    dups = find_duplicates(records)
    assert dups == {}


def test_find_duplicates_empty():
    assert find_duplicates([]) == {}
