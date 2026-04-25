"""Tests for snowflake_pipeline.sql_meta."""
from __future__ import annotations

import snowflake_pipeline as sp
from snowflake_pipeline.sql_meta import extract_metadata, summarize


def test_extract_tables():
    sql = "CREATE OR REPLACE TABLE db.schema.MY_TABLE (id INT);"
    meta = extract_metadata(sql)
    assert "DB.SCHEMA.MY_TABLE" in meta.tables


def test_extract_streams():
    sql = "CREATE OR REPLACE STREAM db.schema.MY_STREAM ON TABLE db.schema.T;"
    meta = extract_metadata(sql)
    assert "DB.SCHEMA.MY_STREAM" in meta.streams


def test_extract_tasks():
    sql = "CREATE OR REPLACE TASK db.schema.MY_TASK WAREHOUSE = WH AS SELECT 1;"
    meta = extract_metadata(sql)
    assert "DB.SCHEMA.MY_TASK" in meta.tasks


def test_extract_procedures():
    sql = "CREATE OR REPLACE PROCEDURE db.schema.MY_PROC() RETURNS STRING AS $$ $$ ;"
    meta = extract_metadata(sql)
    assert "DB.SCHEMA.MY_PROC()" in meta.procedures or any("MY_PROC" in p for p in meta.procedures)


def test_extract_warehouses():
    sql = "CREATE OR REPLACE WAREHOUSE MY_WH WAREHOUSE_SIZE='XSMALL';"
    meta = extract_metadata(sql)
    assert "MY_WH" in meta.warehouses


def test_extract_sequences():
    sql = "CREATE OR REPLACE SEQUENCE db.schema.MY_SEQ START=1;"
    meta = extract_metadata(sql)
    assert "DB.SCHEMA.MY_SEQ" in meta.sequences


def test_extract_from_real_sql():
    sql = sp.get_sql()
    meta = extract_metadata(sql)
    assert len(meta.tables) >= 5
    assert len(meta.streams) >= 1
    assert len(meta.tasks) >= 1
    assert len(meta.warehouses) >= 1


def test_summarize_returns_string():
    sql = sp.get_sql()
    result = summarize(sql)
    assert isinstance(result, str)
    assert "Tables" in result
    assert "Streams" in result
    assert "Tasks" in result


def test_summarize_empty_sql():
    result = summarize("")
    assert "Tables" in result
    assert "none" in result
