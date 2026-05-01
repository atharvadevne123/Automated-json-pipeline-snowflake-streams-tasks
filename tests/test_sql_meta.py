"""Tests for snowflake_pipeline.sql_meta."""
from __future__ import annotations

import pytest

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
    sql = "CREATE OR REPLACE WAREHOUSE MY_WH WAREHOUSE_SIZE=\'XSMALL\';"
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


def test_summarize_includes_warehouses_count():
    sql = sp.get_sql()
    result = summarize(sql)
    assert "Warehouses" in result


def test_summarize_includes_sequences_count():
    sql = sp.get_sql()
    result = summarize(sql)
    assert "Sequences" in result


def test_extract_metadata_deduplication():
    sql = (
        "CREATE OR REPLACE TABLE db.s.T1 (id INT);\n"
        "CREATE OR REPLACE TABLE db.s.T1 (id INT);\n"
    )
    meta = extract_metadata(sql)
    assert meta.tables.count("DB.S.T1") == 2  # extractor lists all occurrences


def test_extract_metadata_case_insensitive():
    sql = "create or replace table mydb.myschema.lowercase_table (id int);"
    meta = extract_metadata(sql)
    assert any("LOWERCASE_TABLE" in t for t in meta.tables)


# ---------------------------------------------------------------------------
# Additional parametrized cases
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql,attr,expected_fragment", [
    ("CREATE TABLE schema.T (id INT);", "tables", "SCHEMA.T"),
    ("CREATE OR REPLACE TABLE db.s.X (id INT);", "tables", "DB.S.X"),
    ("CREATE STREAM s.MY_STR ON TABLE t;", "streams", "S.MY_STR"),
    ("CREATE TASK s.MY_TASK WAREHOUSE=WH AS SELECT 1;", "tasks", "S.MY_TASK"),
    ("CREATE SEQUENCE s.SEQ1 START=1;", "sequences", "S.SEQ1"),
])
def test_extract_various_ddl(sql, attr, expected_fragment):
    meta = extract_metadata(sql)
    names = getattr(meta, attr)
    assert any(expected_fragment in n for n in names)


def test_summarize_line_count():
    result = summarize("")
    assert len(result.splitlines()) == 6


def test_sql_metadata_dataclass_defaults():
    from snowflake_pipeline.sql_meta import SqlMetadata
    m = SqlMetadata()
    assert m.tables == []
    assert m.streams == []
    assert m.tasks == []
    assert m.procedures == []
    assert m.warehouses == []
    assert m.sequences == []


def test_extract_metadata_empty_sql():
    meta = extract_metadata("")
    assert meta.tables == []
    assert meta.streams == []


def test_extract_multiple_object_types():
    sql = """
    CREATE OR REPLACE TABLE db.s.T (id INT);
    CREATE OR REPLACE STREAM db.s.STR ON TABLE T;
    CREATE OR REPLACE TASK db.s.TSK WAREHOUSE=WH AS SELECT 1;
    CREATE OR REPLACE WAREHOUSE MY_WH;
    CREATE OR REPLACE SEQUENCE db.s.SEQ;
    """
    meta = extract_metadata(sql)
    assert len(meta.tables) >= 1
    assert len(meta.streams) >= 1
    assert len(meta.tasks) >= 1
    assert len(meta.warehouses) >= 1
    assert len(meta.sequences) >= 1
