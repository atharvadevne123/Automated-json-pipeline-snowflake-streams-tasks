"""Snowflake JSON ETL Pipeline — utilities to load SQL scripts and sample data."""
from __future__ import annotations
import importlib.resources as _res
import pathlib as _pl

_PKG = _pl.Path(__file__).parent


def get_sql(name: str = "snowflake_optimized.sql") -> str:
    """Return the text of a bundled SQL script."""
    return (_PKG / "sql" / name).read_text(encoding="utf-8")


def list_sql() -> list[str]:
    """Return names of all bundled SQL scripts."""
    return [f.name for f in (_PKG / "sql").iterdir() if f.suffix == ".sql"]


def get_sample_data_path() -> str:
    """Return the filesystem path to the bundled sample JSON."""
    return str(_PKG / "data" / "sample_amazon_reviews.json")
