# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [1.1.0] — 2026-05-01

### Added
- `snowflake_pipeline.validators` — standalone batch-aware validator module
- `snowflake_pipeline.transformers` — review record normalisation helpers
- `snowflake_pipeline.utils` — date parsing, chunking, and string helpers
- `snowflake_pipeline.config` — environment-based `PipelineConfig` / `SnowflakeConfig`
- `snowflake_pipeline.batch_processor` — `process_batch` / `process_stream` with `BatchResult`
- `snowflake_pipeline.metrics` — `PipelineMetrics` with JSON export and throughput calc
- `snowflake_pipeline.retry` — `@retry` decorator with exponential backoff
- `snowflake_pipeline.io` — NDJSON streaming reader / writer helpers
- `snowflake_pipeline.export` — CSV, JSON, and NDJSON export utilities
- `snowflake_pipeline.filters` — composable record filter predicates
- `snowflake_pipeline.exceptions` — typed exception hierarchy
- `snowflake_pipeline.constants` — shared named constants
- `snowflake_pipeline.pipeline` — `ReviewPipeline` end-to-end orchestrator
- `Makefile`, `docker-compose.yml`, `.pre-commit-config.yaml`
- Expanded test suite: 100+ tests across 15 test files

### Changed
- `validate_review()` now enforces YYYY-MM-DD date format and non-empty strings
- `sql_meta.py` uses pre-compiled regex patterns for better performance
- `__init__.py` exposes `__all__` for clean public API surface

### Fixed
- Path traversal guard in `get_sql()` now validated against resolved path

## [1.0.0] — 2026-04-25

### Added
- Initial release: `get_sql()`, `list_sql()`, `load_sample_reviews()`, `validate_review()`
- `sql_meta.py` for DDL metadata extraction
- CI workflow (Python 3.9 / 3.11 / 3.12 matrix)
- Dockerfile with non-root user and HEALTHCHECK
- `.env.example`, `pyproject.toml`, `requirements.txt`
