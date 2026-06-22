# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [2026-06-22] — Pipeline Enhancement

### Added
- `count_by` and `rating_histogram` aggregation helpers
- `validation_report` for structured error summaries
- `sample_records` utility for random record sampling
- `read_csv` I/O helper for CSV ingestion
- `export_summary` to export aggregate statistics as JSON
- `by_min_review_length` and `by_product_ids` filter predicates
- `dump_metrics` helper for compact log-line metric output
- `jitter` parameter in `retry` decorator for thundering-herd prevention
- `__all__` exports in all modules for clean public API surface
- `scripts/benchmark.py`, `scripts/check_env.py`, `scripts/export_metrics.py`
- GitHub issue templates and PR template
- `docs/ARCHITECTURE.md` with component diagram
- `lru_cache` on `is_valid_date` for repeated date validation calls
- Test fixtures: `multi_category_reviews`, `mixed_validity_reviews`
- Parametrized edge-case tests for transformers and validators
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
