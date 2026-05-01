# Automated JSON Pipeline — Snowflake Streams & Tasks

[![CI](https://github.com/atharvadevne123/Automated-json-pipeline-snowflake-streams-tasks/actions/workflows/ci.yml/badge.svg)](https://github.com/atharvadevne123/Automated-json-pipeline-snowflake-streams-tasks/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

A real-time, event-driven data pipeline that ingests JSON from **Amazon S3** into **Snowflake** using Streams and Tasks. Automates transformation into dimension and fact tables with support for incremental loads and analytics-ready insights.

---

## Features

| Feature | Description |
|---------|-------------|
| **Event-driven ingestion** | S3 → Snowflake Streams → Tasks pipeline |
| **Incremental loads** | Change Data Capture via Snowflake Streams |
| **Auto transformation** | Raw JSON → dimension and fact tables |
| **Batch processing** | Configurable chunk size with metrics tracking |
| **Data validation** | Schema validation with detailed error reporting |
| **Data transformation** | Text normalisation, type coercion, date parsing |
| **Composable filters** | Star-rating, date-range, category, and verified-purchase filters |
| **Export utilities** | CSV, JSON array, and NDJSON output |
| **Retry logic** | Exponential-backoff decorator for transient failures |
| **Metrics** | Per-run throughput, duration, and validation stats |
| **Pipeline orchestrator** | `ReviewPipeline` end-to-end ETL class |
| **CI** | Python 3.9 / 3.11 / 3.12 matrix with coverage and mypy |

---

## Quick Start

### Prerequisites

- Python 3.9+
- Snowflake account
- AWS S3 bucket (for production; tests run without it)

### Installation

```bash
git clone https://github.com/atharvadevne123/Automated-json-pipeline-snowflake-streams-tasks
cd Automated-json-pipeline-snowflake-streams-tasks

# Install the package and dev dependencies
pip install -e ".[dev]"

# Copy and fill in environment variables
cp .env.example .env
```

### Configuration

Set the following environment variables (or populate `.env`):

```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_db
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_wh
SNOWFLAKE_ROLE=your_role        # optional
LOG_LEVEL=INFO
PIPELINE_BATCH_SIZE=100
```

### Apply the Snowflake DDL

```bash
# Review and run the DDL script in your Snowflake worksheet
cat snowflake_pipeline/sql/snowflake_optimized.sql
```

### Use the Python API

```python
import snowflake_pipeline as sp

# Load and inspect bundled SQL
sql = sp.get_sql()
print(sql[:200])

# Load and validate sample data
records = sp.load_sample_reviews()
for rec in records:
    errors = sp.validate_review(rec)
    if errors:
        print(f"Invalid: {errors}")

# End-to-end pipeline run
from pathlib import Path
from snowflake_pipeline.pipeline import ReviewPipeline
from snowflake_pipeline.filters import by_star_rating

pipeline = ReviewPipeline()
result = pipeline.run(
    source=Path("data/reviews.ndjson"),
    destination=Path("output/valid_reviews.ndjson"),
    filters=[by_star_rating(4, 5)],
)
print(f"Processed {result.processed}/{result.total} records in {result.duration_s:.2f}s")
```

---

## Project Structure

```
.
├── snowflake_pipeline/
│   ├── __init__.py          # Core API: get_sql, load_sample_reviews, validate_review
│   ├── sql_meta.py          # DDL metadata extractor
│   ├── pipeline.py          # ReviewPipeline orchestrator
│   ├── validators.py        # Batch-aware record validators
│   ├── transformers.py      # Text normalisation & type coercion
│   ├── filters.py           # Composable record filter predicates
│   ├── batch_processor.py   # Chunked processing with BatchResult
│   ├── metrics.py           # PipelineMetrics with JSON export
│   ├── retry.py             # Exponential-backoff @retry decorator
│   ├── io.py                # NDJSON streaming reader/writer
│   ├── export.py            # CSV / JSON / NDJSON export utilities
│   ├── config.py            # PipelineConfig / SnowflakeConfig
│   ├── constants.py         # Shared named constants
│   ├── exceptions.py        # Typed exception hierarchy
│   ├── sql/                 # Bundled SQL scripts
│   └── data/                # Bundled sample data
├── tests/                   # 15 test files, 100+ tests
├── scripts/                 # CLI tools (validate_data, seed_data, generate_report)
├── .github/workflows/       # CI (lint + type-check + coverage)
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

---

## API Reference

### Core (`snowflake_pipeline`)

| Function | Description |
|----------|-------------|
| `get_sql(name?)` | Return bundled SQL script text (path-traversal safe) |
| `list_sql()` | List all bundled SQL script names |
| `get_sample_data_path()` | Return path to bundled NDJSON sample data |
| `load_sample_reviews()` | Parse and return all sample review records |
| `validate_review(record)` | Validate one record; return list of error strings |

### Validators (`snowflake_pipeline.validators`)

```python
from snowflake_pipeline.validators import validate_record, assert_valid, validate_batch

errors = validate_record(record)           # returns list[str]
assert_valid(record)                       # raises ValidationError if invalid
valid, invalid = validate_batch(records)  # partition into valid / invalid
```

### Filters (`snowflake_pipeline.filters`)

```python
from snowflake_pipeline.filters import apply_filters, by_star_rating, by_verified_purchase, by_date_range

result = apply_filters(records, by_star_rating(4, 5), by_verified_purchase(True))
```

### Export (`snowflake_pipeline.export`)

```python
from snowflake_pipeline.export import to_csv, to_json, to_ndjson

to_csv(records, Path("output.csv"))
to_json(records, Path("output.json"))
to_ndjson(records, Path("output.ndjson"))
```

### Pipeline (`snowflake_pipeline.pipeline`)

```python
from snowflake_pipeline.pipeline import ReviewPipeline

pipeline = ReviewPipeline()
result = pipeline.run(source=Path("in.ndjson"), destination=Path("out.ndjson"))
print(pipeline.metrics.to_json())
```

---

## Testing

```bash
make test           # run all tests
make test-cov       # run with coverage report
pytest tests/test_validators.py -v   # run a specific module
```

The test suite covers:
- Unit tests for every module
- Parametrized edge cases for validators, transformers, filters
- Integration tests for full ETL cycle
- Security tests (path traversal guards)

---

## Docker

```bash
# Build
make docker-build

# Run with docker-compose (requires .env)
docker-compose up pipeline

# Run tests in Docker
docker-compose run --rm test-runner
```

---

## CLI Tools

```bash
# Validate an NDJSON file
python scripts/validate_data.py data/reviews.ndjson --strict

# Generate synthetic test data
python scripts/seed_data.py output/synthetic.ndjson -n 1000 --seed 42

# Generate a run report from metrics JSON
python scripts/generate_report.py output/metrics.json
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). All PRs require passing CI (lint + tests).

## License

[MIT](LICENSE)
