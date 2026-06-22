# Architecture

## Overview

The Automated JSON Pipeline is a modular ETL system that reads review data from NDJSON files, validates and transforms it, and writes the results to output files suitable for loading into Snowflake.

## Component Diagram

```
Input NDJSON
     │
     ▼
┌─────────────┐
│   io.py     │  read_ndjson / stream_ndjson
└──────┬──────┘
       │ list[dict]
       ▼
┌─────────────────┐
│ transformers.py │  normalise_review, coerce_star_rating, parse_review_date
└────────┬────────┘
         │ list[dict]
         ▼
┌───────────────┐
│ validators.py │  validate_record, validate_batch, validation_report
└───────┬───────┘
        │ (valid, invalid)
        ▼
┌──────────────┐
│  filters.py  │  by_star_rating, by_date_range, by_category, ...
└──────┬───────┘
       │ list[dict]
       ▼
┌──────────────────────┐
│  batch_processor.py  │  process_batch, process_stream
└──────────┬───────────┘
           │ BatchResult
           ▼
┌──────────────┐     ┌──────────────┐
│   export.py  │     │  metrics.py  │
│  to_csv      │     │  PipelineMetrics │
│  to_json     │     │  dump_metrics    │
│  to_ndjson   │     └──────────────┘
└──────────────┘
```

## Module Responsibilities

| Module | Responsibility |
|--------|---------------|
| `io.py` | File I/O: read/write NDJSON, JSON, CSV |
| `transformers.py` | Data normalisation and type coercion |
| `validators.py` | Schema validation and error reporting |
| `filters.py` | Composable record predicates |
| `batch_processor.py` | Chunked processing with error handling |
| `aggregators.py` | Statistics, grouping, histograms |
| `deduplicator.py` | Duplicate detection and removal |
| `export.py` | CSV, JSON, NDJSON, summary export |
| `metrics.py` | Run metrics collection and reporting |
| `retry.py` | Exponential-backoff retry decorator |
| `config.py` | Environment-based configuration |
| `pipeline.py` | High-level `ReviewPipeline` orchestrator |
| `sql_meta.py` | Snowflake DDL metadata extraction |

## Data Flow

1. **Ingest** — `io.read_ndjson` loads raw records from disk
2. **Normalise** — `transformers.normalise_review` cleans string fields
3. **Validate** — `validators.validate_batch` splits valid/invalid records
4. **Filter** — composable `FilterFn` predicates select desired records
5. **Process** — `batch_processor.process_batch` invokes a handler per record
6. **Export** — `export.*` serialises results to CSV/JSON/NDJSON
7. **Metrics** — `PipelineMetrics` tracks throughput, duration, and errors
