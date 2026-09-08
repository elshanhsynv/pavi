# Pavi

Pavi is a native desktop explorer for **Parquet** files with a bounded Arrow-based query pipeline.

## Goals

* Open large Parquet files through a fixed background worker pool
* Virtualized scrolling, projected page caching, column resizing, and cell selection
* Filters, SQL (`SELECT` / `WHERE` / `ORDER BY` / aggregates), and deterministic single-column sorting
* Bounded charts, Inspector metadata/statistics, query history, recent files, and restored session preferences
* Streaming CSV and Parquet export for the current source/query/filter result, plus cached selected-row export and clipboard copy

## Tech Stack

* Rust
* Apache Arrow
* Apache Parquet
* egui / eframe / egui_extras
* Standard-library threads and bounded channels (no async runtime)

## Project Structure

```text
crates/
├── cli/        # CLI for development and testing
├── parquet/    # Parquet data source, caching, decoding, metadata
├── runtime/    # Fixed worker pool and cooperative cancellation
├── query/      # Logical plans, SQL, filters, sorting, aggregates
├── app/        # Native explorer, charts, inspector, sessions, export
└── bench/      # Reproducible performance measurements
```

## Current limits

Pavi deliberately keeps pages, result windows, chart points, history, and export events bounded. Query/filter/export work runs through `Query → Runtime → ParquetSource`; exports stream one result batch at a time to a temporary file before finalization. CSV supports the scalar types displayed by Pavi (including strings, binary as hexadecimal, dates, and timestamps); unsupported nested values produce a clear error. Parquet export preserves Arrow batches and schema directly.

