# Pavi

Pavi is a native desktop explorer for **Parquet** files with a bounded Arrow-based query pipeline.

## Goals

* Open large Parquet files through a fixed background worker pool
* Virtualized scrolling to any row of very large files (a moving 500,000-row window with a whole-file scrollbar), projected page caching, column resizing, and cell selection
* Filters, SQL (`SELECT` / `WHERE` / `ORDER BY` / aggregates), and deterministic sorting by up to 8 columns over booleans, numbers, decimals, text, dates, timestamps and times of day (e.g. `FL_DATE >= 2006-02-15`), on files of any size (large sorts spill to temporary files); filters combine tests with `AND` / `OR` and support `IN`, `BETWEEN`, `LIKE` and `IS NULL` (e.g. `DEP_DELAY > 60 AND DISTANCE BETWEEN 500 AND 1000`); filtered results stream from a resume cursor, and `ORDER BY … LIMIT n` keeps only the top rows so it works on files of any size
* Bounded charts, Inspector metadata/statistics, query history, recent files, and restored session preferences
* A SQL editor with highlighting, column-name completion, query history and saved queries
* Find in the loaded rows, pinned columns, a whole-row view in the Inspector, and clipboard copy of full values (tab-separated, up to 16 MB)
* Streaming CSV and Parquet export for the current source/query/filter result, plus cached selected-row export

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

The desktop entry point is in `crates/app/src/main.rs`; application state and background work live in `app.rs`. The `ui/` directory separates the top bar, right-side inspector, virtualized grid, SQL editor, charts, status bar, theme, and shared icon widgets. `ui/spacing.rs` defines the shared `Spacing` scale (0, 2, 4, 8, 12 px) used for gaps and padding. Rows are 24 px high; column type labels and frozen headers can be toggled without disabling virtualization. File properties and cache metrics come from the loaded source.

Pavi deliberately keeps pages, result windows, chart points, history, and export events bounded. Query/filter/export work runs through `Query → Runtime → ParquetSource`; exports stream one result batch at a time to a temporary file before finalization. Grid, CLI, clipboard, CSV export and Inspector statistics share one value formatter (`parquet_reader::value`): ISO dates, RFC 3339 timestamps (with an offset only for time-zone-aware columns), scaled decimals, and dictionary values. CSV supports every non-nested type (binary as `0x` hexadecimal); nested values produce a clear error. Parquet export preserves Arrow batches and schema directly.

