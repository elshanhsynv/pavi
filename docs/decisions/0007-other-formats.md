# 0007. Other file formats are converted to Parquet when opened

- Status: accepted
- Date: 2026-11-12

## Context
Users also keep tables as CSV, newline-delimited JSON and Arrow IPC (Feather v2) files, and want to open them in PAVI with the same grid, filters, sorts, SQL, profiling, charts and export. Everything below the app works on a `ParquetSource`: paging by row group, statistics pruning, the page cache, sorts that spill, and aggregates. A second kind of source would need a trait across every read path and its tests, and CSV and JSON can't be paged or pruned without reading them from the start anyway.

## Decision
When `ParquetSource::open` gets a `.csv`/`.tsv`, `.json`/`.jsonl`/`.ndjson` or `.arrow`/`.feather`/`.ipc`/`.arrows` file, it converts it to a temporary Parquet file and opens that (`parquet_reader::convert`):

- The conversion streams: 8,192 rows at a time are read with `arrow-csv`, `arrow-json` or `arrow-ipc` and written with `ArrowWriter` (Snappy, row groups of at most 262,144 rows or 64 MB buffered). Memory stays bounded whatever the file size. The temporary file is capped at 32 GiB (`MAX_CONVERTED_BYTES`).
- CSV and JSON have no schema, so column types are inferred from the first 10,000 records (`MAX_INFER_ROWS`). A later value that doesn't fit stops the open with an error that says so. CSV must have a header row; the delimiter is a tab for `.tsv`, otherwise the most frequent of `,` `;` tab `|` in the first line. JSON can be one object per line or a single array of objects.
- The file lives in `<temp>/pavi-open-<pid>-<n>/` and is deleted when the source is dropped (its tab closed or reloaded). Folders a crash left behind are removed by a later conversion once they are a day old, as for sort spills (ADR 0005).
- The open reports how far it has read and can be cancelled between batches.
- The readers are untrusted-input parsers like the Parquet decoder: their panics become errors, and an Arrow file's block, message and decompressed buffer sizes are checked against the file before `arrow-ipc` allocates for them (both found by the `other_formats` fuzz target).
- Folders (ADR 0006) still hold Parquet files only.

ORC is not supported: the one Rust ORC reader (`orc-rust`) is built on an older arrow major version and pulls in tokio by default, which ADR 0001 rules out.

## Consequences
- Every feature works on these formats with no change to the reading, query or runtime code, and filters get row-group statistics that the original files don't have.
- Opening reads the whole file once before the first row shows, and needs about its compressed size again in temporary disk space. Progress shows while it runs.
- Type inference can be wrong for columns whose first 10,000 values are unrepresentative; the error names the problem, and the fix is to convert the file with another tool.
- Changes to the original file after opening are not seen until Reload.
