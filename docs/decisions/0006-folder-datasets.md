# 0006. A folder of Parquet files is one table

- Status: accepted
- Date: 2026-11-12

## Context
Parquet data is often written as a folder of files rather than one file: Spark, DuckDB, Polars and pyarrow split output into parts, and partition it into `key=value` folders (`year=2024/month=02/part-0.parquet`). Users want to browse, filter, sort and query such a folder as one table, without first merging it into a single file.

## Decision
`ParquetSource::open` accepts a folder. It finds every `.parquet` file below it, in path order (names starting with `.` or `_` are skipped), reads each file's footer, and presents them as one table:

- Row groups are numbered across all files in that order, so paging, filters, sorts, aggregates and exports work unchanged on top of `row_groups()`. A read that crosses a file boundary opens one reader per file.
- Every file must have the same columns: names and types in the same order. A file that differs is an error that names it and the first difference. A column is nullable if it is nullable in any file. There is no schema merging or type widening.
- `key=value` folders become extra columns after the file columns (Hive-style partitions). A partition column is `Int64` when every value is a plain integer, otherwise `Utf8`; `__HIVE_DEFAULT_PARTITION__` and an empty value are null. Every file must sit under the same partition keys. Partition values have exact statistics, so filters on them skip other partitions' files. When the files already store a column of the key's name (Polars writes both), that column is used and the folder name adds nothing.
- The search and the footers are bounded: at most 4,096 files (`MAX_DATASET_FILES`), 16 folder levels, 100,000 directory entries looked at, and 256 MB of footer metadata. Past any of them, the open fails with a message saying which.

## Consequences
- A folder opens wherever a file does: the GUI (file menu, start screen, drop, recent list), the CLI and the bench.
- Opening reads every footer before the first row shows, so a folder of thousands of files takes a moment to open; the open can be cancelled between files.
- Folders of files with different schemas (schema evolution) are refused rather than guessed at. Supporting them would need a merge rule and is a separate decision.
- Row order is file order. Reopening a folder after files were added or removed changes row numbers.
