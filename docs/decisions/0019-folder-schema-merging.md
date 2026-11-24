# 0019. The files of a folder may differ: their columns are merged by name and widened

- Status: accepted
- Date: 2026-11-24
- Amends: [0006](0006-folder-datasets.md) ("Every file must have the same columns … There is no schema merging or type widening")

## Context
ADR 0006 refused a folder whose files didn't have exactly the same columns. Real datasets drift: a pipeline adds a column, a writer stores a small integer as `Int32` in one run and `Int64` in the next, pyarrow writes a column with no values as `Null`, Polars writes text as `LargeUtf8` where pandas wrote `Utf8`. Such a folder could only be opened file by file. Spark (`mergeSchema`), DuckDB (`union_by_name`) and Polars (`allow_missing_columns`) read them as one table by name.

## Decision
- **By name, in order of appearance.** The table's columns are the first file's columns in its order, then each column a later file adds, in the order it first appears (`dataset::merge_columns`). Column order within a file doesn't matter. A file with two columns of one name is refused.
- **Missing is null.** A column a file lacks reads as null in that file's rows, and is nullable. A column is also nullable when any file has it nullable.
- **Types are widened to one that holds every file's values** (`types::common_type`): the wider integer (signed and unsigned give a signed type that holds both, and none for `UInt64` with a signed type); the wider float, and `Float64` for an integer with a float; a decimal with enough digits on both sides of the point (up to 38); `LargeUtf8` for different text types and `LargeBinary` for binary; a `Null` column takes the other type. Everything else must be equal: dates, times, timestamps (unit and zone), durations, booleans and nested types. Otherwise the open fails, naming both files, the column and the two types.
- **Cast on the way in.** Each file keeps a map from the table's columns to its own (`SourceFile::columns`). A read decodes only that file's columns, casts each to the table's type and adds null arrays for missing ones (`reorder_batch`). Everything above the reader (filters, sorts, SQL, export, charts) sees one schema.
- **Pruning only where it is exact.** Row-group statistics, page indexes and bloom filters of a file are used for a column only when the file stores it as the table's type; a widened column is scanned in that file. A missing column has no statistics to prune with; it is scanned too.
- **Statistics shown per file.** The Inspector and `pavi-cli --stats` show each file's own statistics; a missing column's row groups show no min/max and every row null.

## Consequences
- Folders written over time open as one table, as in Spark, DuckDB and Polars. Folders that used to open behave as before: identical files give the same schema and the same reads.
- An integer column widened to `Float64` loses exactness past 2^53, as in those tools.
- A filter on a column that changed type in some files reads those files' row groups instead of skipping them by statistics.
- Merging costs one hash lookup per column of each footer at open, within the existing 4,096-file and 256 MB footer bounds.
