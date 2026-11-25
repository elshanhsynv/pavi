# Architecture decisions (ADRs)

One short file per decision that would be expensive to reverse or easy to "improve" by accident.
Before changing something an ADR covers, write a new ADR that supersedes it.

Name files `NNNN-short-title.md` and use this template:

```markdown
# NNNN. Title

- Status: accepted | superseded by NNNN
- Date: YYYY-MM-DD

## Context
What problem or force made a decision necessary.

## Decision
What we do, in one or two sentences.

## Consequences
What gets easier, what gets harder, what we must keep doing because of this.
```

0001–0004 were reconstructed after the fact from the code and docs. **Owner: confirm or correct the
"Context" and "Decision" of each**; until then treat their reasons as informed guesses, not intent.

| ADR | Decision |
|---|---|
| [0001](0001-no-async-runtime.md) | Std threads and bounded channels, no async runtime |
| [0002](0002-bounded-memory.md) | Every buffer has an explicit cap |
| [0003](0003-windowed-grid.md) | The grid shows huge files through a 500,000-row window |
| [0004](0004-custom-query-engine.md) | A small custom query engine instead of DataFusion/DuckDB/Polars |
| [0005](0005-external-sort.md) | Sorts larger than memory spill to temporary files (amends 0002) |
| [0006](0006-folder-datasets.md) | A folder of Parquet files (with `key=value` partitions) is one table |
| [0007](0007-other-formats.md) | CSV, JSON and Arrow files are converted to a temporary Parquet file when opened |
| [0008](0008-remote-files.md) | Remote files (S3, GCS, Azure, HTTPS) are read with blocking HTTP range requests |
| [0009](0009-joins-and-subqueries.md) | Joins and subqueries are evaluated into a temporary table, then queried (amends 0004) |
| [0010](0010-light-theme.md) | A light theme beside the dark one, from one `Palette` type (amends DESIGN.md principle 5) |
| [0011](0011-cli-sql.md) | The CLI runs `--sql` through `pavi_query` and the worker pool (amends "the CLI uses only `parquet_reader`") |
| [0012](0012-result-navigation.md) | Results are entered anywhere: counted filters start at a row group, other results read on or again (amends 0003) |
| [0013](0013-sql-calculations.md) | SQL calculations are made on the way out in a plain SELECT, otherwise on prepared rows first (amends 0009) |
| [0014](0014-page-index-and-bloom-filters.md) | Filtered scans read the page index and bloom filters of the columns they test, row group by row group |
| [0015](0015-interruptible-remote-requests.md) | Remote requests wait on a helper thread, so cancelling a task ends the wait at once (amends 0008) |
| [0016](0016-counts-from-statistics.md) | Counts take whole row groups from statistics that prove every row matches; the CLI counts on threads (amends 0011) |
| [0017](0017-locked-temp-folders.md) | Temporary folders are locked while in use; left-over ones are removed when the app or CLI starts (amends 0005, 0007, 0011) |
| [0018](0018-file-changes-on-disk.md) | Files that change on disk are noticed, refused for reads and reloaded on request, keeping the filter and sort |
| [0019](0019-folder-schema-merging.md) | The files of a folder may differ: columns are merged by name, types widened, missing columns read as null (amends 0006) |
| [0020](0020-import-options.md) | CSV and JSON are read with import options (delimiter, header, encoding, all text); a late value retypes the column instead of failing (amends 0007) |
| [0021](0021-excel-workbooks.md) | Excel and OpenDocument workbooks are read a sheet at a time (calamine) and results exported to .xlsx (rust_xlsxwriter) (amends 0007) |
| [0022](0022-whole-file-search.md) | Find looks through the whole file on a worker; `* contains` filters rows by text in any column |
| [0023](0023-sql-statistics-dates-union.md) | SQL gains MEDIAN, quantiles, STDDEV/VARIANCE, REPLACE, DATE_TRUNC, DATE_DIFF, INTERVAL, NOW, UNION and WITH (amends 0009, 0013) |
