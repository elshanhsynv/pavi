# 0011. The CLI runs SQL through the query engine and its worker pool

- Status: accepted
- Date: 2026-11-14
- Amends: the architecture invariant "The CLI uses only `parquet_reader`" (`docs/ARCHITECTURE.md`)

## Context
The CLI was a synchronous tool over `parquet_reader` alone. It now streams rows, filters, sorts, counts and converts, but scripts also want what the app's SQL editor does: `GROUP BY` with `COUNT/SUM/AVG/MIN/MAX`, joins between files and subqueries (ADR 0009). That logic lives in `pavi_query`, which runs plans on the `pavi_runtime` worker pool and prepares joins and subqueries on its own thread. Reimplementing any of it in the CLI would give two SQL dialects that drift apart.

## Decision
`cli FILE --sql "…"` runs the query with `pavi_query`, exactly as the app does:

- The CLI depends on `pavi_query` and `pavi_runtime` (dependencies still point down: cli → query → runtime → parquet_reader). Only `--sql` starts a `Runtime`; every other option keeps the synchronous path on the main thread.
- Tables are named as in the app: `dataset` is `FILE`, which is also reachable by its file name; each `--table [NAME=]PATH` adds one more file, named `NAME` or, by default, by `pavi_query::table_name` (the function the app uses for tabs, moved there from the app). They are opened before the query runs, on the main thread, which is fine without a UI.
- The main thread waits on `QueryExecution::next_batch`. Rows go through the same output path as other rows: the table (≤ 1,000 rows, no file summary) or a `BatchWriter` to stdout or `--output`. `--head` and `--offset` cut the result; once `--head` rows are out, the execution is cancelled.
- The query's memory and disk are bounded by the engine's own budgets (sort, group, join build side, prepared table), unchanged.
- `--sql` excludes the options SQL itself covers (`--filter`, `--sort`, `--columns`) and the file facts (`--count`, `--schema`, `--stats`).

## Consequences
- One SQL dialect for the app and the CLI; a change to the subset or its limits reaches both.
- The CLI binary links the runtime and query crates and the SQL parser (already in the workspace; no new third-party crate).
- A process killed mid-query (Ctrl+C) can leave a spill or prepared-table folder in the temporary directory, as a crashed app would; the next run that creates a folder of the same kind removes those older than a day (`TempFolder::create`, ADR 0005).
- Long queries print nothing until rows arrive; there is no progress output yet.
