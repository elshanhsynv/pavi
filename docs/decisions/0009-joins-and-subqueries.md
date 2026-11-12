# 0009. Joins and subqueries are evaluated into a temporary table first

- Status: accepted
- Date: 2026-11-12
- Amends: [0004](0004-custom-query-engine.md) (the engine reads one source per query)

## Context
PAVI's query engine (ADR 0004) runs one flat plan (filter, projection, sort, aggregate, limit) over one `ParquetSource`. Users want to combine datasets: join a fact table with a lookup table, filter by the result of another query, or query a query. Rebuilding the engine as a general operator tree would touch every read path, the runtime and the result window, and would need a memory model for every operator.

## Decision
SQL can name other tables, join them and nest queries. Anything beyond the single-table shape is evaluated first into a temporary Parquet table, and the existing engine then runs the rest of the query on it:

- **Tables** are the open tabs: `dataset` is the tab running the query; every other open tab is a table named after its file (`flights.parquet` → `flights`, made a valid name, with `_2`, `_3` … for repeats). A table can have an alias (`FROM flights AS f`), and columns can be qualified (`f.ORIGIN`).
- **Joins:** `[INNER] JOIN` and `LEFT [OUTER] JOIN` with `ON` equalities between one column of each side, joined by `AND` (at most 4 keys), up to 4 joins in a query. A hash join (`parquet_reader::join`) keeps one side in memory: the right side, or for an inner join whichever side has fewer rows. The kept side is bounded by `MAX_JOIN_BUILD_ROWS` (1,000,000) and `MAX_JOIN_BUILD_BYTES` (256 MB), with an error that says to filter it or join the other way round. The other side streams. Key types must match (any integers, any strings, otherwise the same type), and null keys never match.
- **Subqueries** are not correlated:
  - `FROM (SELECT …) AS t`: the inner query runs to completion and its rows become a table.
  - `column [NOT] IN (SELECT one_column …)`: a semi join or an anti join. `NOT IN` follows SQL: a null in the subquery's results means no row passes.
  - `column <op> (SELECT …)`: the subquery must return one row and one column. Its value replaces it, as text formatted by `value::scalar_text`.
  - Subqueries nest at most 3 deep.
- **Pushdown:** a `WHERE` test that names one column of one table is applied while that table is read, except on the optional side of a `LEFT JOIN`, where it must apply after the join. Only the columns the query uses are kept.
- **Execution:** this preparation runs on its own thread (`pavi-query-prep`), like export and profiling, so subqueries can drive the worker pool without waiting on themselves. It reports progress and stops between batches when cancelled. `QueryExecution` starts in a preparing state, then plans and runs the outer query on the prepared table exactly as before. The temporary table (≤ 32 GiB, `MAX_PREPARED_BYTES`) is deleted with the result.
- Queries on a single table without subqueries take the existing direct path, unchanged.

## Consequences
- Joins work on files of any size as long as one side, after its filters, fits the build budget; the result can be larger than memory.
- A query with a join or subquery can't show its first row until the preparation is done: it reads every input once and writes the prepared table. `LIMIT` alone does not shorten that.
- Not supported, each with a clear error: `RIGHT`/`FULL`/`CROSS` joins, non-equality join conditions, correlated subqueries, `EXISTS`, `UNION`, `WITH`, and expressions in `SELECT`.
- Only open tabs can be named, so a query never opens files on the UI thread.
