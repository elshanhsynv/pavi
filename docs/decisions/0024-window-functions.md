# 0024. SQL window functions, streamed over sorted rows

- Status: accepted
- Date: 2026-11-25
- Amends: [0023](0023-sql-statistics-dates-union.md) (`… OVER (…)` was refused)

## Context
After ADR 0023, PAVI's SQL could summarise groups but not compare a row with its neighbours or its group. People need to number rows within a group, find the top row per group, see the change from the previous row, keep a running total or a moving average, and compare a value with its group's total. Each needs a window function: `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)`.

A window function sees other rows, so a simple engine would hold a partition in memory. PAVI's files have billions of rows, and a partition (or the whole table, for `OVER ()`) can be most of them. That breaks the rule that every buffer is bounded (ADR 0002).

## Decision
- **Supported:**
  - `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`;
  - `LAG(x [, n [, default]])` and `LEAD(…)`;
  - `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` `… OVER (PARTITION BY … ORDER BY …)`.
- **Frames:**
  - With no `ORDER BY`, an aggregate covers the whole partition.
  - With `ORDER BY`, the default covers the partition up to the last of the row's peers (rows tied in `ORDER BY`), as the SQL standard says.
  - `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` stops at the row itself.
  - `ROWS BETWEEN n PRECEDING AND CURRENT ROW` is a moving frame.
  - `… AND UNBOUNDED FOLLOWING` from `UNBOUNDED PRECEDING` is the whole partition.
  - Other frames are refused.
- **Where:** window functions work in the SELECT list, and in its calculations (`amount - LAG(amount) OVER (…)`). `ORDER BY` can use their alias or position.
  - In `WHERE`, `GROUP BY`, `HAVING` or inside an aggregate, they are refused with a message that points to a subquery or `WITH`.
  - In the same SELECT as `GROUP BY`, `DISTINCT` or an aggregate, they are refused too. Group in a subquery or `WITH` first.
- **Order of work:** the query's rows are prepared (ADR 0009): joined, then `WHERE` filters them. Then the window functions run, then the calculations that use their results, then `ORDER BY` and `LIMIT`.
- **Execution** (`Prep::Window`, `parquet_reader::window`):
  - Window functions with the same `PARTITION BY` and `ORDER BY` share one pass. The rows are sorted by `PARTITION BY`, then `ORDER BY`, through the query engine's sort, which spills to disk (ADR 0005). `OVER ()` reads the rows as they are.
  - `compute_windows` then streams the sorted rows once and writes them, with a column per window function, to a temporary table. It finds where partitions and sets of peers start by comparing each row with the one before (`arrow_ord::partition`).
  - `ROW_NUMBER`, the ranks and the `ROWS` frames are counted as the rows pass.
  - `LAG`/`LEAD` read the same rows through a second cursor that runs `n` rows behind or ahead. The value is used only when that row is in the same partition.
  - The moving frame keeps its last `n + 1` values. Sums and counts add the value that enters and take away the one that leaves. MIN and MAX keep only the values that can still be the extreme.
  - A whole-partition aggregate, or one up to the row's peers, needs rows that come later. A first pass writes one total per partition, or per set of peers, to a small temporary table. The second pass reads it in step with the rows.
  - Nothing is held per partition, so memory doesn't grow with partition size.
- **Bounds:**
  - `LAG`/`LEAD` offsets and `n PRECEDING` are at most 1,000 (`MAX_WINDOW_OFFSET`).
  - At most 16 window functions per query (`MAX_WINDOWS`).
  - At most 4 different `OVER` orders per query (`MAX_WINDOW_SORTS`), because each sorts the rows.
  - `PARTITION BY` and `ORDER BY` together name at most 8 columns, the sort's limit.
  - The temporary tables count against the 32 GiB prepared limit (ADR 0009).
- **Types:**
  - The ranks are `Int64`.
  - `LAG`/`LEAD` keep the column's type; their default must convert to it.
  - The aggregates give the same types as in `GROUP BY` (ADR 0023).
- Named windows (`OVER w … WINDOW w AS (…)`), `NTILE`, `FIRST_VALUE`, `PERCENT_RANK` and other window functions are refused with a message that lists the supported ones.

## Consequences
- The usual analytical questions are one query each: the top row per group (`ROW_NUMBER` in a subquery or `WITH`), the change from the previous row, running totals, moving averages, and share of total (`x / SUM(x) OVER ()`).
- A window query reads its rows up to three more times: a sort (when there's a `PARTITION BY` or `ORDER BY`), a totals pass (only for whole-partition or peer aggregates), and the pass that writes the result. It uses temporary disk space rather than memory.
- A moving `SUM` or `AVG` of floating-point values adds and takes away values, so it can differ from a fresh sum in the last digits. Integer and decimal sums are exact.
- The rows a window query shows come from a temporary table, like a join's, so they appear once the window step finishes, with progress and cancellation as for joins.
