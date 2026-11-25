# 0023. SQL gains spread and quantile aggregates, date functions, UNION and WITH

- Status: accepted; window functions are added by [0024](0024-window-functions.md)
- Date: 2026-11-25
- Amends: [0009](0009-joins-and-subqueries.md) (`UNION` and `WITH` were refused), [0013](0013-sql-calculations.md) (the function list)

## Context
PAVI's SQL could count, sum, average and find extremes, but couldn't describe a distribution. There was no median, percentile or standard deviation. Dates could only be split into parts (`YEAR`, `MONTH`), not truncated to a period, moved by an interval or compared to today. Two queries' rows couldn't be stacked (`UNION`), and a subquery used twice had to be written twice (no `WITH`). These are the first things people reach for when exploring data. Window functions (`ROW_NUMBER`, `LAG`, running totals) are a separate decision, because they need a new kind of execution.

## Decision
- **Spread:** `STDDEV`/`STDDEV_SAMP`, `STDDEV_POP`, `VARIANCE`/`VAR_SAMP`, `VAR_POP` accumulate a count, mean and sum of squared differences per group (Welford's method), in constant memory. The sample forms are null for fewer than two values.
- **Quantiles:**
  - `MEDIAN(x)`, `QUANTILE_CONT(x, p)` and `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x [DESC])` keep their group's values.
  - Each sorts them when the group finishes and interpolates linearly between the two values either side of `p × (n − 1)`.
  - They take numbers and decimals (in the column's units) and give a `Float64`.
  - The values kept don't count against the group budget (8 MB by default). They have their own: `MAX_QUANTILE_VALUES`, 16,777,216 values (128 MB) per query. Past it, the query stops with a message to filter first.
- **Text:** `REPLACE(text, from, to)`.
- **Dates** (`pavi_query::dates`, using `chrono`, already in the tree through `arrow-array`):
  - **`DATE_TRUNC('unit', x)`** and **`DATE_DIFF('unit', start, end)`**: the unit is year, quarter, month, week (from Monday), day, hour, minute or second. `DATE_DIFF` counts unit boundaries crossed.
  - **`x ± INTERVAL '7 days'`** / `INTERVAL 7 DAY`: months move first (clamped to the month's last day), then days, then time. A date stays a date unless the interval has hours, minutes or seconds.
  - **Time zones:** these work on wall-clock time in the column's time zone, so a day starts at local midnight.
  - **Typed literals:** `DATE '…'` and `TIMESTAMP '…'` are accepted.
- **The current moment:** `NOW()`/`CURRENT_TIMESTAMP` and `CURRENT_DATE` are fixed when the query is parsed, so every row and every batch sees the same moment. `NOW()` is a timestamp *without* a zone holding UTC's wall-clock time, so it compares with the timestamps most files hold. `CURRENT_DATE` is UTC's date.
- **`UNION [ALL]`:**
  - The parts are planned as subqueries. Their column counts must match. Each column gets the type that holds every part's values (`types::common_type`, the rule folders use, ADR 0019) and the first part's name.
  - The rows are written, part after part, to one temporary table (`Prep::Union`).
  - `UNION` without `ALL` then groups by every column, as `SELECT DISTINCT` does, so it has the same bounds: at most 8 columns (`MAX_GROUP_COLUMNS`) and the group budget.
  - `ORDER BY` and `LIMIT` apply to the whole union, by column name or position.
  - Mixing `UNION` and `UNION ALL` without brackets is refused, as are `INTERSECT` and `EXCEPT`. At most 16 parts.
- **`WITH name AS (…)`:** each name is a subquery, put in place of every table that uses it, in `FROM`, `JOIN` and `IN (SELECT …)`. Later names can use earlier ones. A name used twice is computed twice. `WITH RECURSIVE` and `WITH name(columns)` are refused. At most 16 names.
- **Window functions** (`… OVER (…)`) are refused with a message that says they aren't supported yet.

## Consequences
- The first questions about a column's distribution (median, spread, p90) are one query. Quantiles need memory for the values they summarise; the budget makes that explicit.
- `UNION` and `WITH` reuse the prepared-rows path (ADR 0009): they read their inputs once into temporary tables before the first row shows, with the same progress, cancellation and 32 GiB bound.
- A calculation can now read no column (`NOW()`, `DATE '…' + INTERVAL …`). Preparing such a calculation used to read every column. An empty projection means "all" to the reader, so it now reads one column only for the row count.
- `chrono` becomes a direct dependency of `pavi_query`, with no default features. It was already compiled as part of `arrow-array`.
