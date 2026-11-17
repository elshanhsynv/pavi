# 0013. SQL calculations: on the way out in a plain SELECT, otherwise prepared first

- Status: accepted
- Date: 2026-11-15
- Amends: [0009](0009-joins-and-subqueries.md) (prepared rows also hold calculations)

## Context
SQL in PAVI could only name columns: no `price * qty`, `CAST`, `CASE`, `UPPER(name)` or `YEAR(day)`. The reader filters, sorts and groups by *source columns* (ADR 0004), and every bounded path relies on that: row-group pruning, the external sort (ADR 0005), the group budget. Teaching each read path to evaluate expressions would touch all of them.

## Decision
A calculation is parsed into a small expression tree (`pavi_query::expr`), typed against its input's schema before anything is read, and evaluated one batch at a time with Arrow kernels (`arrow-arith`, `arrow-ord`, `arrow-cast`, `arrow-select`). It never holds more than the batch it works on. Where it is evaluated depends on where it is used:

- **Only in a plain SELECT list** (no aggregate, no `ORDER BY` of a calculated item): it is made **on the way out**. The query reads the columns the calculations need, and the output step computes the result's columns from each batch. Nothing extra is read or written, so the first rows of a billion-row file show as fast as without it.
- **Anywhere else** (in `WHERE`, `GROUP BY`, `ORDER BY`, `HAVING`, inside an aggregate, or any calculation on rows' values in an aggregate query): the query runs on **prepared rows** (ADR 0009), and a compute step (`Prep::Compute`) writes the input's kept columns plus one column per calculation to a temporary table. The rest of the query treats each calculation as a hidden column (`#calc0` …) and filters, groups and sorts it with the existing bounded readers. A test on a calculation applies after the compute step. `WHERE a > b` and `WHERE NOT (…)` are calculated tests that must be true.
- **On an aggregate's results** (`ROUND(AVG(x), 2)`, `SUM(a) / COUNT(*)`, `carrier || ':' || COUNT(*)`), in the SELECT list of an aggregate query: each aggregate inside is computed with the others (once, even if written twice), and the calculation is made by the output step from the aggregate's one result batch, which the group budget bounds. An aggregate query's result columns are listed in SELECT order (`SqlItem`: a GROUP BY column, an aggregate, or such a calculation), so SELECT may list GROUP BY columns in any order or leave them out; `ORDER BY` and `HAVING` name these columns by alias or position.

What a calculation can do:
- Arithmetic: whole numbers stay whole (Int64), anything with a fraction or a decimal becomes a Float64, `/` always gives a fraction, dividing by zero gives NULL, and overflow is an error.
- `||`, comparisons, `AND`/`OR`/`NOT` (with SQL's three-valued logic), `IS [NOT] NULL`.
- `CAST` and `TRY_CAST` to INT/BIGINT/DOUBLE/DECIMAL(p,s)/VARCHAR/BOOLEAN/DATE/TIMESTAMP. A value that doesn't convert is an error, or NULL with `TRY_CAST`.
- `CASE` (searched and simple).
- The functions `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTRING`, `CONCAT`, `ABS`, `ROUND`, `COALESCE`, `YEAR`, `QUARTER`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DAYOFWEEK` and `EXTRACT`.

Untrusted SQL is bounded: a calculation has at most `MAX_EXPR_NODES` (256) parts and a query at most `MAX_CALCULATIONS` (32) different calculations. The same calculation written twice is computed once.

## Consequences
- Calculations in a SELECT list cost only their own evaluation. Calculations used to filter, group or sort read the input once and write it with the calculated columns before the first row shows. That is the same cost as a join, under the same 32 GiB temporary-table limit, with the same progress and cancellation.
- Aggregates can't be used in WHERE (that is HAVING's job) or inside another aggregate; both are refused with a clear message.
- Decimals in arithmetic become floats; `CAST(… AS DECIMAL(p, s))` brings a result back to a decimal.
- `arrow-arith` is a new dependency: the Arrow project's own kernels, the same version as the other arrow crates.
