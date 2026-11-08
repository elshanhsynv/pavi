---
name: add-query-feature
description: Use when extending PAVI's SQL subset or the filter-box grammar, e.g. a new operator (LIKE, IN, BETWEEN), a second predicate (AND/OR), a new aggregate, multi-column ORDER BY, or a new clause. Walks the parser → plan → execution → reader path, the tests each layer needs, fuzz seeds and the docs to update.
---

# Add a SQL or filter feature

PAVI's query engine is deliberately small (ADR `docs/decisions/0004-custom-query-engine.md`).
Unsupported SQL must fail with a clear parse error; it must never silently run differently.
Read `docs/architecture/data-flows.md` §4.3 (filter) and §4.5 (SQL) first.

## 1. Decide the shape

- Where does it live? Filter text only (`FilterExpr`), SQL only (`SqlAst`), or both?
- Which column types does it accept? Type rules live **only** in `crates/parquet/src/types.rs`
  (`is_comparable`, `is_numeric`, …). Add a predicate there instead of matching types inline.
- What is its memory bound? Anything that buffers rows needs a named budget constant and a
  clear "exceeds budget, add a filter or LIMIT" error (`docs/PERFORMANCE.md`, ADR 0002).
- Can it be cancelled within one 4,096-row batch? Long scans must call `stop()` every batch.

## 2. Change each layer, in order

| Layer | File | What to touch |
|---|---|---|
| Filter grammar | `crates/parquet/src/filter.rs` | `FilterOp`, `FilterExpr::parse`, `validate_type`, `ordered_value`, `evaluate_batch`, and `might_match_statistics` (row-group pruning must stay correct: when unsure, return `true`) |
| SQL parsing | `crates/query/src/sql.rs` | `SqlAst::parse` (reject what you don't support, with a specific message), `SqlPredicate`/`SqlProjection`/`SqlSort`, `to_logical_plan` |
| Logical plan | `crates/query/src/logical.rs` | New node or field, builder method |
| Planner | `crates/query/src/planner.rs` | `Planner::plan` validation; `PhysicalPlan` field + accessor |
| Execution | `crates/query/src/execution.rs` | `schedule_next` strategy precedence (aggregate → sort → filter → scan) |
| Reader | `crates/parquet/src/source.rs` | The `read_*` function; reuse `scan_filtered`, `validate_sort`, `validate_aggregate` |
| Runtime | `crates/runtime/src/scheduler.rs` | Only if a new `ReadOperation` is needed (`read_source`) |
| UI | `crates/app/src/app.rs`, `ui/sql.rs`, `ui/toolbar.rs` | Error text under the input; history entry |

## 3. Tests (see `docs/TESTING.md`)

- Parser: accepted forms, and each rejected form with its exact error message.
- Planner: invalid column, wrong type, combination with LIMIT/ORDER BY/GROUP BY.
- Reader: happy path, nulls, empty file, a match across a row-group boundary, statistics pruning
  does not drop matching row groups, the budget error, and cancellation mid-scan.
- App: one `crates/app/src/app_tests.rs` test driving `PaviApp` (`run_sql` / `apply_filter`).

## 4. Fuzzing

- Add 2–3 example inputs to `FILTER_SEEDS` / `SQL_SEEDS` in `fuzz/src/lib.rs`.
- If the feature needs a column type the fixture lacks, add it to `fixture_batch` there.
- Run locally (WSL on Windows; see `docs/FUZZING.md`):
  `cargo +nightly fuzz run sql_query -- -max_total_time=300` (and `filter_expr`).

## 5. Docs, then done

- `README.md` feature list and `docs/architecture/data-flows.md` §4.3/§4.5.
- `docs/architecture/boundaries.md` type table if type support changed.
- `docs/PERFORMANCE.md` if you added a budget.
- `CHANGELOG.md` under `Unreleased`.
- Done = `cargo fmt`, `cargo clippy … -D warnings`, `cargo test --workspace`, `cargo deny check` pass.
  If execution or reading changed, run the `perf-check` skill and put the table in the PR.
