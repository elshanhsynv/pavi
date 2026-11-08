---
name: perf-check
description: Use before opening a PR that touches reading, the runtime, the query engine, caching or the grid, or when asked whether a change made PAVI faster or slower. Benchmarks main against the current branch with pavi_bench in release mode and produces a before/after table for the PR.
---

# Compare performance against main

Numbers only mean something when both sides run the same release build settings, on the same
files, on the same machine, one after the other. Budgets and targets are in `docs/PERFORMANCE.md`.

## 1. Inputs

- Sample files in `data/` (git-ignored; may be missing). Use, in order of preference:
  `data/flights.parquet` (1M rows), `data/dummy_5m.parquet` (5M rows), `data/dummy_1b.parquet` (1B rows, slow).
  If none exist, say so and stop; don't benchmark tiny test fixtures.
- Close other heavy programs. Never compare debug builds.

## 2. Build both sides

```bash
git worktree add ../pavi-perf-main main
cargo build --release -p pavi_bench --manifest-path ../pavi-perf-main/Cargo.toml
cargo build --release -p pavi_bench
```

Both builds use the root `[profile.release]` settings, so they are comparable.

## 3. Run

For each file, alternate main and branch, 3 runs each (alternating evens out disk cache and
thermal effects):

```bash
../pavi-perf-main/target/release/pavi-bench data/flights.parquet > perf-main-1.csv
target/release/pavi-bench data/flights.parquet > perf-branch-1.csv
```

Output is `metric,value` CSV: file shape (`rows`, `columns`, `row_groups`), timings in ms
(`first_page_all_cold_ms`, `first_page_all_warm_ms`, …) and byte sizes. Optionally pass a second
argument, the number of sequential pages to read.

## 4. Compare

- Take the **median** of the 3 runs per metric and side.
- Report every `*_ms` metric and every byte metric; skip the shape rows.
- Flag a regression when the branch is >10% worse **and** >5 ms worse (tiny timings are noise).
- If a budget or target in `docs/PERFORMANCE.md` is now exceeded, say so explicitly.

Put this in the PR description:

```markdown
### Performance (pavi_bench, release, median of 3, <machine>, <date>)
| file | metric | main | branch | change |
|---|---|---|---|---|
| flights | first_page_all_cold_ms | 41 | 38 | -7% |
```

## 5. Clean up

```bash
git worktree remove ../pavi-perf-main
```

Delete the `perf-*.csv` files; don't commit them.
