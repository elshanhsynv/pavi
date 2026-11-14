# Testing

## Commands

```bash
cargo test --workspace --all-targets --locked
```

CI runs the same command on Windows and Ubuntu, plus `cargo fmt --check`, `cargo clippy ... -D warnings` and `cargo deny check`.

## Where tests live

Tests are inline `#[cfg(test)] mod tests` blocks next to the code they test. There are no `tests/` directories.

| Crate | What to test there |
|---|---|
| `parquet_reader` | Reading, projection, filters and pruning, sort/aggregate budgets, value formatting, statistics |
| `pavi_runtime` | Queueing, back-pressure, cancellation, shutdown |
| `pavi_query` | SQL subset parsing and rejections, plan validation, execution order |
| `pavi` (app) | `app_tests.rs` drives `PaviApp` without a window: open → poll → act → assert on state. Also unit tests in `state.rs`, `chart.rs`, `export.rs`, `profile.rs`, `session.rs` |
| `cli` | Argument validation and `--help` text, `run()` against a temporary Parquet file written into a `Vec<u8>` with 2-row batches (every format, `--offset`/`--head`/`--filter`, `--count`, `--schema`, `--stats`, `--sort` with ties, nulls and several keys, column names, nothing printed before an error, broken pipes in every mode), a sort forced to spill with a 2-row `SortBudget`, excluded argument combinations, table layout (`output.rs`); standard input as CSV, JSON and Parquet bytes, `--output` conversions by extension, no file (and no temporary file) left after a failed write, refusing to overwrite the input (`files.rs` and `run_to_file`); `--sql` with aggregates, GROUP BY, a join and a `NOT IN` subquery over a `--table` CSV, `--offset`/`--head` on results, results to `--output`, SQL on standard input, and SQL and table-name errors printing nothing (`query.rs`); `--profile` of every or chosen columns with `--filter`, a capped distinct count, a list column, and the 256-column limit; `--ascii` tables being pure ASCII and aligned; the progress line's text and that hidden progress never writes (whether it shows on a terminal is checked by hand) |
| `parquet_reader::profile` | Numbers, text, decimals, booleans and text views; footer null counts; the exact distinct limit and the whole-file cap |
| `parquet_reader::export` | Each format written to a `Vec<u8>`; CSV and JSON Lines text for every scalar type and nested values |

## Fixtures

Write Parquet fixtures at test time with `parquet::arrow::ArrowWriter` into a `tempfile` directory. Do not commit `.parquet` files and do not read from `data/`; tests must run on a clean checkout.

## What every change needs

- **Bug fix:** a test that fails before the fix and passes after.
- **New operation or type support:** happy path, unsupported type error, null handling, empty file, and one case across a row-group boundary.
- **New buffer or cache:** a test that hits the cap and checks the error or eviction.
- **Background work:** a test that cancels mid-run and one that checks a stale generation is ignored.
- **UI behavior:** an `app_tests.rs` test on the resulting state, not on pixels.

Changes to the reader, filter parser or SQL should also get a local fuzz run; see `docs/FUZZING.md`.

`unwrap`, `expect` and `panic!` are fine in tests (`clippy.toml`).
