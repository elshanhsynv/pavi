# Testing

## Commands

```bash
cargo test --workspace --all-targets --locked
```

CI runs the same command on Windows and Ubuntu, plus `cargo fmt --check`, `cargo clippy ... -D warnings` and `cargo deny check`.

## Where tests live

Tests are inline `#[cfg(test)] mod tests` blocks next to the code they test, with one exception: `crates/cli/tests/` holds end-to-end tests that run the built `pavi-cli` program (Cargo gives integration tests its path as `CARGO_BIN_EXE_pavi-cli`). They cover what only the real process shows (exit codes 0, 1 and 2, what reaches stdout and stderr, colour on and off a terminal, a reader that closes the pipe early, real standard input, and files left on disk after `-o`). Put a test there only when it needs the running program; test behaviour inline. That file starts with `#![allow(clippy::unwrap_used)]`, as `allow-unwrap-in-tests` covers only its `#[test]` functions, not its helpers.

| Crate | What to test there |
|---|---|
| `parquet_reader` | Reading, projection, filters and pruning (with each match's source row), opening a file as a named format, sort/aggregate budgets, value formatting, statistics |
| `pavi_runtime` | Queueing, back-pressure, cancellation, shutdown |
| `pavi_query` | SQL subset parsing and rejections, plan validation, execution order |
| `pavi` (app) | `app_tests.rs` drives `PaviApp` without a window: open → poll → act → assert on state. Also unit tests in `state.rs`, `chart.rs`, `export.rs`, `profile.rs`, `session.rs` |
| `cli` | Argument validation and help (completion scripts for every shell and a colour-free man page with its sections, every option described and under a heading, `-h` within 100 columns, examples and exit codes in `--help`, short flags), `run()` against a temporary Parquet file written into a `Vec<u8>` with 2-row batches (every format, `--offset`/`--head`/`--filter`, `--count`, `--schema`, `--stats`, `--sort` with ties, nulls and several keys, column names, nothing printed before an error, broken pipes in every mode), a sort forced to spill with a 2-row `SortBudget`, excluded argument combinations, table layout (`output.rs`); standard input as CSV, JSON and Parquet bytes, `--output` conversions by extension, no file (and no temporary file) left after a failed write, refusing to overwrite the input (`files.rs` and `run_to_file`); `--sql` with aggregates, GROUP BY, a join and a `NOT IN` subquery over a `--table` CSV, `--offset`/`--head` on results, results to `--output`, SQL on standard input, and SQL and table-name errors printing nothing (`query.rs`); `--profile` of every or chosen columns with `--filter`, a capped distinct count, a list column, and the 256-column limit; `--ascii` tables being pure ASCII and aligned; tables narrowed to a width, then with middle columns left out, every line within it; readable floats; null cells marked apart from the text "null"; widths and cuts by grapheme (skin tones, family emoji, flags, combining accents); profile text lengths; the progress line's text and that hidden progress never writes (whether it shows on a terminal is checked by hand); end to end, through the built program (`tests/cli.rs`): exit codes, stderr and its colour, a closed pipe, stdin, `-o` leaving no partial file, help, version, completions and the man page |
| `parquet_reader::profile` | Numbers, text, decimals, booleans and text views; footer null counts; the exact distinct limit and the whole-file cap |
| `parquet_reader::export` | Each format written to a `Vec<u8>`, and tab-separated text; CSV and JSON Lines text for every scalar type and nested values |

## Fixtures

Write Parquet fixtures at test time with `parquet::arrow::ArrowWriter` into a `tempfile` directory. Do not commit `.parquet` files and do not read from `data/`; tests must run on a clean checkout.

## What every change needs

- **Bug fix:** a test that fails before the fix and passes after.
- **New operation or type support:** happy path, unsupported type error, null handling, empty file, and one case across a row-group boundary.
- **New buffer or cache:** a test that hits the cap and checks the error or eviction.
- **Background work:** a test that cancels mid-run and one that checks a stale generation is ignored.
- **UI behavior:** an `app_tests.rs` test on the resulting state, not on pixels.

Changes to the reader, filter parser or SQL should also get a local fuzz run; see `docs/FUZZING.md`.

Measurements that aren't checks are `#[ignore]`d tests (for example `frame_time_while_scrolling` in `app_tests.rs`); run them with `cargo test --release -p pavi -- --ignored --nocapture <name>` and record the numbers in `docs/PERFORMANCE.md`.

`unwrap`, `expect` and `panic!` are fine in tests (`clippy.toml`).
