# PAVI — agent guide

PAVI is a native Windows-first desktop explorer for Parquet files (Rust, egui/eframe, arrow-rs/parquet 60).
It must stay responsive and memory-bounded on files of any size, up to billions of rows.

## Commands

```bash
cargo fmt --all                                                      # format
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo deny check                                                     # advisories, licenses, bans
cargo run -p pavi -- data/flights.parquet                            # GUI (path optional)
cargo run -p cli -- data/flights.parquet --head 20 --filter "DEP_DELAY > 60"
cargo run --release -p pavi_bench -- data/dummy_5m.parquet           # performance numbers
cargo +nightly fuzz run parquet_file -- -max_total_time=300         # fuzzing (see docs/FUZZING.md)
```

**Done means:** fmt, clippy (warnings are errors), tests and `cargo deny check` all pass. CI runs the same on Windows and Ubuntu with the toolchain pinned in `rust-toolchain.toml`.

## Layout

```text
crates/parquet  (parquet_reader)  all Parquet/Arrow I/O, filter, sort, aggregate, value formatting
crates/runtime  (pavi_runtime)    fixed worker pool, bounded channels, cancellation, generation IDs
crates/query    (pavi_query)      SQL subset → LogicalPlan → PhysicalPlan → incremental execution
crates/app      (pavi)            GUI: app.rs (PaviApp), ui/*.rs (impl PaviApp blocks), export, charts, session
crates/cli      (cli)             synchronous preview tool; uses only parquet_reader
crates/bench    (pavi_bench)      timing and cache metrics
```

Dependencies point down only: app → query → runtime → parquet_reader.

## Rules

- **Never block the UI thread** on file I/O or decoding. Submit to the runtime, keep the receiver, poll with `try_recv()`.
- **Bound every buffer** that grows with the file: a named constant cap plus a clear error. Update `docs/PERFORMANCE.md` when you add or change one.
- **Tag background results with a `GenerationId`** and drop stale ones. Long scans check cancellation every batch.
- **No `unsafe`, `unwrap`, `expect`, `panic!`, `todo!` or `dbg!` outside tests.** The lints enforce this; use `anyhow::Result` with `.context(...)` instead.
- **No async runtime** (tokio is banned in `deny.toml`). No new dependency without saying why in the PR.
- **One source of truth:** value → text only in `parquet_reader::value`; type support only in `parquet_reader::types`; colors only in `ui/theme.rs`; spacing only via `ui/spacing.rs`.
- **Writes are atomic** (temp file + rename).
- **Tests:** inline `#[cfg(test)]` modules; fixtures written with `ArrowWriter` into `tempfile`, never from `data/`. App behavior is tested through `crates/app/src/app_tests.rs` without a window.
- Keep changes small and focused. Don't refactor unrelated code in the same change.

## Read before you change

| Task touches | Read first |
|---|---|
| Anything structural, or you are new here | `docs/ARCHITECTURE.md` (overview + invariants) |
| Open, paging, filter, sort, SQL, export, charts | `docs/architecture/data-flows.md` |
| Threads, cancellation, locks, session file | `docs/architecture/concurrency.md` |
| Supported types, large-file limits, platform | `docs/architecture/boundaries.md` |
| Crate/module details, `PaviApp` state | `docs/architecture/modules.md` |
| UI look and behavior | `docs/DESIGN.md` |
| Budgets, speed, benchmarks | `docs/PERFORMANCE.md` |
| Tests | `docs/TESTING.md` |
| Parsers, file reading, untrusted input | `docs/FUZZING.md` |
| Untrusted input, dependencies | `SECURITY.md` |
| Versions, CHANGELOG, publishing a release | `docs/RELEASING.md` |
| "Why is it like this?" | `docs/decisions/` |

## Skills

Step-by-step recipes in `.claude/skills/`. Use them when the task matches:

- `add-query-feature`: extend the SQL subset or filter grammar.
- `add-arrow-type`: support a new column type across display, filters, sort, aggregates, profiling and export.
- `perf-check`: benchmark `main` against the branch and produce the before/after table for the PR.

## Keeping docs true

If your change alters behavior, a budget, a data flow or a rule described in these docs, update the doc in the same commit.
A decision that reverses an ADR needs a new ADR. Stale docs are worse than none.

## Workflow

- Branch from `main` (`feat/…`, `fix/…`, `chore/…`, `docs/…`); never commit directly to `main`.
- Add a line under `## [Unreleased]` in `CHANGELOG.md` for user-visible changes.
- Commit messages: imperative summary line describing the user-visible effect (e.g. "Resume filtered scans from a cursor").
- For changes to reading, runtime, query or grid code, include before/after `pavi_bench` numbers in the PR (the `perf-check` skill).

## Gotchas

- `data/` and `*.parquet` are git-ignored. Sample files may be missing; `create_dummy_data.py` (needs `polars`) generates the 1-billion-row file.
- `docs/` is git-ignored except for the files allowlisted in `.gitignore`. Add new doc files there.
- `-D warnings` is on in CI: a new Rust release can add lints, which is why the toolchain is pinned.
- `file:line` references in the docs drift; search for the symbol name.
