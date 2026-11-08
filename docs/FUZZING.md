# Fuzzing

Fuzzing feeds PAVI millions of random and mutated inputs to find crashes, hangs and runaway
memory use before users do. It covers the three inputs PAVI does not control (see `SECURITY.md`).

| Target | Input | What it exercises |
|---|---|---|
| `parquet_file` | Raw bytes saved as a `.parquet` file | `ParquetSource::open` (footer, schema, statistics), first/last page reads, top-N sort, grouped aggregate, cell formatting for grid/CSV |
| `filter_expr` | Filter-box text | `FilterExpr::parse`, type checks, evaluation on every fixture column type, filtered scan with statistics pruning |
| `sql_query` | SQL-workspace text | `SqlAst::parse` → logical plan → `Planner::plan` → the matching aggregate/sort/filter/window read, then formatting |

The fixture file the text targets run against has integers, floats, text, booleans, dates,
time-zoned timestamps, decimals, a dictionary, a list and a struct column (`fuzz/src/lib.rs`).

## Running

Needs nightly Rust and cargo-fuzz (`rustup toolchain install nightly` and `cargo +nightly install cargo-fuzz`).

**On Windows, run it in WSL (or Linux).** libFuzzer on MSVC needs the AddressSanitizer runtime
(`clang_rt.asan_*.lib`), even with `-s none`, which only ships with the Visual Studio
"C++ AddressSanitizer" component. Without it the build fails at link time. In WSL, point
`CARGO_TARGET_DIR` at the Linux home directory so it doesn't clash with Windows builds:

```bash
export CARGO_TARGET_DIR=$HOME/pavi-fuzz-target
```

```bash
cargo +nightly run --manifest-path fuzz/Cargo.toml --bin seeds
cargo +nightly fuzz run parquet_file -- -max_total_time=300
cargo +nightly fuzz run filter_expr -- -max_total_time=300
cargo +nightly fuzz run sql_query -- -max_total_time=300
```

The first command writes seed inputs to `fuzz/corpus/<target>/`: small valid Parquet files in every
codec (plain, snappy, zstd, gzip, lz4, brotli), and example filters and queries. The fuzzer mutates
those, which reaches deep code much faster than starting from nothing. The corpus grows as it runs
and is git-ignored.

Useful flags after `--`: `-max_total_time=N` (seconds), `-jobs=4 -workers=4` (parallel), `-rss_limit_mb=2048` (memory cap; default 2048).

## When it finds something

The input is saved to `fuzz/artifacts/<target>/crash-…` (or `oom-…`, `timeout-…`).

A crash means a panic **escaped** PAVI's code; the log shows `panic escaped PAVI: <message> at <file:line>`.
Panics that PAVI contains on purpose do not stop the fuzzer. libfuzzer-sys would normally abort
on every panic, so each target replaces its hook (`pavi_fuzz::allow_contained_panics` and `run`).

```bash
cargo +nightly fuzz run parquet_file fuzz/artifacts/parquet_file/crash-<hash>   # reproduce
cargo +nightly fuzz tmin parquet_file fuzz/artifacts/parquet_file/crash-<hash>  # shrink it
```

Then fix it like any bug, with a regular `#[test]` that fails before the fix. Minimized Parquet
inputs are usually still several KB, so prefer building the bad file in the test (see
`opens_files_whose_embedded_arrow_schema_is_corrupt` in `source.rs`) over checking bytes in.

Panics inside `parquet`/`arrow` are dependency bugs. `parquet_reader` already contains them
(`load_arrow_metadata` at open, `GuardedReader` while decoding), so they surface as errors. If a new
one escapes, extend those guards rather than adding `catch_unwind` at call sites, and report it upstream.

## Findings

| Found | Bug | Where | Upstream | PAVI handling |
|---|---|---|---|---|
| 2026-11-08 | Panics on corrupt embedded Arrow schemas (invalid Int width, unknown type id, …; 10 variants) | `arrow-ipc` 59 `convert.rs` | Fixed in arrow-rs 60 (now returns an error) | PAVI uses 60. `load_arrow_metadata` still reopens without the hint, so the file opens |
| 2026-11-08 | "Decoder for dict should have been set": dictionary-encoded data page with no dictionary page | `parquet` 59–60 `column/reader/decoder.rs` | Reported: [apache/arrow-rs#12129](https://github.com/apache/arrow-rs/issues/12129) | `GuardedReader` returns "malformed Parquet data" |
| 2026-11-09 | "column start and length should not be negative": negative column chunk offset in metadata | `parquet` 59–60 `file/metadata/mod.rs` | Open: [apache/arrow-rs#6255](https://github.com/apache/arrow-rs/issues/6255) (reproducer added 2026-11-09) | `GuardedReader` |
| 2026-11-09 | "offset + len out of bounds": bit-packed definition-level run longer than the page | `arrow-buffer` 59–60 `bit_chunk_iterator.rs` | Fixed upstream ([apache/arrow-rs#11691](https://github.com/apache/arrow-rs/issues/11691)), not yet released | `GuardedReader` |

The 2026-11-09 rows came from replaying the whole corpus against the plain `parquet` reader (without PAVI's guards) on 59.3.2 and 60.1.0.
When a new arrow-rs release fixes the remaining ones, keep the guard: new decoder bugs will appear.

## CI

`.github/workflows/fuzz.yml` runs each target for 10 minutes on Ubuntu (with AddressSanitizer)
nightly, on pushes to `main` that touch the reader, query or fuzz code, and on manual trigger
(with a configurable duration). Crash inputs are uploaded as build artifacts. It is separate from
`ci.yml` so a newly found dependency bug does not fail unrelated pull requests.
