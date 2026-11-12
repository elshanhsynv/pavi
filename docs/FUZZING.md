# Fuzzing

Fuzzing feeds PAVI millions of random and mutated inputs to find crashes, hangs and runaway
memory use before users do. It covers the inputs PAVI does not control (see `SECURITY.md`).

| Target | Input | What it exercises |
|---|---|---|
| `parquet_file` | Raw bytes saved as a `.parquet` file | `ParquetSource::open` (footer, schema, statistics), first/last page reads, top-N sort, grouped aggregate, cell formatting for grid/CSV |
| `other_formats` | First byte picks `.csv`, `.tsv`, `.jsonl` or `.arrow`; the rest is the file | Delimiter sniffing, CSV/JSON type inference, the JSON-array reader, Arrow file/stream detection, conversion to the temporary Parquet file (ADR 0007), first page read, cell formatting |
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
(`load_arrow_metadata` at open, `GuardedReader` while decoding, and `convert::convert`, which
guards building the CSV, JSON or Arrow reader and reads its batches through `GuardedReader`), so
they surface as errors. If a new
one escapes, extend those guards rather than adding `catch_unwind` at call sites, and report it upstream.

## Findings

| Found | Bug | Where | Upstream | PAVI handling |
|---|---|---|---|---|
| 2026-11-08 | Panics on corrupt embedded Arrow schemas (invalid Int width, unknown type id, …; 10 variants) | `arrow-ipc` 59 `convert.rs` | Fixed in arrow-rs 60 (now returns an error) | PAVI uses 60. `load_arrow_metadata` still reopens without the hint, so the file opens |
| 2026-11-08 | "Decoder for dict should have been set": dictionary-encoded data page with no dictionary page | `parquet` 59–60 `column/reader/decoder.rs` | Reported: [apache/arrow-rs#12129](https://github.com/apache/arrow-rs/issues/12129) | `GuardedReader` returns "malformed Parquet data" |
| 2026-11-09 | "column start and length should not be negative": negative column chunk offset in metadata | `parquet` 59–60 `file/metadata/mod.rs` | Open: [apache/arrow-rs#6255](https://github.com/apache/arrow-rs/issues/6255) (reproducer added 2026-11-09) | `GuardedReader` |
| 2026-11-09 | "offset + len out of bounds": bit-packed definition-level run longer than the page | `arrow-buffer` 59–60 `bit_chunk_iterator.rs` | Fixed upstream ([apache/arrow-rs#11691](https://github.com/apache/arrow-rs/issues/11691)), not yet released | `GuardedReader` |
| 2026-11-09 | CPU hang: skipping an unknown Thrift field that is a `list<bool>` loops over the *claimed* size without consuming input (~5 s per 9-byte field; a valid-looking 10 KB file could take over an hour) | `parquet` 60 `parquet_thrift.rs` `skip_till_depth` | Not reported yet | `footer::check_footer` rejects footers whose collections claim more elements than bytes left, reading known fields by declared type like the parser. Mislabelled fields deep in row-group metadata, and page headers, are not covered |
| 2026-11-09 | Out of memory: a dictionary page's `num_values` sizes the dictionary before decoding (20-byte page claiming 1.5 billion values → 5.9 GB; failed allocations abort the process) | `parquet` 60 `encodings/decoding.rs` `DictDecoder::set_dict` | Not reported yet | `ParquetSource::check_dictionary_pages` rejects dictionary pages claiming more values than they can hold, or over `MAX_DICTIONARY_PAGE_BYTES` (once per column chunk) |
| 2026-11-12 | Out of memory: Arrow file blocks and messages are read into buffers sized from their declared lengths (a footer claiming a 176 TB block), and LZ4/Zstd buffers are decompressed into a `Vec::with_capacity` of the size the buffer claims, which aborts the process when it can't be allocated | `arrow-ipc` 60 `reader.rs` `read_block`, `compression.rs` `decompress_lz4` | Not reported yet | `convert::check_arrow_layout` checks every footer block and message against the file size, caps metadata messages at `MAX_ARROW_MESSAGE_BYTES` (64 MB) and claimed decompressed buffer sizes at `MAX_ARROW_BUFFER_BYTES` (1 GB) |
| 2026-11-12 | "the offset of the new Buffer cannot exceed the existing length": an Arrow IPC record batch whose buffer offset points past the message body (found within 5 minutes of the first `other_formats` run) | `arrow-buffer` 60 `buffer/immutable.rs`, called by the `arrow-ipc` 60 reader | Not reported yet | `convert::convert` returns "malformed Arrow data" (`GuardedReader`, and `catch_unwind` around building the reader) |
| 2026-11-09 | Out of memory at open: a schema element's `num_children` sizes `Vec::with_capacity` before the children are read (3.3 GB) | `parquet` 60 `schema/types.rs` `schema_from_array_helper` | Not reported yet | `footer::check_footer` rejects child counts larger than the schema |

The 2026-11-09 panic rows came from replaying the whole corpus against the plain `parquet` reader (without PAVI's guards) on 59.3.2 and 60.1.0.
When a new arrow-rs release fixes the remaining ones, keep the guard: new decoder bugs will appear.

## CI

`.github/workflows/fuzz.yml` runs each target for 10 minutes on Ubuntu (with AddressSanitizer)
nightly, on pushes to `main` that touch the reader, query or fuzz code, and on manual trigger
(with a configurable duration). Crash inputs are uploaded as build artifacts. It is separate from
`ci.yml` so a newly found dependency bug does not fail unrelated pull requests.
