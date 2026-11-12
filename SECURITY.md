# Security

PAVI is a local desktop app with no network access, accounts or server. Its security comes down to
**handling untrusted input without crashing, hanging, running out of memory or damaging files**.

## Reporting a vulnerability

Do not open a public issue. Use GitHub's private vulnerability reporting on this repository
(Security → Report a vulnerability), with a sample file or steps to reproduce if you can.

## Threat model

| Input | Where it comes from | Must hold |
|---|---|---|
| Parquet, CSV, JSON and Arrow files | Any file the user opens; may be corrupt, truncated or crafted | Never panic, never allocate without a bound, never hang. A bad file is an error in the status bar. CSV, JSON and Arrow files are read in batches into a temporary Parquet file capped at 32 GB (ADR 0007); one Arrow record batch is read whole, as the file stores it. |
| Filter text and SQL | Typed or pasted by the user, or restored from the session file | Parsed into a small fixed subset; anything else is a parse error. Nothing is executed as code or passed to a shell. |
| File paths | Path box, dialogs, `argv[1]`, recent files, open tabs, dropped files and folders | Opened read-only. PAVI only writes to the export target the user picked (via a temp file + rename) and its own session file. A folder is searched within fixed limits (16 levels, 100,000 entries, 4,096 files, 256 MB of footers), so a deep, huge or self-linking tree ends with an error rather than exhausting memory or time. |
| Session file | `%LOCALAPPDATA%\PAVI\session-v1.json` | A missing, corrupt or oversized file falls back to defaults; every list in it is capped on load. |
| Dependencies | crates.io | Checked by `cargo deny check` (advisories, licenses, sources). |

## Rules for code

- No `unsafe`, `unwrap`, `expect` or `panic!` outside tests. Enforced by `[workspace.lints]` in `Cargo.toml`.
- Every collection that grows with file content has a named cap and returns an error when exceeded (see the invariants in `docs/ARCHITECTURE.md`).
- Worker reads and opens run inside `catch_unwind` (`guarded` in `crates/runtime/src/scheduler.rs`): a decoder panic on a malformed file becomes a normal read/open error and the worker keeps running. Export and profiling read through the same pool, so they are covered too.
- `parquet_reader` contains known panics in its dependencies: a decoder panic while reading becomes a "malformed Parquet data" error (`GuardedReader`), and a corrupt embedded Arrow schema is ignored at open (`load_arrow_metadata`). This covers the CLI and bench too, not only the app. Known upstream bugs are listed in `docs/FUZZING.md` (Findings).
- Before the `parquet` crate acts on sizes written in a file, PAVI checks them (`crates/parquet/src/footer.rs`). At open, the footer walk rejects any list, set or map that claims more elements than there are bytes left, and any schema element claiming more children than the schema has. Before a column chunk is first read, its dictionary page header must not claim more values than the page can hold, or exceed `MAX_DICTIONARY_PAGE_BYTES`. These stop CPU-exhaustion and multi-gigabyte allocation bugs in the parser; an allocation failure aborts the process, which no panic guard can catch.
- Parquet files, filter text and SQL are fuzzed (`fuzz/`, see `docs/FUZZING.md`), nightly in CI and locally after reader or parser changes.
- Display text is truncated (cells to 256 chars, nested values to 8 levels / 16 items, statistics to 128 chars) so a crafted value cannot freeze rendering.
- Writes are atomic (temp file + rename), and temp files are removed on failure or cancel.
- No new dependency without a reason in the PR. Prefer crates already in the tree.
- No network access. Adding any (update checks, telemetry) needs an ADR in `docs/decisions/`.

## Known gaps

- **Data page sizes.** Data pages are not pre-checked: a crafted page can claim a large uncompressed size, and decompression may allocate it. Dictionary pages and the footer are covered.
- **Mislabelled nested footer fields.** The footer walk reads `FileMetaData`, `SchemaElement` and page headers field by field exactly like the `parquet` crate, but deeper structures (row groups, column metadata) by their wire types. A crafted file that mislabels a field there can still steer the parser into the slow skip. Waiting on the upstream fix.
- **Slow page headers.** The same Thrift skip bug can be reached through data page headers while reading. A crafted file could keep a read busy on a worker thread (cancellation is only checked between batches). Waiting on the upstream fix.

- **File changes during reads.** Metadata is read once at open; if the file is replaced on disk, later reads use stale metadata (reported as read errors, not memory unsafety).
