# External boundaries

> Part of the [architecture docs](../ARCHITECTURE.md). `file:line` references can drift; search for the symbol name if a line doesn't match.

## File system
- Local files and folders, by path, up to 16 open at once in tabs (`MAX_TABS`). A folder is one table of every `.parquet` file below it (ADR 0006, `parquet_reader::dataset`): searched in path order, skipping names that start with `.` or `_`, at most 16 folder levels (`MAX_FOLDER_DEPTH`), 100,000 directory entries (`MAX_FOLDER_ENTRIES`), 4,096 files (`MAX_DATASET_FILES`) and 256 MB of footer metadata (`MAX_DATASET_METADATA_BYTES`). All files must have the same column names and types; `key=value` folders become partition columns (`Int64` if every value is an integer, else `Utf8`). Symbolic links are followed, within the depth limit. No globbing, URLs or object storage.
- Each file is **re-opened by path for every read** (a read that crosses files opens each) (`source.rs:670-671`); metadata is cached from open time. If the file changes on disk, reads use stale metadata against new bytes.
- Writes: export target (+ temp file next to it) and the session JSON.

## Network (ADR 0008)
- `https://`, `http://`, `s3://bucket/key`, `gs://bucket/key` and `az://account/container/blob` URLs, one object each (no prefix listing). Requests are blocking `ureq` GETs on the worker threads; TLS uses `rustls` with the operating system's root certificates.
- A Parquet object on a server that answers `Range` requests is read in place: the footer at open, then the column chunks each read needs, through `remote::RemoteReader` (a `ChunkReader`). Ranges up to 128 MB are cached per object, 256 MB together (`REMOTE_CACHE_BYTES`). Otherwise, and for CSV, JSON and Arrow objects, the object is downloaded to `<temp>/pavi-download-<pid>-<n>/` (≤ 32 GiB, `MAX_DOWNLOAD_BYTES`) and opened as a local file.
- Credentials only from the environment: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` or `~/.aws/credentials` (`AWS_PROFILE`), signed with SigV4; `AWS_REGION`, `AWS_ENDPOINT_URL` for S3-compatible stores; `GOOGLE_OAUTH_ACCESS_TOKEN`; `AZURE_STORAGE_SAS_TOKEN`. Pre-signed URLs work as they are. Without credentials requests are anonymous.
- Timeouts: 15 s to connect, 30 s for the response headers, 300 s for a body; 3 attempts on connection errors and 5xx answers. A request in flight can't be interrupted.
- A URL is shown without its query string, and a URL with a query string is never saved in the session.

## Other file formats (ADR 0007)

| Format | Extensions | Read as | Limits |
|---|---|---|---|
| CSV | `.csv`, `.tsv` | Header row required. Delimiter: tab for `.tsv`, else the most frequent of `,` `;` tab `|` in the first line. Types inferred from the first 10,000 records (`MAX_INFER_ROWS`): integers, floats, booleans, dates, timestamps, else text; empty fields are null | A later value that does not fit its column's type stops the open with an error. UTF-8 only |
| JSON | `.json`, `.jsonl`, `.ndjson` | One object per line, or one array of objects. Types inferred like CSV; nested objects become structs, arrays become lists | Same as CSV |
| Arrow IPC | `.arrow`, `.feather`, `.ipc`, `.arrows` | File format (Feather v2, `ARROW1` magic) or stream format; LZ4 and Zstd buffers | Feather v1 is refused. Each record batch is read whole. Before reading, every block and message must lie inside the file; metadata messages are capped at 64 MB and a compressed buffer at 1 GB uncompressed |

All are read into a temporary Parquet file of at most 32 GiB (`MAX_CONVERTED_BYTES`) before the first row shows. Other extensions are opened as Parquet. ORC is not supported (see the ADR).

Export writes CSV, Parquet, JSON Lines (`.jsonl`) and Arrow IPC file format (`.arrow`).

## Parquet/Arrow features
| Feature | Status |
|---|---|
| Column projection | Top-level roots only (`ProjectionMask::roots`); nested columns are read whole (tested: `source.rs:1735`) |
| Row-group pruning | For filters, using min/max stats. Each top-level column is mapped to its Parquet leaf (`statistics::leaf_columns`), so plain columns of a file with nested columns are pruned too; a test on a nested column never rules a group out. Decimals stored as byte arrays are not pruned. |
| Page index / bloom filters | Not used (`ArrowReaderMetadata::load(&file, Default::default())`) |
| Display formatting (`value.rs`) | Ints, floats and bools via `Display`; strings (Utf8/LargeUtf8/Utf8View) as-is; binary types as hex; dictionary cells show the referenced value. Dates, timestamps, times, durations, intervals, decimals, Float16 and run-end-encoded values use arrow-cast's rules: `2022-01-08`, naive `2023-11-14T22:13:20`, zoned `2023-11-14T23:13:20+01:00` (UTC as `Z`), `123.45`, `PT90S`. `Date64` shows as a date-time. List/LargeList/FixedSizeList/Struct/Map are bounded (≤8 deep, ≤16 items). |
| Comparable types (`types::is_comparable`) | Bool, ints, floats, Decimal32/64/128 (any precision and scale), Utf8/LargeUtf8/Utf8View, Date32/64, Timestamp (any unit, with or without a time zone), Time32/Time64 (any unit). Not: Decimal256, binary, dictionaries, durations, intervals, nested. |
| Filter types | Comparable types. Bool supports `==`/`!=`; `contains` and `LIKE` are text-only; `IS [NOT] NULL` works on every type. Decimal literals are read in the column's scale (exact when they have no more decimals than the column; otherwise they fall between two values, so `==` matches nothing). Times are `HH:MM:SS[.fff…]`. |
| Sort types | Comparable types; up to 8 keys. Any number of rows: past 100,000 rows / 128 MB the sort spills to temporary files (ADR 0005), up to 32 GiB. |
| MIN/MAX/GROUP BY types | Comparable types; results keep the input type (including time zone, decimal precision and scale). SUM/AVG (`types::is_summable`): ints, floats and decimals. A decimal SUM is `Decimal128(38, scale)` and fails past 38 digits; AVG is a float in the column's units. |
| CSV export types | every non-nested type, as the shared display text (ISO dates, RFC 3339 timestamps, scaled decimals) |
| Profiling types | Comparable types. Numbers and decimals get a mean and histogram, text gets lengths, dates, timestamps and times are ordered by time (not text). |
| Statistics display | Rebuilt as the column's Arrow type and formatted like grid values: unsigned integers, dates, times, timestamps (in their zone) and decimals (INT32/INT64/byte-array storage). INT96 and other physical types show the stored value. In files with nested columns the plain columns keep their statistics; nested columns have none. |
| Nested values | Grid: one bounded line. Inspector: a tree of list items, struct fields and map entries (`value::value_tree`: ≤8 levels, ≤100 children per node, ≤2,000 nodes). Charts: decimals as numbers in their units, times in their stored unit. |
| Files written by polars (the repo's sample data) | Strings load as `LargeUtf8` (e.g. `dummy_5m`, `dummy_1b`), which every operation accepts like `Utf8`. |
| Large files | Row counts are `u64`. The source grid shows files over 500,000 rows through a moving 500,000-row window with its own scrollbar, so every row (including the last of a billion) is reachable. Filtered results are ≤ 32,768 rows and are never windowed |

## Platform-specific code
- No `cfg(target_os)` anywhere. The few platform differences are decided at run time from `std::env::consts::OS` by small functions that take the OS name, so every platform's branch is tested on any machine:
  - Session folder (`session::session_dir`): `%LOCALAPPDATA%\PAVI` on Windows, `~/Library/Application Support/PAVI` on macOS, `$XDG_STATE_HOME/pavi` or `~/.local/state/pavi` on Linux and the BSDs; the temp folder when none of these can be found.
  - Text font (`theme::font_files`): Segoe UI on Windows, Arial on macOS, Noto Sans or DejaVu Sans on Linux; egui's built-in font otherwise.
  - Window frame (`app::native_window_frame`): decorations are disabled except on macOS. Elsewhere the top bar is the drag area and draws its own minimize/maximize/close controls (`show_top_bar`, `window_controls` in `ui/toolbar.rs`), and `window_resize_handles` puts drag strips on the edges and corners that send `ViewportCommand::BeginResize`. winit doesn't support that command on macOS, so there the system title bar and border stay.
- Builds and tests run on Windows and Ubuntu in CI. macOS is not built in CI; its branches above are unit-tested by name only.
- Native dialogs: `rfd` (synchronous). Clipboard: egui/winit.

## unsafe / FFI
None in workspace code. Native code exists only transitively (zstd-sys, OpenGL via glow, OS dialogs via rfd).
