# External boundaries

> Part of the [architecture docs](../ARCHITECTURE.md). `file:line` references can drift; search for the symbol name if a line doesn't match.

## File system
- One local file per session, by path (`File::open`). No globbing, directories, multi-file datasets, URLs or object storage.
- The file is **re-opened by path for every read** (`source.rs:670-671`); metadata is cached from open time. If the file changes on disk, reads use stale metadata against new bytes.
- Writes: export target (+ temp file next to it) and the session JSON.

## Parquet/Arrow features
| Feature | Status |
|---|---|
| Column projection | Top-level roots only (`ProjectionMask::roots`); nested columns are read whole (tested: `source.rs:1735`) |
| Row-group pruning | For filters, using min/max stats — **disabled entirely for nested schemas** (`source.rs:694-696`) |
| Page index / bloom filters | Not used (`ArrowReaderMetadata::load(&file, Default::default())`) |
| Display formatting (`value.rs`) | Ints, floats and bools via `Display`; strings (Utf8/LargeUtf8/Utf8View) as-is; binary types as hex; dictionary cells show the referenced value. Dates, timestamps, times, durations, intervals, decimals, Float16 and run-end-encoded values use arrow-cast's rules: `2022-01-08`, naive `2023-11-14T22:13:20`, zoned `2023-11-14T23:13:20+01:00` (UTC as `Z`), `123.45`, `PT90S`. `Date64` shows as a date-time. List/LargeList/FixedSizeList/Struct/Map are bounded (≤8 deep, ≤16 items). |
| Comparable types (`types::is_comparable`) | Bool, ints, floats, Utf8/LargeUtf8/Utf8View, Date32/64, Timestamp (any unit, with or without a time zone). |
| Filter types | Comparable types. Bool supports `==`/`!=`; `contains` is text-only. Not: decimals, times, binary, dictionaries, nested. |
| Sort types | Comparable types |
| MIN/MAX/GROUP BY types | Comparable types; results keep the input type (including time zone). SUM/AVG: ints and floats. |
| CSV export types | every non-nested type, as the shared display text (ISO dates, RFC 3339 timestamps, scaled decimals) |
| Profiling types | Comparable types. Numbers get a mean and histogram, text gets lengths, dates/timestamps are ordered by time (not text). |
| Statistics display | Rebuilt as the column's Arrow type and formatted like grid values: unsigned integers, dates, times, timestamps (in their zone) and decimals (INT32/INT64/byte-array storage). INT96 and other physical types show the stored value. |
| Files written by polars (the repo's sample data) | Strings load as `LargeUtf8` (e.g. `dummy_5m`, `dummy_1b`), which every operation accepts like `Utf8`. |
| Large files | Row counts are `u64`. The source grid shows files over 500,000 rows through a moving 500,000-row window with its own scrollbar, so every row (including the last of a billion) is reachable. Filtered results are ≤ 32,768 rows and are never windowed |

## Platform-specific code
- No `cfg(target_os)` anywhere. Implicitly Windows-oriented: session path uses `LOCALAPPDATA` (`session.rs:192-199`).
- Window decorations are disabled on all platforms; the toolbar draws its own drag/maximize/close controls (`ui/toolbar.rs:26-55`, `159-163`).
- Native dialogs: `rfd` (synchronous). Clipboard: egui/winit.

## unsafe / FFI
None in workspace code. Native code exists only transitively (zstd-sys, OpenGL via glow, OS dialogs via rfd).
