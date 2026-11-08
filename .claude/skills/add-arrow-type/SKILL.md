---
name: add-arrow-type
description: Use when PAVI should support a new Arrow/Parquet column type (e.g. decimals in filters, Time32/Time64, Float16, Utf8View in a new place, intervals, nested types) in display, filters, sort, aggregates, profiling, charts, CSV export or statistics. Lists every place type support is decided and the test checklist.
---

# Support a new column type

Type support must be consistent across features. The rules live in **one** place,
`crates/parquet/src/types.rs`, and value → text in **one** place, `crates/parquet/src/value.rs`.
Never add a `match data_type` that duplicates those decisions elsewhere.
Current support is the table in `docs/architecture/boundaries.md` ("Parquet/Arrow features").

## 1. Decide what the type supports

| Feature | Decided by | Notes |
|---|---|---|
| Grid display, clipboard | `value::format_cell_with_limit` | Bounded output (256 chars; nested ≤ 8 deep / 16 items) |
| CSV export, profiling text | `value::scalar_text`, `value::is_scalar_type` | Full-length text; must match the grid |
| Filter | `types::is_comparable`, `filter.rs` `ordered_value`, `evaluate_batch`, `might_match_statistics` | Parse the literal in the column's own units (time zones, scale) |
| Sort | `types::is_comparable`, `source.rs` `validate_sort` | Stable tie-breaker already exists |
| MIN/MAX/GROUP BY | `types::is_comparable`, `source.rs` `scalar_at` + `Scalar` | Result keeps the input type |
| SUM/AVG | `types::is_numeric` | |
| Profiling | `crates/app/src/profile.rs` `ProfileAccumulator` | Histogram only for numbers; ordering by value, not text |
| Charts | `crates/app/src/chart.rs` `ChartAccumulator` | Has its own checks; keep them in line |
| Row-group statistics | `crates/parquet/src/statistics.rs` `metadata_statistics` | Rebuild min/max as the Arrow type, format like the grid |
| Inspector type label | `crates/app/src/ui/inspector.rs` `type_label` | |

Change `types.rs` first; most other places call its predicates.

## 2. Tests (see `docs/TESTING.md`)

Write the fixture with `ArrowWriter` into a `tempfile` directory. For each feature the type now supports:

- happy path, with a value that exercises the type's quirk (time zone, scale, negative, unicode);
- nulls;
- empty file;
- a value range crossing a row-group boundary (set `set_max_row_group_row_count`);
- statistics: filter pruning keeps matching row groups;
- for features it still does *not* support: the clear "unsupported type" error.

Also check the grid text, CSV text and statistics text are identical for the same value.

## 3. Fuzzing

Add a column of the type to `fixture_batch` in `fuzz/src/lib.rs` (both the text targets and the
Parquet seeds use it), regenerate seeds, and run all three targets locally (`docs/FUZZING.md`).

## 4. Docs, then done

- Update the type table in `docs/architecture/boundaries.md` (it is the user-facing contract).
- `CHANGELOG.md` under `Unreleased`.
- Done = `cargo fmt`, `cargo clippy … -D warnings`, `cargo test --workspace`, `cargo deny check` pass.
