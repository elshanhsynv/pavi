# Crates, modules and layers

> Part of the [architecture docs](../ARCHITECTURE.md). `file:line` references can drift; search for the symbol name if a line doesn't match.

## 1. Overview

### What the app does

PAVI opens local Parquet files, or a folder of them as one table (ADR 0006), each in its own tab. It reads only footer metadata up front (`ParquetSource::open`, `crates/parquet/src/source.rs:125-159`), and then decodes rows lazily in fixed 4,096-row pages projected to the visible columns. On top of that it layers a deliberately small query engine (one filter, one sort key, one group-by column, limit) executed incrementally on a background thread pool, with every in-memory structure capped by an explicit budget. The GUI adds an Inspector, charts, column profiling, query history, session restore and streaming export. The CLI is a thin, synchronous preview tool for the same reader.

### Tech stack

| Area | What's used | Where |
|---|---|---|
| Language | Rust **edition 2024** in every crate. The toolchain is pinned to **1.99.1** with rustfmt and clippy in `rust-toolchain.toml`, used locally and by CI. No `rust-version`/MSRV is declared; the code uses let-chains, which need Rust ≥ 1.88. | `rust-toolchain.toml`, `crates/*/Cargo.toml` |
| GUI | `eframe 0.31.1` with default features → **glow** (OpenGL) renderer, winit, accesskit. **No `persistence` feature.** `egui_extras 0.31` (`TableBuilder`) for the grid. No `egui_plot` — charts are hand-painted. | `Cargo.toml`, `crates/app/Cargo.toml` |
| Dialogs | `rfd 0.15` synchronous `FileDialog` (open/save) | `app.rs:444`, `app.rs:855`, `ui/toolbar.rs:115` |
| Clipboard | egui's `ctx.copy_text` (no extra crate) | `app.rs:1745`, `ui/inspector.rs:66,127` |
| Parquet/Arrow | `parquet 60` (feature `arrow`), `arrow-array/-schema/-select/-ord 60`, `arrow-cast 60` (value display rules), `arrow-csv`/`arrow-json`/`arrow-ipc 60` (opening CSV, JSON and Arrow files, Arrow export; ADR 0007), `arrow-array`'s `chrono-tz` feature (named time zones). **No** polars/datafusion/duckdb. | workspace `Cargo.toml` |
| SQL | `sqlparser 0.62` (GenericDialect) used only for parsing; execution is custom | `crates/query/src/sql.rs` |
| Network | `ureq 3` (blocking HTTP), `rustls` with `ring` and `rustls-native-certs` (system root certificates); `ring` also signs S3 requests (SigV4). ADR 0008 | `crates/parquet/src/remote.rs` |
| Threading | `std::thread` + `std::sync::mpsc::sync_channel` + `Arc<AtomicBool>` cancellation. No tokio, no rayon. | `crates/runtime/` |
| CLI | `clap 4` derive | `crates/cli/src/args.rs` |
| Errors | `anyhow` everywhere; small hand-written error enums in the runtime (`RuntimeConfigError`, `SubmitError`). No `thiserror`. | |
| Serialization | `serde` + `serde_json` for the session file only | `crates/app/src/session.rs` |
| Time | `chrono` (std only) for formatting file timestamps in the Inspector | `ui/inspector.rs:316` |
| Logging | **None** (no `log`/`tracing`; errors go to the status bar) | |
| Tests | built-in `#[test]` + `tempfile` dev-dep; fixtures are written with `ArrowWriter` at test time | |


## 2. Crate & module structure

### Crates and dependency direction

| Crate (package → target) | Kind | Responsibility | Depends on |
|---|---|---|---|
| `parquet_reader` → lib | library | Open file, metadata/statistics model, page/window reads with projection, filter evaluation & row-group pruning, in-memory sort, streaming aggregates, page cache, cell → string formatting | arrow-*, parquet, anyhow |
| `pavi_runtime` → lib | library | Fixed worker pool; typed tasks (`OpenTask`, `PageTask`) with one bounded response channel each; cooperative cancellation; generation IDs | parquet_reader |
| `pavi_query` → lib | library | SQL subset parser, `LogicalPlan` builder, `Planner` (flattens to `PhysicalPlan`), `QueryExecution` (incremental, blocking or polling) | parquet_reader, pavi_runtime, sqlparser |
| `pavi` → bin `pavi` | GUI | eframe app, all UI, export, profiling, charts, session persistence | all three libs + eframe, egui_extras, rfd, serde(_json), chrono, parquet (writer) |
| `cli` → bin `cli` | CLI | metadata + preview printer, row streaming, row count | **parquet_reader only** + clap |
| `pavi_bench` → bin `pavi-bench` | tool | prints CSV-like timing/cache metrics for open, page reads, a scan query | all three libs |

Direction is strictly downward: `app → query → runtime → parquet_reader`. No cycles. No library depends on the GUI.

### Module trees

```text
crates/parquet/src/            (parquet_reader)
├── lib.rs          re-exports the public surface
├── source.rs       ParquetSource (1,924 lines): open, read_page/window/filtered/sorted/aggregated,
│                   GroupCollector/Accumulator/Scalar (aggregation)
├── dataset.rs      folder datasets (ADR 0006): find the files, `key=value` partition columns, their values and statistics
├── convert.rs      FileFormat; CSV, JSON and Arrow files read into a temporary Parquet file (ADR 0007)
├── join.rs         hash joins (inner, left, semi, anti) into a temporary table; TempTableWriter (ADR 0009)
├── remote.rs       URLs (ADR 0008): resolving s3/gs/az URLs, SigV4, RemoteObject (range requests, range cache, download), RemoteReader (ChunkReader)
├── statistics.rs   row-group statistics → display text, typed by the column's Arrow type
├── metadata.rs     DatasetMetadata, RowGroupInfo, ColumnInfo, ColumnStatistics; row→row-group math
├── page.rs         PAGE_ROWS = 4096, PageKey, RowWindow, DataPage
├── cache.rs        PageCache (LRU in a VecDeque), PageCacheLimits (8 pages / 128 MB), stats
├── projection.rs   Projection (ordered, de-duplicated top-level column indexes)
├── filter.rs       FilterExpr (AND/OR tree), Comparison, FilterOp: text parser, schema validation, batch evaluation, stats pruning, Display
├── sort.rs         SortSpec, SortDirection, NullOrder, SortBudget (100k rows / 128 MB), MAX_SORT_KEYS
├── spill.rs        external sort: SortKeys, RunWriter (sorted runs in temp Parquet files), SortMerge (ADR 0005)
├── types.rs        which Arrow types each operation accepts (is_comparable, is_numeric, is_string, …)
├── aggregate.rs    AggregateFunction/Expr/Spec, GroupBudget (10k groups / 8 MB)
└── value.rs        shared value → text: format_cell* (bounded display), scalar_text (full, for CSV/profiling)

crates/runtime/src/            (pavi_runtime)
├── scheduler.rs    Runtime, RuntimeHandle, RuntimeConfig, ReadOperation, worker_loop, execute
├── task.rs         TaskId, GenerationId, PageTask, OpenTask (receiver + cancel token; IDs arrive with the response)
├── response.rs     PageOutcome/PageResponse, OpenOutcome/OpenResponse
└── cancellation.rs CancellationToken(Arc<AtomicBool>), ScanProgress(Arc<AtomicU64>): source rows a scan has passed

crates/query/src/              (pavi_query)
├── sql.rs          SqlAst::parse, Tables, to_logical_plan(_with): direct plans, and prepared plans for joins and subqueries
├── prep.rs         Prep tree (tables, subqueries, filters, joins) and PrepTask, the thread that runs it (ADR 0009)
├── logical.rs      LogicalPlan tree (Scan/Prepared/Projection/Filter/Limit/Sort/Aggregate), replace_sort, bind
├── planner.rs      Planner::validate (schema only, also for prepared plans) and Planner::plan → PhysicalPlan
└── execution.rs    QueryEngine, QueryExecution (a preparing stage, then Running: next_batch / poll_next_batch), QueryPoll

crates/app/src/                (pavi)
├── main.rs         NativeOptions + eframe::run_native
├── app.rs          PaviApp + Dataset: actions (open, filter, SQL, sort, export, chart, profile), polling, grid window
├── pages.rs        SourcePages: source-grid page reads in flight, 8-page cache, off-screen cancellation
├── results.rs      FilteredGrid, QueryKind: the bounded filter/SQL/sort result window
├── app_tests.rs    57 tests driving PaviApp without a window
├── tabs.rs         TabState (one tab's fields, parked), Tabs, switch/new/close tab, open_path (mounted inside app.rs)
├── state.rs        GridState, ColumnLayout (order, hidden, widths, pinned count), SelectionRange, LoadState, Workspace, ToolbarCommand
├── export.rs       ExportState, ExportTask (own thread), progress, temp-file + rename; the writers are parquet_reader::export::BatchWriter
├── find.rs         find text in the loaded rows: bounded steps that resume where they stopped
├── profile.rs      ProfileState, ProfileTask and FileProfileTask (own threads), ProfileAccumulator
├── chart.rs        ChartState, ChartConfig, ChartAccumulator, ChartModel (bounded sampling/binning, series), image saving
├── compare.rs      CompareState, SchemaDiff, key_query (SQL for rows only in one tab)
├── inspector.rs    CellDetails, column_summary, nested_schema_preview
├── session.rs      SessionState (serde: recent files, history, saved queries, layouts), SessionStore (JSON in the platform's folder, `session_dir`), ThemeChoice
├── sql_editor.rs   SQL tokens for highlighting, word-at-cursor completions, column quoting
└── ui/             (mounted via #[path = "ui/mod.rs"] inside app.rs; every file is `impl PaviApp`)
    ├── toolbar.rs     top bar: file menu (path box, open file or folder, new tab, recent), Grid/SQL switch, filter box, Sort/Columns/Export menus, window controls, window resize strips
    ├── tabs.rs        tab strip under the top bar (from two tabs on)
    ├── grid.rs        virtualized source grid + filtered/SQL/sort grid, keyboard, header click → select column, header and cell menus, pinned-column overlay, find bar
    ├── sql.rs         SQL workspace: highlighted editor with completions, History and Saved menus
    ├── inspector.rs   right panel: cell, whole row, column and profile, file info, schema, row-group stats
    ├── charts.rs / profile.rs / file_profile.rs / compare.rs
    ├── status.rs      status bar: message, dataset facts, jump-to-row
    └── theme.rs, spacing.rs, widgets.rs (colors, fonts and sizes, spacing scale, painted icons and shared controls)
```

### Sharing between GUI and CLI

- **Shared:** `ParquetSource::open`, `read_window`, `read_filtered_window`, `read_filtered_from`, `FilterExpr::parse`, `Projection`, the `value` module (`format_cell*` for display, `scalar_text` for CSV and profiling), and the `export` module (`ExportFormat`, `BatchWriter`: CSV, JSON Lines, Parquet and Arrow IPC written batch by batch to any `Write`).
- **GUI-only:** the runtime, the query engine/SQL, sort, aggregates, page cache use (`read_page`), the export task (thread, progress, temp file + rename), profiling, charts.
- **Duplicated logic:** CLI has its own table printer and truncation (`crates/cli/src/output.rs`). Value → text conversion lives in one place, `value.rs`: display, CSV export, profiling and row-group statistics all use it. Aggregation keeps its own typed `Scalar` values (`source.rs::scalar_at`), which are turned back into Arrow arrays and displayed through `value.rs`. Type support is defined once in `types.rs` and used by sort, aggregates, filters and profiling; CSV export uses `value::is_scalar_type`. Charts keep their own checks.

### Cargo features

None. No crate defines `[features]`, there are no optional dependencies, no `build.rs`, no `[profile.*]` overrides, no `.cargo/config.toml`. `clippy.toml` only allows `unwrap`/`expect`/`panic` inside tests; the lint policy itself is `[workspace.lints]` in the root `Cargo.toml`. `--all-features` is therefore identical to a default build. eframe's own defaults are used (glow, x11, wayland, accesskit; `persistence` **off**).


## 3. Layered architecture

**Pattern actually used:** layered library crates underneath, and a **central `PaviApp` with direct mutation** on top, which delegates page loading, the result window, charts, profiling and export to their own state types. UI code calls `PaviApp` methods (`begin_open`, `apply_filter`, `run_sql`, `toggle_sort`, `start_export`, …) inline from inside rendering closures. The only "command"-like enum is `ToolbarCommand`, used solely to compute button enablement (`state.rs:13-40`), not to dispatch actions. Asynchronous work follows a consistent *submit task → keep receiver in a field → poll every frame → check generation* pattern.

### Entry / shell

| Type / fn | Responsibility | Ref |
|---|---|---|
| `main()` (GUI) | Takes optional path from `argv[1]`; window 1448×900 (min 900×600), **`with_decorations(false)`** (custom title bar), centered; `run_native("PAVI", …)` | `crates/app/src/main.rs:12-27` |
| `PaviApp::new` / `new_with_store` | Starts `Runtime` (worker count = CPU count, queue 32), loads session, reopens `argv[1]` or last file | `app.rs:335-396` |
| `eframe::App for PaviApp` | `update()` = poll everything then draw; `save()` = `persist_session()` but **never called** (eframe `persistence` feature off) | `app.rs:1775-1831` |
| `main()` / `run()` (CLI) | `Args::parse` → open → check columns and filter → count, stream rows (`stream_rows`) or print summary and preview | `crates/cli/src/main.rs` |

### UI layer

| Piece | Responsibility | Ref |
|---|---|---|
| `show_top_bar` | File menu (path box + Enter → open a file or folder, Open file, Open folder, New tab, Reload, Recent), Grid/SQL switch, filter box, Sort menu, Columns menu (types, pinned header, column layout), Export menu, Reload, Charts, Inspector, window buttons | `ui/toolbar.rs` |
| `show_tab_strip` | One tab per open file or folder; click shows, × or middle click closes, + opens an empty tab. Drawn only with two or more tabs | `ui/tabs.rs` |
| `show_grid` / `show_grid_content` | Source grid via `TableBuilder::body.rows()` (virtualized); requests pages for rendered rows | `ui/grid.rs:11-196` |
| `show_filtered_grid` | Same, over `FilteredGrid.batches`; asks for more when the last row is rendered | `ui/grid.rs:198-386` |
| `render_headers` | Painted header with name/type. A click selects the column; a right-click opens a menu (sort, profile, fit, hide, copy name). The choice is stored as a `HeaderAction` and run by `apply_header_action` after the table is drawn | `ui/grid.rs`, `app.rs` |
| `show_inspector_panel` | File props, schema, nested preview, per-row-group stats (virtualized `show_rows`), selected cell, profile | `ui/inspector.rs` |
| `show_sql_workspace`, `show_chart_window`, `show_file_profile_window`, `show_compare_window`, `status::show` | SQL editor (highlighting, completions) with history and saved queries, charts, the whole-file profile, comparing two tabs, status bar with dataset facts and jump-to-row (returns `true` when Enter was pressed in the row box) | `ui/sql.rs`, `ui/charts.rs`, `ui/file_profile.rs`, `ui/compare.rs`, `ui/status.rs` |
| `theme`, `spacing`, `widgets` | Dark and light `Palette` read through colour functions (ADR 0010), system fonts per platform and widget sizes, spacing scale, painted icons and shared controls | `ui/theme.rs`, `ui/spacing.rs`, `ui/widgets.rs` |

### Application state

| Type | Holds | Ref |
|---|---|---|
| `PaviApp` | runtime, the open tab's id and the parked tabs (`tabs: Tabs`), the Compare window (`compare: CompareState`, shared by all tabs), dataset, open task, `pages: SourcePages`, `filtered: Option<FilteredGrid>`, column layout, grid window offsets, `export: ExportState`, chart/profile state (the profile includes the whole-file one), text inputs, session, UI toggles, status string | `app.rs` |
| `TabState`, `Tabs` | One parked tab: the same fields as `PaviApp` from `tab_id` to `workspace` (dataset, pages, result, layout, inputs, chart, profile, export). Switching swaps them with `PaviApp`'s; nothing is re-run. `Tabs` holds the parked tabs (≤16) and the open tab's position | `tabs.rs` |
| `SourcePages` | page reads in flight, loaded pages (LRU ≤8), this frame's requests and drawn page range; `poll`, `track`, `cancel_offscreen`, `clear` | `pages.rs` |
| `Dataset` | `Arc<ParquetSource>`, path, column names | `app.rs:45-49` |
| `GridState` | generation, `first_row` (absolute index of the first row in a slid result window), row/column counts, load state, requested/loaded page sets, selection (absolute rows) | `state.rs:198-319` |
| `FilteredGrid` | query kind, `LogicalPlan`, running `QueryExecution`, sliding `VecDeque<RecordBatch>` (≤8), `first_result`, `rows`, `target_result` | `results.rs` |
| `ColumnLayout` | display order, hidden set, widths (72–480 px) | `state.rs:72-196` |
| `ChartState`, `ProfileState`, `ExportState` | own generation, running execution/task, result, error. `ExportState` also drops events from replaced exports and turns events into status text | `chart.rs`, `profile.rs`, `export.rs` |
| `SessionState` | recent files and folders (≤12), last file, files and folders open in tabs (≤16) and the shown one, SQL input, history (≤50 / 256 KB), inspector/chart visibility, per-schema grid layouts (≤8) | `session.rs:46-53` |

State changes happen by direct method calls. Every "restart" (open, filter, SQL, sort, clear) bumps `GridState.generation` via `GridState::reset()` (`state.rs:214-224`) so late responses are ignored.

### Core / domain

| Type | Responsibility | Ref |
|---|---|---|
| `DatasetMetadata`, `RowGroupInfo`, `ColumnInfo`, `ColumnStatistics` | Schema, cumulative row ranges per row group, stringified min/max/null/distinct per (column, row group) | `crates/parquet/src/metadata.rs` |
| `Projection` | Ordered unique top-level column indexes; `parquet_columns()` gives sorted order for the reader | `projection.rs` |
| `FilterExpr` | A tree of `Comparison`s joined by `And` / `Or`. A `Comparison` is `col OP value` with `== != > >= < <= contains LIKE NOT LIKE IS NULL IS NOT NULL`; a null cell passes only `IS NULL`. `IN` / `BETWEEN` are parsed into comparisons. The value is parsed for the column's type (`ordered_value`): integers exactly (as `i128`), floats as `f64` (rounded to the nearest `f32` for `Float32` columns, so a filter on the displayed value matches the cell), decimals as unscaled integers in the column's scale (`decimal_value`), times as `HH:MM:SS[.fff]` in the column's unit, dates as `YYYY-MM-DD`, timestamps as `YYYY-MM-DD[THH:MM:SS][offset]` (read in the column's time zone when no offset is given). Row-group pruning reads statistics in the same units | `filter.rs:28-157` |
| `SortSpec`/`SortBudget`, `AggregateSpec`/`GroupBudget` | Up to 8 sort keys (a `&[SortSpec]`, most significant first); up to 8 GROUP BY columns with COUNT/SUM/AVG/MIN/MAX | `sort.rs`, `aggregate.rs` |
| `SqlAst` → `LogicalPlan` → `Planner` → `PhysicalPlan` | Parse → tree → validated flat struct `{projection, filter, limit, sort, aggregate}` (operator order is ignored) | `crates/query/src/{sql,logical,planner}.rs` |
| `format_cell_with_limit` / `scalar_text` | Arrow value → text. Display is truncated (nested ≤8 deep/≤16 items); `scalar_text` is full-length for scalars | `value.rs` |
| `value_tree` / `decimal_f64` | A nested cell as a bounded tree for the Inspector; a decimal as a float for charts and profile means | `value.rs` |

### Data access

| Function | What it reads | Ref |
|---|---|---|
| `ParquetSource::open` | A file, a URL (read with range requests or downloaded, ADR 0008), or every `.parquet` file in a folder (`open_cancellable` stops between files). Bytes are read through `Handle` (a local `File` or a `RemoteReader`), the `ChunkReader` every read path uses. Footers only (`ArrowReaderMetadata::load`, default options → **no page index**); builds row-group index + statistics strings | `source.rs:125-159` |
| `reader_for` | **Re-opens the row group's file** for every read (a window that crosses files uses one reader per file; partition columns are added by `reorder_batch`), builds a `ParquetRecordBatchReader` with `ProjectionMask::roots`, row groups, optional `RowSelection`, batch size 4096 | `source.rs:664-686` |
| `read_window_batches` / `read_window` | Rows `[first, first+n)` → overlapping row groups + skip/select `RowSelection` | `source.rs:613-662` |
| `read_page` | `read_window` for page `i` through the `Mutex<PageCache>` | `source.rs:223-242` |
| `read_filtered_from` | From source row `start_row` (stats-pruned row groups; the first group is entered with a `RowSelection` skip), filter each batch, return ≤n matches plus `next_row`, the row after the last match taken (`FilteredWindow`). Paging with `next_row` reads each row once | `source.rs` (`scan_filtered`) |
| `read_filtered_window` | Offset-based variant (skip the first N matches, scanning from row 0); used by the CLI | `source.rs` |
| `read_sorted` | Rows that pass the filter are buffered (kept columns + a sequence-number column, the stable tie-breaker). With `limit` ≤ budget rows the buffer is cut back to the best `limit` rows every ~32k rows (top-N). Otherwise a buffer past the budget is sorted and written as a run; the result is `SortOutput::InMemory(batches)` or `SortOutput::Spilled(SortMerge)` | `source.rs`, `spill.rs` |
| `read_aggregated` | Streaming row-by-row accumulation into `GroupCollector` (hash map of `Scalar` keys) | `source.rs:451-526`, `965-1177` |
| Export writers | `BatchWriter`: `CsvWriter` and `JsonWriter` (hand-rolled), `parquet::arrow::ArrowWriter`, `arrow_ipc::writer::FileWriter`, to any `Write`. The app writes to `<name>.pavi-<gen>.tmp`, then `fs::rename`; the CLI writes to stdout | `crates/parquet/src/export.rs`, `crates/app/src/export.rs` |

Caching: two independent LRU layers of 8 pages each — `PageCache` inside `ParquetSource` (keyed by page index **and** projection, 128 MB cap) and `PaviApp.pages` (8 pages, no byte cap).

### CLI

One implicit command. Arguments (`crates/cli/src/args.rs`):

| Arg | Meaning |
|---|---|
| `FILE` (positional, required) | Anything `ParquetSource::open` opens: a Parquet file, a folder, a CSV/JSON/Arrow file or a remote URL |
| `--format F` (default `table`) | `table`, or `csv`, `jsonl` (alias `json`), `parquet`, `arrow`: rows only, streamed with `parquet_reader::export::BatchWriter` |
| `--head N` | Rows to output. Table: default 10, rejected if >1000 (`validate_head`, `MAX_HEAD_ROWS`). Other formats: default every row |
| `--offset N` (default 0) | Skip N rows, or N matching rows with `--filter`. The table's `#` counts from N + 1 |
| `--columns a,b` | Column names or 0-based indexes; a name is tried first, so a column called `7` is found by name |
| `--filter "…"` | Same `FilterExpr` grammar as the GUI filter box |
| `--count` | Print only the row count (footer), or the matching rows (a scan of the filter's columns). Excludes `--head`, `--offset`, `--columns`, `--format`, `--quiet` |
| `-q`, `--quiet` | Table without the summary |

Table output goes to stdout: unless `--quiet`, for a folder its file count and partition columns; row/column/row-group counts, schema (`{:?}` of `DataType`), first 10 row-group ranges; then a box-drawn table with headers and cells cut to 4–32 terminal columns (numbers right-aligned, control characters escaped). Arguments are all checked before the first line is written. Exit codes: `0` success, also when the reader of stdout closes early (broken pipe); `1` for any `anyhow` error (printed as `Error: …` with Debug formatting on stderr, the same text Rust prints for an error returned from `main`); `2` for clap usage errors.
