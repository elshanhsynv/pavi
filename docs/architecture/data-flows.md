# Data flows

> Part of the [architecture docs](../ARCHITECTURE.md). `file:line` references can drift; search for the symbol name if a line doesn't match.

## 4.1 Open a file (GUI)

**Triggers:** `argv[1]` at startup, or else the tabs open when the app closed (`SessionState.open_files`), or else the last file; Ctrl+O / Ctrl+Shift+O (`handle_global_shortcuts`: a file / a folder); the file menu in the top bar (`file_menu` in `ui/toolbar.rs`: Open file, Open folder, a recent file or folder, or the path box + Enter); the same on the start screen (`show_closed_state` in `ui/grid.rs`); files and folders dropped on the window (`open_dropped_files`, each in its own tab); Reload/Ctrl+R.

**Tabs** (`tabs.rs`): everything the user opens goes through `open_path`. A path already open in a tab shows that tab; otherwise it opens in the open tab when that tab is empty, else in a new tab (≤16, `MAX_TABS`). Reload and session restore call `begin_open` on the tab directly. Each tab's state is the set of `PaviApp` fields from `tab_id` to `workspace`; `switch_tab` swaps them with the parked `TabState`, so a tab keeps its result, layout and inputs and nothing is re-run. A parked tab's reads finish into their own channels and are polled once the tab is shown again. Closing a tab cancels its work (`cancel_tab_work`). The central panel's widgets are drawn under `push_id(tab_id)`, so each tab keeps its own scroll positions.

**Steps**
1. `begin_open(path)` (`app.rs:643-673`, UI thread) makes the path absolute (`std::path::absolute`, no disk access), so one file has one spelling in the recent list, then cancels everything (open, pages, export, filter, chart, profile), clears the dataset, bumps the generation, and calls `RuntimeHandle::submit_open` → `try_send` on the bounded queue (`scheduler.rs:349-374`).
2. A worker runs `execute_open` (`scheduler.rs:516-533`) → `ParquetSource::open_cancellable`. A URL (ADR 0008) goes to `open_remote`: `RemoteObject::open` resolves it to an HTTPS request (signing S3 requests), asks for byte 0 to learn the size and whether ranges work, and either reads the Parquet footer through `RemoteReader` or downloads the object to a temporary file and continues below with that file. A `.csv`/`.tsv`, `.json`/`.jsonl`/`.ndjson` or `.arrow`/`.feather`/`.ipc`/`.arrows` file is first read into a temporary Parquet file (ADR 0007, `convert::convert`): column types are inferred from the first 10,000 records (CSV, JSON), then 8,192-row batches are read and written with `ArrowWriter` (Snappy, ≤262,144 rows or 64 MB buffered per row group) into `<temp>/pavi-open-<pid>-<n>/`, which is deleted with the source; that file is then opened as below. The worker reports the share read through `OpenTask::progress` (shown in the status bar) and stops between batches when cancelled. For a folder (ADR 0006), `dataset::find_files` walks it in path order and every file below is opened in turn (stopping between files if the open is cancelled); all must have the same columns, and `key=value` folders add partition columns. For each file: `File::open`, `footer::check_footer` (rejects impossible collection sizes and schema child counts before the parser sees them), `ArrowReaderMetadata::load` (footer read + Arrow schema), compression set, `DatasetMetadata` + statistics strings. Result is `Arc<ParquetSource>` sent on a capacity-1 channel.
3. Each frame `update()` calls `poll_open()` (`app.rs:675-736`): `try_recv`; drops if generation differs; on success builds `Dataset`, restores the saved column layout for this schema, `grid.ready(rows, cols)`, records the recent file and **writes the session JSON synchronously**, presets chart columns.
4. The next frame renders the grid; rendering rows triggers page loads (§4.2). Inspector reads metadata already in memory.

**Thread / blocking:** footer I/O on a worker; UI thread only polls. The rfd open dialog **blocks `update()`** while it is open (normal for native modal dialogs). Session save (JSON write + `is_file()` stat on every recent file, `session.rs:139-163`) runs on the UI thread.
**Memory:** footer metadata only (`ParquetMetaData` + per-row-group stats strings); for a folder, every file's footer, ≤256 MB together.
**Errors:** `OpenOutcome::OpenFailed` → `LoadState::Error` shown centered in red in the grid area + status bar; the path is removed from recents (`app.rs:729-734`). No panics on this path.
The path submitted with the open task is kept in `opening_path` and used for the dataset and recent-file entry, so edits to the path box while loading don't affect them.

```mermaid
sequenceDiagram
    actor User
    participant UI as PaviApp (UI thread)
    participant RT as Runtime queue
    participant W as Worker thread
    participant PS as ParquetSource
    User->>UI: Ctrl+O / Open / path+Enter / argv
    UI->>UI: begin_open(): cancel work, grid.reset() → gen N
    UI->>RT: submit_open(path, gen N) (try_send)
    RT->>W: Request::Open
    W->>PS: ParquetSource::open(path)
    PS-->>W: footer metadata, schema, stats
    W-->>UI: OpenResponse via sync_channel(1)
    loop every frame (repaint 16 ms while pending)
        UI->>UI: poll_open() try_recv
    end
    UI->>UI: gen == N? build Dataset, restore layout, grid.ready()
    UI->>UI: session.save() (sync JSON write)
    UI->>UI: render grid → page requests (4.2)
```

## 4.2 Scrolling / paging (unfiltered)

1. `show_grid_content` builds a `TableBuilder` with `body.rows(24 px, rows)`; egui calls the row closure only for visible rows. Files with more than `GRID_WINDOW_ROWS` (500,000) rows are **windowed**, because egui lays rows out in `f32` points, which stop being exact past about 16.7 million points (≈700,000 rows):
   - The table holds only 500,000 rows starting at `GridState::window_base`; row closure indexes are offset by it, so selection, page requests and cell text use absolute rows.
   - The table's own scrollbar is hidden. PAVI draws `row_scrollbar` in a 12 px strip on the right. Its thumb shows the drawn rows within the whole file, and clicking or dragging it scrolls there (`scroll_grid_to`).
   - After each frame, if the drawn rows are within a quarter window of an inner edge, `GridState::recentered_base` picks a window centred on them. The table's post-frame scroll offset is shifted by the same number of rows, so the view doesn't move.
2. For each visible row: `grid.visible_pages()` → `request_page(page)` (`app.rs:1286-1322`). `GridState::request_page` dedupes via `requested`/`loaded` sets. The projection is the **currently visible columns** in display order. `submit_page` → worker.
3. Each visible cell calls `cell_text` (`app.rs:1324-1345`), which also prefetches the next page when the row is a page boundary, then formats the value with `format_cell_with_limit(…, 256)` **every frame** — or shows `…` if the page isn't loaded.
4. Worker: `read_page` checks the source `PageCache` (mutex held only for lookup/insert), else `read_window_batches` decodes rows `[i·4096, (i+1)·4096)` from overlapping row groups using `RowSelection::skip/select`.
5. `SourcePages::poll` (`pages.rs`) `try_recv`s each pending task and keeps ≤8 loaded pages, evicting the oldest and un-marking it as loaded so it will be re-requested.
6. Jump-to-row (the "Go to row" box in `ui/status.rs`, handled by `jump_to_input`) and keyboard navigation (`ui/grid.rs`) also call `request_page`. Jumps use `scroll_to_row`; in windowed grids they instead move the window around the target and set the scroll offset directly, which applies in the same frame.

**Memory:** ≤8 UI pages × 4,096 rows × visible columns, plus ≤8 pages / 128 MB in the source cache (often the same pages → double retention of `Arc`'d arrays, mostly shared buffers).
**Back-pressure:** queue capacity 32; on `QueueFull` the request is dropped, the status bar shows `queue page N: runtime request queue is full`, and it's retried on a later frame. After drawing, the grid cancels pending page reads more than one page away from the pages it drew (`cancel_offscreen_pages`), except pages requested in the same frame such as a jump target; they are requested again if they scroll back into view.
**Errors:** `ReadFailed` → status bar message; page stays `…`.
**Column layout change** (hide/move/reset) cancels and clears all pages because the projection key changes (`app.rs:568-575`).

```mermaid
sequenceDiagram
    participant G as grid.rs (row closure)
    participant A as PaviApp
    participant RT as Runtime
    participant W as Worker
    participant C as PageCache (Mutex)
    G->>A: request_page(p) for each visible row
    A->>A: GridState.request_page dedupe
    A->>RT: submit_page(source, p, visible projection, gen)
    RT->>W: ReadOperation::Page
    W->>C: get(PageKey{p, projection})
    alt miss
        W->>W: reader_for(): File::open + RowSelection skip/select
        W->>C: insert(DataPage)
    end
    W-->>A: PageOutcome::Loaded(page)
    A->>A: SourcePages::poll → cache page (LRU ≤ 8)
    G->>A: cell_text(row, col) → format_cell_with_limit (every frame)
```

## 4.3 Filter (filter box, cell right-click)

1. Two entry points build a `Filter` and call `start_filter`:
   - the filter box: `apply_filter` → `FilterExpr::parse` (text grammar, `filter.rs`): tests joined by `AND` / `OR` with brackets; each test is `column OP value`, `[NOT] IN (…)`, `[NOT] BETWEEN a AND b`, `[NOT] LIKE pattern`, `contains` or `IS [NOT] NULL`. `IN` and `BETWEEN` are stored as the comparisons they stand for, so the tree has only `Compare`, `And` and `Or` nodes. A filter holds at most `MAX_FILTER_TERMS` (64) comparisons and `MAX_FILTER_DEPTH` (16) levels;
   - a cell's right-click menu ("Filter to this value" / "Exclude this value"): `cell_menu` in `ui/grid.rs` stores a `CellAction`; `apply_cell_action` runs it after the table is drawn. `selected_cell_filter` builds the test directly from the selected cell (column `==` or `!=` the cell's `value::scalar_text`, or `IS NULL` / `IS NOT NULL` for a null cell), so no text is re-parsed. "Add to filter" joins it with `AND` to the filter the result was made with (`FilteredGrid.filter`, kept across a sort). The filter box gets the text `FilterExpr`'s `Display` writes. A cell is refused, with the reason shown in the menu, for types that are not comparable, aggregate results, values over `MAX_FILTER_VALUE_BYTES` (1,024), and any value whose `==` filter does not match its own cell.

   `start_filter`: `LogicalPlan::scan().filter().project(all)` → `Planner::plan` validates column/type → cancel pages & previous filter → `grid.reset()` → `QueryEngine::execute`.
2. `execute` (`execution.rs:91-120`) schedules the first read: `submit_filtered_from(filter, start_row 0, 4096 rows)`.
3. Worker: `read_filtered_from` starts at `start_row`, reports its position in the file before each batch (`checkpoint`, see concurrency.md), skips row groups whose statistics can't match (`row_group_might_match`: an `AND` is skipped when any part cannot match, an `OR` only when no part can; `LIKE` and null tests never skip), decodes `projection ∪ filter columns` (consecutive kept row groups of one file share one reader, up to `SCAN_RUN_ROWS` = 1,048,576 rows, so files with many small row groups aren't reopened per group; sorts and aggregates scan the same way), evaluates a `BooleanArray` mask (`FilterExpr::evaluate`: one mask per comparison, combined bitwise), and returns up to 4,096 matches as `PageOutcome::Filtered` with `next_row`.
4. UI `poll_filtered` (`app.rs:1219-1278`) calls `poll_next_batch()` while `needs_more()`; batches go into `FilteredGrid.push`, which keeps only the last 8 batches and advances `first_result`. `grid.rows` is set to the **window size**, not the total.
5. When the last window row is rendered, `request_more_filtered` raises `target_result` by 4,096 (`ui/grid.rs:347-349`), causing the next `submit_filtered_from` at `next_filter_row`, the cursor returned by the previous read.
6. `Finished` when `next_row` reaches the file's row count.

**Memory:** ≤8 batches (≈32k rows) in the UI. Each worker request decodes only from the cursor onward, so reading a whole filtered result (scrolling to the end, export) touches each row once.
**Errors:** parse/validation errors → red text under the filter box (`filter_error`); execution errors → `FilteredGrid.error` shown in the grid + status.

```mermaid
sequenceDiagram
    participant UI as PaviApp
    participant Q as QueryExecution
    participant RT as Runtime
    participant W as Worker
    UI->>UI: apply_filter(): parse, plan, grid.reset()
    UI->>Q: QueryEngine.execute(plan, gen)
    Q->>RT: submit_filtered_from(start_row=0, 4096)
    RT->>W: read_filtered_from
    W->>W: from start_row: prune row groups by stats, decode, mask
    W-->>Q: PageOutcome::Filtered(batch ≤4096, next_row)
    loop each frame while needs_more
        UI->>Q: poll_next_batch()
        Q-->>UI: Batch / Pending / Finished
        UI->>UI: FilteredGrid.push (keep last 8 batches)
    end
    UI->>UI: last row rendered → request_more()
    Q->>RT: submit_filtered_from(start_row=next_row, 4096)
```

## 4.4 Sort (Sort menu, header right-click, SQL ORDER BY)

1. The Sort menu in the top bar calls `toggle_sort(source_column)` (Asc → Desc → Asc, by that column alone). A right-click on a grid header offers "Sort ascending" / "Sort descending" (`HeaderAction::Sort`, by that column alone) and, once something is sorted, "Then by ascending" / "Then by descending" (`HeaderAction::ThenSort`, adds the column as the next key or changes its direction). All end in `apply_sort(keys)`, which replaces the plan's sort (`LogicalPlan::replace_sort`) and keeps any filter. A plain header click only selects the column. SQL `ORDER BY a DESC, b` builds the same `Vec<SortSpec>`. At most `MAX_SORT_KEYS` (8) keys; nulls are last unless SQL says `NULLS FIRST`.
2. `execute` submits **one** `ReadOperation::Sorted` with a `SortedRead { sort: Vec<SortSpec>, budget, limit }` (`execution.rs`).
3. Worker `read_sorted` keeps, per row, the output columns, the sort keys and a sequence number (the last key, so equal keys keep file order):
   - `LIMIT n` with n ≤ the budget (100,000 rows): only the best n rows are kept while scanning (top-N). Never touches disk.
   - Otherwise, while the rows fit the budget (100,000 rows / 128 MB): sorted in memory → `SortOutput::InMemory` → `PageOutcome::Batches`.
   - Past the budget (ADR 0005): each full buffer is sorted and written as an uncompressed Parquet *run* in `<temp>/pavi-sort-<pid>-<n>/` (`spill::RunWriter`); more than 16 runs are merged 16 at a time into longer runs; the rest become a `SortMerge` → `PageOutcome::SortSpilled(Arc<Mutex<SortMerge>>)`. Total spill size is capped at `MAX_SPILL_BYTES` (32 GiB).
4. For a spilled sort, `QueryExecution` keeps the merge and, each time the grid or an export wants more rows, submits `ReadOperation::SortMerge { merge, rows }`, which returns `PageOutcome::Merged { batches, finished }` (≈4,096 rows). A merge round concatenates one buffered batch per run, sorts it, and hands out everything up to the smallest "last buffered row" of any run; nothing unread can sort before that. A `LIMIT` past the budget is applied by the merge.
5. While the scan runs, `QueryExecution::scanned_rows()` returns the position the worker last reported; the status bar and the grid's "query running…" placeholder show it as a percentage. Writing runs is part of the scan; the intermediate merges and the final merge are not counted.
6. Results flow into the 8-batch window like a filter's. When the merge has handed out its last row, or the result is replaced or cancelled, the `SortMerge` is dropped and its folder deleted. Spill folders older than a day (from a crash) are removed by the next sort that spills.

**Memory:** at most the sort budget (100k rows / 128 MB of kept columns, plus the sorted copy) while scanning; a merge holds one 4,096-row batch per run (≤ 16 runs) plus one merged round.
**Disk:** a spilled sort writes about its kept columns, uncompressed, once per pass (one pass up to 16 × 100k rows, one more per ×16).
**Errors:** "this sort needs more than 32 GB of temporary disk space; filter the data or add a LIMIT"; I/O errors creating or reading spill files; a top-N whose kept rows alone exceed 128 MB. All end up in the status bar / grid error.
"Back to file order" in the Sort menu = `clear_filter`.

```mermaid
sequenceDiagram
    participant H as Sort menu / header menu / SQL
    participant UI as PaviApp
    participant Q as QueryExecution
    participant W as Worker
    H->>UI: apply_sort(keys)
    UI->>UI: plan.replace_sort(keys)
    UI->>Q: execute(plan)
    Q->>W: ReadOperation::Sorted (one task)
    alt fits the budget (or LIMIT ≤ budget)
        W-->>Q: PageOutcome::Batches(all)
    else spills
        W->>W: sorted runs → temp files, merge passes of 16
        W-->>Q: PageOutcome::SortSpilled(merge)
        loop as the grid scrolls / export reads
            Q->>W: ReadOperation::SortMerge (≈4,096 rows)
            W-->>Q: PageOutcome::Merged
        end
    end
    Q-->>UI: batches one per poll → 8-batch window
```

## 4.5 SQL (SQL workspace) and aggregates

1. `run_sql` (`app.rs:1029-1111`): `SqlAst::parse` (`sql.rs:61-95`) enforces the subset (one statement, `SELECT`, up to 8 `ORDER BY` keys and 8 `GROUP BY` columns (SELECT lists the GROUP BY columns first, in order), a `WHERE` of comparisons, `IN`, `BETWEEN`, `LIKE` and `IS NULL` joined by `AND` / `OR`; a general `NOT`, `ESCAPE`, `ILIKE`, `EXISTS` and correlated subqueries are rejected, `LIMIT`, aggregates). Column names are resolved case-insensitively, and may be qualified (`f.ORIGIN`). The tables are the open tabs (`sql_tables` in `tabs.rs`): `dataset` for this tab and every tab by a name from its file. → `to_logical_plan_with(&Tables)` → `Planner::validate`.
   - A query on one table without subqueries reads it directly, as below.
   - A query with `JOIN`s, a `FROM (SELECT …)` or a `WHERE` subquery gets a **prepared plan** (ADR 0009): a `LogicalPlan::Prepared` leaf holding a `Prep` tree (tables with their kept columns, subqueries, filters, joins) and the schema of the rows it will produce, with projection, aggregate, sort and limit above it. `WHERE` tests on one table are pushed into that table's read (not on the optional side of a `LEFT JOIN`); `col [NOT] IN (SELECT …)` becomes a semi/anti join; `col <op> (SELECT …)` a filter whose value the subquery supplies.
   - `QueryEngine::execute` starts a prepared plan in a preparing stage: `PrepTask` runs the `Prep` tree on thread `pavi-query-prep`, running subqueries through the engine into temporary tables (`TempTableWriter`) and joins with `parquet_reader::join` (hash join; the in-memory side ≤ 1,000,000 rows / 256 MB, the other side streams; result ≤ 32 GiB in `<temp>/pavi-query-<pid>-<n>/`). `preparing_progress` is the share of joins done. When it finishes, `LogicalPlan::bind` turns the leaf into a scan of the result (with its filter and columns) and the query runs as below. The result grid shows prepared columns as the result names them (`FilteredGrid.aggregate`).
2. Execution picks one strategy in `schedule_next` (`execution.rs:319-433`), by precedence: **aggregate** (one `Aggregated` task) → **sort** (one `Sorted` task) → **filter** (repeated filtered reads, each resuming at the previous cursor) → **plain scan** (`Page` tasks, or a final `Window` for the LIMIT remainder).
3. Aggregates (`read_aggregated`, `source.rs:451-526`) stream all row groups, converting every value to a `Scalar` and updating accumulators per row; `COUNT(*)` with no filter uses the metadata row count (fast path, `source.rs:481-485`).
4. Results flow into the same `FilteredGrid`. History is recorded with duration/row count on finish/failure (`app.rs:628-641`) and the session is saved.

Errors: parse/plan errors → red text in SQL workspace (`sql_error`) + failed history entry; runtime errors → grid error + status.

```mermaid
sequenceDiagram
    actor User
    participant UI as PaviApp
    participant P as SqlAst / Planner
    participant Q as QueryExecution
    participant W as Worker
    User->>UI: Run SQL / Ctrl+Enter
    UI->>P: parse → LogicalPlan → PhysicalPlan
    P-->>UI: plan or error (sql_error)
    UI->>Q: execute(plan, gen)
    alt aggregate
        Q->>W: Aggregated (whole file, streaming)
    else ORDER BY
        Q->>W: Sorted (budgeted, top-N with LIMIT)
    else WHERE
        Q->>W: FilteredFrom × n (cursor)
    else plain
        Q->>W: Page × n / Window
    end
    W-->>Q: outcomes
    Q-->>UI: poll_next_batch → FilteredGrid
    UI->>UI: record_sql_history + session save
```

## 4.6 Inspector: schema, metadata, row-group statistics, profiling

- **Metadata/schema/stats** are computed once at open (`statistics::metadata_statistics`; min/max rebuilt as typed values and formatted like grid cells, byte strings capped at 128 chars) and rendered from memory each frame; long lists use `ScrollArea::show_rows` (virtualized) (`ui/inspector.rs:203-240`). Each top-level column is mapped to its Parquet leaf (`statistics::leaf_columns`); a nested column (several leaves, or one leaf under a group) has no statistics, and the other columns keep theirs. The selected cell, when it is a list, struct or map, is shown as a tree (`value::value_tree`, bounded).
- **Selected cell:** `selected_cell_details` → `inspect_cell` on the in-memory batch (`app.rs:1560-1603`).
- **Profile column:** `run_profile` (`app.rs:1482-1503`) → `ProfileTask::start` spawns thread `pavi-profile` (`profile.rs:83-119`), which runs a `scan().project([col])` query and polls it with a 5 ms sleep loop (`profile.rs:143-176`), building exact distinct counts, min/max/mean and string lengths (`parquet_reader::profile::StatsAccumulator`, shared with the CLI) and a distribution chart (the app's `ProfileAccumulator` adds it). Result returns via a capacity-1 channel; UI polls in `poll_profile` (`app.rs:1513-1543`). Exact distinct counting is capped at 4,096 values and 128 bytes per value; past that the profile ends with an error (`profile.rs:275-294`).
- **Profile all columns:** `run_file_profile` → `FileProfileTask::start` spawns thread `pavi-file-profile`, which runs one `scan().project(columns)` query over the first 256 columns and feeds each batch to a `parquet_reader::profile::FileProfiler`, one accumulator per column (no distribution chart). The CLI's `--profile` feeds the same `FileProfiler` from its own synchronous reads, so both report the same numbers. Lists, structs and maps get their type and footer null count only. Each column counts at most 1,000 distinct values / 64 KB and then stops counting and marks the count as a lower bound instead of failing, so the whole profile holds about 256 × 64 KB at most. Rows read are shared through an atomic counter for the progress text; cancellation is checked every batch. `poll_file_profile` drops results of an older generation; the Profile window shows the rest.

```mermaid
sequenceDiagram
    participant UI as PaviApp
    participant PT as pavi-profile thread
    participant Q as QueryExecution
    participant W as Worker
    UI->>PT: ProfileTask::start(source, col, gen)
    PT->>Q: execute(scan.project([col]))
    loop until Finished/cancel
        PT->>Q: poll_next_batch() (sleep 5 ms on Pending)
        Q->>W: Page tasks (full column scan)
        W-->>Q: pages
        PT->>PT: accumulate (exact distinct ≤ 4096)
    end
    PT-->>UI: ProfileEvent via sync_channel(1)
    UI->>UI: poll_profile → Inspector shows result
```

## 4.7 Export and clipboard

- **Export** (`app.rs:842-886`, `export.rs:76-206`): save dialog (blocks UI), build `ExportInput` = the current filter/SQL/sort `LogicalPlan`, or a full-scan plan, or one selected row; spawn `pavi-export` thread. It writes to `<file>.pavi-<gen>.tmp`, driving `poll_next_batch` with a 5 ms sleep loop, writing each batch as CSV, Parquet (`ArrowWriter`), JSON Lines (`parquet_reader::export::BatchWriter`, shared with the CLI; `JsonWriter`: one object per row; scalars are `value::scalar_text`, numbers and booleans unquoted, NaN and infinity as strings, binary as `0x` hex; lists as arrays, structs and maps as objects, ≤64 levels) or Arrow IPC (`arrow_ipc::writer::FileWriter`), sending best-effort progress (`try_send`, capacity 1), then `fs::rename` to the target. Cancel/failure deletes the temp file. CSV accepts every non-nested type and writes each field with `value::scalar_text`, the same text the grid shows, untruncated; nulls are empty fields and binary is `0x`-prefixed hex.
- **Copy:** Ctrl+C copies the selected cell range (TSV) from in-memory batches (`app.rs:1659-1751`) using the same 256-char display formatting, capped at 64 KB; fails if any selected row has been evicted from the window.

```mermaid
sequenceDiagram
    actor User
    participant UI as PaviApp
    participant EX as pavi-export thread
    participant Q as QueryExecution
    participant FS as File system
    User->>UI: Export ▸ CSV/Parquet
    UI->>UI: rfd save dialog (blocking)
    UI->>EX: ExportTask::start(plan|batch, path, gen)
    EX->>Q: execute(plan)
    loop batches
        EX->>Q: poll_next_batch (sleep 5 ms)
        EX->>FS: write batch to .tmp
        EX-->>UI: Progress (try_send)
    end
    EX->>FS: rename .tmp → target
    EX-->>UI: Finished / Failed / Cancelled
    UI->>UI: poll_export → status bar
```

## 4.8 Charts

`run_chart` (`app.rs:1352-1414`): either uses the current 8-batch result window synchronously on the UI thread (small), or runs a chart SQL query whose batches are fed to `ChartAccumulator` in `poll_chart` (`app.rs:1433-1480`) until 16,384 input rows, then cancels the query. Up to six Y columns (`MAX_SERIES`) are read in the same query and become one series each; bars group them per label. Rendering is custom painting in `ui/charts.rs` through a `Canvas` trait with two implementations: the egui painter and an SVG writer, so a saved SVG matches the screen.

**Save image:** SVG is built from the last model and written by a background thread (`ChartState::save_image`: `.pavi-image.tmp`, then rename). PNG asks the window for a screenshot (`ViewportCommand::Screenshot`); when `Event::Screenshot` arrives, the image is cropped to the plot's rect, encoded with `png` and written the same way.

## 4.8a Compare

`Compare with another tab` (file menu) opens a window that reads only footer schemas: `compare::SchemaDiff` matches columns by name. Rows are compared by the SQL workspace, not by new code: Only here / Only there / In both write `SELECT * FROM dataset WHERE k [NOT] IN (SELECT k FROM other)` (`compare::key_query`) into the editor and call `run_sql`, so the anti or semi join is bounded as ADR 0009 describes.

## 4.9 CLI

```mermaid
sequenceDiagram
    actor Shell
    participant M as cli main
    participant PS as ParquetSource
    participant O as output::print_batch
    participant W as export::BatchWriter
    participant Q as pavi_query (--sql)
    Shell->>M: pavi-cli FILE|- [--format F] [-o PATH] [--head N] [--offset N] [--columns a,b] [--filter "…"] [--sort "a:desc,b"] [--count | --schema | --stats | --profile | --sql "…" [--table [NAME=]PATH]…] [-q]
    M->>M: Args::parse (clap; usage error or excluded combination → exit 2)
    M->>M: validate_head (table only: default 10, or 1,000 for --stats; ≤1000)
    opt FILE is -
        M->>M: copy stdin to a temporary stdin.<ext> (format from the first bytes, ≤32 GiB)
    end
    opt -o PATH
        M->>M: refuse PATH = FILE; format from PATH's extension unless --format; write into PATH.pavi-cli-<pid>.tmp
    end
    M->>PS: open(FILE)
    M->>M: parse_columns (names first, then 0-based indexes)
    M->>PS: parse_filter → FilterExpr::parse + validate_filter
    M->>PS: parse_sort → validate_sort (≤8 keys, sortable types)
    alt --count
        M->>PS: row_count, or read_filtered_from over the filter's columns, 4,096 rows a step
        M->>Shell: the number
    else --schema / --stats
        M->>PS: schema / metadata().columns() row-group statistics (footer only)
        M->>Shell: one batch → table (stats: first --head rows, ≤1000) or BatchWriter
    else --profile
        M->>PS: FileProfiler over --columns (≤256); Rows::for_each (filtered or window reads, 4,096 rows a step) → push_batch
        M->>Shell: one row per column → table or BatchWriter
    else --sql
        M->>PS: open each --table; Tables = dataset + table_name(FILE) + --table names
        M->>Q: SqlAst::parse → to_logical_plan_with(tables) → Planner::validate (errors before output)
        M->>Q: Runtime::new, QueryEngine::execute, next_batch until --head rows (then cancel)
        M->>Shell: rows → table ("Result", no file summary) or BatchWriter
    else rows (Rows::for_each, skipping --offset rows, stopping after --head)
        alt --sort
            M->>PS: read_sorted(filter, proj, keys, SortBudget, top = offset + head)
            Note over M,PS: InMemory batches, or a SortMerge read 4,096 rows a step after spilling (ADR 0005)
        else --filter
            M->>PS: read_filtered_from(next_row, 4,096)
        else
            M->>PS: read_window(row, 4,096) from row = offset
        end
        alt --format csv / jsonl / parquet / arrow
            M->>W: write(batch) → stdout, then finish (Parquet/Arrow footer)
        else table
            M->>M: collect ≤ head rows, concat
            M->>Shell: Rows / Columns / Row groups / Schema / ≤10 row-group ranges (unless --quiet)
            M->>O: print_batch → box table to stdout, # counting from offset + 1
        end
    end
    M->>M: with -o: flush, sync_all, rename the temporary file to PATH (removed on any error)
    M-->>Shell: exit 0 (also when stdout's reader closed), or anyhow error → stderr, exit 1
```

All of it runs on the main thread, except `--sql`, which starts a worker pool and runs the plan exactly as the app's SQL editor does (ADR 0011): the main thread blocks on `QueryExecution::next_batch` instead of polling, and joins and subqueries are prepared on `pavi-query-prep` first (ADR 0009). The table holds ≤1,000 rows of the projected columns; the streamed formats and `--count` hold one 4,096-row batch (`STREAM_BATCH_ROWS`) at a time, plus the Parquet writer's open row group, so they have no row limit (a selective filter may still scan the whole file). `--sort` goes through `read_sorted` like the app's sort: with `--head`, only the best `offset + head` rows are kept while scanning (when that fits the sort budget); otherwise up to `SortBudget` rows stay in memory and the rest spill to temporary files, merged 4,096 rows at a time. `--schema` and `--stats` read no data pages: they turn the footer already in memory into rows, so they print or stream like data. The table reads its rows before printing the summary, so a read error leaves stdout empty too. The streamed formats write only rows, no summary, with the same `BatchWriter` the app's export uses, so their text matches an exported file. Every argument is checked before anything is printed, so a bad `--columns` or `--filter` leaves stdout empty. Output goes through one buffered writer; a broken pipe (`pavi-cli … | head`) ends the run quietly with exit 0 instead of a panic. The table measures text in terminal columns (`unicode-width`), cuts headers and values at 32 columns, shows line breaks, tabs and other control characters as `\n`, `\t`, `\u{…}`, and right-aligns numbers (`--ascii` draws it with `+ - |`). A long open or scan reports progress on stderr through the reader's checkpoint callbacks, which the CLI never uses to cancel; the line appears only on a terminal, after a second, and is blanked before any output.
