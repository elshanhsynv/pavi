# Performance

PAVI must stay responsive on files of any size, including the 1-billion-row sample (`data/dummy_1b.parquet`, ~4.3 GB).
It gets there by reading lazily, doing all I/O off the UI thread, and capping every buffer.

## Memory budgets

Each budget is a named constant. Change it in one place and update this table.

| What | Cap | Constant |
|---|---|---|
| Source page cache | 8 pages / 128 MB | `parquet_reader::cache::DEFAULT_MAX_PAGES`, `DEFAULT_MAX_BYTES` |
| UI loaded pages | 8 pages | `pavi::pages::MAX_LOADED_PAGES` |
| Page size | 4,096 rows | `parquet_reader::page::PAGE_ROWS` |
| Filter/SQL/sort result window | 8 batches (~32k rows); a jump keeps 2,048 rows above its target | `pavi::results::MAX_FILTERED_BATCHES`, `JUMP_CONTEXT_ROWS` |
| Filter match count (ADR 0012, 0016) | One `u64` per row group of the file (the footer's list), no other buffer; row groups the statistics settle are not read | `ParquetSource::count_matches_by_row_group` |
| Sort rows in memory (larger sorts spill, ADR 0005); also the size of one spilled run | 1,048,576 rows / 128 MB (the bytes counted three times: the rows, their sorted copy and the concatenation) | `SortBudget::DEFAULT_MAX_ROWS`, `DEFAULT_MAX_BYTES` |
| Sort spill files | 32 GiB per sort; a killed process's folder is removed at the next app or CLI start (ADR 0017) | `spill::MAX_SPILL_BYTES` |
| Sort merge | 16 runs at once, one 4,096-row batch each | `spill::MERGE_FAN_IN` |
| Sort keys / GROUP BY columns | 8 / 8 | `sort::MAX_SORT_KEYS`, `aggregate::MAX_GROUP_COLUMNS` |
| GROUP BY, SELECT DISTINCT (and the values `COUNT(DISTINCT …)` keeps, which count against the bytes, as does each group's encoded key); HAVING and ORDER BY on an aggregate work on this one result | 10,000 groups / 8 MB | `GroupBudget::DEFAULT_MAX_GROUPS`, `DEFAULT_MAX_BYTES` |
| Profile distinct values | 4,096 values / 1 MB, 128 bytes each | `parquet_reader::profile::MAX_DISTINCT_*` |
| Whole-file profile (app's Profile window and CLI `--profile`) | 256 columns; per column 1,000 distinct values / 64 KB, then the count reads "more than"; the CLI also holds one 4,096-row batch of the profiled columns | `parquet_reader::profile::MAX_FILE_PROFILE_COLUMNS`, `FILE_PROFILE_DISTINCT_VALUES`, `FILE_PROFILE_DISTINCT_BYTES` |
| Chart input | 16,384 rows | `chart::MAX_INPUT_ROWS` |
| Chart series | 6 Y columns | `chart::MAX_SERIES` |
| Clipboard copy | 16 MB of text; values are formatted only up to the space left | `MAX_CLIPBOARD_BYTES` |
| Find, one step on the UI thread | 500,000 cells (only rows already loaded) | `find::MAX_FIND_CELLS` |
| Cell value used as a filter | 1,024 bytes | `MAX_FILTER_VALUE_BYTES` |
| SQL calculations (ADR 0013) | 256 parts per calculation, 32 calculations per query; evaluated one batch at a time; calculations used to filter, group or sort write a temporary table under the 32 GiB prepared limit | `expr::MAX_EXPR_NODES`, `sql::MAX_CALCULATIONS` |
| Comparisons in one filter | 64 (each `IN` value counts), 16 levels deep | `filter::MAX_FILTER_TERMS`, `MAX_FILTER_DEPTH` |
| `LIKE` pattern | 256 characters | `filter::MAX_LIKE_PATTERN_CHARS` |
| Grid row window | 500,000 rows | `state::GRID_WINDOW_ROWS` |
| CLI table preview | 1,000 rows (`--head`), each cell ≤ 1,024 bytes, plus one row number per row under `--filter` | `cli::MAX_HEAD_ROWS`, `value::DEFAULT_CELL_LIMIT` |
| CLI streamed output (`--format csv/jsonl/parquet/arrow`) | one 4,096-row batch of the projected columns; Parquet also buffers its open row group, as in the app's export. No row limit | `cli::STREAM_BATCH_ROWS` |
| CLI `--count` with a filter (ADR 0016) | up to 16 threads, each with one reader and one 4,096-row batch of the filter's columns, reading chunks of about 1,048,576 rows; one `u64` per row group per chunk | `cli::MAX_COUNT_THREADS`, `COUNT_CHUNK_ROWS` |
| CLI `--sql` | the query engine's own budgets, as in the app: sort, `GroupBudget`, the join build side (`MAX_JOIN_BUILD_ROWS` / `_BYTES`) and the prepared table (`MAX_PREPARED_BYTES`); one worker pool of `available_parallelism` threads with a 32-task queue. The table holds ≤1,000 result rows | `RuntimeConfig::default()`, ADR 0009, 0011 |
| CLI `--sort` | the app's sort budget in memory (1,048,576 rows / 128 MB, or the best `offset + head` rows with `--head`), then spill files up to 32 GiB in the temporary folder, deleted when the run ends | `SortBudget::DEFAULT_MAX_*`, `spill::MAX_SPILL_BYTES` |
| CLI standard input (`-`) | copied to a temporary file, at most 32 GiB (as a remote download); CSV, JSON and Arrow input is then converted as for any file | `cli::files::MAX_STDIN_BYTES`, `MAX_CONVERTED_BYTES` |
| CLI `--stats` table | 1,000 rows (row group × column) unless `--head` is lower; streamed formats print every row. The rows come from the footer, which is bounded at open | `cli::MAX_HEAD_ROWS`, `MAX_DATASET_METADATA_BYTES` |
| Rows per scan reader (filter, sort and aggregate scans, query results, exports, the CLI's streamed output and join scans read consecutive row groups of one file with one reader; its dictionary pages are checked when it opens). A query result or stream keeps its reader open between reads (`ScanCursor`), with at most one decoded batch it has not handed out | 1,048,576 rows | `SCAN_RUN_ROWS` in `crates/parquet/src/source.rs` |
| Page index read by a filtered scan (ADR 0014) | 4 MB per column chunk (column index + offset index each); larger ones aren't read and the chunk is scanned whole. The selected row ranges a scan reader keeps (`RowMap`) are at most one per page of its run | `page_prune::MAX_PAGE_INDEX_BYTES` |
| Bloom filter read for an `==` test (ADR 0014) | 16 MB per column chunk, only when the footer records its length; one at a time | `page_prune::MAX_BLOOM_FILTER_BYTES` |
| Dictionary page (uncompressed) | 256 MB; also never more values than its bytes can hold | `MAX_DICTIONARY_PAGE_BYTES` in `crates/parquet/src/source.rs` |
| Restored tab views | one per open tab (≤16): filter ≤ 16 KB, ≤ 8 sort keys | `session::MAX_SAVED_FILTER_BYTES`, `MAX_SAVED_SORT_KEYS` |
| Log file and crash reports | 1 MB, then it becomes `pavi.old.log` (2 MB in all); the newest 5 crash reports | `logging::MAX_LOG_BYTES`, `MAX_CRASH_REPORTS` |
| Remote request in flight (ADR 0015) | one `pavi-http` helper thread per waiting worker; a cancel is noticed within 50 ms, and the abandoned request stops at its next 64 KB of body or its timeout | `remote::INTERRUPT_POLL`, `BODY_CHUNK_BYTES` |
| Query history | 50 entries / 256 KB | `session::MAX_HISTORY_*` |
| Saved queries | 100, each ≤ 16 KB, names ≤ 80 characters | `session::MAX_SAVED_QUERIES`, `MAX_QUERY_BYTES`, `MAX_SAVED_NAME_CHARS` |
| SQL completions shown | 8 | `sql_editor::MAX_SUGGESTIONS` |
| Open tabs | 16; each parked tab keeps its own bounded pages, result window and footer metadata, so the per-dataset budgets above apply per tab | `tabs::MAX_TABS` (= `session::MAX_OPEN_FILES`) |
| Folder dataset search (ADR 0006) | 4,096 files, 16 folder levels, 100,000 directory entries looked at | `dataset::MAX_DATASET_FILES`, `MAX_FOLDER_DEPTH`, `MAX_FOLDER_ENTRIES` |
| Folder dataset footers in memory | 256 MB for all files together | `dataset::MAX_DATASET_METADATA_BYTES` |
| Change check of the open tab (ADR 0018) | one in flight, every 2 s; one `stat` per file (≤ 4,096) and per searched folder (the folder times kept are bounded by the 100,000 entries a search looks at) | `watch::CHANGE_CHECK_INTERVAL` |
| CSV / JSON type inference (ADR 0007) | first 10,000 records | `convert::MAX_INFER_ROWS` |
| Reading CSV, JSON, Arrow into Parquet | 8,192-row batches; a row group is written at 262,144 rows or 64 MB buffered; the temporary file ≤ 32 GiB | `convert::BATCH_ROWS`, `ROW_GROUP_ROWS`, `MAX_BUFFERED_BYTES`, `MAX_CONVERTED_BYTES` |
| Remote range cache (ADR 0008) | 256 MB per object; ranges over 128 MB are not kept | `remote::REMOTE_CACHE_BYTES`, `MAX_CACHED_RANGE_BYTES` |
| Remote download (no range requests; CSV, JSON, Arrow) | 32 GiB | `remote::MAX_DOWNLOAD_BYTES` |
| Arrow file metadata message / decompressed buffer | 64 MB / 1 GB | `convert::MAX_ARROW_MESSAGE_BYTES`, `MAX_ARROW_BUFFER_BYTES` |
| JSON export nesting | 64 levels | `export::MAX_JSON_DEPTH` |
| Join, side kept in memory (ADR 0009) | 1,000,000 rows / 256 MB after its filters | `join::MAX_JOIN_BUILD_ROWS`, `MAX_JOIN_BUILD_BYTES` |
| Joined or subquery rows written to disk | 32 GiB per temporary table | `join::MAX_PREPARED_BYTES` |
| Joins, join keys, subquery nesting | 4 joins, 4 `ON` pairs each, 3 levels | `prep::MAX_JOINS`, `join::MAX_JOIN_KEYS`, `prep::MAX_SUBQUERY_DEPTH` |

## Responsiveness rules

- The UI thread only polls (`try_recv`). Nothing on it may scale with file size.
- Per-frame work is limited to visible rows × visible columns.
- Cancelled or replaced work stops within one 4,096-row batch. The one longer step is a sort sorting the rows it holds in memory (one run, ≤ 1,048,576 rows / 128 MB; about 0.1 s for flights' million rows), which finishes before the cancel is seen.
- Repaint every 16 ms only while something is pending; idle means no repaint.

## Targets

Measured 2026-11-22 on the reference machine: Intel i5-11400H (12 threads), 16 GB RAM, SSD,
Windows 11, on mains power (on battery every timing is about 3–4× slower), release build (thin LTO),
warm OS file cache, median of 3 runs. The first run after a build is slower (cold file cache) and is not counted.
Files: `flights` (1M rows, 7 cols), `dummy_5m` (5M rows, 4 cols), `dummy_1b` (1B rows, 10 cols).

Each target is about 1.5× the slowest measured file, rounded up, with a 50 ms floor so
sub-frame operations aren't held to timer noise. A change that breaks a target needs a reason in the PR.

| Scenario | How measured | flights | dummy_5m | dummy_1b | Target |
|---|---|---|---|---|---|
| Read footer metadata | bench `open_metadata_ms` | 0 ms | 0 ms | 9 ms | < 50 ms |
| First page, all columns, empty cache | bench `first_page_all_cold_ms` | 4 ms | 2 ms | 8 ms | < 50 ms |
| Jump to the last page | bench `large_jump_ms` | 1 ms | 1 ms | 8 ms | < 50 ms |
| First batch of a `LIMIT` query | bench `query_first_batch_ms` | 3 ms | 2 ms | 6 ms | < 50 ms |
| Scroll 8 pages (32,768 rows) | bench `sequential_scroll_ms` | 33 ms | 17 ms | 55 ms | < 220 ms |
| App start to window, file given on the command line | `Start-Process` until the window exists | 57 ms | 49 ms | 41 ms | < 300 ms |
| App memory after loading a file | working set 5 s after start | 107 MB | 106 MB | 115 MB | < 200 MB |
| CLI open + 1,000-row preview (includes process start) | wall time | 73 ms | 74 ms | 84 ms | < 400 ms |
| Full scan: filter matching nothing on a text column | `pavi-cli --filter "category contains zzz"` | — | 318 ms | — | < 800 ms (≥ 6M rows/s) |
| Lookup in a column sorted within row groups (page index, ADR 0014) | `pavi-cli --filter "user_id == 123" --count` | — | — | 394 ms (2,126 ms before ADR 0016, 15,354 ms before ADR 0014) | < 3,200 ms |
| Frame time while scrolling (UI thread: layout + tessellation, p95) | `cargo test --release -p pavi -- --ignored --nocapture frame_time` on a generated 1M-row, 12-column file, 20 rows a frame (the grid's work doesn't grow with the file): 4.8 ms | — | — | — | < 16 ms (60 fps) |
| Cancel a running scan (cancel to return, all columns) | bench `cancel_scan_ms` | 0.7 ms | 0.4 ms | — | < 50 ms |

Whole-file jobs, measured the same day (no targets yet; `pavi_bench` doesn't finish these on
`dummy_1b`, whose whole-file sort would spill hundreds of GB):

| Job | How measured | flights | dummy_5m | dummy_1b |
|---|---|---|---|---|
| Read every row and column through the query engine | bench `scan_all_ms` | 25 ms | 182 ms | — |
| Whole-file sort by one column: first rows / all rows | bench `sort_first_batch_ms` / `sort_all_ms` | 133 / 134 ms | 991 / 1,233 ms | — |
| Export to CSV / Parquet | `pavi-cli --format csv/parquet -o …` | 607 / 152 ms | 1,860 / 727 ms | — |
| Profile every column | `pavi-cli --profile` | 899 ms | 1,552 ms | — |
| Count a filter the statistics settle (half the file matches) | `pavi-cli --filter "value_int > 5000000000" --count` | — | — | 93 ms |
| Count a filter that reads every row (text search) | `pavi-cli --filter "label contains zzz" --count` | — | — | 5,631 ms |

Not yet enforced in CI: GitHub runners are too noisy for millisecond targets. Check them with the
`perf-check` skill before merging changes to reading, the runtime, the query engine or the grid,
and re-measure this table (same machine) when a target is changed.

## Measuring

```bash
cargo run --release -p pavi_bench -- data/dummy_5m.parquet
```

Prints CSV-like timings and cache metrics for open, page reads and a scan query. Pass a second argument to set the number of sequential pages.
Always use `--release`. Compare against `main` before and after a change that touches reading, the runtime, the query engine or the grid, and put the numbers in the PR.

Sample files are not in git. `create_dummy_data.py` (needs `polars`) writes `dummy_1b.parquet`; change `N` and `OUTPUT` for smaller files.
