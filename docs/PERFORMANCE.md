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
| Filter/SQL/sort result window | 8 batches (~32k rows) | `pavi::results::MAX_FILTERED_BATCHES` |
| Sort rows in memory (larger sorts spill, ADR 0005) | 100,000 rows / 128 MB | `SortBudget::DEFAULT_MAX_ROWS`, `DEFAULT_MAX_BYTES` |
| Sort spill files | 32 GiB per sort | `spill::MAX_SPILL_BYTES` |
| Sort merge | 16 runs at once, one 4,096-row batch each | `spill::MERGE_FAN_IN` |
| Sort keys / GROUP BY columns | 8 / 8 | `sort::MAX_SORT_KEYS`, `aggregate::MAX_GROUP_COLUMNS` |
| GROUP BY | 10,000 groups / 8 MB | `GroupBudget::DEFAULT_MAX_GROUPS`, `DEFAULT_MAX_BYTES` |
| Profile distinct values | 4,096 values / 1 MB, 128 bytes each | `profile::MAX_DISTINCT_*` |
| Chart input | 16,384 rows | `chart::MAX_INPUT_ROWS` |
| Clipboard copy | 16 MB of text; values are formatted only up to the space left | `MAX_CLIPBOARD_BYTES` |
| Find, one step on the UI thread | 500,000 cells (only rows already loaded) | `find::MAX_FIND_CELLS` |
| Cell value used as a filter | 1,024 bytes | `MAX_FILTER_VALUE_BYTES` |
| Comparisons in one filter | 64 (each `IN` value counts), 16 levels deep | `filter::MAX_FILTER_TERMS`, `MAX_FILTER_DEPTH` |
| `LIKE` pattern | 256 characters | `filter::MAX_LIKE_PATTERN_CHARS` |
| Grid row window | 500,000 rows | `state::GRID_WINDOW_ROWS` |
| Rows per scan reader (filter, sort and aggregate scans read consecutive row groups of one file with one reader; its dictionary pages are checked when it opens) | 1,048,576 rows | `SCAN_RUN_ROWS` in `crates/parquet/src/source.rs` |
| Dictionary page (uncompressed) | 256 MB; also never more values than its bytes can hold | `MAX_DICTIONARY_PAGE_BYTES` in `crates/parquet/src/source.rs` |
| Query history | 50 entries / 256 KB | `session::MAX_HISTORY_*` |
| Saved queries | 100, each ≤ 16 KB, names ≤ 80 characters | `session::MAX_SAVED_QUERIES`, `MAX_QUERY_BYTES`, `MAX_SAVED_NAME_CHARS` |
| SQL completions shown | 8 | `sql_editor::MAX_SUGGESTIONS` |
| Open tabs | 16; each parked tab keeps its own bounded pages, result window and footer metadata, so the per-dataset budgets above apply per tab | `tabs::MAX_TABS` (= `session::MAX_OPEN_FILES`) |
| Folder dataset search (ADR 0006) | 4,096 files, 16 folder levels, 100,000 directory entries looked at | `dataset::MAX_DATASET_FILES`, `MAX_FOLDER_DEPTH`, `MAX_FOLDER_ENTRIES` |
| Folder dataset footers in memory | 256 MB for all files together | `dataset::MAX_DATASET_METADATA_BYTES` |
| CSV / JSON type inference (ADR 0007) | first 10,000 records | `convert::MAX_INFER_ROWS` |
| Reading CSV, JSON, Arrow into Parquet | 8,192-row batches; a row group is written at 262,144 rows or 64 MB buffered; the temporary file ≤ 32 GiB | `convert::BATCH_ROWS`, `ROW_GROUP_ROWS`, `MAX_BUFFERED_BYTES`, `MAX_CONVERTED_BYTES` |
| Arrow file metadata message / decompressed buffer | 64 MB / 1 GB | `convert::MAX_ARROW_MESSAGE_BYTES`, `MAX_ARROW_BUFFER_BYTES` |
| JSON export nesting | 64 levels | `export::MAX_JSON_DEPTH` |

## Responsiveness rules

- The UI thread only polls (`try_recv`). Nothing on it may scale with file size.
- Per-frame work is limited to visible rows × visible columns.
- Cancelled or replaced work stops within one 4,096-row batch.
- Repaint every 16 ms only while something is pending; idle means no repaint.

## Targets

Measured 2026-11-09 on the reference machine: Intel i5-11400H (12 threads), 16 GB RAM, SSD,
Windows 11, release build (thin LTO), warm OS file cache, median of 3 runs.
Files: `flights` (1M rows, 7 cols), `dummy_5m` (5M rows, 4 cols), `dummy_1b` (1B rows, 10 cols).

Each target is about 1.5× the slowest measured file, rounded up, with a 50 ms floor so
sub-frame operations aren't held to timer noise. A change that breaks a target needs a reason in the PR.

| Scenario | How measured | flights | dummy_5m | dummy_1b | Target |
|---|---|---|---|---|---|
| Read footer metadata | bench `open_metadata_ms` | 0 ms | 0 ms | 16 ms | < 50 ms |
| First page, all columns, empty cache | bench `first_page_all_cold_ms` | 8 ms | 3 ms | 19 ms | < 50 ms |
| Jump to the last page | bench `large_jump_ms` | 5 ms | 1 ms | 11 ms | < 50 ms |
| First batch of a `LIMIT` query | bench `query_first_batch_ms` | 7 ms | 3 ms | 15 ms | < 50 ms |
| Scroll 8 pages (32,768 rows) | bench `sequential_scroll_ms` | 66 ms | 30 ms | 144 ms | < 220 ms |
| App start to window, file given on the command line | `Start-Process` until the window exists | 177 ms | — | 76 ms | < 300 ms |
| App memory after loading a file | working set 5 s after start | 96 MB | — | 114 MB | < 200 MB |
| CLI open + 1,000-row preview (includes process start) | wall time | 167 ms | 167 ms | 237 ms | < 400 ms |
| Full scan: filter matching nothing on a text column | `cli --filter "category contains zzz"` | — | 536 ms | — | < 800 ms (≥ 6M rows/s) |
| Frame time while scrolling | not yet measured | | | | < 16 ms (60 fps) |
| Cancel a running scan | not yet measured | | | | < 50 ms |

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
