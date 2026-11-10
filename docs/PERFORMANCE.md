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
| Sort input (no LIMIT) | 100,000 rows / 128 MB | `SortBudget::DEFAULT_MAX_ROWS`, `DEFAULT_MAX_BYTES` |
| Rows a GUI sort returns | first 100,000 in sort order | `pavi::app::SORT_ROW_CAP` (= `SortBudget::DEFAULT_MAX_ROWS`) |
| GROUP BY | 10,000 groups / 8 MB | `GroupBudget::DEFAULT_MAX_GROUPS`, `DEFAULT_MAX_BYTES` |
| Profile distinct values | 4,096 values / 1 MB, 128 bytes each | `profile::MAX_DISTINCT_*` |
| Chart input | 16,384 rows | `chart::MAX_INPUT_ROWS` |
| Clipboard copy | 64 KB | `MAX_CLIPBOARD_BYTES` |
| Grid row window | 500,000 rows | `state::GRID_WINDOW_ROWS` |
| Dictionary page (uncompressed) | 256 MB; also never more values than its bytes can hold | `MAX_DICTIONARY_PAGE_BYTES` in `crates/parquet/src/source.rs` |
| Query history | 50 entries / 256 KB | `session::MAX_HISTORY_*` |

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
