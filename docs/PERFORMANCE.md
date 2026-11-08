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
| GROUP BY | 10,000 groups / 8 MB | `GroupBudget::DEFAULT_MAX_GROUPS`, `DEFAULT_MAX_BYTES` |
| Profile distinct values | 4,096 values / 1 MB, 128 bytes each | `profile::MAX_DISTINCT_*` |
| Chart input | 16,384 rows | `chart::MAX_INPUT_ROWS` |
| Clipboard copy | 64 KB | `MAX_CLIPBOARD_BYTES` |
| Grid row window | 500,000 rows | `state::GRID_WINDOW_ROWS` |
| Query history | 50 entries / 256 KB | `session::MAX_HISTORY_*` |

## Responsiveness rules

- The UI thread only polls (`try_recv`). Nothing on it may scale with file size.
- Per-frame work is limited to visible rows × visible columns.
- Cancelled or replaced work stops within one 4,096-row batch.
- Repaint every 16 ms only while something is pending; idle means no repaint.

## Targets

Proposed, not yet enforced in CI. Measure with the bench below and adjust.

| Scenario | Target |
|---|---|
| Open any file (footer only) | < 300 ms to first rows on a local SSD |
| Scroll / jump to any row | Page visible < 100 ms after it scrolls into view |
| Frame time while scrolling | < 16 ms (60 fps) |
| Cancel a running scan | < 50 ms |
| Peak memory, any file | < 1 GB |

## Measuring

```bash
cargo run --release -p pavi_bench -- data/dummy_5m.parquet
```

Prints CSV-like timings and cache metrics for open, page reads and a scan query. Pass a second argument to set the number of sequential pages.
Always use `--release`. Compare against `main` before and after a change that touches reading, the runtime, the query engine or the grid, and put the numbers in the PR.

Sample files are not in git. `create_dummy_data.py` (needs `polars`) writes `dummy_1b.parquet`; change `N` and `OUTPUT` for smaller files.
