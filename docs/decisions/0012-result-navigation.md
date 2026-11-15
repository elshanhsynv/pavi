# 0012. Results are entered anywhere: counted filters start at a row group, others read on or again

- Status: accepted
- Date: 2026-11-15
- Amends: [0003](0003-windowed-grid.md) ("Filtered results … are never windowed")

## Context
A filter, SQL or sort result keeps only its newest 8 batches (about 32,000 rows, `MAX_FILTERED_BATCHES`) and drops older ones as more arrive. That bound stays. But the grid could only move forward through a result: it never said how many rows matched, rows scrolled past were gone, and "Go to row", Home and End only reached rows already held. On a filter matching hundreds of millions of rows, the end of the result was out of reach.

## Decision
A result can be entered at any row; only the rows around it are held.

- **Counting.** A filter (from the filter box or a cell menu, sorted or not) starts a second background task when it starts: `ParquetSource::count_matches_by_row_group` on the worker pool (`RuntimeHandle::submit_match_counts`). It reads only the filter's columns, skips the row groups the footer statistics rule out, stops between batches when cancelled, and returns one count per row group. Its memory is one number per row group of the file, so it needs no cap of its own. The running totals give the result's size, and the row group that holds each result row. A sort of the whole file knows its size without counting. A SQL result's size is known when it has been read to the end.
- **Jumping** to result row `n` keeps `JUMP_CONTEXT_ROWS` (2,048) rows above it, and, in order of cost:
  1. the rows held: just scroll there;
  2. a counted, unsorted filter: start a new filtered scan (`QueryEngine::execute_from_row`) at the first row of the row group that holds row `n − 2,048`. Rows before that group are never read; at most one row group is read and dropped before the first row kept;
  3. ahead of the rows read so far: keep reading and drop rows until then;
  4. anything else (back in a SQL or sort result, or a filter not counted yet): run the query again and drop rows until then. A sort or a join is redone.
  A jump uses a new grid generation, so batches still arriving for the old position are dropped.
- **In the grid**, a result larger than the rows held gets PAVI's own whole-result scrollbar (the one windowed files use, ADR 0003); dragging it jumps. Scrolling up past the first row held, the arrow keys, Page Up/Down, Home, End and "Go to row" all jump the same way. The strip above the grid says "rows a–b of N", or "of at least N · counting" until the count is in, and the status bar shows the result's size.

## Consequences
- Every row of a filter result is reachable in about the time of reading one row group, once the count is in. The count costs one more scan of the filter's columns, running beside the filtered read. While it runs, a far jump reads up to the target.
- A jump back in a SQL or sort result reruns the query; on a large sort or a join that is slow, with the usual progress. A rerun to jump doesn't add another query history entry.
- Selections still use absolute result rows, as ADR 0003 asks. The selected row survives a jump only as the jump's target.
- The 32,000-row bound on held rows is unchanged.
