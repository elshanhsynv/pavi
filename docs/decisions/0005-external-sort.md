# 0005. Sorts larger than memory spill to temporary files

- Status: accepted
- Date: 2026-11-12
- Amends: [0002](0002-bounded-memory.md) (the sort input cap)

## Context
ADR 0002 caps every in-memory buffer. For sorting, that cap (100,000 rows / 128 MB) meant a sort of a large file either failed or, in the GUI, showed only the first 100,000 rows in sort order. Users expect a sort to cover the whole file, and PAVI's files are often far larger than RAM.

## Decision
A sort whose input does not fit the in-memory sort budget is done as an external merge sort, entirely inside `parquet_reader` and with no new dependencies:

1. The scan collects rows up to the budget, sorts them (keys, then a sequence number so equal keys keep file order), and writes them as one *run*: an uncompressed Parquet file in a per-sort directory under the system temp folder.
2. When there are more than 16 runs, groups of 16 are merged into longer runs until at most 16 remain.
3. The remaining runs are merged a page at a time, only as the grid or an export asks for rows. One merge step holds at most one 4,096-row batch per run.

Memory stays bounded by the same sort budget. Disk use is bounded by a new named cap, `MAX_SPILL_BYTES` (32 GiB); a sort that needs more fails with a clear error naming the cap and the way out (filter or LIMIT). The spill directory is deleted when the sort is finished, replaced or cancelled. Directories a crash left behind are removed by the next sort that spills, once they are more than a day old.

`ORDER BY … LIMIT n` with `n` within the budget keeps the in-memory top-N path: it never touches disk.

## Consequences
- Sorting works on files of any size, with memory still predictable.
- A large sort writes roughly its projected input to disk, in up to log₁₆(runs) passes, so it is slow and needs free disk space; progress shows while the scan runs.
- Temporary files are a new failure mode (disk full, temp folder not writable). These surface as ordinary sort errors.
- Grouped aggregates are unchanged: they still have an in-memory group budget and no spill.
