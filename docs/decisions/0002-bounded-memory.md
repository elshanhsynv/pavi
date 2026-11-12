# 0002. Every buffer is bounded

- Status: accepted; the sort input cap is amended by [0005](0005-external-sort.md)
- Date: 2026-11-08 (recorded after the fact)

## Context
PAVI must open files far larger than RAM (the 1-billion-row sample is 4.3 GB compressed). Any structure that grows with the file can crash the app.

## Decision
Every in-memory collection that can grow with file content has a named constant cap (pages, result windows, sort input, groups, profile values, chart rows, clipboard, history). Exceeding a cap returns a clear error that tells the user how to narrow the work (filter, LIMIT).

## Consequences
- Memory use is predictable on any file; see the table in `docs/PERFORMANCE.md`.
- Some operations refuse large inputs (e.g. sorting more than 100,000 rows without a LIMIT). Lifting that needs spill-to-disk, which would be a new ADR.
