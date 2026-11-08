# 0003. Windowed grid for very large files

- Status: accepted
- Date: 2026-11-08 (recorded after the fact)

## Context
egui lays rows out in `f32` points, which lose integer precision past about 16.7 million points (about 600,000 rows at 28 px). Beyond that, rows overlap and the last rows of a big file cannot be reached.

## Decision
Files with more than `GRID_WINDOW_ROWS` (500,000) rows are shown through a moving 500,000-row table window. PAVI draws its own whole-file scrollbar and re-centres the window as the user scrolls, shifting the scroll offset so the view does not jump.

## Consequences
- Every row, including the last of a billion, is reachable.
- Selection, page requests and cell text must always use absolute row numbers, never table indexes.
- Filtered results (at most ~32k rows in memory) are never windowed.
