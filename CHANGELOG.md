# Changelog

All notable changes to PAVI are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). The release workflow publishes the section that
matches the tag as the GitHub Release notes, so write entries for users.

## [Unreleased]

### Added
- Filters can combine tests with `AND`, `OR` and brackets, in the filter box and in SQL `WHERE`.
- New filter tests: `IN (…)`, `BETWEEN … AND …`, `LIKE` with `%` and `_`, `IS NULL`, and their `NOT` forms. The filter box also accepts `=` and `<>`.
- Cell menu: "Add to filter" narrows the filter already applied instead of replacing it, and a null cell can be filtered to or away from nulls.
- Find in the loaded rows (Ctrl+Shift+F): Enter or F3 moves to the next match, Shift+Enter or Shift+F3 to the previous one.
- Pin columns: right-click a header and choose "Pin columns up to here". Pinned columns and the row numbers stay in place when the grid scrolls sideways. The pin is saved with the column layout.
- The Inspector has a Row section listing every shown column of the selected row; click a line to select that cell.

### Changed
- Copying cells copies their full values instead of the first 256 characters, quotes values that contain a tab, line break or quote, and allows up to 16 MB instead of 64 KB.
- In the filter box, quote a value that contains `AND`, `OR`, a comma or brackets, e.g. `title == "war and peace"`. Unquoted, `and` / `or` now join two tests.

### Fixed
- The window can be resized by dragging its edges and corners.

## [0.2.0] - 2026-11-10

### Added
- Drop a Parquet file on the window to open it.
- Ctrl+G jumps to the "Go to row" box.
- Sort menu in the top bar.
- A running filter, sort or SQL query shows how much of the file it has scanned, in the status bar and in the grid.
- Right-click a cell to filter to its value, exclude its value, or copy the cell or row.
- Right-click a column header to sort ascending or descending, profile the column, fit its width, hide it or copy its name.

### Changed
- Redesigned interface: one top bar (file menu, Grid / SQL switch, always-visible filter box, Sort, Columns and Export menus), a denser grid with striped 24 px rows, right-aligned numbers and colored type labels, a collapsible Inspector, and a status bar that shows the file's row, column and row-group counts.
- The path box and recent files moved into the file menu; jump-to-row moved to the status bar; column layout moved to the Columns menu; query history moved to the SQL workspace. The separate dataset bar and "Dataset tools" window are gone.
- The start screen offers Browse and the most recent files.
- Text uses Segoe UI on Windows.
- The Charts window uses the same controls as the rest of the app: switches for the source and chart type, and a column picker next to each column name.
- Clicking a column header selects the column and shows it in the Inspector instead of starting a sort.
- Sorting a large file no longer fails with a budget error: it shows the first 100,000 rows in sort order and says so.
- The status bar shows a spinner while a filter or sort is scanning.
- On narrow windows the Sort, Columns and Export buttons show only their icons, leaving room for the filter box.
- Upgraded Apache Arrow and Parquet to 60.1.0. Files with a corrupt embedded Arrow schema still open, using the Parquet schema.
- Release builds no longer open a console window next to the app.

### Fixed
- A filter on a 32-bit float column now matches the value the grid shows, e.g. `DEP_TIME == 8.916667`.
- A file opened once by a relative path and once by its full path is listed once under recent files.
- A malformed Parquet file shows a "malformed Parquet data" error instead of crashing the CLI or a worker thread.
- A crafted Parquet footer can no longer keep a file open busy for seconds to hours; such files are rejected immediately.
- A crafted Parquet file can no longer make PAVI try to allocate gigabytes (and close) through an oversized dictionary page or schema; it shows an error instead.

## [0.1.0] - 2026-11-08

First tracked version.

### Added
- Native Windows explorer for Parquet files: virtualized grid that reaches every row of files with billions of rows, column resizing, hiding and reordering, frozen headers, and cell selection with copy.
- Filters, a SQL subset (`SELECT … WHERE … ORDER BY … LIMIT`, `COUNT/SUM/AVG/MIN/MAX` with `GROUP BY`), click-to-sort, and query history.
- Charts, column profiling, and an Inspector with schema, file properties and row-group statistics.
- Streaming CSV and Parquet export, session restore and recent files.
- `cli` preview tool and `pavi-bench` benchmark.

### Changed
- Filtered results resume from a cursor, and `ORDER BY … LIMIT n` keeps only the top rows, so both work on files of any size.
- Long scans stop within one batch when cancelled; off-screen page reads are cancelled.
- One shared value formatter and one set of type rules across grid, CLI, CSV, profiling, sort, aggregates and statistics.

### Fixed
- Unsigned statistics, exact integer filters, the opened path, and window selection.

### Security
- Updated zstd to fix advisories reachable from crafted Parquet files; dependency checks with `cargo deny`.
- Worker threads survive decoder panics; inputs are fuzzed.

[Unreleased]: https://github.com/elshanhsynv/pavi/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.2.0
[0.1.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.1.0
