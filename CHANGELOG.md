# Changelog

All notable changes to PAVI are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). The release workflow publishes the section that
matches the tag as the GitHub Release notes, so write entries for users.

## [Unreleased]

### Added
- CLI: a progress line on the terminal ("Counting: 13,064,960 of 1,000,000,000 rows (1%)") while opening, scanning, sorting, counting or profiling takes more than a second. It goes to standard error, is erased when the output appears, and never shows in pipes or logs; `--no-progress` turns it off.
- CLI: `--ascii` draws tables with `+ - |` for consoles and code pages that can't show box-drawing characters.
- CLI: `--profile` profiles every column (or `--columns`, up to 256) in one scan, as the app's Profile window does: rows, nulls, distinct values, min, max, mean and the most frequent value, in any format. With `--filter`, only matching rows are profiled.
- CLI: `--sql "SELECT …"` runs the app's SQL (WHERE, ORDER BY, LIMIT, COUNT/SUM/AVG/MIN/MAX with GROUP BY, joins and subqueries) and outputs the result in any format or to a file. The file is the table `dataset`, or its own name (`flights`); `--table [NAME=]PATH` adds other files to join, named as the app names its tabs.
- CLI: `--format csv`, `jsonl`, `parquet` or `arrow` writes the rows, and only the rows, to standard output, so the CLI works in pipelines. These formats stream every row unless `--head` is given; the table stays at most 1,000 rows.
- CLI: `--columns` takes column names as well as numbers, `--offset N` skips rows (or matching rows with `--filter`), `--count` prints only the number of (matching) rows, and `--quiet` leaves out the summary above the table.
- CLI: `--help` describes every option and the exit codes, and `--version` prints the version.
- CLI: `--sort "DEP_DELAY:desc,FL_DATE"` sorts by up to 8 columns, with `--filter`, `--offset`, `--head` and every format. Sorts larger than memory spill to temporary files, as in the app.
- CLI: `--schema` prints each column's index, name, type and nullability, and `--stats` prints the row-group statistics (rows, nulls, min, max, distinct count) from the file footer without reading any data. Both work with `--columns` and every `--format`.
- CLI: `-o`/`--output PATH` writes to a file instead of standard output, in the format its extension names (`.csv`, `.jsonl`, `.json`, `.parquet`, `.arrow`, `.feather`) unless `--format` is given. The file appears only once it is complete, so converting is `cli in.csv -o out.parquet`. The CLI refuses to overwrite the file it is reading.
- CLI: `-` reads standard input (Parquet, Arrow, JSON or CSV, told apart by their first bytes), e.g. `curl … | cli - --count`.

### Changed
- The command-line program is now `pavi-cli` (`pavi-cli.exe` in the release zip) instead of `cli`.
- The table's `#` column counts from `--offset`, so it shows row numbers in the file (or match numbers with `--filter`).
- The CLI summary shows column types as `Int64` or `Timestamp(µs, "UTC")` instead of Rust's debug form.

## [0.3.0] - 2026-11-12

### Added
- Filters can combine tests with `AND`, `OR` and brackets, in the filter box and in SQL `WHERE`.
- New filter tests: `IN (…)`, `BETWEEN … AND …`, `LIKE` with `%` and `_`, `IS NULL`, and their `NOT` forms. The filter box also accepts `=` and `<>`.
- Cell menu: "Add to filter" narrows the filter already applied instead of replacing it, and a null cell can be filtered to or away from nulls.
- Find in the loaded rows (Ctrl+Shift+F): Enter or F3 moves to the next match, Shift+Enter or Shift+F3 to the previous one.
- Pin columns: right-click a header and choose "Pin columns up to here". Pinned columns and the row numbers stay in place when the grid scrolls sideways. The pin is saved with the column layout.
- The Inspector has a Row section listing every shown column of the selected row; click a line to select that cell.
- Decimal and time-of-day columns can be filtered (`price > 12.50`, `at < 08:30:00`), sorted, grouped, profiled and charted, and used in MIN/MAX. Decimals also work with SUM and AVG.
- A list, struct or map cell opens as a tree in the Inspector.
- Sort by several columns: right-click a header and choose "Then by", or write `ORDER BY a DESC, b` in SQL.
- The SQL editor highlights keywords, strings, numbers and column names, and completes column names and keywords as you type (Tab or Enter takes a suggestion).
- Save SQL queries under a name from the new Saved menu, and load or delete them later. They are kept with the session.
- SQL `GROUP BY` takes several columns, e.g. `SELECT carrier, origin, COUNT(*) FROM dataset GROUP BY carrier, origin`.
- Open a folder (Ctrl+Shift+O, the file menu, or drop it on the window) to read every Parquet file in it as one table. Folders named `key=value`, as Spark, DuckDB, Polars and pyarrow write them, become columns, and filters on them skip the other folders' files. The CLI opens folders too.
- Open CSV, TSV, JSON (one object per line, or an array) and Arrow/Feather files. Column types are guessed from the first 10,000 records; the file is read into a temporary Parquet file while it opens, with progress in the status bar, so filters, sorts, SQL and charts work on it as on Parquet.
- Export to JSON Lines and Arrow, besides CSV and Parquet.
- SQL can join open tabs and use subqueries: every tab is a table named after its file (`dataset` is still the tab you are in), e.g. `SELECT f.ORIGIN, a.name FROM dataset f JOIN airports a ON f.ORIGIN = a.code`. `INNER` and `LEFT` joins on equal columns, `FROM (SELECT …) AS t`, `WHERE x [NOT] IN (SELECT …)` and `WHERE x > (SELECT AVG(…) …)` work. One side of a join, after its filters, must fit in memory (1,000,000 rows).
- Open remote files by URL: `https://…`, `s3://bucket/key`, `gs://bucket/key` or `az://account/container/blob`, in the file menu's path box or on the command line. Parquet files are read in place with range requests, so only the parts you look at are downloaded. Credentials come from the usual environment variables (`AWS_ACCESS_KEY_ID`, `~/.aws/credentials`, `GOOGLE_OAUTH_ACCESS_TOKEN`, `AZURE_STORAGE_SAS_TOKEN`); pre-signed URLs work as they are.
- Tabs: each file or folder opens in its own tab with its own filter, sort, SQL and layout. Ctrl+T opens a tab, Ctrl+W closes one, Ctrl+Tab moves between them. The tabs open when PAVI closes are reopened next time.
- Charts can draw up to six Y columns at once, each in its own colour with a legend. Axes have round ticks, names, shortened numbers (12k, 3.4M) and date or time labels.
- Save a chart as PNG (as on screen) or SVG (960 × 540) from the Charts window.
- Profile all columns at once: the Inspector's Schema section has "Profile all columns", which shows nulls, distinct values, min, max and mean of every column in one scan.
- Compare two open tabs (file menu ▸ Compare with another tab): see the columns only one has or with another type, and list the rows whose key is only in one tab, or in both.
- A light theme: file menu ▸ Theme ▸ Light, or System to follow the operating system's setting, also when it changes while PAVI runs. The choice is kept with the session.
- macOS and Linux: PAVI uses a system font (Arial on macOS, Noto Sans or DejaVu Sans on Linux) and keeps the session in the platform's own folder (`~/Library/Application Support/PAVI`, or `$XDG_STATE_HOME/pavi`, by default `~/.local/state/pavi`) instead of the temp folder. On macOS the window keeps the system title bar, so it can be resized.

### Changed
- Line charts join their points from left to right instead of in the order the rows came in.
- Sorting sorts the whole file instead of showing only the first 100,000 rows. A sort too large for memory writes temporary files (up to 32 GB) and may take a while.
- Copying cells copies their full values instead of the first 256 characters, quotes values that contain a tab, line break or quote, and allows up to 16 MB instead of 64 KB.
- In the filter box, quote a value that contains `AND`, `OR`, a comma or brackets, e.g. `title == "war and peace"`. Unquoted, `and` / `or` now join two tests.
- Opening a file while another is shown opens it in a new tab instead of replacing it. Dropping several files opens each of them.
- Filtering, sorting and aggregating are faster on files and folders with many small row groups: consecutive row groups of a file are read together instead of one at a time.

### Fixed
- The window can be resized by dragging its edges and corners.
- Files with a list, struct or map column now show row-group statistics for their other columns, and filters on those columns skip row groups again.
- CLI: piping the output into a command that stops reading early (`cli file.parquet | head`) no longer crashes.
- CLI: an invalid `--filter` is reported before anything is printed.
- CLI: the preview table stays aligned with long column names, Chinese/Japanese/Korean text and emoji, and values with line breaks or tabs (shown as `\n` and `\t`). Numbers are right-aligned, and "1 rows" now reads "1 row".

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

[Unreleased]: https://github.com/elshanhsynv/pavi/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/elshanhsynv/pavi/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.2.0
[0.1.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.1.0
