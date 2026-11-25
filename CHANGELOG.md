# Changelog

All notable changes to PAVI are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). The release workflow publishes the section that
matches the tag as the GitHub Release notes, so write entries for users.

## [Unreleased]

### Added
- SQL window functions: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG(x, n, default)`, `LEAD(…)` and `COUNT` / `SUM` / `AVG` / `MIN` / `MAX … OVER (PARTITION BY … ORDER BY …)`, with running totals, `ROWS BETWEEN n PRECEDING AND CURRENT ROW` moving frames and whole-partition totals (`x / SUM(x) OVER ()`). They work on files of any size: rows are sorted on disk if needed, and no partition is held in memory.
- SQL: `MEDIAN(x)`, `QUANTILE_CONT(x, 0.9)` and `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY x)`, and `STDDEV` / `STDDEV_POP` / `VARIANCE` / `VAR_POP`, also per group and on calculations.
- SQL: `REPLACE(text, from, to)`; `DATE_TRUNC('month', day)`; `DATE_DIFF('day', start, end)`; `day + INTERVAL '7 days'` (or `- INTERVAL 1 MONTH`); `NOW()`, `CURRENT_TIMESTAMP` and `CURRENT_DATE`; and `DATE '2024-03-01'` / `TIMESTAMP '…'` values.
- SQL: `UNION` and `UNION ALL` stack the rows of several queries (column types widen to fit both sides), and `WITH name AS (SELECT …)` names a subquery to use in the query.
- Find (Ctrl+Shift+F) looks through the whole file. Enter, F3 and the arrows check the loaded rows first, then search the rest of the file in the background, from the selected cell forward or back, and jump to the next match. The find bar shows how far it has got, and a new search or closing the bar stops it.
- `* contains text` in the filter box (and the CLI's `--filter`) keeps the rows where any column contains the text, ignoring case, e.g. `* contains boston AND year > 2020`. The find bar's new "Show matching rows" button applies it.
- Open Excel and OpenDocument workbooks (`.xlsx`, `.xlsm`, `.xlsb`, `.xls`, `.ods`), a sheet at a time: the first one, or one chosen in the Import options window or with `--sheet`. Columns get their types from the cells (numbers, whole numbers, booleans, dates and times), and the first row names them unless you say otherwise.
- Export to Excel (`.xlsx`): from the Export menu, or with `-o result.xlsx` / `--format xlsx` in the CLI. Numbers, booleans and dates stay numbers, booleans and dates in Excel. A sheet holds at most 1,048,575 rows; larger results need CSV or Parquet.
- Folders whose files have different columns open as one table. Columns are matched by name; a column some files lack is empty (null) in their rows; a column stored as different types is read as one that holds both (`Int32` and `Int64` as `Int64`, an integer and a float as `Float64`, text types as large text, decimals with enough digits). Columns whose types can't be combined (say, a number in one file and text in another) still stop the open, with a message naming both files.
- Import options for CSV and JSON files: the delimiter, whether the first line holds the column names, the text encoding (UTF-8, UTF-16 as Excel's "Unicode text" saves it, or Windows-1252), and reading every column as text (to keep values such as `007`). In the app: File menu → Open with import options…, the Inspector of a CSV or JSON tab, or the screen of a file that couldn't be opened; the choice is remembered for that file. In the CLI: `--delimiter`, `--no-header`, `--encoding` and `--all-text`. The CLI summary and the Inspector say how the file was read.
- PAVI notices when the open file changes on disk, or when files are added to, removed from or rewritten in an open folder. A bar above the grid says what changed and offers Reload. Remote files are not checked.

### Changed
- Reload (Ctrl+R) keeps the tab's filter and sort.

### Fixed
- A CSV or JSON file whose value after the first 10,000 records doesn't fit the type guessed from them (text in a number column, a fraction among whole numbers) now opens: the column types are chosen again from every value. Before, the open stopped with an error.
- A JSON field that first appears after the first 10,000 records is no longer left out.
- Files in UTF-16 or Windows-1252 open with their text intact instead of failing or showing garbled characters, and a UTF-8 byte order mark no longer ends up in the first column's name.
- Reading a Parquet file that another program rewrote after PAVI opened it no longer mixes the old file's layout with the new bytes, which could fail with a confusing error or show wrong rows. The read stops with a message that says the file changed; Reload reads it as it is now. The CLI does the same.
- CLI: a table wider than the terminal no longer wraps into an unreadable mess. Its widest columns are narrowed (down to 10 characters) until it fits; if that isn't enough, middle columns are left out and shown as one `…` column, and the title says how many columns are shown. Tables written to a pipe or a file keep every column.
- CLI: floating-point numbers in the table read as people write them: `0.009` instead of `0.009000000000000001`, and `1e-20` instead of twenty zeros. CSV, JSON and the other formats keep the full value.

### Security
- Updated Apache Arrow and Parquet to 60.2.0 for RUSTSEC-2026-0595 (bloom filters written unencrypted in encrypted Parquet files). PAVI doesn't write encrypted files, so it wasn't affected, but the dependency check flagged the old version.

## [0.7.1] - 2026-11-22

### Changed
- `GROUP BY`, `COUNT(DISTINCT …)` and other aggregates are 30–60% faster: each row's group is found without copying its key, and a query without `GROUP BY` skips the group lookup. Results are unchanged.
- Exports from the app are much faster (about 20× for Arrow and 4–5× for CSV on the sample files), and so are column profiles and the CLI's `--sql` output: they no longer pause 5 ms each time they ask for the next 4,096 rows.
- CSV, TSV and JSON Lines export is about twice as fast, and column profiles are about 40% faster: each column's values are turned into text with one prepared formatter per batch instead of one per value. The text written is unchanged.
- Sorting large files is about 4× faster. A sort keeps up to about a million rows in memory (still within 128 MB) before it writes to disk, so a sort of a few million rows needs no extra merge pass, and the final merge sorts each row once. A sort by a column of a 5-million-row file now shows its first rows in about 1 second instead of 4. A large `LIMIT` whose rows don't fit in memory now spills to disk instead of failing.
- Counting the rows that match a filter is faster. Row groups whose footer statistics show that every row matches are counted without being read (on a sorted or clustered column, most of the file). In the CLI, `--count` with a filter also reads the rest on all CPU cores. The app's match count uses the statistics too.
- Exports, conversions and other whole-file reads are faster: a file is read with one open reader instead of reopening it every 4,096 rows. In the app this speeds up exporting, scrolling through filter and SQL results, and joins; in the CLI, `--format csv/jsonl/parquet/arrow`, `--count` with a filter, `--profile` and `--sql`.

### Fixed
- An export or `pavi-cli -o` that was stopped part-way no longer leaves its unfinished `<name>.pavi-….tmp` file next to the target for good: the next export to the same file removes it.
- A large sort stopped with Ctrl+C (or a crash) no longer leaves its temporary files, up to 32 GB, on disk for a day: they are removed the next time `pavi-cli` runs or the app starts.

### Security
- Updated the `url` dependency to 2.5.9 for RUSTSEC-2026-0592 (a stack overflow on deeply nested `blob:` URLs).

## [0.7.0] - 2026-11-22

### Added
- CLI: shell completions. `pavi-cli --completions bash` (or `zsh`, `fish`, `powershell`, `elvish`) prints a script that completes options and their values; `pavi-cli --help` says where to save it.
- CLI: a manual page. `pavi-cli --man > pavi-cli.1` writes it; the Linux release includes it as `man/man1/pavi-cli.1`.
- Releases include the completion scripts: PowerShell in the Windows zip, bash, zsh and fish in the Linux archive.

## [0.6.0] - 2026-11-22

### Added
- Releases include a Linux x64 build (`pavi-X.Y.Z-linux-x64.tar.gz`) besides the Windows zip.
- CLI: short flags `-c` (`--columns`), `-f` (`--filter`), `-s` (`--sort`) and `-n` (`--head`).

### Changed
- CLI: `pavi-cli -h` groups its options (Input, Choosing rows, Output, Facts instead of rows, SQL), gives each a one-line description with an example, wraps to the terminal and shows a few examples; `pavi-cli --help` adds the details, more examples, the exit codes and how colours work.
- CLI: help and the "Error:" label are coloured on a terminal. Colours are left out in pipes and files, and `NO_COLOR=1` turns them off.

## [0.5.0] - 2026-11-21

### Added
- PAVI keeps a log of what it did and what failed (File menu → Open log folder). When the app crashes it writes a crash report, and the next start says so.
- Cancelling the open or a read of a remote file stops at once, instead of waiting up to minutes for a slow or silent server.
- macOS is built and tested in CI.
- Tabs reopen with the filter and sort they had when the app closed.
- Number columns can show a fixed number of decimals and thousands separators (header right-click → Number format). Copy and export keep every digit.
- "Copy as" in the cell menu: the selected cells as a Markdown table, as JSON, or as a SQL `IN (…)` list of their distinct values.
- "Count values" in the header menu counts each value of a column, most frequent first.
- Zoom the interface with Ctrl+= / Ctrl+- / Ctrl+0 or from the file menu; the zoom is remembered.
- A list of keyboard shortcuts (F1, or the file menu).
- Drag tabs to reorder them, or move the open tab with Ctrl+Shift+PgUp / PgDn.
- "Clear recent files" in the file menu.
- Filters skip data pages, not only row groups: a lookup in a sorted or clustered column reads one page of each row group (`user_id == 123` on a billion rows: 2 s instead of 15 s), using the page index most Parquet writers include. An `==` test also skips row groups whose bloom filter says the value is absent, in files that have bloom filters.
- More column types can be filtered, sorted, grouped, profiled and used in MIN/MAX and SQL: dictionary-encoded columns (pandas categoricals), which now work like their values everywhere; durations, written as the grid shows them (`wait >= PT1M30S`) or as a number in the column's unit; binary, as hex (`hash == 0x6869`); and Decimal256 columns of up to 38 digits.
- SQL calculations: arithmetic (`price * qty`), `||`, comparisons and `AND`/`OR`/`NOT`, `CAST`/`TRY_CAST`, `CASE`, and the functions `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTRING`, `CONCAT`, `ABS`, `ROUND`, `COALESCE`, `YEAR`, `QUARTER`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DAYOFWEEK` and `EXTRACT`. They work in SELECT, WHERE (`WHERE a > b`, `WHERE NOT (…)`), GROUP BY (`GROUP BY YEAR(day)`), ORDER BY, HAVING, inside aggregates (`SUM(price * qty)`) and on aggregates' results (`ROUND(AVG(delay), 2)`, `SUM(a) / COUNT(*)`). In a plain SELECT they cost nothing extra; used to filter, group or sort, the rows are prepared first, as for a join.
- SQL: `ORDER BY` an aggregate's results (`ORDER BY COUNT(*) DESC`, an alias, or a position such as `ORDER BY 2`), `HAVING` (e.g. `HAVING COUNT(*) > 100`), `SELECT DISTINCT`, `COUNT(DISTINCT column)`, and `AS` aliases, which name the result's columns. `GROUP BY` without aggregates lists each value once.
- Filter, SQL and sort results can be entered anywhere: "Go to row", Home, End, the arrow keys and a new scrollbar for the whole result reach any row, and scrolling up brings back rows scrolled past. A filter's matches are counted in the background ("rows 1–4,096 of 999,999,989"), after which a jump only reads the row group that holds the row, even in a billion-row file.
- CLI: `--sql` shows the progress line too: how much of a join or subquery is prepared, then the rows scanned.
- CLI: `--profile` reports text lengths (`min_length`, `max_length`, `mean_length`, in characters) for text columns.
- CLI: `--input-format csv|json|arrow|parquet` reads a file whatever its extension says (a CSV saved as `.txt`), or standard input whatever its first bytes suggest.
- CLI: `--format tsv` (and `-o out.tsv`) writes tab-separated text.

### Changed
- SQL: SELECT may list GROUP BY columns in any order, or leave them out (`SELECT COUNT(*) FROM dataset GROUP BY carrier`).
- CLI: with `--filter`, the table's `#` column shows each row's number in the file, not its place among the matches. With `--sort` or `--sql` it still counts the result.

### Fixed
- The arrow keys, Page Up/Down, Home and End scroll the selected cell into view.
- Text wider than its column ends in "…" instead of being cut off; numbers keep their first digits instead of losing them to the left edge.
- CLI: a null cell in the table shows as `∅` (`NULL` with `--ascii`), so it can't be mistaken for the text "null".
- CLI: the table stays aligned with emoji that have a skin tone, emoji made of several (👨‍👩‍👧), flags and accents typed as separate marks, and never cuts them in half.

## [0.4.0] - 2026-11-14

This release turns the command-line tool into a full companion to the app: it streams, converts, sorts, queries and profiles files in scripts and pipelines. **The program is renamed from `cli` to `pavi-cli`**; scripts that call it need the new name. Run `pavi-cli --help` for every option.

### Added
- CLI: `--format csv`, `jsonl`, `parquet` or `arrow` writes the rows, and only the rows, to standard output, so the CLI works in pipelines. These formats stream every row unless `--head` is given; the table stays at most 1,000 rows.
- CLI: `-o`/`--output PATH` writes to a file instead of standard output, in the format its extension names (`.csv`, `.jsonl`, `.json`, `.parquet`, `.arrow`, `.feather`) unless `--format` is given. The file appears only once it is complete, so converting is `pavi-cli in.csv -o out.parquet`. The CLI refuses to overwrite the file it is reading.
- CLI: `-` reads standard input (Parquet, Arrow, JSON or CSV, told apart by their first bytes), e.g. `curl … | pavi-cli - --count`.
- CLI: `--sql "SELECT …"` runs the app's SQL (WHERE, ORDER BY, LIMIT, COUNT/SUM/AVG/MIN/MAX with GROUP BY, joins and subqueries) and outputs the result in any format or to a file. The file is the table `dataset`, or its own name (`flights`); `--table [NAME=]PATH` adds other files to join, named as the app names its tabs.
- CLI: `--sort "DEP_DELAY:desc,FL_DATE"` sorts by up to 8 columns, with `--filter`, `--offset`, `--head` and every format. Sorts larger than memory spill to temporary files, as in the app.
- CLI: `--profile` profiles every column (or `--columns`, up to 256) in one scan, as the app's Profile window does: rows, nulls, distinct values, min, max, mean and the most frequent value, in any format. With `--filter`, only matching rows are profiled.
- CLI: `--schema` prints each column's index, name, type and nullability, and `--stats` prints the row-group statistics (rows, nulls, min, max, distinct count) from the file footer without reading any data. Both work with `--columns` and every `--format`.
- CLI: `--columns` takes column names as well as numbers, `--offset N` skips rows (or matching rows with `--filter`), `--count` prints only the number of (matching) rows, and `--quiet` leaves out the summary above the table.
- CLI: a progress line on the terminal ("Counting: 13,064,960 of 1,000,000,000 rows (1%)") while opening, scanning, sorting, counting or profiling takes more than a second. It goes to standard error, is erased when the output appears, and never shows in pipes or logs; `--no-progress` turns it off.
- CLI: `--ascii` draws tables with `+ - |` for consoles and code pages that can't show box-drawing characters.
- CLI: `--help` describes every option and the exit codes, and `--version` prints the version.

### Changed
- The command-line program is now `pavi-cli` (`pavi-cli.exe` in the release zip) instead of `cli`.
- A number in a filter is a column's name first: `2023 > 5` now means the column named `2023` if there is one, and the column at position 2023 only if there isn't (see Fixed).
- The table's `#` column counts from `--offset`, so it shows row numbers in the file (or match numbers with `--filter`).
- The CLI summary shows column types as `Int64` or `Timestamp(µs, "UTC")` instead of Rust's debug form.

### Fixed
- Filters and SQL `WHERE` on a column whose name is a number (such as `2023`) use that column. A bare number in a filter was always read as a column position, so such a column couldn't be filtered and SQL could read the wrong column.

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

[Unreleased]: https://github.com/elshanhsynv/pavi/compare/v0.7.1...HEAD
[0.7.1]: https://github.com/elshanhsynv/pavi/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/elshanhsynv/pavi/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/elshanhsynv/pavi/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/elshanhsynv/pavi/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/elshanhsynv/pavi/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/elshanhsynv/pavi/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.2.0
[0.1.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.1.0
