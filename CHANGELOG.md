# Changelog

All notable changes to PAVI are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). The release workflow publishes the section that
matches the tag as the GitHub Release notes, so write entries for users.

## [Unreleased]

### Changed
- Upgraded Apache Arrow and Parquet to 60.1.0. Files with a corrupt embedded Arrow schema still open, using the Parquet schema.
- Release builds no longer open a console window next to the app.

### Fixed
- A malformed Parquet file shows a "malformed Parquet data" error instead of crashing the CLI or a worker thread.

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

[Unreleased]: https://github.com/elshanhsynv/pavi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/elshanhsynv/pavi/releases/tag/v0.1.0
