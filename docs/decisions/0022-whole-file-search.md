# 0022. Find looks through the whole file, and `* contains` filters by text in any column

- Status: accepted
- Date: 2026-11-25

## Context
The find bar (Ctrl+Shift+F) looked only at the rows loaded in memory: at most eight 4,096-row pages of the source, or a result's window. On anything but a small file, "Not in the loaded rows" was the usual answer. Its hint said to filter instead, but filters test one named column. A value that could be in any of thirty columns needed thirty `OR`ed tests.

## Decision
- **One matcher.** `value::TextNeedle` and `value::text_matches` decide whether a cell matches. The cell's text as the grid shows it (the shared value text, `ScalarTexts`; nested values through `format_cell`) must contain the needle, ignoring case. Nulls never match. An ASCII needle is compared byte by byte without allocating. A column whose type can never show the needle (letters in a number column, anything but `true`/`false` text in a boolean one, letters in a date) is skipped without formatting its values.
- **Find next across the file.** Enter, F3 and the arrows look at the loaded rows first, which is instant. When the grid shows the file itself (no filter, sort or SQL result) and the loaded rows don't hold the text, `ParquetSource::find_text` runs on a worker (`RuntimeHandle::submit_find_text`). It starts at the selected cell and goes forward (or back) through the whole file, wrapping around once, and stops at the first matching cell. Only the visible columns are read, in display order, [`FIND_WINDOW_ROWS`] (16,384) rows at a time, with a progress and cancellation check between windows. The find bar shows the share searched. A new query, closing the bar, or leaving the file cancels the search; a result for another file or column layout is dropped.
- **`* contains text`.** In the filter grammar (the filter box and the CLI's `--filter`), `*` as the column means every column, with `contains` only. It's `FilterExpr::AnyContains`, which reads every column and ORs `text_matches` over them. Statistics, page indexes and bloom filters can't rule row groups in or out for it (they hold values, not text), so it scans. It combines with other tests (`* contains Boston AND year > 2020`) and goes through the ordinary filter path: streamed results, the match count, jumping, sorting and export. SQL's `WHERE` doesn't take it.
- **Show matching rows.** The find bar's button applies `* contains <text>`, joined with `AND` to the filter already applied. This is how to search inside a filter, sort or SQL result: find there still looks only at the loaded rows, because the result's rows are not file rows to scan.

## Consequences
- Find works on files of any size. On a file whose matches are rare it scans until it finds one, at the speed text can be formatted (slower than a numeric filter), and can be stopped at any time.
- `* contains` costs a full scan of every column; it can't skip anything.
- Matching on the shown text means numbers match as written in the grid (`* contains 7` finds 17 and 70), not by value. Number formats set on a column (decimals, thousands separators) are not applied when searching.
- The filter grammar has one more form; `*` can't be a column name in a filter.
