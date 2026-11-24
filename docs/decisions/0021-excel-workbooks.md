# 0021. Excel workbooks are read one sheet at a time and written with two new dependencies

- Status: accepted
- Date: 2026-11-24
- Amends: [0007](0007-other-formats.md) (the formats read into a temporary Parquet file), the export formats

## Context
Spreadsheets are where most tables people get handed live. Before this, opening one meant saving it as CSV first, which loses types (dates become text in the locale's format) and every sheet but one. Results could only leave as CSV, Parquet, JSON Lines or Arrow, so sending one to someone who works in Excel needed another tool.

Writing an `.xlsx` (zip + several XML parts) or reading `.xls` (the binary BIFF format), `.xlsb` or `.ods` by hand is a lot of format code to own. Rust has two maintained, MIT-licensed crates for it: `calamine` reads all five formats, and `rust_xlsxwriter` writes `.xlsx`. Neither pulls in an async runtime or anything `cargo deny` refuses. Together they add `zip` (deflate only), `quick-xml`, `encoding_rs`/`codepage` (for `.xls` text), `fast-float2`, `atoi_simd` and `tempfile`.

## Decision
- **Read with `calamine`, no default features.** `.xlsx`, `.xlsm`, `.xlsb`, `.xls` and `.ods` are `FileFormat::Excel`, read into the temporary Parquet file like CSV (ADR 0007). A file with another name can be read as Excel (`--input-format excel`, or the Import options window); its kind is then found from its first bytes and, for a zip, the parts it holds.
- **One sheet.** The first worksheet, or one chosen by name (`ImportOptions::sheet`, `--sheet`, the Sheet row of the Import options window). The report lists the workbook's worksheets, so the window can offer them once the file has been read.
- **Header row and types.** The first row that holds a cell names the columns, unless "no header" is chosen. Missing names become `column_N`, and repeated ones get `_2`, `_3`. Each column's type comes from every cell of it, in a first pass over the sheet:
  - whole numbers become `Int64` (Excel keeps every number as a float);
  - other numbers become `Float64`;
  - booleans become `Boolean`;
  - dates at midnight become `Date32`, other dates `Timestamp(ms)`;
  - durations become `Duration(ms)`;
  - anything mixed becomes text.

  "Every column as text" applies too. Error cells (`#N/A`) are null in typed columns and their text in text columns. Empty rows between data rows are kept as rows of nulls.
- **Bounded memory.** `.xlsx` and `.xlsm` sheets are streamed cell by cell (`worksheet_cells_reader`), twice: types, then rows. Only the shared strings, styles and workbook parts are held, and the zip's recorded sizes for those must total at most 256 MB (`MAX_EXCEL_SHARED_BYTES`) before `calamine` opens the file. `.xlsb` and `.ods` (whose sheet `calamine` reads whole) are limited to 256 MB uncompressed in all, and `.xls` to a 256 MB file (`MAX_EXCEL_WHOLE_BYTES`); the error suggests saving as `.xlsx`. At most 16,384 columns. `calamine` panics become errors, as for the other readers.
- **Write with `rust_xlsxwriter`, constant-memory mode.** `ExportFormat::Xlsx` (app Export menu, CLI `--format xlsx` or `-o *.xlsx`) writes one sheet, "Data": a bold header row, frozen, then one row per result row. Rows go to temporary files in a locked PAVI temp folder (ADR 0017) as they are written; the workbook is assembled into the output at the end. Cells must be written row by row in this mode, so each batch's columns are prepared first and then written row by row.
  - Numbers and decimals become Excel numbers.
  - Booleans become booleans.
  - Dates and naive timestamps become Excel dates with a date format. Before 1900-03-01, past 9999 or with a time zone, they are written as text.
  - Times of day become times.
  - Everything else (text, binary as `0x…`, nested values, durations) is the grid's text, cut at 32,767 characters, Excel's limit.
  - Nulls are empty cells.
  - A result of more than 1,048,575 rows is refused with a message pointing to CSV or Parquet.

## Consequences
- Workbooks open with real types and can be filtered, sorted and queried like any file; results reach Excel users directly.
- Two dependencies (and their small trees) to keep up to date and fuzz. The `other_formats` fuzz target picks `.xlsx` too, seeded with a workbook PAVI writes.
- Excel numbers are 64-bit floats: integers past 2^53 lose digits on export, and on import a whole number above 2^53 is read as a float.
- `.xls`, `.xlsb` and `.ods` files over the size limit need saving as `.xlsx` first.
- Formulas are read as their cached results; formatting, merged cells and charts are not read.
