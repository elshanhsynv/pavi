# 0020. CSV and JSON files are read with import options, and late values retype instead of failing

- Status: accepted
- Date: 2026-11-24
- Amends: [0007](0007-other-formats.md) ("A later value that doesn't fit stops the open"; "CSV must have a header row"; UTF-8 only)

## Context
ADR 0007 reads CSV and JSON into a temporary Parquet file, guessing column types from the first 10,000 records. Real exports broke it in a few ways:
- A value after those records that doesn't fit the guess (an `N/A` after 50,000 numbers, a `2.5` after whole numbers) stopped the open. The only advice was to convert the file with another tool.
- A JSON field that first appeared after the guessed records was dropped without a word.
- Only UTF-8 was read. Excel's "Unicode text" export (UTF-16), and files from older Windows programs (Windows-1252), failed or showed mojibake. A UTF-8 byte order mark ended up in the first column's name.
- The delimiter and the header line were always guessed, with no way to say otherwise. Values such as `007` could only be read as numbers.

## Decision
- **Retype instead of failing.** When a batch hits a value that doesn't fit its column's guessed type (`ValueMismatch`: an arrow-csv parse error, or an arrow-json type or unknown-field error), the conversion reads the whole file once more to choose types from every value (the readers' own inference, with no record limit), then converts again with them. Integers that meet a fraction become floats; columns that meet anything else become text. That is at most two extra passes, and only for files that need them. A value that still doesn't fit stops the open, and the message suggests reading every column as text. The columns retyped are reported (`ImportReport::retyped`).
- **JSON is read strictly.** A field the guessed columns lack is an error that triggers the same second pass, instead of being dropped. Numbers and booleans in a text column are read as text (`coerce_primitive`).
- **Import options** (`parquet_reader::ImportOptions`), each guessed when not given:
  - the delimiter (CSV): one ASCII character;
  - whether the first line is a header (CSV); without one, columns are `column_1`, `column_2`, …;
  - the text encoding: UTF-8, UTF-16 LE/BE or Windows-1252;
  - every column as text (JSON: every column that isn't a list or object).
- **Encodings without a dependency.** `encoding::Decoded` turns the file into UTF-8 as it is read, a chunk at a time, and drops a byte order mark. UTF-16 uses the standard library's decoder; Windows-1252 is a 32-entry table plus Latin-1. Detection (`TextEncoding::detect`) looks at the first 64 KB: a byte order mark; then UTF-16 when nearly every other byte is zero; then UTF-8 when the sample is valid UTF-8 (a character cut off at its end allowed); else Windows-1252. When forced UTF-8 meets invalid text, the error says to choose another encoding.
- **What was used is reported.** `ParquetSource::import_report` gives the options with every guess resolved, plus the retyped columns. The CLI summary shows it as "Read as", and so does the Inspector.
- **CLI:** `--delimiter`, `--no-header`, `--encoding`, `--all-text`, next to `--input-format`. They apply to FILE.
- **App:** an Import options window (format, delimiter, header line, encoding, every column as text). It opens from the file menu ("Open with import options…"), from the Inspector of a CSV or JSON tab, and from the "could not be opened" screen. The choice is kept per file in the session (`SessionState::imports`, ≤ 32, never for a URL with a query string). Every later open of that file uses it: Reload, the recent list, tabs at start.

## Consequences
- A file with an unrepresentative start opens; the cost is reading it two more times, which shows as the progress starting over.
- Reading options are per file, so two different files can't share one option set by accident. Choosing "Guess everything" forgets them.
- The `other_formats` fuzz target now also picks the encoding, header and all-text options from its first byte.
- Excel workbooks are still not read: that needs a new dependency and its own decision.
