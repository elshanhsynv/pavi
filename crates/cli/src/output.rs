use std::io::{self, Write};

use arrow_array::{Array, RecordBatch};
use parquet_reader::value::format_cell_readable;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthChar;

/// Column widths are terminal columns (a CJK character or most emoji take two).
const MIN_COLUMN_WIDTH: usize = 4;
const MAX_COLUMN_WIDTH: usize = 32;

/// The characters a table is drawn with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Borders {
    /// Box-drawing lines, `…` for cut text and `×` in the title.
    Unicode,
    /// `+ - |`, `~` for cut text and `x` in the title (`--ascii`), for
    /// consoles and code pages without box-drawing characters.
    Ascii,
}

impl Borders {
    fn horizontal(self) -> char {
        match self {
            Self::Unicode => '─',
            Self::Ascii => '-',
        }
    }

    fn vertical(self) -> char {
        match self {
            Self::Unicode => '│',
            Self::Ascii => '|',
        }
    }

    /// Left, middle and right joints of the top, header and bottom rules.
    fn joints(self, rule: Rule) -> [char; 3] {
        match (self, rule) {
            (Self::Unicode, Rule::Top) => ['┌', '┬', '┐'],
            (Self::Unicode, Rule::Middle) => ['├', '┼', '┤'],
            (Self::Unicode, Rule::Bottom) => ['└', '┴', '┘'],
            (Self::Ascii, _) => ['+', '+', '+'],
        }
    }

    fn ellipsis(self) -> char {
        match self {
            Self::Unicode => '…',
            Self::Ascii => '~',
        }
    }

    fn times(self) -> char {
        match self {
            Self::Unicode => '×',
            Self::Ascii => 'x',
        }
    }

    /// A null cell: a mark no text value is likely to be, so a null can be
    /// told from the text "null" (nested nulls inside lists and structs still
    /// read `null`, as in the grid).
    fn null(self) -> &'static str {
        match self {
            Self::Unicode => "∅",
            Self::Ascii => "NULL",
        }
    }
}

#[derive(Clone, Copy)]
enum Rule {
    Top,
    Middle,
    Bottom,
}

/// What the table's `#` column shows.
#[derive(Clone, Copy, Debug)]
pub enum RowNumbers<'a> {
    /// Consecutive numbers from this one.
    From(u64),
    /// One number per row, such as the file rows filtered rows came from.
    Listed(&'a [u64]),
}

impl RowNumbers<'_> {
    fn get(self, row: usize) -> u64 {
        match self {
            Self::From(first) => first.saturating_add(row as u64),
            Self::Listed(numbers) => numbers.get(row).copied().unwrap_or_default(),
        }
    }
}

/// Prints `batch` as a table under `title` ("Preview: 3 rows × 2 columns");
/// the `#` column shows `numbers`. With `max_width` (the terminal's width),
/// a table that would be wider narrows its widest columns, down to
/// [`SQUEEZED_COLUMN_WIDTH`], then leaves out middle columns, shown as one
/// `…` column, so no line wraps; the title says how many columns are shown.
pub fn print_batch(
    batch: &RecordBatch,
    title: &str,
    numbers: RowNumbers<'_>,
    borders: Borders,
    max_width: Option<usize>,
    out: &mut impl Write,
) -> io::Result<()> {
    let rows = batch.num_rows();
    let columns = batch.num_columns();

    let schema = batch.schema();
    let headers: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| one_line(field.name()))
        .collect();
    // Numbers are right-aligned, as in the grid.
    let right_aligned: Vec<bool> = schema
        .fields()
        .iter()
        .map(|field| field.data_type().is_numeric())
        .collect();

    let cells: Vec<Vec<String>> = (0..rows)
        .map(|row| {
            (0..columns)
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        borders.null().to_owned()
                    } else {
                        one_line(&format_cell_readable(array.as_ref(), row))
                    }
                })
                .collect()
        })
        .collect();

    // The row number column, then one width per data column.
    let number_width = (0..rows)
        .map(|row| numbers.get(row).to_string().len())
        .max()
        .unwrap_or(0)
        .max(3);
    let mut widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            let value_width = cells
                .iter()
                .map(|row| display_width(&row[column]))
                .max()
                .unwrap_or(0);
            display_width(header)
                .max(value_width)
                .clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH)
        })
        .collect();
    let slots = fit(number_width, &mut widths, max_width);
    let shown = slots.iter().flatten().count();

    writeln!(out)?;
    write!(
        out,
        "{title}: {rows} {} {} {columns} {}",
        plural(rows, "row", "rows"),
        borders.times(),
        plural(columns, "column", "columns")
    )?;
    if shown < columns {
        write!(
            out,
            " ({shown} shown; widen the window, or pick columns with -c)"
        )?;
    }
    writeln!(out)?;
    writeln!(out)?;

    if columns == 0 {
        writeln!(out, "(no columns)")?;
        return Ok(());
    }

    let rule_widths: Vec<usize> = std::iter::once(number_width)
        .chain(
            slots
                .iter()
                .map(|slot| slot.map_or(1, |column| widths[column])),
        )
        .collect();
    let bar = borders.vertical();
    let gap = borders.ellipsis();
    write_rule(out, &rule_widths, borders, Rule::Top)?;

    write!(out, "{bar} {:>number_width$} {bar}", "#")?;
    for slot in &slots {
        match *slot {
            Some(column) => {
                write!(out, " ")?;
                write_padded(out, &headers[column], widths[column], false, borders)?;
            }
            None => write!(out, " {gap}")?,
        }
        write!(out, " {bar}")?;
    }
    writeln!(out)?;

    write_rule(out, &rule_widths, borders, Rule::Middle)?;

    for (index, row) in cells.iter().enumerate() {
        let number = numbers.get(index);
        write!(out, "{bar} {number:>number_width$} {bar}")?;
        for slot in &slots {
            match *slot {
                Some(column) => {
                    write!(out, " ")?;
                    write_padded(
                        out,
                        &row[column],
                        widths[column],
                        right_aligned[column],
                        borders,
                    )?;
                }
                None => write!(out, " {gap}")?,
            }
            write!(out, " {bar}")?;
        }
        writeln!(out)?;
    }

    write_rule(out, &rule_widths, borders, Rule::Bottom)
}

/// Narrowest a column is made when a table is wider than the terminal;
/// columns already narrower keep their width.
const SQUEEZED_COLUMN_WIDTH: usize = 10;

/// Which data columns to draw, in order, with `None` for one `…` column
/// standing for those left out. Without `max_width` every column is drawn as
/// it is. Otherwise, while the table is wider than `max_width`, every column
/// is capped at one width, the largest from [`MAX_COLUMN_WIDTH`] down to
/// [`SQUEEZED_COLUMN_WIDTH`] that fits; if even that is too wide, columns are
/// kept from the left and the right in turn, around the gap.
fn fit(number_width: usize, widths: &mut [usize], max_width: Option<usize>) -> Vec<Option<usize>> {
    let all: Vec<Option<usize>> = (0..widths.len()).map(Some).collect();
    let Some(max_width) = max_width else {
        return all;
    };
    // `│ 12 │ value │ … │`: a border, then each column with a space either
    // side and a border after it.
    let line = |widths: &[usize], slots: &[Option<usize>]| {
        1 + number_width
            + 3
            + slots
                .iter()
                .map(|slot| slot.map_or(1, |column| widths[column]) + 3)
                .sum::<usize>()
    };
    if line(widths, &all) <= max_width {
        return all;
    }

    let natural = widths.to_vec();
    let capped = |cap: usize| -> Vec<usize> {
        natural
            .iter()
            .map(|&width| width.min(cap.max(SQUEEZED_COLUMN_WIDTH)))
            .collect()
    };
    let cap = (SQUEEZED_COLUMN_WIDTH..=MAX_COLUMN_WIDTH)
        .rev()
        .find(|&cap| line(&capped(cap), &all) <= max_width)
        .unwrap_or(SQUEEZED_COLUMN_WIDTH);
    widths.copy_from_slice(&capped(cap));
    if line(widths, &all) <= max_width {
        return all;
    }

    // Still too wide: keep columns from both ends, one gap between them.
    let budget = max_width.saturating_sub(line(widths, &[None]));
    let (mut left, mut right) = (Vec::new(), Vec::new());
    let (mut next_left, mut next_right) = (0, widths.len());
    let mut used = 0;
    let mut from_left = true;
    while next_left < next_right {
        let column = if from_left { next_left } else { next_right - 1 };
        let width = widths[column] + 3;
        if used + width > budget {
            break;
        }
        used += width;
        if from_left {
            left.push(column);
            next_left += 1;
        } else {
            right.push(column);
            next_right -= 1;
        }
        from_left = !from_left;
    }
    // However narrow the terminal, the first column shows.
    if left.is_empty() {
        left.push(0);
        right.retain(|&column| column != 0);
    }
    left.into_iter()
        .map(Some)
        .chain(std::iter::once(None))
        .chain(right.into_iter().rev().map(Some))
        .collect()
}

fn plural<'a>(count: usize, one: &'a str, many: &'a str) -> &'a str {
    if count == 1 { one } else { many }
}

/// Writes a horizontal border with the joints `borders` has for `rule`.
fn write_rule(
    out: &mut impl Write,
    widths: &[usize],
    borders: Borders,
    rule: Rule,
) -> io::Result<()> {
    let [left, middle, right] = borders.joints(rule);
    let line = borders.horizontal().to_string();
    write!(out, "{left}")?;
    for (i, width) in widths.iter().enumerate() {
        let joint = if i + 1 == widths.len() { right } else { middle };
        write!(out, "{}{joint}", line.repeat(width + 2))?;
    }
    writeln!(out)
}

/// Writes `value` cut to `width` terminal columns and padded to exactly `width`.
fn write_padded(
    out: &mut impl Write,
    value: &str,
    width: usize,
    right: bool,
    borders: Borders,
) -> io::Result<()> {
    let value = truncate(value, width, borders.ellipsis());
    let padding = " ".repeat(width.saturating_sub(display_width(&value)));
    if right {
        write!(out, "{padding}{value}")
    } else {
        write!(out, "{value}{padding}")
    }
}

/// Keeps a table row on one line: line breaks, tabs and other control
/// characters are shown as escapes (`\n`, `\t`, `\u{1b}`).
fn one_line(value: &str) -> String {
    if !value.chars().any(char::is_control) {
        return value.to_owned();
    }
    let mut result = String::with_capacity(value.len() + 8);
    for c in value.chars() {
        match c {
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => result.extend(c.escape_unicode()),
            c => result.push(c),
        }
    }
    result
}

/// Cuts `value` to at most `width` terminal columns, ending in `ellipsis`
/// (one column wide) when cut.
fn truncate(value: &str, width: usize, ellipsis: char) -> String {
    if display_width(value) <= width {
        return value.to_owned();
    }
    if width <= 1 {
        return ellipsis.to_string();
    }

    // Cut between whole graphemes, so an emoji with a skin tone or a letter
    // with its accent is never split.
    let mut result = String::new();
    let mut used = 0;
    for grapheme in value.graphemes(true) {
        let grapheme_width = grapheme_width(grapheme);
        if used + grapheme_width > width - 1 {
            break;
        }
        used += grapheme_width;
        result.push_str(grapheme);
    }
    result.push(ellipsis);
    result
}

/// Terminal columns `value` takes, measured per grapheme (what a terminal
/// draws as one character), not per code point.
fn display_width(value: &str) -> usize {
    if value.is_ascii() {
        // Control characters were escaped (`one_line`), so each byte is one column.
        return value.len();
    }
    value.graphemes(true).map(grapheme_width).sum()
}

/// Columns one grapheme takes: two for emoji drawn as pictures (a variation
/// selector U+FE0F, or a flag made of two regional indicators); otherwise its
/// widest code point, so modifiers, zero-width joiners and combining marks add
/// nothing (`👍🏽` and `👨‍👩‍👧` are two columns, `é` written as `e` + `´` one).
fn grapheme_width(grapheme: &str) -> usize {
    let regional = |c: char| ('\u{1F1E6}'..='\u{1F1FF}').contains(&c);
    if grapheme.contains('\u{FE0F}')
        || (grapheme.chars().count() == 2 && grapheme.chars().all(regional))
    {
        return 2;
    }
    grapheme
        .chars()
        .map(|c| c.width().unwrap_or(0))
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn render(batch: &RecordBatch) -> String {
        let mut out = Vec::new();
        print_batch(
            batch,
            "Preview",
            RowNumbers::From(1),
            Borders::Unicode,
            None,
            &mut out,
        )
        .unwrap();
        String::from_utf8(out).unwrap()
    }

    fn table_lines(text: &str) -> Vec<&str> {
        text.lines()
            .filter(|line| line.starts_with(['┌', '│', '├', '└']))
            .collect()
    }

    #[test]
    fn keeps_every_table_line_the_same_width_with_odd_values() {
        let long_header = "a_really_long_header_name_that_exceeds_thirty_two_chars";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(long_header, DataType::Utf8, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 22, 333])),
                Arc::new(StringArray::from(vec![
                    Some("日本語テキスト日本語テキスト日本語テキスト"),
                    Some("Ünïcödé 👍🏽 👨\u{200d}👩\u{200d}👧 🇺🇸 e\u{301}"),
                    None,
                ])),
                Arc::new(StringArray::from(vec!["line1\nline2", "tab\there", "ok"])),
            ],
        )
        .unwrap();

        let text = render(&batch);
        let lines = table_lines(&text);
        assert_eq!(lines.len(), 3 + 1 + 3, "rules, header and rows:\n{text}");
        let width = display_width(lines[0]);
        for line in &lines {
            assert_eq!(
                display_width(line),
                width,
                "misaligned line {line:?} in\n{text}"
            );
        }
        assert!(text.contains("line1\\nline2"));
        assert!(text.contains("tab\\there"));
        assert!(!text.contains(long_header));
        assert!(text.contains("a_really_long_header_name_that_…"));
        // The null cell is marked, so it can't be taken for the text "null".
        assert!(text.contains("│ ∅ "), "{text}");
    }

    #[test]
    fn measures_and_cuts_whole_graphemes() {
        assert_eq!(display_width("abc"), 3);
        assert_eq!(display_width("日本"), 4);
        assert_eq!(display_width("👍🏽"), 2);
        assert_eq!(display_width("👨\u{200d}👩\u{200d}👧"), 2);
        assert_eq!(display_width("🇺🇸"), 2);
        assert_eq!(display_width("❤\u{fe0f}"), 2);
        assert_eq!(display_width("e\u{301}"), 1);
        // A skin tone is never cut off its emoji.
        assert_eq!(truncate("ab👍🏽cd", 4, '…'), "ab…");
        assert_eq!(truncate("ab👍🏽cd", 5, '…'), "ab👍🏽…");
    }

    #[test]
    fn marks_null_cells_apart_from_the_text_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("null"), None]))],
        )
        .unwrap();
        let unicode = render(&batch);
        assert!(unicode.contains("│   1 │ null │"), "{unicode}");
        assert!(unicode.contains("│   2 │ ∅    │"), "{unicode}");

        let mut out = Vec::new();
        print_batch(
            &batch,
            "Preview",
            RowNumbers::From(1),
            Borders::Ascii,
            None,
            &mut out,
        )
        .unwrap();
        let ascii = String::from_utf8(out).unwrap();
        assert!(ascii.contains("|   2 | NULL |"), "{ascii}");
    }

    #[test]
    fn right_aligns_numbers_and_counts_rows_in_words() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();

        let text = render(&batch);
        assert!(text.contains("Preview: 1 row × 1 column\n"), "{text}");
        assert!(text.contains("│    7 │"), "{text}");
    }

    /// Ten columns like the dummy files': a long header, a long float, text.
    fn wide_batch() -> RecordBatch {
        let mut columns: Vec<(String, arrow_array::ArrayRef)> = Vec::new();
        for index in 0..9 {
            columns.push((
                format!("column_number_{index}"),
                Arc::new(Int64Array::from(vec![index, 10 * index])),
            ));
        }
        columns.push((
            "value_float".to_owned(),
            Arc::new(arrow_array::Float64Array::from(vec![9.0 * 0.001, 123.25])),
        ));
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn render_within(batch: &RecordBatch, width: Option<usize>) -> String {
        let mut out = Vec::new();
        print_batch(
            batch,
            "Preview",
            RowNumbers::From(1),
            Borders::Unicode,
            width,
            &mut out,
        )
        .unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn narrows_columns_to_fit_the_terminal() {
        let batch = wide_batch();
        let full = render_within(&batch, None);
        let full_width = display_width(table_lines(&full)[0]);
        assert!(full_width > 150, "{full}");
        // Floats read as people write them.
        assert!(full.contains(" 0.009 │"), "{full}");

        // Narrower than the full table but wide enough for 10-column cells.
        let narrowed = render_within(&batch, Some(150));
        for line in table_lines(&narrowed) {
            assert!(display_width(line) <= 150, "{line}\n{narrowed}");
        }
        assert!(
            narrowed.contains("column_number_0") || narrowed.contains("column_nu"),
            "{narrowed}"
        );
        assert!(!narrowed.contains("shown"), "every column still shows");
    }

    #[test]
    fn leaves_out_middle_columns_when_narrowing_is_not_enough() {
        let batch = wide_batch();
        let text = render_within(&batch, Some(80));
        let lines = table_lines(&text);
        for line in &lines {
            assert!(display_width(line) <= 80, "{line}\n{text}");
        }
        assert!(
            lines
                .iter()
                .all(|line| display_width(line) == display_width(lines[0])),
            "{text}"
        );
        // The first and last columns stay, with … between them.
        assert!(lines[1].contains("column_nu…"), "{text}");
        assert!(lines[1].contains("value_flo…"), "{text}");
        assert!(lines[1].contains("│ … │"), "{text}");
        assert!(
            text.contains("Preview: 2 rows × 10 columns (") && text.contains(" shown; "),
            "{text}"
        );

        // However narrow, the first column shows.
        let tiny = render_within(&batch, Some(10));
        assert!(table_lines(&tiny)[1].contains("column_nu…"), "{tiny}");
    }

    #[test]
    fn draws_ascii_tables_with_only_ascii() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(StringArray::from(vec!["x".repeat(40)])),
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        print_batch(
            &batch,
            "Preview",
            RowNumbers::From(1),
            Borders::Ascii,
            None,
            &mut out,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();

        assert!(text.is_ascii(), "{text}");
        assert!(text.contains("Preview: 1 row x 2 columns\n"), "{text}");
        assert!(text.contains("+-----+------+"), "{text}");
        assert!(
            text.contains("|   1 |    7 | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx~ |"),
            "{text}"
        );
        let lines = table_lines_ascii(&text);
        assert_eq!(lines.len(), 5);
        assert!(
            lines.iter().all(|line| line.len() == lines[0].len()),
            "{text}"
        );
    }

    fn table_lines_ascii(text: &str) -> Vec<&str> {
        text.lines()
            .filter(|line| line.starts_with(['+', '|']))
            .collect()
    }

    #[test]
    fn truncates_by_terminal_columns() {
        assert_eq!(truncate("abc", 3, '…'), "abc");
        assert_eq!(truncate("abcd", 3, '…'), "ab…");
        // Each of these takes two columns; a half-filled column is left as padding.
        assert_eq!(truncate("日本語", 4, '…'), "日…");
        assert_eq!(truncate("日本語", 1, '…'), "…");
        assert_eq!(one_line("a\u{1b}b\r"), "a\\u{1b}b\\r");
    }
}
