use std::io::{self, Write};

use arrow_array::RecordBatch;
use parquet_reader::value::format_cell;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// Column widths are terminal columns (a CJK character or most emoji take two).
const MIN_COLUMN_WIDTH: usize = 4;
const MAX_COLUMN_WIDTH: usize = 32;

pub fn print_batch(batch: &RecordBatch, out: &mut impl Write) -> io::Result<()> {
    let rows = batch.num_rows();
    let columns = batch.num_columns();

    writeln!(out)?;
    writeln!(
        out,
        "Preview: {rows} {} × {columns} {}",
        plural(rows, "row", "rows"),
        plural(columns, "column", "columns")
    )?;
    writeln!(out)?;

    if columns == 0 {
        writeln!(out, "(no columns)")?;
        return Ok(());
    }

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
                .map(|column| one_line(&format_cell(batch.column(column).as_ref(), row)))
                .collect()
        })
        .collect();

    // First column is the row number.
    let mut widths = Vec::with_capacity(columns + 1);
    widths.push(rows.to_string().len().max(3));

    for (column, header) in headers.iter().enumerate() {
        let value_width = cells
            .iter()
            .map(|row| row[column].width())
            .max()
            .unwrap_or(0);

        widths.push(
            header
                .width()
                .max(value_width)
                .clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH),
        );
    }

    write_rule(out, &widths, ['┌', '┬', '┐'])?;

    write!(out, "│ {:>width$} │", "#", width = widths[0])?;
    for (header, &width) in headers.iter().zip(&widths[1..]) {
        write!(out, " ")?;
        write_padded(out, header, width, false)?;
        write!(out, " │")?;
    }
    writeln!(out)?;

    write_rule(out, &widths, ['├', '┼', '┤'])?;

    for (row_index, row) in cells.iter().enumerate() {
        write!(out, "│ {:>width$} │", row_index + 1, width = widths[0])?;

        for ((value, &width), &right) in row.iter().zip(&widths[1..]).zip(&right_aligned) {
            write!(out, " ")?;
            write_padded(out, value, width, right)?;
            write!(out, " │")?;
        }
        writeln!(out)?;
    }

    write_rule(out, &widths, ['└', '┴', '┘'])
}

fn plural<'a>(count: usize, one: &'a str, many: &'a str) -> &'a str {
    if count == 1 { one } else { many }
}

/// Writes a horizontal border: `corners` are the left, middle and right joints.
fn write_rule(out: &mut impl Write, widths: &[usize], corners: [char; 3]) -> io::Result<()> {
    let [left, middle, right] = corners;
    write!(out, "{left}")?;
    for (i, width) in widths.iter().enumerate() {
        let joint = if i + 1 == widths.len() { right } else { middle };
        write!(out, "{}{joint}", "─".repeat(width + 2))?;
    }
    writeln!(out)
}

/// Writes `value` cut to `width` terminal columns and padded to exactly `width`.
fn write_padded(out: &mut impl Write, value: &str, width: usize, right: bool) -> io::Result<()> {
    let value = truncate(value, width);
    let padding = " ".repeat(width.saturating_sub(value.width()));
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

/// Cuts `value` to at most `width` terminal columns, ending in `…` when cut.
fn truncate(value: &str, width: usize) -> String {
    if value.width() <= width {
        return value.to_owned();
    }
    if width <= 1 {
        return "…".to_owned();
    }

    let mut result = String::new();
    let mut used = 0;
    for c in value.chars() {
        let char_width = c.width().unwrap_or(0);
        if used + char_width > width - 1 {
            break;
        }
        used += char_width;
        result.push(c);
    }
    result.push('…');
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn render(batch: &RecordBatch) -> String {
        let mut out = Vec::new();
        print_batch(batch, &mut out).unwrap();
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
                    Some("Ünïcödé 👍"),
                    None,
                ])),
                Arc::new(StringArray::from(vec!["line1\nline2", "tab\there", "ok"])),
            ],
        )
        .unwrap();

        let text = render(&batch);
        let lines = table_lines(&text);
        assert_eq!(lines.len(), 3 + 1 + 3, "rules, header and rows:\n{text}");
        let width = lines[0].width();
        for line in &lines {
            assert_eq!(line.width(), width, "misaligned line {line:?} in\n{text}");
        }
        assert!(text.contains("line1\\nline2"));
        assert!(text.contains("tab\\there"));
        assert!(!text.contains(long_header));
        assert!(text.contains("a_really_long_header_name_that_…"));
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

    #[test]
    fn truncates_by_terminal_columns() {
        assert_eq!(truncate("abc", 3), "abc");
        assert_eq!(truncate("abcd", 3), "ab…");
        // Each of these takes two columns; a half-filled column is left as padding.
        assert_eq!(truncate("日本語", 4), "日…");
        assert_eq!(truncate("日本語", 1), "…");
        assert_eq!(one_line("a\u{1b}b\r"), "a\\u{1b}b\\r");
    }
}
