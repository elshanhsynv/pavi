//! "Copy as": the grid selection as a Markdown table, JSON or a SQL `IN` list.

use std::collections::HashSet;

use anyhow::{Result, bail};
use arrow_array::RecordBatch;
use parquet_reader::value::{format_cell_with_limit, sql_literal};

use crate::app::CopyFormat;

/// Builds the copied text one selected row at a time, never past `limit` bytes.
pub(crate) struct CopyBuilder {
    format: CopyFormat,
    limit: usize,
    text: String,
    rows: usize,
    /// SQL list values already written, so each is listed once.
    seen: HashSet<String>,
}

impl CopyBuilder {
    pub(crate) fn new(format: CopyFormat, limit: usize) -> Self {
        Self {
            format,
            limit,
            text: String::new(),
            rows: 0,
            seen: HashSet::new(),
        }
    }

    /// Adds row `row` of `batch`, columns `columns` (batch columns, in order).
    pub(crate) fn add_row(
        &mut self,
        batch: &RecordBatch,
        row: usize,
        columns: &[usize],
    ) -> Result<()> {
        let schema = batch.schema();
        let name = |column: usize| schema.field(column).name().clone();
        match self.format {
            // Spreadsheets take the default copy; it never comes here.
            CopyFormat::Tsv => {}
            CopyFormat::Markdown => {
                if self.rows == 0 {
                    let header = columns
                        .iter()
                        .map(|column| markdown_cell(&name(*column)))
                        .collect::<Vec<_>>();
                    self.text.push_str(&format!("| {} |\n", header.join(" | ")));
                    self.text
                        .push_str(&format!("|{}\n", " --- |".repeat(columns.len())));
                }
                let mut cells = Vec::with_capacity(columns.len());
                for column in columns {
                    let room = self.limit.saturating_sub(self.text.len());
                    let text =
                        format_cell_with_limit(batch.column(*column).as_ref(), row, room + 1);
                    cells.push(markdown_cell(&text));
                }
                self.text.push_str(&format!("| {} |\n", cells.join(" | ")));
            }
            CopyFormat::Json => {
                self.text
                    .push_str(if self.rows == 0 { "[\n  {" } else { ",\n  {" });
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        self.text.push_str(", ");
                    }
                    self.text.push_str(&serde_json::to_string(&name(*column))?);
                    self.text.push_str(": ");
                    self.text.push_str(&parquet_reader::export::json_value(
                        batch.column(*column).as_ref(),
                        row,
                    )?);
                    self.check_size()?;
                }
                self.text.push('}');
            }
            CopyFormat::SqlList => {
                for column in columns {
                    let literal =
                        sql_literal(batch.column(*column).as_ref(), row).map_err(|_| {
                            anyhow::anyhow!("Lists, structs and maps can't go in a SQL IN list")
                        })?;
                    if self.seen.insert(literal.clone()) {
                        if !self.text.is_empty() {
                            self.text.push_str(", ");
                        }
                        self.text.push_str(&literal);
                    }
                    self.check_size()?;
                }
            }
        }
        self.rows += 1;
        self.check_size()
    }

    fn check_size(&self) -> Result<()> {
        if self.text.len() > self.limit {
            bail!(
                "The selection is larger than the {} MB clipboard limit; select fewer cells or export instead",
                self.limit / (1024 * 1024)
            );
        }
        Ok(())
    }

    /// The text to copy and a status line saying what was copied.
    pub(crate) fn finish(self) -> (String, String) {
        let rows = match self.rows {
            1 => "1 row".to_owned(),
            rows => format!("{} rows", thousands(rows)),
        };
        match self.format {
            CopyFormat::Tsv => (self.text, format!("{rows} copied")),
            CopyFormat::Markdown => (self.text, format!("{rows} copied as a Markdown table")),
            CopyFormat::Json => (
                format!("{}\n]", self.text),
                format!("{rows} copied as JSON"),
            ),
            CopyFormat::SqlList => {
                let values = match self.seen.len() {
                    1 => "1 value".to_owned(),
                    count => format!("{} values", thousands(count)),
                };
                (
                    format!("IN ({})", self.text),
                    format!("{values} copied as a SQL IN list"),
                )
            }
        }
    }
}

fn thousands(count: usize) -> String {
    parquet_reader::value::format_number(
        &count.to_string(),
        parquet_reader::NumberFormat {
            decimals: None,
            thousands: true,
        },
    )
}

/// A cell of a Markdown table: `|` escaped and line breaks as spaces.
fn markdown_cell(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Float64Array, Int32Array, ListArray, StringArray, types::Int32Type,
    };

    use super::*;

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2, 1])) as ArrayRef),
            (
                "name",
                Arc::new(StringArray::from(vec![
                    Some("a|b"),
                    None,
                    Some("O'Hare\nT1"),
                ])),
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![1.5, 2.0, f64::NAN])),
            ),
        ])
        .unwrap()
    }

    fn copy(format: CopyFormat, columns: &[usize]) -> (String, String) {
        let batch = batch();
        let mut builder = CopyBuilder::new(format, 1024);
        for row in 0..batch.num_rows() {
            builder.add_row(&batch, row, columns).unwrap();
        }
        builder.finish()
    }

    #[test]
    fn copies_a_markdown_table() {
        let (text, status) = copy(CopyFormat::Markdown, &[0, 1]);
        assert_eq!(
            text,
            "| id | name |\n| --- | --- |\n| 1 | a\\|b |\n| 2 | null |\n| 1 | O'Hare T1 |\n"
        );
        assert_eq!(status, "3 rows copied as a Markdown table");
    }

    #[test]
    fn copies_json_objects() {
        let (text, _) = copy(CopyFormat::Json, &[0, 1, 2]);
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!([
                {"id": 1, "name": "a|b", "score": 1.5},
                {"id": 2, "name": null, "score": 2},
                {"id": 1, "name": "O'Hare\nT1", "score": "NaN"},
            ])
        );
    }

    #[test]
    fn copies_distinct_values_as_a_sql_list() {
        let (text, status) = copy(CopyFormat::SqlList, &[0]);
        assert_eq!(text, "IN (1, 2)");
        assert_eq!(status, "2 values copied as a SQL IN list");
        let (text, _) = copy(CopyFormat::SqlList, &[1]);
        assert_eq!(text, "IN ('a|b', NULL, 'O''Hare\nT1')");
    }

    #[test]
    fn refuses_more_than_the_limit_and_nested_sql_values() {
        let batch = batch();
        let mut builder = CopyBuilder::new(CopyFormat::Json, 20);
        assert!(builder.add_row(&batch, 0, &[0, 1, 2]).is_err());
        let lists = RecordBatch::try_from_iter(vec![(
            "items",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1)]),
            ])) as ArrayRef,
        )])
        .unwrap();
        let mut builder = CopyBuilder::new(CopyFormat::SqlList, 1024);
        assert!(builder.add_row(&lists, 0, &[0]).is_err());
        let mut builder = CopyBuilder::new(CopyFormat::Json, 1024);
        builder.add_row(&lists, 0, &[0]).unwrap();
        assert_eq!(builder.finish().0, "[\n  {\"items\": [1]}\n]");
    }
}
