use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use parquet_reader::{ColumnInfo, value::format_cell_with_limit};

pub const PREVIEW_LIMIT: usize = 256;
pub const EXPANDED_NESTED_LIMIT: usize = PREVIEW_LIMIT * 8;
const MAX_SCHEMA_DEPTH: usize = 8;
const MAX_SCHEMA_FIELDS: usize = 64;

#[derive(Debug, Eq, PartialEq)]
pub struct ColumnSummary {
    pub index: usize,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub available_statistics: usize,
    pub row_groups: usize,
    pub null_count: Option<u64>,
}

pub fn column_summary(column: &ColumnInfo) -> ColumnSummary {
    let null_count = column
        .row_group_statistics
        .iter()
        .map(|statistics| statistics.as_ref()?.null_count)
        .sum();
    ColumnSummary {
        index: column.index,
        name: column.name.clone(),
        data_type: format!("{:?}", column.data_type),
        nullable: column.nullable,
        available_statistics: column
            .row_group_statistics
            .iter()
            .filter(|statistics| statistics.is_some())
            .count(),
        row_groups: column.row_group_statistics.len(),
        null_count,
    }
}

pub fn nested_schema_preview(name: &str, data_type: &DataType) -> String {
    let mut lines = Vec::new();
    append_schema(&mut lines, name, data_type, 0, &mut 0);
    lines.join("\n")
}

fn append_schema(
    lines: &mut Vec<String>,
    name: &str,
    data_type: &DataType,
    depth: usize,
    fields_seen: &mut usize,
) {
    if *fields_seen >= MAX_SCHEMA_FIELDS {
        lines.push(format!("{}...", "  ".repeat(depth)));
        return;
    }
    *fields_seen += 1;
    lines.push(format!(
        "{}{}: {}",
        "  ".repeat(depth),
        name,
        type_label(data_type)
    ));
    if depth >= MAX_SCHEMA_DEPTH {
        lines.push(format!("{}...", "  ".repeat(depth + 1)));
        return;
    }

    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                append_schema(
                    lines,
                    field.name(),
                    field.data_type(),
                    depth + 1,
                    fields_seen,
                );
            }
        }
        DataType::List(field) | DataType::LargeList(field) => {
            append_schema(
                lines,
                field.name(),
                field.data_type(),
                depth + 1,
                fields_seen,
            );
        }
        DataType::Map(entries, _) => {
            append_schema(
                lines,
                entries.name(),
                entries.data_type(),
                depth + 1,
                fields_seen,
            );
        }
        _ => {}
    }
}

fn type_label(data_type: &DataType) -> String {
    match data_type {
        DataType::Struct(_) => "struct".to_string(),
        DataType::List(_) => "list".to_string(),
        DataType::LargeList(_) => "large list".to_string(),
        DataType::Map(_, _) => "map".to_string(),
        other => format!("{other:?}"),
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct CellDetails {
    pub row: u64,
    pub column: usize,
    pub name: String,
    pub data_type: String,
    pub is_null: bool,
    pub value: String,
    pub full_value: Option<String>,
    pub expanded_value: Option<String>,
}

pub fn inspect_cell(
    row: u64,
    column: usize,
    name: &str,
    array: &ArrayRef,
    offset: usize,
    show_safe_full_value: bool,
    show_nested_value: bool,
) -> CellDetails {
    let is_null = offset >= array.len() || array.is_null(offset);
    CellDetails {
        row,
        column,
        name: name.to_owned(),
        data_type: format!("{:?}", array.data_type()),
        is_null,
        value: format_cell_with_limit(array.as_ref(), offset, PREVIEW_LIMIT),
        full_value: (show_safe_full_value && !is_null && safe_full_value(array.data_type()))
            .then(|| format_cell_with_limit(array.as_ref(), offset, PREVIEW_LIMIT)),
        expanded_value: (show_nested_value && !is_null && nested_data_type(array.data_type()))
            .then(|| format_cell_with_limit(array.as_ref(), offset, EXPANDED_NESTED_LIMIT)),
    }
}

fn nested_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}

fn safe_full_value(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, Int32Array, ListArray, StringArray, StructArray, types::Int32Type,
    };
    use arrow_schema::Field;
    use parquet_reader::ColumnStatistics;

    use super::*;

    #[test]
    fn summarizes_available_and_missing_statistics_without_guessing_nulls() {
        let column = ColumnInfo {
            index: 2,
            name: "value".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            row_group_statistics: vec![
                Some(ColumnStatistics {
                    min: Some("1".to_string()),
                    max: Some("4".to_string()),
                    null_count: Some(2),
                    distinct_count: None,
                }),
                None,
            ],
        };
        let summary = column_summary(&column);
        assert_eq!(summary.available_statistics, 1);
        assert_eq!(summary.null_count, None);
        assert_eq!(summary.row_groups, 2);
    }

    #[test]
    fn inspects_nulls_and_bounds_long_text_and_binary_values() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some(
            "a".repeat(PREVIEW_LIMIT + 20),
        )]));
        let detail = inspect_cell(4, 1, "text", &text, 0, true, false);
        assert!(detail.value.ends_with("..."));
        assert_eq!(detail.full_value, None);

        let binary: ArrayRef = Arc::new(BinaryArray::from_iter_values([vec![7; PREVIEW_LIMIT]]));
        let detail = inspect_cell(4, 2, "binary", &binary, 0, false, false);
        assert!(detail.value.ends_with("..."));

        let null: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let detail = inspect_cell(5, 0, "id", &null, 0, true, false);
        assert!(detail.is_null);
        assert_eq!(detail.value, "null");
        assert_eq!(detail.full_value, None);
    }

    #[test]
    fn handles_supported_and_nested_values_without_panicking() {
        let number: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        assert_eq!(
            inspect_cell(0, 0, "id", &number, 0, true, false)
                .full_value
                .as_deref(),
            Some("7")
        );

        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
            vec![Some(1), Some(2)],
        )]));
        let detail = inspect_cell(0, 1, "nested", &list, 0, true, true);
        assert!(detail.full_value.is_none());
        assert_eq!(detail.value, "[1, 2]");
        assert_eq!(detail.expanded_value.as_deref(), Some("[1, 2]"));
    }

    #[test]
    fn describes_nested_schema_without_expanding_unboundedly() {
        let nested = DataType::Struct(
            vec![Arc::new(Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ))]
            .into(),
        );
        let preview = nested_schema_preview("payload", &nested);
        assert!(preview.contains("payload: struct"));
        assert!(preview.contains("items: list"));
        assert!(preview.contains("item: Int32"));

        let value: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![7])) as ArrayRef,
        )]));
        assert_eq!(
            inspect_cell(0, 0, "payload", &value, 0, false, true)
                .expanded_value
                .as_deref(),
            Some("{id: 7}")
        );
    }
}
