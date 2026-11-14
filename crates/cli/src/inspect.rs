//! `--schema` and `--stats`: the footer as rows, so they print as a table or
//! stream in any `--format` like data rows. Neither reads any data pages.

use std::sync::Arc;

use anyhow::Result;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet_reader::{ParquetSource, Projection};

/// One row per column of `projection`: index, name, type, nullable.
pub fn schema_batch(parquet: &ParquetSource, projection: &Projection) -> Result<RecordBatch> {
    let schema = parquet.schema();
    let fields = projection
        .as_slice()
        .iter()
        .map(|&index| (index, schema.field(index)))
        .collect::<Vec<_>>();

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("index", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
        ])),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                fields.iter().map(|(index, _)| *index as u64),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                fields.iter().map(|(_, field)| field.name()),
            )),
            Arc::new(StringArray::from_iter_values(
                fields
                    .iter()
                    .map(|(_, field)| field.data_type().to_string()),
            )),
            Arc::new(BooleanArray::from_iter(
                fields.iter().map(|(_, field)| Some(field.is_nullable())),
            )),
        ],
    )?)
}

/// One row per row group and column of `projection`, from footer statistics
/// (min and max as the Inspector shows them). Values Parquet did not record
/// are null. The rows already exist in memory as the footer's metadata, which
/// is bounded at open (`MAX_DATASET_METADATA_BYTES`).
pub fn stats_batch(parquet: &ParquetSource, projection: &Projection) -> Result<RecordBatch> {
    let columns = parquet.metadata().columns();
    let mut row_group = Vec::new();
    let mut column = Vec::new();
    let mut rows = Vec::new();
    let mut nulls = Vec::new();
    let mut min = Vec::new();
    let mut max = Vec::new();
    let mut distinct = Vec::new();

    for group in parquet.row_groups() {
        for &index in projection.as_slice() {
            let Some(info) = columns.get(index) else {
                continue;
            };
            let statistics = info
                .row_group_statistics
                .get(group.index)
                .and_then(Option::as_ref);
            row_group.push(group.index as u64);
            column.push(info.name.as_str());
            rows.push(group.row_count);
            nulls.push(statistics.and_then(|s| s.null_count));
            min.push(statistics.and_then(|s| s.min.as_deref()));
            max.push(statistics.and_then(|s| s.max.as_deref()));
            distinct.push(statistics.and_then(|s| s.distinct_count));
        }
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("row_group", DataType::UInt64, false),
            Field::new("column", DataType::Utf8, false),
            Field::new("rows", DataType::UInt64, false),
            Field::new("nulls", DataType::UInt64, true),
            Field::new("min", DataType::Utf8, true),
            Field::new("max", DataType::Utf8, true),
            Field::new("distinct", DataType::UInt64, true),
        ])),
        vec![
            Arc::new(UInt64Array::from(row_group)) as ArrayRef,
            Arc::new(StringArray::from(column)),
            Arc::new(UInt64Array::from(rows)),
            Arc::new(UInt64Array::from(nulls)),
            Arc::new(StringArray::from(min)),
            Arc::new(StringArray::from(max)),
            Arc::new(UInt64Array::from(distinct)),
        ],
    )?)
}
