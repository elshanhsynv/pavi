//! Display text for Parquet row-group statistics.
//!
//! Parquet stores statistics in physical types (INT32, INT64, byte arrays). To show
//! them the way the grid shows values, each min/max is rebuilt as a one-element Arrow
//! array of the column's type and formatted with the shared value formatter.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Date32Array, Date64Array, Decimal128Array, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use parquet::file::{metadata::ParquetMetaData, statistics::Statistics};

use crate::{ColumnStatistics, value::scalar_text};

const STAT_VALUE_LIMIT: usize = 128;

/// For each top-level column, the Parquet leaf column that stores it, or `None`
/// for a nested column (a list, struct or map is stored in one or more leaves
/// under a group, and has no single min/max).
///
/// Leaf statistics are indexed by leaf column, so in a file with nested columns
/// the top-level and leaf positions differ; this is the map between them.
pub(crate) fn leaf_columns(
    metadata: &ParquetMetaData,
    top_level_columns: usize,
) -> Vec<Option<usize>> {
    let schema = metadata.file_metadata().schema_descr();
    let roots = schema.root_schema().get_fields();
    if roots.len() != top_level_columns {
        return vec![None; top_level_columns];
    }
    let mut leaves = vec![(0_usize, None); roots.len()];
    for leaf in 0..schema.num_columns() {
        if let Some((count, first)) = leaves.get_mut(schema.get_column_root_idx(leaf)) {
            *count += 1;
            first.get_or_insert(leaf);
        }
    }
    roots
        .iter()
        .zip(leaves)
        .map(|(root, (count, leaf))| {
            (root.is_primitive() && count == 1)
                .then_some(leaf)
                .flatten()
        })
        .collect()
}

/// Per top-level column, per row group: display statistics, or `None` when absent.
/// Nested columns have none; the other columns of a nested schema keep theirs.
pub(crate) fn metadata_statistics(
    metadata: &ParquetMetaData,
    schema: &Schema,
) -> Vec<Vec<Option<ColumnStatistics>>> {
    let row_groups = metadata.num_row_groups();
    leaf_columns(metadata, schema.fields().len())
        .into_iter()
        .enumerate()
        .map(|(column, leaf)| {
            let data_type = schema.field(column).data_type();
            (0..row_groups)
                .map(|row_group| {
                    metadata
                        .row_group(row_group)
                        .columns()
                        .get(leaf?)
                        .and_then(|column| column.statistics())
                        .map(|statistics| statistics_summary(statistics, data_type))
                })
                .collect()
        })
        .collect()
}

fn statistics_summary(statistics: &Statistics, data_type: &DataType) -> ColumnStatistics {
    let (min, max) = match statistics {
        Statistics::Boolean(stats) => display_pair(stats.min_opt(), stats.max_opt()),
        Statistics::Int32(stats) => (
            stats.min_opt().map(|value| int32_text(*value, data_type)),
            stats.max_opt().map(|value| int32_text(*value, data_type)),
        ),
        Statistics::Int64(stats) => (
            stats.min_opt().map(|value| int64_text(*value, data_type)),
            stats.max_opt().map(|value| int64_text(*value, data_type)),
        ),
        Statistics::Int96(stats) => display_pair(stats.min_opt(), stats.max_opt()),
        Statistics::Float(stats) => display_pair(stats.min_opt(), stats.max_opt()),
        Statistics::Double(stats) => display_pair(stats.min_opt(), stats.max_opt()),
        Statistics::ByteArray(_) | Statistics::FixedLenByteArray(_) => (
            statistics
                .min_bytes_opt()
                .map(|value| bytes_text(value, data_type)),
            statistics
                .max_bytes_opt()
                .map(|value| bytes_text(value, data_type)),
        ),
    };
    ColumnStatistics {
        min,
        max,
        null_count: statistics.null_count_opt(),
        distinct_count: statistics.distinct_count_opt(),
    }
}

/// INT32 statistics: unsigned integers are reinterpreted; dates, times and decimals
/// are formatted like grid values. Other types show the stored integer.
fn int32_text(value: i32, data_type: &DataType) -> String {
    let array: ArrayRef = match data_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            return (value as u32).to_string();
        }
        DataType::Date32 => Arc::new(Date32Array::from(vec![value])),
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(vec![value])),
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(vec![value]))
        }
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale) => match decimal(value.into(), *scale) {
            Some(array) => array,
            None => return value.to_string(),
        },
        _ => return value.to_string(),
    };
    typed_text(&array).unwrap_or_else(|| value.to_string())
}

/// INT64 statistics: like [`int32_text`], plus timestamps in the column's time zone.
fn int64_text(value: i64, data_type: &DataType) -> String {
    let array: ArrayRef = match data_type {
        DataType::UInt64 => return (value as u64).to_string(),
        DataType::Date64 => Arc::new(Date64Array::from(vec![value])),
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(vec![value]))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(vec![value]))
        }
        DataType::Timestamp(unit, timezone) => {
            let timezone = timezone.clone();
            match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(vec![value]).with_timezone_opt(timezone))
                }
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(vec![value]).with_timezone_opt(timezone),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(vec![value]).with_timezone_opt(timezone),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(vec![value]).with_timezone_opt(timezone),
                ),
            }
        }
        DataType::Decimal64(_, scale) | DataType::Decimal128(_, scale) => {
            match decimal(value.into(), *scale) {
                Some(array) => array,
                None => return value.to_string(),
            }
        }
        _ => return value.to_string(),
    };
    typed_text(&array).unwrap_or_else(|| value.to_string())
}

/// Byte-array statistics: decimals stored as big-endian two's complement are scaled;
/// UTF-8 is shown as text and anything else as bounded hex.
fn bytes_text(value: &[u8], data_type: &DataType) -> String {
    if let DataType::Decimal32(_, scale)
    | DataType::Decimal64(_, scale)
    | DataType::Decimal128(_, scale) = data_type
        && let Some(text) = big_endian_i128(value)
            .and_then(|unscaled| decimal(unscaled, *scale))
            .and_then(|array| typed_text(&array))
    {
        return text;
    }
    format_stat_bytes(value)
}

/// A one-element Decimal128 array. Precision is set to the maximum so any stored
/// unscaled value is displayable; only the scale affects the text.
fn decimal(unscaled: i128, scale: i8) -> Option<ArrayRef> {
    Decimal128Array::from(vec![unscaled])
        .with_precision_and_scale(38, scale)
        .ok()
        .map(|array| Arc::new(array) as ArrayRef)
}

fn big_endian_i128(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let mut buffer = if bytes[0] & 0x80 == 0 {
        [0_u8; 16]
    } else {
        [0xff_u8; 16]
    };
    buffer[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buffer))
}

fn typed_text(array: &ArrayRef) -> Option<String> {
    scalar_text(array.as_ref(), 0).ok().flatten()
}

fn display_pair<T: std::fmt::Display>(
    min: Option<&T>,
    max: Option<&T>,
) -> (Option<String>, Option<String>) {
    (min.map(ToString::to_string), max.map(ToString::to_string))
}

fn format_stat_bytes(value: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(value) {
        let mut output = text.chars().take(STAT_VALUE_LIMIT).collect::<String>();
        if output.len() < text.len() {
            output.push_str("...");
        }
        return output;
    }
    let displayed = value.len().min(16);
    let mut output = String::with_capacity(displayed * 2 + 24);
    for byte in &value[..displayed] {
        output.push_str(&format!("{byte:02x}"));
    }
    if displayed < value.len() {
        output.push_str("...");
    }
    format!("0x{output} ({} bytes)", value.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_physical_integers_by_arrow_type() {
        assert_eq!(int32_text(-1_294_967_296, &DataType::UInt32), "3000000000");
        assert_eq!(int32_text(19_000, &DataType::Date32), "2022-01-08");
        assert_eq!(int32_text(12_345, &DataType::Decimal128(10, 2)), "123.45");
        assert_eq!(
            int32_text(3_723, &DataType::Time32(TimeUnit::Second)),
            "01:02:03"
        );
        assert_eq!(int32_text(-7, &DataType::Int32), "-7");

        assert_eq!(int64_text(-1, &DataType::UInt64), u64::MAX.to_string());
        assert_eq!(
            int64_text(1_000, &DataType::Timestamp(TimeUnit::Millisecond, None)),
            "1970-01-01T00:00:01"
        );
        assert_eq!(
            int64_text(
                1_700_000_000,
                &DataType::Timestamp(TimeUnit::Second, Some("Europe/Berlin".into()))
            ),
            "2023-11-14T23:13:20+01:00"
        );
        assert_eq!(int64_text(-5, &DataType::Decimal128(18, 2)), "-0.05");
    }

    #[test]
    fn decodes_byte_array_decimals_and_keeps_text_and_hex() {
        let decimal = DataType::Decimal128(20, 3);
        assert_eq!(bytes_text(&[0x30, 0x39], &decimal), "12.345");
        assert_eq!(bytes_text(&[0xff, 0xfb], &decimal), "-0.005");
        assert_eq!(bytes_text(b"alpha", &DataType::Utf8), "alpha");
        assert_eq!(
            bytes_text(&[0xff, 0x00], &DataType::Binary),
            "0xff00 (2 bytes)"
        );
    }
}
