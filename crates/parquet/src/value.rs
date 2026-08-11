use std::fmt::Display;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, LargeBinaryArray, LargeStringArray,
    PrimitiveArray, StringArray,
    types::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::DataType;

pub const DEFAULT_CELL_LIMIT: usize = 512;

pub fn format_cell(array: &dyn Array, row: usize) -> String {
    format_cell_with_limit(array, row, DEFAULT_CELL_LIMIT)
}

pub fn format_cell_with_limit(array: &dyn Array, row: usize, limit: usize) -> String {
    if row >= array.len() || array.is_null(row) {
        return "null".to_string();
    }

    match array.data_type() {
        DataType::Boolean => typed(array, row, limit, |a: &BooleanArray, row| {
            a.value(row).to_string()
        }),
        DataType::Int8 => primitive::<Int8Type>(array, row),
        DataType::Int16 => primitive::<Int16Type>(array, row),
        DataType::Int32 => primitive::<Int32Type>(array, row),
        DataType::Int64 => primitive::<Int64Type>(array, row),
        DataType::UInt8 => primitive::<UInt8Type>(array, row),
        DataType::UInt16 => primitive::<UInt16Type>(array, row),
        DataType::UInt32 => primitive::<UInt32Type>(array, row),
        DataType::UInt64 => primitive::<UInt64Type>(array, row),
        DataType::Float32 => primitive::<Float32Type>(array, row),
        DataType::Float64 => primitive::<Float64Type>(array, row),
        DataType::Utf8 => typed(array, row, limit, |a: &StringArray, row| {
            truncate(a.value(row), limit)
        }),
        DataType::LargeUtf8 => typed(array, row, limit, |a: &LargeStringArray, row| {
            truncate(a.value(row), limit)
        }),
        DataType::Binary => typed(array, row, limit, |a: &BinaryArray, row| {
            format_binary(a.value(row), limit)
        }),
        DataType::LargeBinary => typed(array, row, limit, |a: &LargeBinaryArray, row| {
            format_binary(a.value(row), limit)
        }),
        DataType::Date32 => typed(array, row, limit, |a: &Date32Array, row| {
            format!("date32:{}", a.value(row))
        }),
        DataType::Date64 => typed(array, row, limit, |a: &Date64Array, row| {
            format!("date64_ms:{}", a.value(row))
        }),
        DataType::Timestamp(unit, timezone) => {
            let suffix = timezone.as_deref().unwrap_or("UTC");
            match unit {
                arrow_schema::TimeUnit::Second => {
                    timestamp::<TimestampSecondType>(array, row, "s", suffix)
                }
                arrow_schema::TimeUnit::Millisecond => {
                    timestamp::<TimestampMillisecondType>(array, row, "ms", suffix)
                }
                arrow_schema::TimeUnit::Microsecond => {
                    timestamp::<TimestampMicrosecondType>(array, row, "us", suffix)
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    timestamp::<TimestampNanosecondType>(array, row, "ns", suffix)
                }
            }
        }
        other => truncate(&format!("{other:?}"), limit),
    }
}

fn typed<T: 'static>(
    array: &dyn Array,
    row: usize,
    limit: usize,
    format: impl FnOnce(&T, usize) -> String,
) -> String {
    array
        .as_any()
        .downcast_ref::<T>()
        .map(|array| truncate(&format(array, row), limit))
        .unwrap_or_else(|| "<type mismatch>".to_string())
}

fn primitive<T>(array: &dyn Array, row: usize) -> String
where
    T: ArrowPrimitiveType,
    T::Native: Display,
{
    typed(
        array,
        row,
        DEFAULT_CELL_LIMIT,
        |a: &PrimitiveArray<T>, row| a.value(row).to_string(),
    )
}

fn timestamp<T>(array: &dyn Array, row: usize, unit: &str, timezone: &str) -> String
where
    T: ArrowPrimitiveType<Native = i64>,
{
    typed(
        array,
        row,
        DEFAULT_CELL_LIMIT,
        |a: &PrimitiveArray<T>, row| format!("timestamp_{unit}:{} {timezone}", a.value(row)),
    )
}

fn truncate(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_string();
    }

    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= limit)
        .last()
        .unwrap_or(0);

    format!("{}...", &value[..end])
}

fn format_binary(value: &[u8], limit: usize) -> String {
    let bytes = value.len().min(limit / 2);
    let mut out = String::with_capacity(bytes * 2 + 3);

    for byte in &value[..bytes] {
        out.push_str(&format!("{byte:02x}"));
    }

    if bytes < value.len() {
        out.push_str("...");
    }

    out
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, StringArray,
        TimestampMillisecondArray, UInt64Array,
    };

    use super::*;

    #[test]
    fn formats_basic_values_and_nulls() {
        assert_eq!(
            format_cell(&BooleanArray::from(vec![Some(true)]), 0),
            "true"
        );
        assert_eq!(format_cell(&Int32Array::from(vec![Some(-7)]), 0), "-7");
        assert_eq!(format_cell(&UInt64Array::from(vec![Some(7)]), 0), "7");
        assert_eq!(format_cell(&Float64Array::from(vec![Some(1.5)]), 0), "1.5");
        assert_eq!(
            format_cell(&StringArray::from(vec![None::<&str>]), 0),
            "null"
        );
        assert_eq!(
            format_cell(&Date32Array::from(vec![Some(1)]), 0),
            "date32:1"
        );
        assert_eq!(
            format_cell(&TimestampMillisecondArray::from(vec![Some(1000)]), 0),
            "timestamp_ms:1000 UTC"
        );
    }

    #[test]
    fn bounds_large_values() {
        let string = StringArray::from(vec![Some("abcdef")]);
        assert_eq!(format_cell_with_limit(&string, 0, 3), "abc...");

        let binary = BinaryArray::from_vec(vec![b"abcdef"]);
        assert_eq!(format_cell_with_limit(&binary, 0, 6), "616263...");
    }
}
