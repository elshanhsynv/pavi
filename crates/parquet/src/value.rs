use std::fmt::Display;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, PrimitiveArray, StringArray, StructArray,
    types::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::DataType;

pub const DEFAULT_CELL_LIMIT: usize = 1024;
pub const MAX_NESTED_DEPTH: usize = 8;
pub const MAX_NESTED_ITEMS: usize = 16;

pub fn format_cell(array: &dyn Array, row: usize) -> String {
    format_cell_with_limit(array, row, DEFAULT_CELL_LIMIT)
}

pub fn format_cell_with_limit(array: &dyn Array, row: usize, limit: usize) -> String {
    format_value(array, row, limit, 0)
}

fn format_value(array: &dyn Array, row: usize, limit: usize, depth: usize) -> String {
    if row >= array.len() || array.is_null(row) {
        return "null".to_string();
    }

    if depth >= MAX_NESTED_DEPTH {
        return "<nested...>".to_string();
    }

    match array.data_type() {
        DataType::Boolean => typed(array, row, limit, |a: &BooleanArray, row| {
            a.value(row).to_string()
        }),
        DataType::Int8 => primitive::<Int8Type>(array, row, limit),
        DataType::Int16 => primitive::<Int16Type>(array, row, limit),
        DataType::Int32 => primitive::<Int32Type>(array, row, limit),
        DataType::Int64 => primitive::<Int64Type>(array, row, limit),
        DataType::UInt8 => primitive::<UInt8Type>(array, row, limit),
        DataType::UInt16 => primitive::<UInt16Type>(array, row, limit),
        DataType::UInt32 => primitive::<UInt32Type>(array, row, limit),
        DataType::UInt64 => primitive::<UInt64Type>(array, row, limit),
        DataType::Float32 => primitive::<Float32Type>(array, row, limit),
        DataType::Float64 => primitive::<Float64Type>(array, row, limit),
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
                    timestamp::<TimestampSecondType>(array, row, limit, "s", suffix)
                }
                arrow_schema::TimeUnit::Millisecond => {
                    timestamp::<TimestampMillisecondType>(array, row, limit, "ms", suffix)
                }
                arrow_schema::TimeUnit::Microsecond => {
                    timestamp::<TimestampMicrosecondType>(array, row, limit, "us", suffix)
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    timestamp::<TimestampNanosecondType>(array, row, limit, "ns", suffix)
                }
            }
        }
        DataType::List(_) => typed(array, row, limit, |a: &ListArray, row| {
            format_sequence(a.value(row).as_ref(), limit, depth + 1)
        }),
        DataType::LargeList(_) => typed(array, row, limit, |a: &LargeListArray, row| {
            format_sequence(a.value(row).as_ref(), limit, depth + 1)
        }),
        DataType::Struct(fields) => typed(array, row, limit, |a: &StructArray, row| {
            format_struct(a, row, fields, limit, depth + 1)
        }),
        DataType::Map(_, _) => typed(array, row, limit, |a: &MapArray, row| {
            format_map(a, row, limit, depth + 1)
        }),
        other => truncate(&format!("{other:?}"), limit),
    }
}

fn format_sequence(values: &dyn Array, limit: usize, depth: usize) -> String {
    format_collection("[", "]", values.len(), limit, |index| {
        format_value(values, index, limit, depth)
    })
}

fn format_struct(
    array: &StructArray,
    row: usize,
    fields: &arrow_schema::Fields,
    limit: usize,
    depth: usize,
) -> String {
    format_collection("{", "}", fields.len(), limit, |index| {
        format!(
            "{}: {}",
            fields[index].name(),
            format_value(array.column(index).as_ref(), row, limit, depth)
        )
    })
}

fn format_map(array: &MapArray, row: usize, limit: usize, depth: usize) -> String {
    let entries = array.value(row);
    if entries.num_columns() < 2 {
        return "<invalid map>".to_string();
    }

    format_collection("{", "}", entries.len(), limit, |index| {
        format!(
            "{}: {}",
            format_value(entries.column(0).as_ref(), index, limit, depth),
            format_value(entries.column(1).as_ref(), index, limit, depth)
        )
    })
}

fn format_collection(
    open: &str,
    close: &str,
    total: usize,
    limit: usize,
    mut item: impl FnMut(usize) -> String,
) -> String {
    if limit == 0 {
        return String::new();
    }

    let mut output = open.to_string();
    let content_limit = limit.saturating_sub(close.len());
    let displayed = total.min(MAX_NESTED_ITEMS);
    let mut truncated = false;

    for index in 0..displayed {
        if index > 0 && !append_bounded(&mut output, ", ", content_limit) {
            truncated = true;
            break;
        }
        if !append_bounded(&mut output, &item(index), content_limit) {
            truncated = true;
            break;
        }
    }

    if displayed < total || truncated {
        append_ellipsis(&mut output, content_limit);
    }
    append_bounded(&mut output, close, limit);
    output
}

fn append_ellipsis(output: &mut String, limit: usize) {
    while output.len() + 3 > limit {
        if output.pop().is_none() {
            return;
        }
    }
    output.push_str("...");
}

fn append_bounded(output: &mut String, value: &str, limit: usize) -> bool {
    if output.len() >= limit {
        return false;
    }
    let remaining = limit - output.len();
    if value.len() <= remaining {
        output.push_str(value);
        true
    } else {
        output.push_str(&truncate_exact(value, remaining));
        false
    }
}

fn truncate_exact(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_string();
    }
    if limit <= 3 {
        return value
            .char_indices()
            .map(|(index, _)| index)
            .take_while(|index| *index < limit)
            .last()
            .map_or_else(String::new, |end| value[..end].to_string());
    }
    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= limit - 3)
        .last()
        .unwrap_or(0);
    format!("{}...", &value[..end])
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

fn primitive<T>(array: &dyn Array, row: usize, limit: usize) -> String
where
    T: ArrowPrimitiveType,
    T::Native: Display,
{
    typed(array, row, limit, |a: &PrimitiveArray<T>, row| {
        a.value(row).to_string()
    })
}

fn timestamp<T>(array: &dyn Array, row: usize, limit: usize, unit: &str, timezone: &str) -> String
where
    T: ArrowPrimitiveType<Native = i64>,
{
    typed(array, row, limit, |a: &PrimitiveArray<T>, row| {
        format!("timestamp_{unit}:{} {timezone}", a.value(row))
    })
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
        ArrayRef, BinaryArray, BooleanArray, Date32Array, DurationSecondArray, Float64Array,
        Int32Array, LargeListArray, ListArray, StringArray, StructArray, TimestampMillisecondArray,
        UInt64Array,
        builder::{Int32Builder, MapBuilder, StringBuilder},
        types::Int32Type,
    };
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

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

    #[test]
    fn safely_falls_back_for_unusual_arrow_types() {
        let duration = DurationSecondArray::from(vec![Some(1)]);

        assert_eq!(format_cell(&duration, 0), "Duration(Second)");
    }

    #[test]
    fn formats_struct_lists_large_lists_and_maps() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![]),
            None,
        ]);
        assert_eq!(format_cell(&list, 0), "[1, null, 3]");
        assert_eq!(format_cell(&list, 1), "[]");
        assert_eq!(format_cell(&list, 2), "null");

        let large_list =
            LargeListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(9)])]);
        assert_eq!(format_cell(&large_list, 0), "[9]");

        let nested_list =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1), None, Some(3)])]);
        let structure = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("tags", nested_list.data_type().clone(), true)),
                Arc::new(nested_list) as ArrayRef,
            ),
        ]);
        assert_eq!(format_cell(&structure, 0), "{id: 7, tags: [1, null, 3]}");

        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("first");
        map.values().append_value(1);
        map.keys().append_value("second");
        map.values().append_value(2);
        map.append(true).unwrap();
        assert_eq!(format_cell(&map.finish(), 0), "{first: 1, second: 2}");
    }

    #[test]
    fn caps_nested_values_by_item_count_and_display_limit() {
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
            (0..MAX_NESTED_ITEMS as i32 + 20)
                .map(Some)
                .collect::<Vec<_>>(),
        )]);
        let formatted = format_cell_with_limit(&values, 0, 24);
        assert!(formatted.ends_with(']'));
        assert!(formatted.contains("..."));
        assert!(formatted.len() <= 24);
        assert!(format_cell_with_limit(&values, 0, 200).contains("..."));
    }
}
