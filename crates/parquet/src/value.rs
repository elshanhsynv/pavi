use std::fmt::Display;

use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    PrimitiveArray, StringArray, StringViewArray, StructArray,
    types::{
        ArrowDictionaryKeyType, ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type,
        Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::DataType;

pub const DEFAULT_CELL_LIMIT: usize = 1024;
pub const MAX_NESTED_DEPTH: usize = 8;
pub const MAX_NESTED_ITEMS: usize = 16;

/// Bounded display text for one cell (grid, CLI, clipboard, Inspector).
pub fn format_cell(array: &dyn Array, row: usize) -> String {
    format_cell_with_limit(array, row, DEFAULT_CELL_LIMIT)
}

/// Bounded display text for one cell, truncated to about `limit` bytes.
///
/// Scalars use the same text as [`scalar_text`]; nested values are capped by
/// depth and item count, and strings/binary are truncated without copying them whole.
pub fn format_cell_with_limit(array: &dyn Array, row: usize, limit: usize) -> String {
    format_value(array, row, limit, 0)
}

/// Full, untruncated text of one scalar value, or `None` for null.
///
/// This is the shared conversion for export and profiling. Dates are ISO 8601,
/// timestamps are RFC 3339 (with an offset only when the column has a time zone),
/// decimals use their scale, and dictionary cells show the value they reference.
/// Nested types are rejected; see [`is_scalar_type`].
pub fn scalar_text(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if row >= array.len() {
        bail!(
            "row {row} is out of range for an array of {} values",
            array.len()
        );
    }
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some((values, index)) = dictionary_value(array, row) {
        return scalar_text(values, index);
    }
    if !is_scalar_type(array.data_type()) {
        bail!(
            "{:?} values are nested and have no scalar text",
            array.data_type()
        );
    }
    leaf_text(array, row).map(Some)
}

/// Whether [`scalar_text`] supports values of this type (everything except nested types).
pub fn is_scalar_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => false,
        DataType::Dictionary(_, values) => is_scalar_type(values),
        DataType::RunEndEncoded(_, values) => is_scalar_type(values.data_type()),
        _ => true,
    }
}

fn format_value(array: &dyn Array, row: usize, limit: usize, depth: usize) -> String {
    if row >= array.len() || array.is_null(row) {
        return "null".to_string();
    }

    if depth >= MAX_NESTED_DEPTH {
        return "<nested...>".to_string();
    }

    if let Some((values, index)) = dictionary_value(array, row) {
        return format_value(values, index, limit, depth);
    }

    match array.data_type() {
        DataType::Utf8 => typed(array, row, limit, |a: &StringArray, row| {
            truncate(a.value(row), limit)
        }),
        DataType::LargeUtf8 => typed(array, row, limit, |a: &LargeStringArray, row| {
            truncate(a.value(row), limit)
        }),
        DataType::Utf8View => typed(array, row, limit, |a: &StringViewArray, row| {
            truncate(a.value(row), limit)
        }),
        DataType::Binary => typed(array, row, limit, |a: &BinaryArray, row| {
            format_binary(a.value(row), limit)
        }),
        DataType::LargeBinary => typed(array, row, limit, |a: &LargeBinaryArray, row| {
            format_binary(a.value(row), limit)
        }),
        DataType::BinaryView => typed(array, row, limit, |a: &BinaryViewArray, row| {
            format_binary(a.value(row), limit)
        }),
        DataType::FixedSizeBinary(_) => {
            typed(array, row, limit, |a: &FixedSizeBinaryArray, row| {
                format_binary(a.value(row), limit)
            })
        }
        DataType::List(_) => typed(array, row, limit, |a: &ListArray, row| {
            format_sequence(a.value(row).as_ref(), limit, depth + 1)
        }),
        DataType::LargeList(_) => typed(array, row, limit, |a: &LargeListArray, row| {
            format_sequence(a.value(row).as_ref(), limit, depth + 1)
        }),
        DataType::FixedSizeList(_, _) => typed(array, row, limit, |a: &FixedSizeListArray, row| {
            format_sequence(a.value(row).as_ref(), limit, depth + 1)
        }),
        DataType::Struct(fields) => typed(array, row, limit, |a: &StructArray, row| {
            format_struct(a, row, fields, limit, depth + 1)
        }),
        DataType::Map(_, _) => typed(array, row, limit, |a: &MapArray, row| {
            format_map(a, row, limit, depth + 1)
        }),
        _ => match leaf_text(array, row) {
            Ok(text) => truncate(&text, limit),
            Err(error) => truncate(&format!("<{error}>"), limit),
        },
    }
}

/// Text for one non-null, non-nested, non-dictionary value.
fn leaf_text(array: &dyn Array, row: usize) -> Result<String> {
    let text = match array.data_type() {
        DataType::Boolean => downcast::<BooleanArray>(array)?.value(row).to_string(),
        DataType::Int8 => primitive_text::<Int8Type>(array, row)?,
        DataType::Int16 => primitive_text::<Int16Type>(array, row)?,
        DataType::Int32 => primitive_text::<Int32Type>(array, row)?,
        DataType::Int64 => primitive_text::<Int64Type>(array, row)?,
        DataType::UInt8 => primitive_text::<UInt8Type>(array, row)?,
        DataType::UInt16 => primitive_text::<UInt16Type>(array, row)?,
        DataType::UInt32 => primitive_text::<UInt32Type>(array, row)?,
        DataType::UInt64 => primitive_text::<UInt64Type>(array, row)?,
        DataType::Float32 => primitive_text::<Float32Type>(array, row)?,
        DataType::Float64 => primitive_text::<Float64Type>(array, row)?,
        DataType::Utf8 => downcast::<StringArray>(array)?.value(row).to_owned(),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array)?.value(row).to_owned(),
        DataType::Utf8View => downcast::<StringViewArray>(array)?.value(row).to_owned(),
        DataType::Binary => hex(downcast::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => hex(downcast::<LargeBinaryArray>(array)?.value(row)),
        DataType::BinaryView => hex(downcast::<BinaryViewArray>(array)?.value(row)),
        DataType::FixedSizeBinary(_) => hex(downcast::<FixedSizeBinaryArray>(array)?.value(row)),
        // Dates, timestamps, times, durations, intervals, decimals, Float16, run-end
        // encoded values and anything else: arrow-rs's own display rules.
        _ => ArrayFormatter::try_new(array, &FormatOptions::new())?
            .value(row)
            .try_to_string()?,
    };
    Ok(text)
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("{:?} array type mismatch", array.data_type()))
}

fn primitive_text<T>(array: &dyn Array, row: usize) -> Result<String>
where
    T: ArrowPrimitiveType,
    T::Native: Display,
{
    Ok(downcast::<PrimitiveArray<T>>(array)?.value(row).to_string())
}

fn hex(value: &[u8]) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// For a dictionary-encoded cell, the values array and the index it references.
fn dictionary_value(array: &dyn Array, row: usize) -> Option<(&dyn Array, usize)> {
    fn lookup<K: ArrowDictionaryKeyType>(
        array: &dyn Array,
        row: usize,
    ) -> Option<(&dyn Array, usize)> {
        let dictionary = array.as_any().downcast_ref::<DictionaryArray<K>>()?;
        Some((dictionary.values().as_ref(), dictionary.key(row)?))
    }

    let DataType::Dictionary(key, _) = array.data_type() else {
        return None;
    };
    match key.as_ref() {
        DataType::Int8 => lookup::<Int8Type>(array, row),
        DataType::Int16 => lookup::<Int16Type>(array, row),
        DataType::Int32 => lookup::<Int32Type>(array, row),
        DataType::Int64 => lookup::<Int64Type>(array, row),
        DataType::UInt8 => lookup::<UInt8Type>(array, row),
        DataType::UInt16 => lookup::<UInt16Type>(array, row),
        DataType::UInt32 => lookup::<UInt32Type>(array, row),
        DataType::UInt64 => lookup::<UInt64Type>(array, row),
        _ => None,
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
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        DurationSecondArray, Float64Array, Int32Array, LargeListArray, ListArray, StringArray,
        StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampSecondArray, UInt64Array,
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
            "1970-01-02"
        );
        assert_eq!(
            format_cell(&TimestampMillisecondArray::from(vec![Some(1000)]), 0),
            "1970-01-01T00:00:01",
            "naive timestamps have no zone suffix"
        );
    }

    #[test]
    fn formats_temporal_values_in_their_time_zone() {
        let fixed = TimestampSecondArray::from(vec![1_700_000_000]).with_timezone("+01:00");
        assert_eq!(format_cell(&fixed, 0), "2023-11-14T23:13:20+01:00");
        let named = TimestampSecondArray::from(vec![1_700_000_000]).with_timezone("Europe/Berlin");
        assert_eq!(format_cell(&named, 0), "2023-11-14T23:13:20+01:00");
        let utc = TimestampMicrosecondArray::from(vec![1_500_000]).with_timezone("UTC");
        assert_eq!(format_cell(&utc, 0), "1970-01-01T00:00:01.500Z");
        assert_eq!(
            format_cell(&Date64Array::from(vec![86_400_000]), 0),
            "1970-01-02T00:00:00",
            "Date64 stores milliseconds, so arrow shows it as a date-time"
        );
        assert_eq!(
            format_cell(&Time64MicrosecondArray::from(vec![3_723_000_001]), 0),
            "01:02:03.000001"
        );
    }

    #[test]
    fn formats_decimals_dictionaries_and_views() {
        let decimal = Decimal128Array::from(vec![12_345, -5])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(format_cell(&decimal, 0), "123.45");
        assert_eq!(format_cell(&decimal, 1), "-0.05");

        let dictionary: DictionaryArray<Int32Type> =
            vec![Some("red"), None, Some("green"), Some("red")]
                .into_iter()
                .collect();
        assert_eq!(format_cell(&dictionary, 0), "red");
        assert_eq!(format_cell(&dictionary, 1), "null");
        assert_eq!(format_cell(&dictionary, 2), "green");
        assert_eq!(format_cell_with_limit(&dictionary, 2, 2), "gr...");

        assert_eq!(format_cell(&StringViewArray::from(vec!["view"]), 0), "view");
        let fixed =
            FixedSizeBinaryArray::try_from_iter(vec![vec![0xab_u8, 0x01]].into_iter()).unwrap();
        assert_eq!(format_cell(&fixed, 0), "ab01");
        let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            [Some(vec![Some(1), Some(2)])],
            2,
        );
        assert_eq!(format_cell(&list, 0), "[1, 2]");
    }

    #[test]
    fn scalar_text_is_untruncated_and_rejects_nested_values() {
        let long = "x".repeat(DEFAULT_CELL_LIMIT * 2);
        let strings = StringArray::from(vec![Some(long.as_str()), None]);
        assert_eq!(scalar_text(&strings, 0).unwrap(), Some(long.clone()));
        assert_eq!(scalar_text(&strings, 1).unwrap(), None);
        assert_eq!(
            scalar_text(&Date32Array::from(vec![19_000]), 0).unwrap(),
            Some("2022-01-08".to_string())
        );
        let dictionary: DictionaryArray<Int32Type> = vec!["a", "b"].into_iter().collect();
        assert_eq!(scalar_text(&dictionary, 1).unwrap(), Some("b".to_string()));

        let list = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1)])]);
        assert!(scalar_text(&list, 0).is_err());
        assert!(scalar_text(&strings, 5).is_err());

        assert!(is_scalar_type(&DataType::Decimal128(10, 2)));
        assert!(is_scalar_type(dictionary.data_type()));
        assert!(!is_scalar_type(list.data_type()));
    }

    #[test]
    fn bounds_large_values() {
        let string = StringArray::from(vec![Some("abcdef")]);
        assert_eq!(format_cell_with_limit(&string, 0, 3), "abc...");

        let binary = BinaryArray::from_vec(vec![b"abcdef"]);
        assert_eq!(format_cell_with_limit(&binary, 0, 6), "616263...");
    }

    #[test]
    fn formats_durations_with_arrow_rules() {
        let duration = DurationSecondArray::from(vec![Some(1)]);

        assert_eq!(format_cell(&duration, 0), "PT1S");
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
