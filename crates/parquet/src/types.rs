//! Which Arrow types each PAVI operation accepts.
//!
//! | Operation                                          | Accepted types        |
//! |----------------------------------------------------|-----------------------|
//! | sort, MIN/MAX, GROUP BY, filters, profiling        | [`is_comparable`]     |
//! | SUM, AVG                                           | [`is_summable`]       |
//! | `contains` filter                                  | [`is_string`]         |
//! | CSV export                                         | `value::is_scalar_type` |
//!
//! Columns are read as [`read_type`]: dictionary-encoded columns as their
//! values, so every operation treats them as it treats those values.

use arrow_schema::DataType;

pub fn is_signed_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

pub fn is_unsigned_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}

pub fn is_float(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

/// Integers and floats: the types SUM and AVG accept.
pub fn is_numeric(data_type: &DataType) -> bool {
    is_signed_integer(data_type) || is_unsigned_integer(data_type) || is_float(data_type)
}

/// Decimals PAVI can compare and sum: their unscaled values fit in an `i128`.
/// A `Decimal256` of up to 38 digits is read as a `Decimal128` ([`read_type`]);
/// a wider one is shown and exported but not compared.
pub fn is_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _)
    )
}

/// Time of day, in any unit.
pub fn is_time(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Time32(_) | DataType::Time64(_))
}

/// Types SUM and AVG accept: integers, floats and decimals.
pub fn is_summable(data_type: &DataType) -> bool {
    is_numeric(data_type) || is_decimal(data_type)
}

/// UTF-8 text in any Arrow layout.
pub fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Dates and timestamps (with or without a time zone).
pub fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// Lengths of time, in any unit.
pub fn is_duration(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Duration(_))
}

/// Bytes in any Arrow layout; compared byte by byte.
pub fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Types with a total order that PAVI can compare: sort keys, MIN/MAX, GROUP BY
/// keys, filter predicates and profiling.
pub fn is_comparable(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Boolean)
        || is_numeric(data_type)
        || is_decimal(data_type)
        || is_string(data_type)
        || is_temporal(data_type)
        || is_time(data_type)
        || is_duration(data_type)
        || is_binary(data_type)
}

/// The type a column of `data_type` is read as. A dictionary-encoded column
/// (pandas' categoricals, for one) is read as its values; a `Decimal256` of
/// up to 38 digits as the `Decimal128` that holds it exactly. Everything else
/// as it is. Both conversions keep every value, and cost one cast per batch.
pub fn read_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, values) => read_type(values),
        DataType::Decimal256(precision, scale) if *precision <= 38 => {
            DataType::Decimal128(*precision, *scale)
        }
        other => other.clone(),
    }
}

/// The type that holds the values of two files' versions of a folder column
/// (ADR 0019), or `None` when there is none and the folder can't be read as one
/// table. Both are [`read_type`]s. Values are cast to it as they are read:
///
/// - integers: the wider one; signed and unsigned give a signed type that holds
///   both (none for `UInt64` with a signed type);
/// - floats: the wider one; an integer with a float gives `Float64`;
/// - decimals: enough digits on both sides of the point for both, up to 38;
/// - text: `LargeUtf8` when the two differ (`Utf8`, `LargeUtf8`, `Utf8View`),
///   and binary likewise `LargeBinary`;
/// - a column written as `Null` (no value in that file) takes the other type.
///
/// Anything else must be equal: dates, times, timestamps (unit and zone),
/// durations, booleans and nested types.
pub fn common_type(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    if a == b {
        return Some(a.clone());
    }
    match (a, b) {
        (Null, other) | (other, Null) => Some(other.clone()),
        _ if is_number(a) && is_number(b) => common_number(a, b),
        _ if is_text(a) && is_text(b) => Some(LargeUtf8),
        _ if is_bytes(a) && is_bytes(b) => Some(LargeBinary),
        _ => {
            let ((left_precision, left_scale), (right_precision, right_scale)) =
                (decimal_digits(a)?, decimal_digits(b)?);
            let scale = left_scale.max(right_scale);
            let whole = (i16::from(left_precision) - i16::from(left_scale))
                .max(i16::from(right_precision) - i16::from(right_scale));
            let precision = whole + i16::from(scale);
            if whole < 0 || precision > 38 || scale < 0 {
                return None;
            }
            Some(Decimal128(u8::try_from(precision).ok()?, scale))
        }
    }
}

/// Numbers: see [`common_type`].
fn common_number(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    let float_rank = |data_type: &DataType| match data_type {
        Float16 => Some(1),
        Float32 => Some(2),
        Float64 => Some(3),
        _ => None,
    };
    match (float_rank(a), float_rank(b)) {
        (Some(left), Some(right)) => {
            return Some(match left.max(right) {
                1 => Float16,
                2 => Float32,
                _ => Float64,
            });
        }
        // An integer with a float.
        (Some(_), None) | (None, Some(_)) => return Some(Float64),
        (None, None) => {}
    }
    // Bits of magnitude each integer type needs, and whether it is signed.
    let bits = |data_type: &DataType| match data_type {
        Int8 => Some((8, true)),
        Int16 => Some((16, true)),
        Int32 => Some((32, true)),
        Int64 => Some((64, true)),
        UInt8 => Some((8, false)),
        UInt16 => Some((16, false)),
        UInt32 => Some((32, false)),
        UInt64 => Some((64, false)),
        _ => None,
    };
    let ((a_bits, a_signed), (b_bits, b_signed)) = (bits(a)?, bits(b)?);
    let signed = a_signed || b_signed;
    // An unsigned type needs one more bit to fit in a signed one.
    let need = |bits: u32, is_signed: bool| if signed && !is_signed { bits * 2 } else { bits };
    let width = need(a_bits, a_signed).max(need(b_bits, b_signed));
    Some(match (signed, width) {
        (true, 8) => Int8,
        (true, 16) => Int16,
        (true, 32) => Int32,
        (true, 64) => Int64,
        (false, 8) => UInt8,
        (false, 16) => UInt16,
        (false, 32) => UInt32,
        (false, 64) => UInt64,
        _ => return None,
    })
}

fn is_number(data_type: &DataType) -> bool {
    is_numeric(data_type) || matches!(data_type, DataType::Float16)
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_bytes(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

/// A decimal's precision and scale; `None` for other types and for a
/// `Decimal256` (wider than 38 digits once read, see [`read_type`]).
fn decimal_digits(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::TimeUnit;

    use super::*;

    #[test]
    fn finds_a_type_that_holds_two_files_values() {
        use DataType::*;
        let common = |a: DataType, b: DataType| {
            let forward = common_type(&a, &b);
            assert_eq!(forward, common_type(&b, &a), "{a} and {b}");
            forward
        };
        assert_eq!(common(Int32, Int32), Some(Int32));
        assert_eq!(common(Int8, Int32), Some(Int32));
        assert_eq!(common(UInt8, UInt32), Some(UInt32));
        assert_eq!(common(UInt8, Int8), Some(Int16));
        assert_eq!(common(UInt32, Int16), Some(Int64));
        assert_eq!(common(UInt64, Int8), None);
        assert_eq!(common(UInt64, UInt8), Some(UInt64));
        assert_eq!(common(Float32, Float64), Some(Float64));
        assert_eq!(common(Float16, Float32), Some(Float32));
        assert_eq!(common(Int64, Float32), Some(Float64));
        assert_eq!(common(Utf8, Utf8View), Some(LargeUtf8));
        assert_eq!(common(Binary, LargeBinary), Some(LargeBinary));
        assert_eq!(common(Null, Date32), Some(Date32));
        assert_eq!(
            common(Decimal128(10, 2), Decimal128(12, 4)),
            Some(Decimal128(12, 4))
        );
        assert_eq!(
            common(Decimal64(18, 0), Decimal128(10, 6)),
            Some(Decimal128(24, 6))
        );
        assert_eq!(common(Decimal128(38, 0), Decimal128(38, 2)), None);
        assert_eq!(common(Int32, Utf8), None);
        assert_eq!(common(Int32, Decimal128(10, 2)), None);
        assert_eq!(common(Date32, Date64), None);
        assert_eq!(
            common(
                Timestamp(TimeUnit::Millisecond, None),
                Timestamp(TimeUnit::Microsecond, None)
            ),
            None
        );
    }

    #[test]
    fn classifies_comparable_types() {
        for data_type in [
            DataType::Boolean,
            DataType::UInt64,
            DataType::Float32,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Berlin".into())),
            DataType::Decimal32(9, 2),
            DataType::Decimal128(38, -3),
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Binary,
            DataType::FixedSizeBinary(16),
            DataType::BinaryView,
        ] {
            assert!(is_comparable(&data_type), "{data_type:?}");
        }
        for data_type in [
            DataType::Decimal256(76, 2),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Interval(arrow_schema::IntervalUnit::DayTime),
        ] {
            assert!(!is_comparable(&data_type), "{data_type:?}");
        }
        // Dictionaries are read as their values, narrow Decimal256 as Decimal128.
        assert_eq!(
            read_type(&DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::LargeUtf8)
            )),
            DataType::LargeUtf8
        );
        assert_eq!(
            read_type(&DataType::Decimal256(20, 4)),
            DataType::Decimal128(20, 4)
        );
        assert_eq!(
            read_type(&DataType::Decimal256(60, 4)),
            DataType::Decimal256(60, 4)
        );
        // Durations can be compared, not summed.
        assert!(!is_summable(&DataType::Duration(TimeUnit::Second)));
        assert!(is_numeric(&DataType::Int8) && !is_numeric(&DataType::Date32));
        // Decimals can be summed; times of day cannot.
        assert!(
            is_summable(&DataType::Decimal64(18, 4)) && !is_numeric(&DataType::Decimal64(18, 4))
        );
        assert!(!is_summable(&DataType::Time32(TimeUnit::Second)));
        assert!(is_string(&DataType::Utf8View) && !is_string(&DataType::Binary));
    }
}
