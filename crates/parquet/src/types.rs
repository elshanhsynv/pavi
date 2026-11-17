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

#[cfg(test)]
mod tests {
    use arrow_schema::TimeUnit;

    use super::*;

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
