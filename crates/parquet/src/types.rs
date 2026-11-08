//! Which Arrow types each PAVI operation accepts.
//!
//! | Operation                                          | Accepted types        |
//! |----------------------------------------------------|-----------------------|
//! | sort, MIN/MAX, GROUP BY, filters, profiling        | [`is_comparable`]     |
//! | SUM, AVG                                           | [`is_numeric`]        |
//! | `contains` filter                                  | [`is_string`]         |
//! | CSV export                                         | `value::is_scalar_type` |

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

/// Types with a total order that PAVI can compare: sort keys, MIN/MAX, GROUP BY
/// keys, filter predicates and profiling.
pub fn is_comparable(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Boolean)
        || is_numeric(data_type)
        || is_string(data_type)
        || is_temporal(data_type)
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
        ] {
            assert!(is_comparable(&data_type), "{data_type:?}");
        }
        for data_type in [
            DataType::Binary,
            DataType::Decimal128(10, 2),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            assert!(!is_comparable(&data_type), "{data_type:?}");
        }
        assert!(is_numeric(&DataType::Int8) && !is_numeric(&DataType::Date32));
        assert!(is_string(&DataType::Utf8View) && !is_string(&DataType::Binary));
    }
}
