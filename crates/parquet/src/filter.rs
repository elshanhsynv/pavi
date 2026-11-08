use std::cmp::Ordering;

use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
    timezone::Tz,
    types::{Date32Type, Date64Type},
};
use arrow_cast::parse::{Parser, string_to_datetime};
use arrow_schema::{DataType, Schema, TimeUnit};
use parquet::file::statistics::Statistics;

use crate::types::{is_comparable, is_numeric, is_string};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Contains,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterExpr {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

impl FilterExpr {
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            bail!("filter expression is empty");
        }

        if let Some((column, value)) = input.split_once(" contains ") {
            return Ok(Self {
                column: column.trim().to_string(),
                op: FilterOp::Contains,
                value: unquote(value.trim()),
            });
        }

        for (needle, op) in [
            ("==", FilterOp::Eq),
            ("!=", FilterOp::Ne),
            (">=", FilterOp::Ge),
            ("<=", FilterOp::Le),
            (">", FilterOp::Gt),
            ("<", FilterOp::Lt),
        ] {
            if let Some((column, value)) = input.split_once(needle) {
                return Ok(Self {
                    column: column.trim().to_string(),
                    op,
                    value: unquote(value.trim()),
                });
            }
        }

        bail!("unsupported filter expression: {input}");
    }

    pub fn column_index(&self, schema: &Schema) -> Result<usize> {
        if let Ok(index) = self.column.parse::<usize>() {
            if index < schema.fields().len() {
                return Ok(index);
            }
            bail!("filter column index {index} out of range");
        }

        schema
            .fields()
            .iter()
            .position(|field| field.name() == &self.column)
            .ok_or_else(|| anyhow!("unknown filter column '{}'", self.column))
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<()> {
        let column = self.column_index(schema)?;
        self.validate_type(schema.field(column).data_type())
    }

    /// Checks that this operator and value can be used on a column of `data_type`.
    ///
    /// Filters accept the comparable types (`types::is_comparable`): booleans with
    /// `==`/`!=`, numbers, dates and timestamps with any comparison, and text with any
    /// comparison or `contains`.
    pub fn validate_type(&self, data_type: &DataType) -> Result<()> {
        if !is_comparable(data_type) {
            bail!("filtering is not supported for {data_type:?} columns");
        }
        if self.op == FilterOp::Contains && !is_string(data_type) {
            bail!("contains is only supported for text columns");
        }
        if *data_type == DataType::Boolean {
            if !matches!(self.op, FilterOp::Eq | FilterOp::Ne) {
                bail!("boolean columns only support == and !=");
            }
            self.value
                .parse::<bool>()
                .map_err(|_| anyhow!("'{}' is not a boolean", self.value))?;
        } else if !is_string(data_type) {
            self.ordered_value(data_type)?;
        }
        Ok(())
    }

    /// The comparison value for number, date and timestamp columns, in the column's
    /// own units: days for `Date32`, milliseconds for `Date64`, the column's unit
    /// for timestamps. A timestamp without an offset is read in the column's time
    /// zone (naive columns: as written).
    fn ordered_value(&self, data_type: &DataType) -> Result<Number> {
        let value = self.value.as_str();
        let parsed = match data_type {
            DataType::Date32 => Date32Type::parse(value).map(|days| Number::Int(days.into())),
            DataType::Date64 => Date64Type::parse(value).map(|millis| Number::Int(millis.into())),
            DataType::Timestamp(unit, timezone) => {
                parse_timestamp(value, *unit, timezone.as_deref())
                    .map(|ticks| Number::Int(ticks.into()))
            }
            data_type if is_numeric(data_type) => Number::parse(value),
            _ => None,
        };
        parsed.ok_or_else(|| match data_type {
            DataType::Date32 | DataType::Date64 => {
                anyhow!("'{value}' is not a date (expected YYYY-MM-DD)")
            }
            DataType::Timestamp(_, _) => {
                anyhow!("'{value}' is not a timestamp (expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
            }
            _ => anyhow!("'{value}' is not a number"),
        })
    }

    pub fn evaluate_batch(
        &self,
        batch: &RecordBatch,
        column_position: usize,
    ) -> Result<BooleanArray> {
        let array = batch.column(column_position).as_ref();
        let data_type = array.data_type();
        self.validate_type(data_type)?;
        let mask = match data_type {
            DataType::Boolean => self.evaluate_bool(array)?,
            DataType::Utf8 => self.evaluate_strings(array, |a: &StringArray, row| a.value(row))?,
            DataType::LargeUtf8 => {
                self.evaluate_strings(array, |a: &LargeStringArray, row| a.value(row))?
            }
            DataType::Utf8View => {
                self.evaluate_strings(array, |a: &StringViewArray, row| a.value(row))?
            }
            _ => {
                let expected = self.ordered_value(data_type)?;
                macro_rules! ordered {
                    ($array:ty, $number:path) => {
                        self.evaluate_ordered(array, expected, |a: &$array, row| {
                            $number(a.value(row).into())
                        })?
                    };
                }
                match data_type {
                    DataType::Int8 => ordered!(Int8Array, Number::Int),
                    DataType::Int16 => ordered!(Int16Array, Number::Int),
                    DataType::Int32 => ordered!(Int32Array, Number::Int),
                    DataType::Int64 => ordered!(Int64Array, Number::Int),
                    DataType::UInt8 => ordered!(UInt8Array, Number::Int),
                    DataType::UInt16 => ordered!(UInt16Array, Number::Int),
                    DataType::UInt32 => ordered!(UInt32Array, Number::Int),
                    DataType::UInt64 => ordered!(UInt64Array, Number::Int),
                    DataType::Float32 => ordered!(Float32Array, Number::Float),
                    DataType::Float64 => ordered!(Float64Array, Number::Float),
                    DataType::Date32 => ordered!(Date32Array, Number::Int),
                    DataType::Date64 => ordered!(Date64Array, Number::Int),
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        ordered!(TimestampSecondArray, Number::Int)
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        ordered!(TimestampMillisecondArray, Number::Int)
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        ordered!(TimestampMicrosecondArray, Number::Int)
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        ordered!(TimestampNanosecondArray, Number::Int)
                    }
                    other => bail!("filtering is not supported for {other:?} columns"),
                }
            }
        };

        Ok(mask)
    }

    /// Returns `false` only when row-group statistics prove that no row can match.
    ///
    /// `data_type` is the Arrow type of the filtered column. Parquet stores unsigned
    /// integers, dates and timestamps in signed integer physical types, so statistics
    /// are read in the same units as [`Self::ordered_value`].
    pub fn might_match_statistics(
        &self,
        statistics: Option<&Statistics>,
        data_type: &DataType,
    ) -> bool {
        let Some(statistics) = statistics else {
            return true;
        };
        let expected = || self.ordered_value(data_type).ok();

        match statistics {
            Statistics::Boolean(stats) => {
                let Ok(value) = self.value.parse::<bool>() else {
                    return true;
                };
                bool_range_might_match(
                    stats.min_opt().copied(),
                    stats.max_opt().copied(),
                    value,
                    &self.op,
                )
            }
            Statistics::Int32(stats) => number_range_might_match(
                stats
                    .min_opt()
                    .map(|value| int32_statistic(*value, data_type)),
                stats
                    .max_opt()
                    .map(|value| int32_statistic(*value, data_type)),
                expected(),
                &self.op,
            ),
            Statistics::Int64(stats) => number_range_might_match(
                stats
                    .min_opt()
                    .map(|value| int64_statistic(*value, data_type)),
                stats
                    .max_opt()
                    .map(|value| int64_statistic(*value, data_type)),
                expected(),
                &self.op,
            ),
            Statistics::Float(stats) => number_range_might_match(
                stats.min_opt().map(|value| Number::Float((*value).into())),
                stats.max_opt().map(|value| Number::Float((*value).into())),
                expected(),
                &self.op,
            ),
            Statistics::Double(stats) => number_range_might_match(
                stats.min_opt().map(|value| Number::Float(*value)),
                stats.max_opt().map(|value| Number::Float(*value)),
                expected(),
                &self.op,
            ),
            Statistics::ByteArray(stats) => {
                let min = stats
                    .min_bytes_opt()
                    .and_then(|value| std::str::from_utf8(value).ok());
                let max = stats
                    .max_bytes_opt()
                    .and_then(|value| std::str::from_utf8(value).ok());
                string_range_might_match(min, max, &self.value, &self.op)
            }
            _ => true,
        }
    }

    fn evaluate_bool(&self, array: &dyn Array) -> Result<BooleanArray> {
        let expected = self
            .value
            .parse::<bool>()
            .map_err(|_| anyhow!("'{}' is not a boolean", self.value))?;
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow!("boolean array type mismatch"))?;

        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_bool(array.value(row), expected, &self.op))
        })))
    }

    fn evaluate_ordered<T>(
        &self,
        array: &dyn Array,
        expected: Number,
        value_at: impl Fn(&T, usize) -> Number,
    ) -> Result<BooleanArray>
    where
        T: Array + 'static,
    {
        let array = array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| anyhow!("{:?} array type mismatch", array.data_type()))?;

        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_number(value_at(array, row), expected, &self.op))
        })))
    }

    fn evaluate_strings<T>(
        &self,
        array: &dyn Array,
        value_at: impl Fn(&T, usize) -> &str,
    ) -> Result<BooleanArray>
    where
        T: Array + 'static,
    {
        let array = array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| anyhow!("string array type mismatch"))?;

        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_str(value_at(array, row), &self.value, &self.op))
        })))
    }
}

fn unquote(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn compare_bool(left: bool, right: bool, op: &FilterOp) -> bool {
    match op {
        FilterOp::Eq => left == right,
        FilterOp::Ne => left != right,
        _ => false,
    }
}

/// A filter operand: integers stay exact, everything else is compared as `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Number {
    Int(i128),
    Float(f64),
}

impl Number {
    fn parse(value: &str) -> Option<Self> {
        value
            .parse::<i128>()
            .map(Self::Int)
            .ok()
            .or_else(|| value.parse::<f64>().ok().map(Self::Float))
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    /// `None` when either side is NaN.
    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Some(left.cmp(&right)),
            _ => self.as_f64().partial_cmp(&other.as_f64()),
        }
    }
}

/// Parses a timestamp literal into ticks of `unit`. Text without an offset is read
/// in `timezone`; naive columns (no time zone) store wall-clock time as if UTC.
fn parse_timestamp(value: &str, unit: TimeUnit, timezone: Option<&str>) -> Option<i64> {
    let timezone: Tz = timezone.unwrap_or("+00:00").parse().ok()?;
    let datetime = string_to_datetime(&timezone, value).ok()?;
    match unit {
        TimeUnit::Second => Some(datetime.timestamp()),
        TimeUnit::Millisecond => Some(datetime.timestamp_millis()),
        TimeUnit::Microsecond => Some(datetime.timestamp_micros()),
        TimeUnit::Nanosecond => datetime.timestamp_nanos_opt(),
    }
}

fn int32_statistic(value: i32, data_type: &DataType) -> Number {
    match data_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Number::Int((value as u32).into()),
        _ => Number::Int(value.into()),
    }
}

fn int64_statistic(value: i64, data_type: &DataType) -> Number {
    match data_type {
        DataType::UInt64 => Number::Int((value as u64).into()),
        _ => Number::Int(value.into()),
    }
}

fn compare_number(left: Number, right: Number, op: &FilterOp) -> bool {
    let ordering = left.compare(right);
    match op {
        FilterOp::Eq => ordering == Some(Ordering::Equal),
        FilterOp::Ne => ordering != Some(Ordering::Equal),
        FilterOp::Gt => ordering == Some(Ordering::Greater),
        FilterOp::Ge => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
        FilterOp::Lt => ordering == Some(Ordering::Less),
        FilterOp::Le => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        FilterOp::Contains => false,
    }
}

fn compare_str(left: &str, right: &str, op: &FilterOp) -> bool {
    match op {
        FilterOp::Eq => left == right,
        FilterOp::Ne => left != right,
        FilterOp::Contains => left.contains(right),
        FilterOp::Gt => left > right,
        FilterOp::Ge => left >= right,
        FilterOp::Lt => left < right,
        FilterOp::Le => left <= right,
    }
}

fn bool_range_might_match(
    min: Option<bool>,
    max: Option<bool>,
    value: bool,
    op: &FilterOp,
) -> bool {
    match op {
        FilterOp::Eq => min
            .zip(max)
            .is_none_or(|(min, max)| min <= value && value <= max),
        FilterOp::Ne => min.zip(max) != Some((value, value)),
        _ => true,
    }
}

fn number_range_might_match(
    min: Option<Number>,
    max: Option<Number>,
    value: Option<Number>,
    op: &FilterOp,
) -> bool {
    if *op == FilterOp::Contains {
        return true;
    }

    let Some(value) = value else {
        return true;
    };
    let Some((min, max)) = min.zip(max) else {
        return true;
    };
    // Unordered statistics (e.g. from a writer using a different sort order) prove nothing.
    if min.compare(max) == Some(Ordering::Greater) {
        return true;
    }

    // An unknown ordering (NaN) must never prune.
    let may = |ordering: Option<Ordering>, allowed: &[Ordering]| {
        ordering.is_none_or(|ordering| allowed.contains(&ordering))
    };
    let low = min.compare(value);
    let high = max.compare(value);
    match op {
        FilterOp::Eq => {
            may(low, &[Ordering::Less, Ordering::Equal])
                && may(high, &[Ordering::Greater, Ordering::Equal])
        }
        FilterOp::Ne => !(low == Some(Ordering::Equal) && high == Some(Ordering::Equal)),
        FilterOp::Gt => may(high, &[Ordering::Greater]),
        FilterOp::Ge => may(high, &[Ordering::Greater, Ordering::Equal]),
        FilterOp::Lt => may(low, &[Ordering::Less]),
        FilterOp::Le => may(low, &[Ordering::Less, Ordering::Equal]),
        FilterOp::Contains => true,
    }
}

fn string_range_might_match(
    min: Option<&str>,
    max: Option<&str>,
    value: &str,
    op: &FilterOp,
) -> bool {
    if *op == FilterOp::Contains {
        return true;
    }

    let Some((min, max)) = min.zip(max) else {
        return true;
    };

    match op {
        FilterOp::Eq => min <= value && value <= max,
        FilterOp::Ne => !(min == value && max == value),
        FilterOp::Gt => max > value,
        FilterOp::Ge => max >= value,
        FilterOp::Lt => min < value,
        FilterOp::Le => min <= value,
        FilterOp::Contains => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn parses_supported_filters() {
        assert_eq!(
            FilterExpr::parse("name contains \"abc\"").unwrap(),
            FilterExpr {
                column: "name".to_string(),
                op: FilterOp::Contains,
                value: "abc".to_string()
            }
        );
        assert_eq!(FilterExpr::parse("0 >= 42").unwrap().op, FilterOp::Ge);
        assert_eq!(FilterExpr::parse("0 <= 42").unwrap().op, FilterOp::Le);
        assert_eq!(FilterExpr::parse("0 == 42").unwrap().op, FilterOp::Eq);
        assert_eq!(FilterExpr::parse("0 != 42").unwrap().op, FilterOp::Ne);
        assert_eq!(FilterExpr::parse("0 > 42").unwrap().op, FilterOp::Gt);
        assert_eq!(FilterExpr::parse("0 < 42").unwrap().op, FilterOp::Lt);
    }

    #[test]
    fn rejects_bad_filters() {
        assert!(FilterExpr::parse("").is_err());
        assert!(FilterExpr::parse("abc").is_err());
    }

    #[test]
    fn rejects_scalar_filters_on_nested_columns_clearly() {
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let error = FilterExpr::parse("items == 1")
            .unwrap()
            .validate_schema(&schema)
            .unwrap_err();
        assert!(error.to_string().contains("filtering is not supported"));
    }

    #[test]
    fn evaluates_every_operator() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
            ]))],
        )
        .unwrap();

        for (filter, expected) in [
            ("x == 2", vec![false, true, false, false]),
            ("x != 2", vec![true, false, true, false]),
            ("x > 2", vec![false, false, true, false]),
            ("x >= 2", vec![false, true, true, false]),
            ("x < 2", vec![true, false, false, false]),
            ("x <= 2", vec![true, true, false, false]),
        ] {
            let actual = FilterExpr::parse(filter)
                .unwrap()
                .evaluate_batch(&batch, 0)
                .unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|value| value.unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn evaluates_contains_and_type_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        let mask = FilterExpr::parse("s contains \"ha\"")
            .unwrap()
            .evaluate_batch(&batch, 0)
            .unwrap();
        assert_eq!(
            mask.iter().map(|value| value.unwrap()).collect::<Vec<_>>(),
            vec![true, false, false]
        );
        assert!(
            FilterExpr::parse("b > true")
                .unwrap()
                .evaluate_batch(&batch, 1)
                .is_err()
        );
        assert!(
            FilterExpr::parse("b contains true")
                .unwrap()
                .evaluate_batch(&batch, 1)
                .is_err()
        );
    }

    #[test]
    fn compares_large_integers_exactly() {
        let base = 9_007_199_254_740_992_i64; // 2^53: the first integer f64 can't step past by 1
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![base, base + 1, base + 2]))],
        )
        .unwrap();

        for (filter, expected) in [
            ("x == 9007199254740993", vec![false, true, false]),
            ("x != 9007199254740993", vec![true, false, true]),
            ("x > 9007199254740992", vec![false, true, true]),
            ("x <= 9007199254740993", vec![true, true, false]),
        ] {
            let actual = FilterExpr::parse(filter)
                .unwrap()
                .evaluate_batch(&batch, 0)
                .unwrap();
            assert_eq!(
                actual.iter().map(Option::unwrap).collect::<Vec<_>>(),
                expected,
                "{filter}"
            );
        }
    }

    #[test]
    fn decimal_literals_still_work_on_integer_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let mask = FilterExpr::parse("x > 1.5")
            .unwrap()
            .evaluate_batch(&batch, 0)
            .unwrap();
        assert_eq!(
            mask.iter().map(Option::unwrap).collect::<Vec<_>>(),
            vec![false, true, true]
        );
    }

    #[test]
    fn reads_unsigned_statistics_as_unsigned() {
        // u32 3_000_000_000 and 3_000_000_001 as stored in Parquet's signed INT32.
        let stats = Statistics::int32(
            Some(3_000_000_000_u32 as i32),
            Some(3_000_000_001_u32 as i32),
            None,
            Some(0),
            false,
        );
        let might_match = |filter: &str| {
            FilterExpr::parse(filter)
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::UInt32)
        };
        assert!(might_match("x > 2999999999"));
        assert!(might_match("x == 3000000001"));
        assert!(!might_match("x < 10"));
        assert!(!might_match("x > 3000000001"));

        let stats = Statistics::int64(
            Some(((1_u64 << 63) + 5) as i64),
            Some(((1_u64 << 63) + 6) as i64),
            None,
            Some(0),
            false,
        );
        let filter = FilterExpr::parse("x > 9223372036854775808").unwrap();
        assert!(filter.might_match_statistics(Some(&stats), &DataType::UInt64));
    }

    #[test]
    fn prunes_statistics_without_false_negatives() {
        let stats = Statistics::int32(Some(10), Some(20), None, Some(0), false);

        assert!(
            !FilterExpr::parse("x == 9")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            FilterExpr::parse("x == 10")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            FilterExpr::parse("x > 19")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            !FilterExpr::parse("x > 20")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            FilterExpr::parse("x != 9")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
    }
}
