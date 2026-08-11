use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Schema};
use parquet::file::statistics::Statistics;

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

    pub fn evaluate_batch(
        &self,
        batch: &RecordBatch,
        column_position: usize,
    ) -> Result<BooleanArray> {
        let array = batch.column(column_position).as_ref();
        let mask = match array.data_type() {
            DataType::Boolean => self.evaluate_bool(array)?,
            DataType::Int8 => {
                self.evaluate_numbers(array, |a: &Int8Array, row| a.value(row) as f64)?
            }
            DataType::Int16 => {
                self.evaluate_numbers(array, |a: &Int16Array, row| a.value(row) as f64)?
            }
            DataType::Int32 => {
                self.evaluate_numbers(array, |a: &Int32Array, row| a.value(row) as f64)?
            }
            DataType::Int64 => {
                self.evaluate_numbers(array, |a: &Int64Array, row| a.value(row) as f64)?
            }
            DataType::UInt8 => {
                self.evaluate_numbers(array, |a: &UInt8Array, row| a.value(row) as f64)?
            }
            DataType::UInt16 => {
                self.evaluate_numbers(array, |a: &UInt16Array, row| a.value(row) as f64)?
            }
            DataType::UInt32 => {
                self.evaluate_numbers(array, |a: &UInt32Array, row| a.value(row) as f64)?
            }
            DataType::UInt64 => {
                self.evaluate_numbers(array, |a: &UInt64Array, row| a.value(row) as f64)?
            }
            DataType::Float32 => {
                self.evaluate_numbers(array, |a: &Float32Array, row| a.value(row) as f64)?
            }
            DataType::Float64 => {
                self.evaluate_numbers(array, |a: &Float64Array, row| a.value(row))?
            }
            DataType::Utf8 => self.evaluate_strings(array, |a: &StringArray, row| a.value(row))?,
            DataType::LargeUtf8 => {
                self.evaluate_strings(array, |a: &LargeStringArray, row| a.value(row))?
            }
            other => bail!("filtering is not supported for {other:?} columns"),
        };

        Ok(mask)
    }

    pub fn might_match_statistics(&self, statistics: Option<&Statistics>) -> bool {
        let Some(statistics) = statistics else {
            return true;
        };

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
                stats.min_opt().map(|value| *value as f64),
                stats.max_opt().map(|value| *value as f64),
                &self.value,
                &self.op,
            ),
            Statistics::Int64(stats) => number_range_might_match(
                stats.min_opt().map(|value| *value as f64),
                stats.max_opt().map(|value| *value as f64),
                &self.value,
                &self.op,
            ),
            Statistics::Float(stats) => number_range_might_match(
                stats.min_opt().map(|value| *value as f64),
                stats.max_opt().map(|value| *value as f64),
                &self.value,
                &self.op,
            ),
            Statistics::Double(stats) => number_range_might_match(
                stats.min_opt().copied(),
                stats.max_opt().copied(),
                &self.value,
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

    fn evaluate_numbers<T>(
        &self,
        array: &dyn Array,
        value_at: impl Fn(&T, usize) -> f64,
    ) -> Result<BooleanArray>
    where
        T: Array + 'static,
    {
        if self.op == FilterOp::Contains {
            bail!("contains is only supported for UTF-8 columns");
        }

        let expected = self
            .value
            .parse::<f64>()
            .map_err(|_| anyhow!("'{}' is not a number", self.value))?;
        let array = array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| anyhow!("numeric array type mismatch"))?;

        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_f64(value_at(array, row), expected, &self.op))
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

fn compare_f64(left: f64, right: f64, op: &FilterOp) -> bool {
    match op {
        FilterOp::Eq => left == right,
        FilterOp::Ne => left != right,
        FilterOp::Gt => left > right,
        FilterOp::Ge => left >= right,
        FilterOp::Lt => left < right,
        FilterOp::Le => left <= right,
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
    min: Option<f64>,
    max: Option<f64>,
    value: &str,
    op: &FilterOp,
) -> bool {
    if *op == FilterOp::Contains {
        return true;
    }

    let Ok(value) = value.parse::<f64>() else {
        return true;
    };
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
}
