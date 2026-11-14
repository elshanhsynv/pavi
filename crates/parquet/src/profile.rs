//! Column profiles: row and null counts, distinct values, min, max, mean and
//! string lengths, accumulated batch by batch with bounded memory.
//!
//! The app profiles one column (with a distribution chart, which it adds) or
//! every column of a file; the CLI's `--profile` profiles every column too.
//! Both feed batches to the same accumulators, so they report the same numbers.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use arrow_array::{
    Array, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};

use crate::types::{is_comparable, is_decimal, is_string};
use crate::{ColumnInfo, ParquetSource};

/// Distinct values counted exactly for one column's profile; past it the
/// profile fails rather than report a wrong count.
pub const MAX_DISTINCT_VALUES: usize = 4_096;
/// Bytes of distinct values kept for one column's profile.
pub const MAX_DISTINCT_BYTES: usize = 1_024 * 1_024;
/// Longest value kept as a distinct value.
pub const MAX_PROFILE_VALUE_BYTES: usize = 128;
/// Most frequent values reported.
pub const FREQUENT_VALUE_LIMIT: usize = 10;
/// Columns one whole-file profile covers.
pub const MAX_FILE_PROFILE_COLUMNS: usize = 256;
/// Distinct values counted exactly per column in a whole-file profile; past it
/// the count reads "more than".
pub const FILE_PROFILE_DISTINCT_VALUES: usize = 1_000;
/// Bytes of distinct values kept per column in a whole-file profile.
pub const FILE_PROFILE_DISTINCT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub struct FrequentValue {
    pub value: String,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StringLengthStats {
    pub min: usize,
    pub max: usize,
    pub mean: f64,
}

/// What a profile found in one column.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    pub column: usize,
    pub name: String,
    pub data_type: DataType,
    pub row_count: u64,
    pub null_count: u64,
    pub metadata_null_count: Option<u64>,
    pub distinct_count: usize,
    /// The column has more distinct values than were counted: `distinct_count`
    /// is a lower bound (whole-file profiles only).
    pub distinct_capped: bool,
    pub min: Option<String>,
    pub max: Option<String>,
    pub mean: Option<f64>,
    pub string_lengths: Option<StringLengthStats>,
    pub frequent_values: Vec<FrequentValue>,
    pub non_finite_count: u64,
}

impl ColumnStats {
    pub fn non_null_count(&self) -> u64 {
        self.row_count.saturating_sub(self.null_count)
    }

    /// A list, struct or map column, which can't be profiled: its type and
    /// what the footer says.
    fn footer_only(info: &ColumnInfo, row_count: u64) -> Self {
        let null_count = metadata_null_count(info);
        Self {
            column: info.index,
            name: info.name.clone(),
            data_type: info.data_type.clone(),
            row_count,
            null_count: null_count.unwrap_or(0),
            metadata_null_count: null_count,
            distinct_count: 0,
            distinct_capped: false,
            min: None,
            max: None,
            mean: None,
            string_lengths: None,
            frequent_values: Vec::new(),
            non_finite_count: 0,
        }
    }
}

/// The null count the footer records for every row group, if it does.
pub fn metadata_null_count(column: &ColumnInfo) -> Option<u64> {
    column
        .row_group_statistics
        .iter()
        .try_fold(0_u64, |sum, stats| {
            sum.checked_add(stats.as_ref()?.null_count?)
        })
}

/// Profiles one column from the batches pushed into it.
pub struct StatsAccumulator {
    column: usize,
    name: String,
    data_type: DataType,
    metadata_null_count: Option<u64>,
    row_count: u64,
    null_count: u64,
    distinct: BTreeMap<String, u64>,
    distinct_bytes: usize,
    min: Option<String>,
    max: Option<String>,
    numeric_min: Option<f64>,
    numeric_max: Option<f64>,
    /// Dates and timestamps are ordered by their stored value, not their text.
    temporal_min: Option<i64>,
    temporal_max: Option<i64>,
    numeric_sum: f64,
    numeric_count: u64,
    non_finite_count: u64,
    string_length_min: Option<usize>,
    string_length_max: usize,
    string_length_sum: u64,
    distinct_limit: usize,
    distinct_bytes_limit: usize,
    /// Past the distinct limits, stop counting instead of failing.
    lenient: bool,
    distinct_capped: bool,
}

impl StatsAccumulator {
    /// A one-column profile: exact distinct counts up to [`MAX_DISTINCT_VALUES`],
    /// failing past them.
    pub fn new(
        column: usize,
        name: String,
        data_type: DataType,
        metadata_null_count: Option<u64>,
    ) -> Result<Self> {
        if !is_comparable(&data_type) {
            bail!("profiling does not support {data_type:?}; use Inspector metadata instead");
        }
        Ok(Self {
            column,
            name,
            data_type,
            metadata_null_count,
            row_count: 0,
            null_count: 0,
            distinct: BTreeMap::new(),
            distinct_bytes: 0,
            min: None,
            max: None,
            numeric_min: None,
            numeric_max: None,
            temporal_min: None,
            temporal_max: None,
            numeric_sum: 0.0,
            numeric_count: 0,
            non_finite_count: 0,
            string_length_min: None,
            string_length_max: 0,
            string_length_sum: 0,
            distinct_limit: MAX_DISTINCT_VALUES,
            distinct_bytes_limit: MAX_DISTINCT_BYTES,
            lenient: false,
            distinct_capped: false,
        })
    }

    /// A whole-file profile column: a smaller distinct count that caps
    /// instead of failing.
    pub fn for_file(
        column: usize,
        name: String,
        data_type: DataType,
        metadata_null_count: Option<u64>,
    ) -> Result<Self> {
        let mut accumulator = Self::new(column, name, data_type, metadata_null_count)?;
        accumulator.distinct_limit = FILE_PROFILE_DISTINCT_VALUES;
        accumulator.distinct_bytes_limit = FILE_PROFILE_DISTINCT_BYTES;
        accumulator.lenient = true;
        Ok(accumulator)
    }

    /// The source column this profiles.
    pub fn column(&self) -> usize {
        self.column
    }

    /// Adds the rows of `batch`, whose first column is this one.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let array = batch.column(0);
        if array.data_type() != &self.data_type {
            bail!("profile batch type changed");
        }
        for row in 0..batch.num_rows() {
            self.row_count = self.row_count.saturating_add(1);
            if array.is_null(row) {
                self.null_count = self.null_count.saturating_add(1);
                continue;
            }
            let value = profile_text(array.as_ref(), row)?;
            self.record_distinct(&value)?;
            if let Some(number) = numeric(array.as_ref(), row)? {
                self.record_numeric(number, &value)?;
            } else if let Some(key) = temporal_key(array.as_ref(), row)? {
                self.record_temporal(key, &value);
            } else {
                self.record_text(&value, is_string(&self.data_type));
            }
        }
        Ok(())
    }

    fn record_distinct(&mut self, value: &str) -> Result<()> {
        if self.distinct_capped {
            return Ok(());
        }
        let over = if value.len() > MAX_PROFILE_VALUE_BYTES {
            Some(format!(
                "distinct values longer than {MAX_PROFILE_VALUE_BYTES} bytes are not retained exactly"
            ))
        } else if !self.distinct.contains_key(value) {
            let next_bytes = self.distinct_bytes.saturating_add(value.len());
            if self.distinct.len() == self.distinct_limit {
                Some(format!(
                    "exact distinct count exceeds the {}-value profile limit",
                    self.distinct_limit
                ))
            } else if next_bytes > self.distinct_bytes_limit {
                Some(format!(
                    "exact distinct values exceed the {}-byte profile limit",
                    self.distinct_bytes_limit
                ))
            } else {
                self.distinct_bytes = next_bytes;
                None
            }
        } else {
            None
        };
        if let Some(reason) = over {
            if !self.lenient {
                bail!(reason);
            }
            // Keep the count reached; stop tracking values to bound memory.
            self.distinct_capped = true;
            return Ok(());
        }
        let count = self.distinct.entry(value.to_owned()).or_default();
        *count = count.saturating_add(1);
        Ok(())
    }

    fn record_numeric(&mut self, number: f64, value: &str) -> Result<()> {
        if !number.is_finite() {
            self.non_finite_count = self.non_finite_count.saturating_add(1);
            return Ok(());
        }
        if self.numeric_min.is_none_or(|minimum| number < minimum) {
            self.numeric_min = Some(number);
            self.min = Some(value.to_owned());
        }
        if self.numeric_max.is_none_or(|maximum| number > maximum) {
            self.numeric_max = Some(number);
            self.max = Some(value.to_owned());
        }
        self.numeric_sum += number;
        if !self.numeric_sum.is_finite() {
            bail!("numeric mean overflowed while profiling");
        }
        self.numeric_count = self.numeric_count.saturating_add(1);
        Ok(())
    }

    fn record_temporal(&mut self, key: i64, value: &str) {
        if self.temporal_min.is_none_or(|minimum| key < minimum) {
            self.temporal_min = Some(key);
            self.min = Some(value.to_owned());
        }
        if self.temporal_max.is_none_or(|maximum| key > maximum) {
            self.temporal_max = Some(key);
            self.max = Some(value.to_owned());
        }
    }

    /// Text and booleans: min/max by text; string lengths only for text columns.
    fn record_text(&mut self, value: &str, measure_length: bool) {
        if self.min.as_deref().is_none_or(|minimum| value < minimum) {
            self.min = Some(value.to_owned());
        }
        if self.max.as_deref().is_none_or(|maximum| value > maximum) {
            self.max = Some(value.to_owned());
        }
        if !measure_length {
            return;
        }
        let length = value.chars().count();
        self.string_length_min = Some(self.string_length_min.map_or(length, |min| min.min(length)));
        self.string_length_max = self.string_length_max.max(length);
        self.string_length_sum = self.string_length_sum.saturating_add(length as u64);
    }

    pub fn finish(self) -> ColumnStats {
        let distinct_count = self.distinct.len();
        let distinct_capped = self.distinct_capped;
        let mut frequent_values = self
            .distinct
            .into_iter()
            .map(|(value, count)| FrequentValue { value, count })
            .collect::<Vec<_>>();
        frequent_values.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.value.cmp(&right.value))
        });
        frequent_values.truncate(FREQUENT_VALUE_LIMIT);
        let string_lengths = self.string_length_min.map(|min| StringLengthStats {
            min,
            max: self.string_length_max,
            mean: self.string_length_sum as f64
                / self.row_count.saturating_sub(self.null_count).max(1) as f64,
        });
        ColumnStats {
            column: self.column,
            name: self.name,
            data_type: self.data_type,
            row_count: self.row_count,
            null_count: self.null_count,
            metadata_null_count: self.metadata_null_count,
            distinct_count,
            distinct_capped,
            min: self.min,
            max: self.max,
            mean: (self.numeric_count > 0).then(|| self.numeric_sum / self.numeric_count as f64),
            string_lengths,
            frequent_values,
            non_finite_count: self.non_finite_count,
        }
    }
}

/// Profiles several columns of one source in a single pass: push batches of
/// [`Self::read_columns`], in that order, then [`Self::finish`].
pub struct FileProfiler {
    /// One entry per profiled column: an accumulator, or footer facts for a
    /// column whose type can't be profiled.
    columns: Vec<Result<StatsAccumulator, ColumnStats>>,
    read_columns: Vec<usize>,
    rows: u64,
}

impl FileProfiler {
    /// Profiles `columns` of `source` (at most [`MAX_FILE_PROFILE_COLUMNS`]).
    /// Lists, structs and maps get their type and footer null count only.
    pub fn new(source: &ParquetSource, columns: &[usize]) -> Result<Self> {
        if columns.len() > MAX_FILE_PROFILE_COLUMNS {
            bail!("a profile covers at most {MAX_FILE_PROFILE_COLUMNS} columns");
        }
        let metadata = source.metadata();
        let mut profiled = Vec::with_capacity(columns.len());
        let mut read_columns = Vec::new();
        for &column in columns {
            let info = metadata
                .columns()
                .get(column)
                .context("profile column is out of range")?;
            if is_comparable(&info.data_type) {
                profiled.push(Ok(StatsAccumulator::for_file(
                    column,
                    info.name.clone(),
                    info.data_type.clone(),
                    metadata_null_count(info),
                )?));
                read_columns.push(column);
            } else {
                profiled.push(Err(ColumnStats::footer_only(info, source.row_count())));
            }
        }
        Ok(Self {
            columns: profiled,
            read_columns,
            rows: 0,
        })
    }

    /// The source columns to read, in the order batches must hold them.
    /// Empty when no column can be profiled; then nothing needs reading.
    pub fn read_columns(&self) -> &[usize] {
        &self.read_columns
    }

    /// Adds a batch holding [`Self::read_columns`].
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for (position, accumulator) in self
            .columns
            .iter_mut()
            .filter_map(|column| column.as_mut().ok())
            .enumerate()
        {
            accumulator.push_batch(&batch.project(&[position])?)?;
        }
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        Ok(())
    }

    /// Rows pushed so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// One profile per column, in the order they were asked for.
    pub fn finish(self) -> Vec<ColumnStats> {
        self.columns
            .into_iter()
            .map(|column| match column {
                Ok(accumulator) => accumulator.finish(),
                Err(stats) => stats,
            })
            .collect()
    }
}

/// Profile text for one non-null value, from the shared scalar formatter.
/// Profiling accepts the comparable types (`types::is_comparable`).
fn profile_text(array: &dyn Array, row: usize) -> Result<String> {
    if !is_comparable(array.data_type()) {
        bail!("profiling does not support {:?}", array.data_type());
    }
    crate::value::scalar_text(array, row)?.context("profile value is unexpectedly null")
}

/// The stored value of a date, timestamp or time of day, for ordering; `None` for
/// other types.
fn temporal_key(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    macro_rules! key {
        ($array:ty) => {
            Some(
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .context("read temporal profile value")?
                    .value(row)
                    .into(),
            )
        };
    }
    Ok(match array.data_type() {
        DataType::Date32 => key!(Date32Array),
        DataType::Date64 => key!(Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => key!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => key!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => key!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => key!(TimestampNanosecondArray),
        DataType::Time32(TimeUnit::Second) => key!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => key!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => key!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => key!(Time64NanosecondArray),
        _ => None,
    })
}

fn numeric(array: &dyn Array, row: usize) -> Result<Option<f64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .context("read numeric profile value")?
                .value(row) as f64
        };
    }
    Ok(Some(match array.data_type() {
        DataType::Int8 => value!(Int8Array),
        DataType::Int16 => value!(Int16Array),
        DataType::Int32 => value!(Int32Array),
        DataType::Int64 => value!(Int64Array),
        DataType::UInt8 => value!(UInt8Array),
        DataType::UInt16 => value!(UInt16Array),
        DataType::UInt32 => value!(UInt32Array),
        DataType::UInt64 => value!(UInt64Array),
        DataType::Float32 => value!(Float32Array),
        DataType::Float64 => value!(Float64Array),
        data_type if is_decimal(data_type) => {
            crate::value::decimal_f64(array, row).context("read decimal profile value")?
        }
        data_type if is_comparable(data_type) => return Ok(None),
        data_type => bail!("profiling does not support {data_type:?}"),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, BooleanArray, Decimal128Array, Int32Array, StringArray, StringViewArray,
    };
    use arrow_schema::{Field, Schema};

    use super::*;
    use crate::ColumnStatistics;

    fn profile_array(array: arrow_array::ArrayRef) -> ColumnStats {
        let data_type = array.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            true,
        )]));
        let mut accumulator =
            StatsAccumulator::new(0, "value".to_string(), data_type, None).unwrap();
        accumulator
            .push_batch(&RecordBatch::try_new(schema, vec![array]).unwrap())
            .unwrap();
        accumulator.finish()
    }

    #[test]
    fn profiles_numbers_text_and_decimals() {
        let numbers = profile_array(Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(1),
        ])));
        assert_eq!(numbers.row_count, 4);
        assert_eq!(numbers.null_count, 1);
        assert_eq!(numbers.non_null_count(), 3);
        assert_eq!(numbers.distinct_count, 2);
        assert_eq!(numbers.min.as_deref(), Some("1"));
        assert_eq!(numbers.max.as_deref(), Some("3"));
        assert!((numbers.mean.unwrap() - 5.0 / 3.0).abs() < f64::EPSILON);
        assert_eq!(
            numbers.frequent_values[0],
            FrequentValue {
                value: "1".to_string(),
                count: 2
            }
        );

        let text = profile_array(Arc::new(StringArray::from(vec![
            Some("b"),
            None,
            Some("aa"),
        ])));
        assert_eq!(text.min.as_deref(), Some("aa"));
        assert_eq!(
            text.string_lengths,
            Some(StringLengthStats {
                min: 1,
                max: 2,
                mean: 1.5,
            })
        );

        // Ordered as numbers, not as text ("-2.00" < "10.50" < "9.50" by text).
        let decimals = profile_array(Arc::new(
            Decimal128Array::from(vec![Some(1_050), Some(-200), Some(950)])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ));
        assert_eq!(decimals.min.as_deref(), Some("-2.00"));
        assert_eq!(decimals.max.as_deref(), Some("10.50"));

        let flags = profile_array(Arc::new(BooleanArray::from(vec![true, false, true])));
        assert_eq!(flags.distinct_count, 2);
        assert!(flags.string_lengths.is_none());
        let views = profile_array(Arc::new(StringViewArray::from(vec!["pear", "fig"])));
        assert_eq!(views.min.as_deref(), Some("fig"));
    }

    #[test]
    fn metadata_null_counts_are_used_only_when_complete() {
        let column = ColumnInfo {
            index: 0,
            name: "number".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            row_group_statistics: vec![
                Some(ColumnStatistics {
                    min: None,
                    max: None,
                    null_count: Some(2),
                    distinct_count: None,
                }),
                Some(ColumnStatistics {
                    min: None,
                    max: None,
                    null_count: Some(3),
                    distinct_count: None,
                }),
            ],
        };
        assert_eq!(metadata_null_count(&column), Some(5));
        let missing = ColumnInfo {
            row_group_statistics: vec![None],
            ..column
        };
        assert_eq!(metadata_null_count(&missing), None);
    }

    #[test]
    fn rejects_unsupported_values_and_unbounded_exact_distinct_state() {
        assert!(StatsAccumulator::new(0, "binary".to_string(), DataType::Binary, None).is_err());
        let mut accumulator =
            StatsAccumulator::new(0, "text".to_string(), DataType::Utf8, None).unwrap();
        for index in 0..MAX_DISTINCT_VALUES {
            accumulator.record_distinct(&index.to_string()).unwrap();
        }
        assert!(accumulator.record_distinct("one-too-many").is_err());
        let binary = BinaryArray::from(vec![Some(b"x".as_slice())]);
        assert!(profile_text(&binary, 0).is_err());
    }

    #[test]
    fn whole_file_profiles_cap_distinct_counts_instead_of_failing() {
        let values = (FILE_PROFILE_DISTINCT_VALUES + 500) as i32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(0..values))],
        )
        .unwrap();
        let mut file =
            StatsAccumulator::for_file(0, "number".to_string(), DataType::Int32, None).unwrap();
        file.push_batch(&batch).unwrap();
        let profile = file.finish();
        assert!(profile.distinct_capped);
        assert_eq!(profile.distinct_count, FILE_PROFILE_DISTINCT_VALUES);
        // Min, max and mean still cover every row.
        assert_eq!(
            profile.max.as_deref(),
            Some(values - 1).map(|max| max.to_string()).as_deref()
        );

        let mut column =
            StatsAccumulator::new(0, "number".to_string(), DataType::Int32, None).unwrap();
        column.push_batch(&batch).unwrap();
        assert!(!column.finish().distinct_capped);
    }
}
