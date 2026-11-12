use std::{
    collections::BTreeMap,
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow_array::{
    Array, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use parquet_reader::types::{is_comparable, is_decimal, is_string};
use parquet_reader::{ColumnInfo, ParquetSource};
use pavi_query::{LogicalPlan, QueryEngine, QueryPoll};
use pavi_runtime::{CancellationToken, GenerationId, RuntimeHandle};

use crate::chart::{ChartAccumulator, ChartConfig, ChartKind, ChartModel, MAX_INPUT_ROWS};

pub const MAX_DISTINCT_VALUES: usize = 4_096;
pub const MAX_DISTINCT_BYTES: usize = 1_024 * 1_024;
pub const MAX_PROFILE_VALUE_BYTES: usize = 128;
const FREQUENT_VALUE_LIMIT: usize = 10;

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

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnProfile {
    pub column: usize,
    pub name: String,
    pub data_type: DataType,
    pub row_count: u64,
    pub null_count: u64,
    pub metadata_null_count: Option<u64>,
    pub distinct_count: usize,
    pub min: Option<String>,
    pub max: Option<String>,
    pub mean: Option<f64>,
    pub string_lengths: Option<StringLengthStats>,
    pub frequent_values: Vec<FrequentValue>,
    pub distribution: Option<ChartModel>,
    pub distribution_sampled: bool,
    pub non_finite_count: u64,
}

impl ColumnProfile {
    pub fn non_null_count(&self) -> u64 {
        self.row_count.saturating_sub(self.null_count)
    }
}

pub enum ProfileEvent {
    Finished {
        generation: GenerationId,
        profile: Box<ColumnProfile>,
    },
    Cancelled {
        generation: GenerationId,
    },
    Failed {
        generation: GenerationId,
        error: String,
    },
}

pub struct ProfileTask {
    receiver: Receiver<ProfileEvent>,
    cancellation: CancellationToken,
}

impl ProfileTask {
    pub fn start(
        source: std::sync::Arc<ParquetSource>,
        column: usize,
        runtime: RuntimeHandle,
        generation: GenerationId,
    ) -> std::io::Result<Self> {
        let cancellation = CancellationToken::default();
        let worker_cancellation = cancellation.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("pavi-profile".to_string())
            .spawn(move || {
                let result =
                    profile_column(source, column, runtime, generation, &worker_cancellation);
                let event = match result {
                    Ok(_) if worker_cancellation.is_cancelled() => {
                        ProfileEvent::Cancelled { generation }
                    }
                    Ok(profile) => ProfileEvent::Finished {
                        generation,
                        profile: Box::new(profile),
                    },
                    Err(_) if worker_cancellation.is_cancelled() => {
                        ProfileEvent::Cancelled { generation }
                    }
                    Err(error) => ProfileEvent::Failed {
                        generation,
                        error: format!("{error:#}"),
                    },
                };
                let _ = sender.send(event);
            })?;
        Ok(Self {
            receiver,
            cancellation,
        })
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn try_recv(&self) -> Result<Option<ProfileEvent>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => bail!("profile worker stopped without a result"),
        }
    }
}

pub fn metadata_null_count(column: &ColumnInfo) -> Option<u64> {
    column
        .row_group_statistics
        .iter()
        .try_fold(0_u64, |sum, stats| {
            sum.checked_add(stats.as_ref()?.null_count?)
        })
}

fn profile_column(
    source: std::sync::Arc<ParquetSource>,
    column: usize,
    runtime: RuntimeHandle,
    generation: GenerationId,
    cancellation: &CancellationToken,
) -> Result<ColumnProfile> {
    let metadata_column = source
        .metadata()
        .columns()
        .get(column)
        .context("profile column is out of range")?;
    let name = metadata_column.name.clone();
    let data_type = metadata_column.data_type.clone();
    let metadata_null_count = metadata_null_count(metadata_column);
    let plan = LogicalPlan::scan(source).project(vec![column]);
    let engine = QueryEngine::from_handle(runtime);
    let mut execution = engine.execute(&plan, generation)?;
    let mut accumulator = ProfileAccumulator::new(column, name, data_type, metadata_null_count)?;

    loop {
        if cancellation.is_cancelled() {
            execution.cancel();
            bail!("profile cancelled");
        }
        match execution.poll_next_batch()? {
            QueryPoll::Batch(batch) => accumulator.push_batch(&batch.batch)?,
            QueryPoll::Finished => return Ok(accumulator.finish()),
            QueryPoll::Pending => thread::sleep(Duration::from_millis(5)),
        }
    }
}

struct ProfileAccumulator {
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
    chart: Option<ChartAccumulator>,
    distribution_sampled: bool,
}

impl ProfileAccumulator {
    fn new(
        column: usize,
        name: String,
        data_type: DataType,
        metadata_null_count: Option<u64>,
    ) -> Result<Self> {
        if !is_comparable(&data_type) {
            bail!("profiling does not support {data_type:?}; use Inspector metadata instead");
        }
        let chart = numeric_type(&data_type)
            .map(|_| {
                ChartAccumulator::new(ChartConfig {
                    kind: ChartKind::Histogram,
                    x_column: name.clone(),
                    y_column: String::new(),
                    point_limit: MAX_INPUT_ROWS,
                    bins: 20,
                    title: String::new(),
                })
            })
            .transpose()?;
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
            chart,
            distribution_sampled: false,
        })
    }

    fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let array = batch.column(0);
        if array.data_type() != &self.data_type {
            bail!("profile batch type changed");
        }
        if let Some(chart) = &mut self.chart
            && !self.distribution_sampled
            && !chart.push_batch(batch)?
        {
            self.distribution_sampled = true;
        }
        for row in 0..batch.num_rows() {
            self.row_count = self.row_count.saturating_add(1);
            if array.is_null(row) {
                self.null_count = self.null_count.saturating_add(1);
                continue;
            }
            let value = scalar_text(array.as_ref(), row)?;
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
        if value.len() > MAX_PROFILE_VALUE_BYTES {
            bail!(
                "distinct values longer than {MAX_PROFILE_VALUE_BYTES} bytes are not retained exactly"
            );
        }
        if !self.distinct.contains_key(value) {
            if self.distinct.len() == MAX_DISTINCT_VALUES {
                bail!("exact distinct count exceeds the {MAX_DISTINCT_VALUES}-value profile limit");
            }
            let next_bytes = self.distinct_bytes.saturating_add(value.len());
            if next_bytes > MAX_DISTINCT_BYTES {
                bail!("exact distinct values exceed the {MAX_DISTINCT_BYTES}-byte profile limit");
            }
            self.distinct_bytes = next_bytes;
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

    fn finish(self) -> ColumnProfile {
        let distinct_count = self.distinct.len();
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
        ColumnProfile {
            column: self.column,
            name: self.name,
            data_type: self.data_type,
            row_count: self.row_count,
            null_count: self.null_count,
            metadata_null_count: self.metadata_null_count,
            distinct_count,
            min: self.min,
            max: self.max,
            mean: (self.numeric_count > 0).then(|| self.numeric_sum / self.numeric_count as f64),
            string_lengths,
            frequent_values,
            distribution: self.chart.map(ChartAccumulator::finish),
            distribution_sampled: self.distribution_sampled,
            non_finite_count: self.non_finite_count,
        }
    }
}

fn numeric_type(data_type: &DataType) -> Option<()> {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
    .then_some(())
    .or_else(|| is_decimal(data_type).then_some(()))
}

/// Profile text for one non-null value, from the shared scalar formatter.
/// Profiling accepts the comparable types (`types::is_comparable`).
fn scalar_text(array: &dyn Array, row: usize) -> Result<String> {
    if !is_comparable(array.data_type()) {
        bail!("profiling does not support {:?}", array.data_type());
    }
    parquet_reader::value::scalar_text(array, row)?.context("profile value is unexpectedly null")
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
            parquet_reader::value::decimal_f64(array, row).context("read decimal profile value")?
        }
        data_type if is_comparable(data_type) => return Ok(None),
        data_type => bail!("profiling does not support {data_type:?}"),
    }))
}

/// Inspector profile state: the running task and its result.
pub(crate) struct ProfileState {
    pub(crate) generation: GenerationId,
    pub(crate) task: Option<ProfileTask>,
    pub(crate) column: Option<usize>,
    pub(crate) result: Option<ColumnProfile>,
    pub(crate) error: Option<String>,
    pub(crate) status: String,
}

impl Default for ProfileState {
    fn default() -> Self {
        Self {
            generation: GenerationId(0),
            task: None,
            column: None,
            result: None,
            error: None,
            status: "Select a source column to profile".to_string(),
        }
    }
}

impl ProfileState {
    pub(crate) fn loading(&self) -> bool {
        self.task.is_some()
    }

    pub(crate) fn cancel(&mut self) {
        if let Some(task) = &self.task {
            task.cancel();
        }
        self.task = None;
    }

    pub(crate) fn reset(&mut self) {
        self.cancel();
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.column = None;
        self.result = None;
        self.error = None;
        self.status = "Dataset changed; profile cleared".to_string();
    }

    pub(crate) fn next_generation(&mut self) -> GenerationId {
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.generation
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use arrow_array::{BinaryArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use pavi_runtime::{Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;
    use parquet_reader::ColumnStatistics;

    fn source() -> (TempDir, Arc<ParquetSource>) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("profile.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(2))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(1)])),
                        Arc::new(StringArray::from(vec![
                            Some("b"),
                            None,
                            Some("aa"),
                            Some("b"),
                        ])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        (directory, Arc::new(ParquetSource::open(path).unwrap()))
    }

    fn profile(source: Arc<ParquetSource>, column: usize) -> ColumnProfile {
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        profile_column(
            source,
            column,
            runtime.handle(),
            GenerationId(7),
            &CancellationToken::default(),
        )
        .unwrap()
    }

    #[test]
    fn profiles_numeric_values_nulls_exact_distinct_and_distribution() {
        let (_directory, source) = source();
        let profile = profile(source, 0);
        assert_eq!(profile.row_count, 4);
        assert_eq!(profile.null_count, 1);
        assert_eq!(profile.non_null_count(), 3);
        assert_eq!(profile.distinct_count, 2);
        assert_eq!(profile.min.as_deref(), Some("1"));
        assert_eq!(profile.max.as_deref(), Some("3"));
        assert!((profile.mean.unwrap() - 5.0 / 3.0).abs() < f64::EPSILON);
        assert_eq!(
            profile.frequent_values[0],
            FrequentValue {
                value: "1".to_string(),
                count: 2
            }
        );
        assert!(profile.distribution.is_some());
        assert!(!profile.distribution_sampled);
    }

    #[test]
    fn profiles_strings_and_reports_length_statistics() {
        let (_directory, source) = source();
        let profile = profile(source, 1);
        assert_eq!(profile.null_count, 1);
        assert_eq!(profile.distinct_count, 2);
        assert_eq!(profile.min.as_deref(), Some("aa"));
        assert_eq!(profile.max.as_deref(), Some("b"));
        assert_eq!(
            profile.string_lengths,
            Some(StringLengthStats {
                min: 1,
                max: 2,
                mean: 4.0 / 3.0,
            })
        );
        assert!(profile.distribution.is_none());
    }

    #[test]
    fn profiles_decimals_as_numbers_and_times_in_time_order() {
        use arrow_array::{Decimal128Array, Time32SecondArray};
        let decimals = Decimal128Array::from(vec![Some(1_050), Some(-200), Some(950), None])
            .with_precision_and_scale(9, 2)
            .unwrap();
        let mut accumulator =
            ProfileAccumulator::new(0, "price".to_string(), decimals.data_type().clone(), None)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            decimals.data_type().clone(),
            true,
        )]));
        accumulator
            .push_batch(&RecordBatch::try_new(schema, vec![Arc::new(decimals)]).unwrap())
            .unwrap();
        let profile = accumulator.finish();
        // Ordered as numbers, not as text ("-2.00" < "10.50" < "9.50" by text).
        assert_eq!(profile.min.as_deref(), Some("-2.00"));
        assert_eq!(profile.max.as_deref(), Some("10.50"));
        assert_eq!(profile.mean, Some(6.0));
        assert!(profile.distribution.is_some());

        // 10:00:00 sorts after 9:00:00 although "10" < "9" as text.
        let times = Time32SecondArray::from(vec![36_000, 32_400]);
        let mut accumulator =
            ProfileAccumulator::new(0, "at".to_string(), times.data_type().clone(), None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "at",
            times.data_type().clone(),
            false,
        )]));
        accumulator
            .push_batch(&RecordBatch::try_new(schema, vec![Arc::new(times)]).unwrap())
            .unwrap();
        let profile = accumulator.finish();
        assert_eq!(profile.min.as_deref(), Some("09:00:00"));
        assert_eq!(profile.max.as_deref(), Some("10:00:00"));
        assert!(profile.mean.is_none());
    }

    #[test]
    fn empty_and_all_null_columns_have_clear_metrics() {
        let accumulator =
            ProfileAccumulator::new(0, "number".to_string(), DataType::Int32, None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![None, None]))])
                .unwrap();
        let mut accumulator = accumulator;
        accumulator.push_batch(&batch).unwrap();
        let profile = accumulator.finish();
        assert_eq!(profile.row_count, 2);
        assert_eq!(profile.null_count, 2);
        assert_eq!(profile.distinct_count, 0);
        assert!(profile.min.is_none());
        assert!(profile.mean.is_none());
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
        assert!(ProfileAccumulator::new(0, "binary".to_string(), DataType::Binary, None).is_err());
        let mut accumulator =
            ProfileAccumulator::new(0, "text".to_string(), DataType::Utf8, None).unwrap();
        for index in 0..MAX_DISTINCT_VALUES {
            accumulator.record_distinct(&index.to_string()).unwrap();
        }
        assert!(accumulator.record_distinct("one-too-many").is_err());
        let binary = BinaryArray::from(vec![Some(b"x".as_slice())]);
        assert!(scalar_text(&binary, 0).is_err());
    }

    fn profile_array(array: arrow_array::ArrayRef) -> ColumnProfile {
        let data_type = array.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            true,
        )]));
        let mut accumulator =
            ProfileAccumulator::new(0, "value".to_string(), data_type, None).unwrap();
        accumulator
            .push_batch(&RecordBatch::try_new(schema, vec![array]).unwrap())
            .unwrap();
        accumulator.finish()
    }

    #[test]
    fn profiles_booleans_text_views_and_temporal_values_in_time_order() {
        use arrow_array::{BooleanArray, StringViewArray, TimestampSecondArray};

        // Summer time ends in Berlin on 2023-10-29: 02:30+02:00 happens before
        // 02:15+01:00, so ordering by text would pick the wrong minimum.
        let earlier = 1_698_539_400; // 2023-10-29T00:30:00Z
        let later = 1_698_542_100; // 2023-10-29T01:15:00Z
        let timestamps = profile_array(Arc::new(
            TimestampSecondArray::from(vec![later, earlier]).with_timezone("Europe/Berlin"),
        ));
        assert_eq!(timestamps.min.as_deref(), Some("2023-10-29T02:30:00+02:00"));
        assert_eq!(timestamps.max.as_deref(), Some("2023-10-29T02:15:00+01:00"));
        assert!(timestamps.mean.is_none());
        assert!(timestamps.string_lengths.is_none());

        let flags = profile_array(Arc::new(BooleanArray::from(vec![true, false, true])));
        assert_eq!(flags.min.as_deref(), Some("false"));
        assert_eq!(flags.max.as_deref(), Some("true"));
        assert_eq!(flags.distinct_count, 2);
        assert!(flags.string_lengths.is_none());

        let views = profile_array(Arc::new(StringViewArray::from(vec!["pear", "fig"])));
        assert_eq!(views.min.as_deref(), Some("fig"));
        assert_eq!(views.max.as_deref(), Some("pear"));
        assert_eq!(views.string_lengths.map(|lengths| lengths.max), Some(4));

        assert!(ProfileAccumulator::new(0, "b".to_string(), DataType::Binary, None).is_err());
    }

    #[test]
    fn labels_bounded_numeric_distribution_as_sampled() {
        let mut accumulator =
            ProfileAccumulator::new(0, "number".to_string(), DataType::Int32, None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "number",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
                1,
                MAX_INPUT_ROWS + 1,
            )))],
        )
        .unwrap();
        accumulator.push_batch(&batch).unwrap();
        let profile = accumulator.finish();
        assert!(profile.distribution_sampled);
        assert_eq!(profile.distinct_count, 1);
    }

    #[test]
    fn cancellation_returns_a_typed_cancelled_event() {
        let (_directory, source) = source();
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let task = ProfileTask::start(source, 0, runtime.handle(), GenerationId(9)).unwrap();
        task.cancel();
        for _ in 0..1_000 {
            if let Some(event) = task.try_recv().unwrap() {
                assert!(matches!(
                    event,
                    ProfileEvent::Cancelled {
                        generation: GenerationId(9)
                    }
                ));
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("profile did not cancel");
    }
}
