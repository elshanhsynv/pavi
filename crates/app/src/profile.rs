use std::{
    ops::Deref,
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use parquet_reader::ParquetSource;
pub use parquet_reader::profile::MAX_FILE_PROFILE_COLUMNS;
use parquet_reader::profile::{ColumnStats, FileProfiler, StatsAccumulator, metadata_null_count};
#[cfg(test)]
pub use parquet_reader::profile::{FrequentValue, StringLengthStats};
use parquet_reader::types::{is_decimal, is_numeric};
use pavi_query::{LogicalPlan, QueryEngine, QueryPoll};
use pavi_runtime::{CancellationToken, GenerationId, RuntimeHandle};

use crate::chart::{ChartAccumulator, ChartConfig, ChartKind, ChartModel, MAX_INPUT_ROWS};

/// A column's profile (`parquet_reader::profile`, shared with the CLI) and, for
/// a one-column profile of a number, its distribution chart. It reads as its
/// statistics: `profile.min`, `profile.null_count`, …
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnProfile {
    pub stats: ColumnStats,
    pub distribution: Option<ChartModel>,
    pub distribution_sampled: bool,
}

impl Deref for ColumnProfile {
    type Target = ColumnStats;

    fn deref(&self) -> &ColumnStats {
        &self.stats
    }
}

impl From<ColumnStats> for ColumnProfile {
    fn from(stats: ColumnStats) -> Self {
        Self {
            stats,
            distribution: None,
            distribution_sampled: false,
        }
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

/// A one-column profile: the shared statistics plus, for numbers and
/// decimals, a bounded histogram of the values.
struct ProfileAccumulator {
    stats: StatsAccumulator,
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
        let chart = (is_numeric(&data_type) || is_decimal(&data_type))
            .then(|| {
                ChartAccumulator::new(ChartConfig {
                    kind: ChartKind::Histogram,
                    x_column: name.clone(),
                    y_columns: Vec::new(),
                    point_limit: MAX_INPUT_ROWS,
                    bins: 20,
                    title: String::new(),
                })
            })
            .transpose()?;
        Ok(Self {
            stats: StatsAccumulator::new(column, name, data_type, metadata_null_count)?,
            chart,
            distribution_sampled: false,
        })
    }

    fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.stats.push_batch(batch)?;
        if let Some(chart) = &mut self.chart
            && !self.distribution_sampled
            && !chart.push_batch(batch)?
        {
            self.distribution_sampled = true;
        }
        Ok(())
    }

    fn finish(self) -> ColumnProfile {
        ColumnProfile {
            stats: self.stats.finish(),
            distribution: self.chart.map(ChartAccumulator::finish),
            distribution_sampled: self.distribution_sampled,
        }
    }
}

/// What a whole-file profile reports.
pub enum FileProfileEvent {
    Finished {
        generation: GenerationId,
        profiles: Vec<ColumnProfile>,
    },
    Cancelled {
        generation: GenerationId,
    },
    Failed {
        generation: GenerationId,
        error: String,
    },
}

/// Every column of a file profiled in one scan, on its own thread.
pub struct FileProfileTask {
    receiver: Receiver<FileProfileEvent>,
    cancellation: CancellationToken,
    scanned: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl FileProfileTask {
    /// Profiles `columns` (at most [`MAX_FILE_PROFILE_COLUMNS`]). Columns whose
    /// type can't be profiled are listed with their type and metadata only.
    pub fn start(
        source: std::sync::Arc<ParquetSource>,
        columns: Vec<usize>,
        runtime: RuntimeHandle,
        generation: GenerationId,
    ) -> std::io::Result<Self> {
        let cancellation = CancellationToken::default();
        let worker_cancellation = cancellation.clone();
        let scanned = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let worker_scanned = std::sync::Arc::clone(&scanned);
        let (sender, receiver) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("pavi-file-profile".to_string())
            .spawn(move || {
                let result = profile_file(
                    source,
                    columns,
                    runtime,
                    generation,
                    &worker_cancellation,
                    &worker_scanned,
                );
                let event = match result {
                    Ok(_) | Err(_) if worker_cancellation.is_cancelled() => {
                        FileProfileEvent::Cancelled { generation }
                    }
                    Ok(profiles) => FileProfileEvent::Finished {
                        generation,
                        profiles,
                    },
                    Err(error) => FileProfileEvent::Failed {
                        generation,
                        error: format!("{error:#}"),
                    },
                };
                let _ = sender.send(event);
            })?;
        Ok(Self {
            receiver,
            cancellation,
            scanned,
        })
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Rows read so far.
    pub fn scanned_rows(&self) -> u64 {
        self.scanned.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn try_recv(&self) -> Result<Option<FileProfileEvent>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => bail!("profile worker stopped without a result"),
        }
    }
}

/// The whole-file profile on the worker pool; the statistics are
/// `parquet_reader::profile::FileProfiler`'s, as in the CLI's `--profile`.
fn profile_file(
    source: std::sync::Arc<ParquetSource>,
    columns: Vec<usize>,
    runtime: RuntimeHandle,
    generation: GenerationId,
    cancellation: &CancellationToken,
    scanned: &std::sync::atomic::AtomicU64,
) -> Result<Vec<ColumnProfile>> {
    let mut profiler = FileProfiler::new(&source, &columns)?;
    if !profiler.read_columns().is_empty() {
        let plan = LogicalPlan::scan(source).project(profiler.read_columns().to_vec());
        let engine = QueryEngine::from_handle(runtime);
        let mut execution = engine.execute(&plan, generation)?;
        loop {
            if cancellation.is_cancelled() {
                execution.cancel();
                bail!("profile cancelled");
            }
            match execution.poll_next_batch()? {
                QueryPoll::Batch(batch) => {
                    scanned.fetch_add(
                        batch.batch.num_rows() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    profiler.push_batch(&batch.batch)?;
                }
                QueryPoll::Finished => break,
                QueryPoll::Pending => thread::sleep(Duration::from_millis(5)),
            }
        }
    }
    Ok(profiler
        .finish()
        .into_iter()
        .map(ColumnProfile::from)
        .collect())
}

/// The whole-file profile of a tab: shown in its own window.
#[derive(Default)]
pub(crate) struct FileProfileState {
    pub(crate) visible: bool,
    pub(crate) task: Option<FileProfileTask>,
    pub(crate) result: Option<Vec<ColumnProfile>>,
    pub(crate) error: Option<String>,
    /// Columns the file has beyond those profiled.
    pub(crate) skipped_columns: usize,
}

/// Inspector profile state: the running task and its result.
pub(crate) struct ProfileState {
    /// Every column at once.
    pub(crate) file: FileProfileState,
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
            file: FileProfileState::default(),
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
        self.task.is_some() || self.file.task.is_some()
    }

    pub(crate) fn cancel(&mut self) {
        if let Some(task) = &self.task {
            task.cancel();
        }
        self.task = None;
        if let Some(task) = &self.file.task {
            task.cancel();
        }
        self.file.task = None;
    }

    pub(crate) fn reset(&mut self) {
        self.cancel();
        let visible = self.file.visible;
        self.file = FileProfileState {
            visible,
            ..FileProfileState::default()
        };
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

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use pavi_runtime::{Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;

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
        assert_eq!(
            views.string_lengths.as_ref().map(|lengths| lengths.max),
            Some(4)
        );

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
