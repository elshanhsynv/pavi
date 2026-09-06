use std::{collections::VecDeque, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use parquet_reader::{DataPage, GroupBudget, PAGE_ROWS, SortBudget};
use pavi_runtime::{
    CancellationToken, GenerationId, PageOutcome, PageTask, Runtime, RuntimeHandle, SubmitError,
    TaskId,
};

use crate::{LogicalPlan, PhysicalPlan, Planner};

pub struct QueryEngine {
    runtime: RuntimeHandle,
    sort_budget: SortBudget,
    group_budget: GroupBudget,
}

pub struct QueryExecution {
    runtime: RuntimeHandle,
    sort_budget: SortBudget,
    group_budget: GroupBudget,
    plan: PhysicalPlan,
    generation_id: GenerationId,
    pending: Option<PageTask>,
    in_flight: Option<InFlight>,
    buffered: VecDeque<(TaskId, RecordBatch)>,
    next_page: u64,
    next_filter_offset: u64,
    remaining: Option<usize>,
    finished: bool,
    cancelled: bool,
    scheduled_reads: usize,
    sort_submitted: bool,
    aggregate_submitted: bool,
}

#[derive(Debug)]
pub struct QueryBatch {
    pub task_id: TaskId,
    pub generation_id: GenerationId,
    pub batch: RecordBatch,
}

/// Nonblocking state returned while incrementally driving a query from an event loop.
pub enum QueryPoll {
    Batch(QueryBatch),
    Pending,
    Finished,
}

enum InFlight {
    Page,
    Window,
    Filtered { requested_rows: usize },
    Sorted,
    Aggregate,
}

impl QueryEngine {
    pub fn new(runtime: &Runtime) -> Self {
        Self {
            runtime: runtime.handle(),
            sort_budget: SortBudget::default(),
            group_budget: GroupBudget::default(),
        }
    }

    pub fn with_sort_budget(runtime: &Runtime, sort_budget: SortBudget) -> Self {
        Self {
            runtime: runtime.handle(),
            sort_budget,
            group_budget: GroupBudget::default(),
        }
    }

    pub fn with_group_budget(runtime: &Runtime, group_budget: GroupBudget) -> Self {
        Self {
            runtime: runtime.handle(),
            sort_budget: SortBudget::default(),
            group_budget,
        }
    }

    pub fn execute(
        &self,
        logical: &LogicalPlan,
        generation_id: GenerationId,
    ) -> Result<QueryExecution> {
        let plan = Planner::plan(logical)?;
        let mut execution = QueryExecution {
            runtime: self.runtime.clone(),
            sort_budget: self.sort_budget,
            group_budget: self.group_budget,
            remaining: plan.limit,
            plan,
            generation_id,
            pending: None,
            in_flight: None,
            buffered: VecDeque::new(),
            next_page: 0,
            next_filter_offset: 0,
            finished: false,
            cancelled: false,
            scheduled_reads: 0,
            sort_submitted: false,
            aggregate_submitted: false,
        };
        if let Err(error) = execution.schedule_next()
            && !is_queue_full(&error)
        {
            return Err(error);
        }
        Ok(execution)
    }
}

impl QueryExecution {
    pub fn generation_id(&self) -> GenerationId {
        self.generation_id
    }

    pub fn cancellation_token(&self) -> Option<CancellationToken> {
        self.pending.as_ref().map(PageTask::cancellation_token)
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
        if let Some(task) = &self.pending {
            task.cancel();
        }
    }

    pub fn next_batch(&mut self) -> Result<Option<QueryBatch>> {
        loop {
            if self.cancelled {
                bail!("query execution was cancelled");
            }
            if let Some(batch) = self.buffered.pop_front() {
                return Ok(self.apply_limit(batch));
            }
            if self.finished {
                return Ok(None);
            }

            self.schedule_next()?;
            if self.finished {
                return Ok(None);
            }

            let task = self
                .pending
                .take()
                .context("query execution has no pending runtime task")?;
            let response = task.recv().context("receive query runtime response")?;
            let in_flight = self
                .in_flight
                .take()
                .context("query execution lost its pending read")?;

            match response.outcome {
                PageOutcome::Loaded(page) => self.buffer_page(page, in_flight, response.task_id)?,
                PageOutcome::Batch(batch) => {
                    self.buffer_batch(batch, in_flight, response.task_id)?
                }
                PageOutcome::Batches(batches) => {
                    self.buffer_batches(batches, in_flight, response.task_id)?
                }
                PageOutcome::Cancelled => bail!("query task {} was cancelled", response.task_id.0),
                PageOutcome::ReadFailed(error) => {
                    return Err(error).context(format!("query task {} failed", response.task_id.0));
                }
            }
        }
    }

    /// Polls for one next batch without waiting for a runtime response.
    pub fn poll_next_batch(&mut self) -> Result<QueryPoll> {
        loop {
            if self.cancelled {
                bail!("query execution was cancelled");
            }
            if let Some(batch) = self.buffered.pop_front() {
                return Ok(self
                    .apply_limit(batch)
                    .map_or(QueryPoll::Finished, QueryPoll::Batch));
            }
            if self.finished {
                return Ok(QueryPoll::Finished);
            }

            if let Err(error) = self.schedule_next() {
                if is_queue_full(&error) {
                    return Ok(QueryPoll::Pending);
                }
                return Err(error);
            }
            if self.finished {
                return Ok(QueryPoll::Finished);
            }
            let response = match self
                .pending
                .as_ref()
                .context("query execution has no pending runtime task")?
                .try_recv()
            {
                Ok(response) => response,
                Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(QueryPoll::Pending),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.pending = None;
                    self.in_flight = None;
                    bail!("query runtime response channel disconnected");
                }
            };
            self.pending = None;
            let in_flight = self
                .in_flight
                .take()
                .context("query execution lost its pending read")?;

            match response.outcome {
                PageOutcome::Loaded(page) => self.buffer_page(page, in_flight, response.task_id)?,
                PageOutcome::Batch(batch) => {
                    self.buffer_batch(batch, in_flight, response.task_id)?
                }
                PageOutcome::Batches(batches) => {
                    self.buffer_batches(batches, in_flight, response.task_id)?
                }
                PageOutcome::Cancelled => bail!("query task {} was cancelled", response.task_id.0),
                PageOutcome::ReadFailed(error) => {
                    return Err(error).context(format!("query task {} failed", response.task_id.0));
                }
            }
        }
    }

    pub fn scheduled_reads(&self) -> usize {
        self.scheduled_reads
    }

    fn buffer_page(&mut self, page: DataPage, in_flight: InFlight, task_id: TaskId) -> Result<()> {
        if !matches!(in_flight, InFlight::Page) {
            bail!("runtime returned a page for a non-page query read");
        }
        self.buffered
            .extend(page.batches.into_iter().map(|batch| (task_id, batch)));
        Ok(())
    }

    fn buffer_batch(
        &mut self,
        batch: RecordBatch,
        in_flight: InFlight,
        task_id: TaskId,
    ) -> Result<()> {
        match in_flight {
            InFlight::Window => {}
            InFlight::Filtered { requested_rows } => {
                self.next_filter_offset += batch.num_rows() as u64;
                if batch.num_rows() < requested_rows {
                    self.finished = true;
                }
            }
            InFlight::Aggregate => self.finished = true,
            InFlight::Page => bail!("runtime returned a batch for a page query read"),
            InFlight::Sorted => bail!("runtime returned one batch for a sorted query read"),
        }
        if batch.num_rows() > 0 {
            self.buffered.push_back((task_id, batch));
        }
        Ok(())
    }

    fn buffer_batches(
        &mut self,
        batches: Vec<RecordBatch>,
        in_flight: InFlight,
        task_id: TaskId,
    ) -> Result<()> {
        if !matches!(in_flight, InFlight::Sorted) {
            bail!("runtime returned sorted batches for a non-sort query read");
        }
        self.buffered.extend(
            batches
                .into_iter()
                .filter(|batch| batch.num_rows() > 0)
                .map(|batch| (task_id, batch)),
        );
        self.finished = true;
        Ok(())
    }

    fn apply_limit(&mut self, (task_id, batch): (TaskId, RecordBatch)) -> Option<QueryBatch> {
        let rows = self.remaining.unwrap_or(usize::MAX).min(batch.num_rows());
        if let Some(remaining) = &mut self.remaining {
            *remaining -= rows;
            if *remaining == 0 {
                self.finished = true;
                self.buffered.clear();
            }
        }
        (rows > 0).then(|| QueryBatch {
            task_id,
            generation_id: self.generation_id,
            batch: batch.slice(0, rows),
        })
    }

    fn schedule_next(&mut self) -> Result<()> {
        if self.finished || self.pending.is_some() {
            return Ok(());
        }
        let row_count = self
            .remaining
            .unwrap_or(PAGE_ROWS as usize)
            .min(PAGE_ROWS as usize);
        if row_count == 0 {
            self.finished = true;
            return Ok(());
        }

        let source = Arc::clone(&self.plan.source);
        let projection = self.plan.projection.clone();
        let (task, in_flight, advance_page) = if let Some(aggregate) = &self.plan.aggregate {
            if self.aggregate_submitted {
                return Ok(());
            }
            (
                self.runtime.submit_aggregated(
                    source,
                    self.plan.filter.clone(),
                    aggregate.clone(),
                    self.group_budget,
                    self.generation_id,
                ),
                InFlight::Aggregate,
                false,
            )
        } else if let Some(sort) = self.plan.sort {
            if self.sort_submitted {
                return Ok(());
            }
            (
                self.runtime.submit_sorted(
                    source,
                    self.plan.filter.clone(),
                    projection,
                    sort,
                    self.sort_budget,
                    self.generation_id,
                ),
                InFlight::Sorted,
                false,
            )
        } else if let Some(filter) = &self.plan.filter {
            (
                self.runtime.submit_filtered_window(
                    source,
                    filter.clone(),
                    self.next_filter_offset,
                    row_count,
                    projection,
                    self.generation_id,
                ),
                InFlight::Filtered {
                    requested_rows: row_count,
                },
                false,
            )
        } else {
            let first_row = self.next_page.saturating_mul(PAGE_ROWS);
            if first_row >= self.plan.source.row_count() {
                self.finished = true;
                return Ok(());
            }
            if row_count < PAGE_ROWS as usize {
                (
                    self.runtime.submit_window(
                        source,
                        first_row,
                        row_count,
                        projection,
                        self.generation_id,
                    ),
                    InFlight::Window,
                    true,
                )
            } else {
                (
                    self.runtime.submit_page(
                        source,
                        self.next_page,
                        projection,
                        self.generation_id,
                    ),
                    InFlight::Page,
                    true,
                )
            }
        };
        let task = task.context("submit query source read")?;
        if advance_page {
            self.next_page = self.next_page.saturating_add(1);
        }
        if matches!(in_flight, InFlight::Sorted) {
            self.sort_submitted = true;
        }
        if matches!(in_flight, InFlight::Aggregate) {
            self.aggregate_submitted = true;
        }
        self.pending = Some(task);
        self.in_flight = Some(in_flight);
        self.scheduled_reads += 1;
        Ok(())
    }
}

fn is_queue_full(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<SubmitError>()
            .is_some_and(|error| *error == SubmitError::QueueFull)
    })
}

impl QueryBatch {
    pub fn is_stale_for(&self, current_generation: GenerationId) -> bool {
        self.generation_id != current_generation
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::File, sync::Arc};

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use parquet_reader::ParquetSource;
    use pavi_runtime::{Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;
    use crate::{AggregateExpr, Filter, GroupBudget, NullOrder, SortBudget, SortDirection};

    const ROWS: i32 = PAGE_ROWS as i32 + 10;

    fn source() -> (TempDir, Arc<ParquetSource>, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("query.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
            Field::new("enabled", DataType::Boolean, false),
        ]));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1_000))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..ROWS)),
                        Arc::new(Int32Array::from_iter_values((0..ROWS).map(|row| row * 10))),
                        Arc::new(BooleanArray::from_iter((0..ROWS).map(|row| row % 2 == 0))),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        (dir, Arc::new(ParquetSource::open(&path).unwrap()), path)
    }

    fn runtime() -> Runtime {
        Runtime::new(RuntimeConfig {
            worker_count: 1,
            queue_capacity: 2,
        })
        .unwrap()
    }

    fn sort_source() -> (TempDir, Arc<ParquetSource>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sort.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
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
                        Arc::new(Int32Array::from(vec![Some(3), None, Some(1), Some(2)])),
                        Arc::new(StringArray::from(vec![
                            Some("z"),
                            Some("a"),
                            None,
                            Some("a"),
                        ])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        (dir, Arc::new(ParquetSource::open(path).unwrap()))
    }

    fn aggregate_source() -> (TempDir, Arc<ParquetSource>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aggregate.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
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
                        Arc::new(StringArray::from(vec![
                            Some("alpha"),
                            Some("beta"),
                            Some("alpha"),
                            None,
                            Some("beta"),
                        ])),
                        Arc::new(Int64Array::from(vec![
                            Some(10),
                            None,
                            Some(20),
                            Some(5),
                            Some(-3),
                        ])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        (dir, Arc::new(ParquetSource::open(path).unwrap()))
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn scans_pages_without_blocking_execute() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(source), GenerationId(1))
            .unwrap();

        assert_eq!(execution.scheduled_reads(), 1);
        let batch = execution.next_batch().unwrap().unwrap();
        assert_eq!(batch.batch.num_rows(), PAGE_ROWS as usize);
        assert_eq!(batch.batch.num_columns(), 3);
    }

    #[test]
    fn projects_requested_columns_and_plans_pushdown() {
        let (_dir, source, _) = source();
        let logical = LogicalPlan::scan(source).project(vec![1, 0]);
        let physical = Planner::plan(&logical).unwrap();
        assert_eq!(physical.projected_columns(), &[1, 0]);

        let runtime = runtime();
        let batch = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(1))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(batch.schema().field(0).name(), "value");
        assert_eq!(batch.schema().field(1).name(), "id");
    }

    #[test]
    fn filters_with_source_pushdown() {
        let (_dir, source, _) = source();
        let filter = Filter::parse("id >= 4100").unwrap();
        let logical = LogicalPlan::scan(source).filter(filter);
        let physical = Planner::plan(&logical).unwrap();
        assert!(physical.filter().is_some());

        let runtime = runtime();
        let batch = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(1))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(ids(&batch), (4100..ROWS).collect::<Vec<_>>());
    }

    #[test]
    fn limit_stops_after_one_bounded_read() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(source).limit(2), GenerationId(1))
            .unwrap();

        assert_eq!(
            ids(&execution.next_batch().unwrap().unwrap().batch),
            vec![0, 1]
        );
        assert!(execution.next_batch().unwrap().is_none());
        assert_eq!(execution.scheduled_reads(), 1);
    }

    #[test]
    fn executes_scan_filter_projection_limit_through_runtime() {
        let (_dir, source, _) = source();
        let logical = LogicalPlan::scan(source)
            .filter(Filter::parse("id >= 1000").unwrap())
            .project(vec![1])
            .limit(3);
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(9))
            .unwrap();
        let batch = execution.next_batch().unwrap().unwrap();

        assert_eq!(batch.generation_id, GenerationId(9));
        assert_eq!(
            batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[10_000, 10_010, 10_020]
        );
        assert!(execution.next_batch().unwrap().is_none());
    }

    #[test]
    fn rejects_invalid_columns_and_filter_types_before_execution() {
        let (_dir, source, _) = source();
        assert!(Planner::plan(&LogicalPlan::scan(source.clone()).project(vec![3])).is_err());
        assert!(
            Planner::plan(
                &LogicalPlan::scan(source).filter(Filter::parse("enabled > true").unwrap())
            )
            .is_err()
        );
    }

    #[test]
    fn returns_empty_results_without_reading_more_pages() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(source).filter(Filter::parse("id > 999999").unwrap()),
                GenerationId(1),
            )
            .unwrap();

        assert!(execution.next_batch().unwrap().is_none());
        assert_eq!(execution.scheduled_reads(), 1);
    }

    #[test]
    fn streams_multiple_pages_and_row_groups_without_collecting_them() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(source), GenerationId(1))
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = execution.next_batch().unwrap() {
            assert!(batch.batch.num_rows() <= PAGE_ROWS as usize);
            rows += batch.batch.num_rows();
        }
        assert_eq!(rows, ROWS as usize);
        assert_eq!(execution.scheduled_reads(), 2);
    }

    #[test]
    fn keeps_only_one_page_of_unconsumed_results() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(source), GenerationId(1))
            .unwrap();
        let _ = execution.next_batch().unwrap().unwrap();

        assert_eq!(execution.scheduled_reads(), 1);
        assert!(execution.pending.is_none());
        assert!(
            execution
                .buffered
                .iter()
                .map(|(_, batch)| batch.num_rows())
                .sum::<usize>()
                <= PAGE_ROWS as usize
        );
    }

    #[test]
    fn propagates_runtime_errors_and_cancellation() {
        let (_dir, failing_source, path) = source();
        fs::remove_file(path).unwrap();
        let runtime = runtime();
        let mut failed = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(failing_source).limit(1), GenerationId(1))
            .unwrap();
        assert!(
            failed
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("query task")
        );

        let (_cancel_dir, cancel_source, _) = source();
        let mut cancelled = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(cancel_source), GenerationId(2))
            .unwrap();
        cancelled.cancel();
        assert!(
            cancelled
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    fn preserves_generation_for_stale_results() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let batch = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(source).limit(1), GenerationId(7))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap();
        assert!(batch.is_stale_for(GenerationId(8)));
        assert!(!batch.is_stale_for(GenerationId(7)));
    }

    #[test]
    fn executes_all_aggregates_with_sql_null_semantics() {
        let (_dir, source) = aggregate_source();
        let runtime = runtime();
        let logical = LogicalPlan::scan(source)
            .aggregate(vec![
                AggregateExpr::count_all(),
                AggregateExpr::count(1),
                AggregateExpr::sum(1),
                AggregateExpr::avg(1),
                AggregateExpr::min(1),
                AggregateExpr::max(1),
                AggregateExpr::min(0),
                AggregateExpr::max(0),
            ])
            .unwrap();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(20))
            .unwrap();
        let batch = execution.next_batch().unwrap().unwrap().batch;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            5
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            4
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            32
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            8.0
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            -3
        );
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        let min = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let max = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(min.value(0), "alpha");
        assert_eq!(max.value(0), "beta");
        assert!(execution.next_batch().unwrap().is_none());
    }

    #[test]
    fn groups_incrementally_in_first_seen_order_and_obeys_its_budget() {
        let (_dir, source) = aggregate_source();
        let runtime = runtime();
        let logical = LogicalPlan::scan(Arc::clone(&source))
            .aggregate_grouped(
                0,
                vec![
                    AggregateExpr::count_all(),
                    AggregateExpr::count(1),
                    AggregateExpr::sum(1),
                ],
            )
            .unwrap();
        let batch = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(21))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("alpha"), Some("beta"), None]
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[2, 2, 1]
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[2, 1, 1]
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(30), Some(-3), Some(5)]
        );

        let mut limited = QueryEngine::with_group_budget(
            &runtime,
            GroupBudget::new(2, GroupBudget::DEFAULT_MAX_BYTES).unwrap(),
        )
        .execute(&logical, GenerationId(22))
        .unwrap();
        assert!(
            format!("{:#}", limited.next_batch().unwrap_err()).contains("group budget of 2 groups")
        );

        let mut byte_limited =
            QueryEngine::with_group_budget(&runtime, GroupBudget::new(10, 1).unwrap())
                .execute(&logical, GenerationId(23))
                .unwrap();
        assert!(
            format!("{:#}", byte_limited.next_batch().unwrap_err())
                .contains("group budget of 1 bytes")
        );
    }

    #[test]
    fn aggregates_multi_page_input_and_preserves_generation_and_cancellation() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let logical = LogicalPlan::scan(Arc::clone(&source))
            .aggregate(vec![
                AggregateExpr::count_all(),
                AggregateExpr::sum(0),
                AggregateExpr::avg(0),
            ])
            .unwrap();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(23))
            .unwrap();
        let batch = execution.next_batch().unwrap().unwrap();
        assert_eq!(batch.generation_id, GenerationId(23));
        assert!(batch.is_stale_for(GenerationId(24)));
        let values = batch
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(values, i64::from(ROWS - 1) * i64::from(ROWS) / 2);
        assert_eq!(execution.scheduled_reads(), 1);

        let single = LogicalPlan::scan(Arc::clone(&source))
            .filter(Filter::parse("id == 1").unwrap())
            .aggregate(vec![AggregateExpr::count_all(), AggregateExpr::sum(0)])
            .unwrap();
        let batch = QueryEngine::new(&runtime)
            .execute(&single, GenerationId(24))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );

        let mut cancelled = QueryEngine::new(&runtime)
            .execute(&logical, GenerationId(24))
            .unwrap();
        cancelled.cancel();
        assert!(
            cancelled
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    fn rejects_aggregate_overflow_and_incompatible_types() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("overflow.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(
                &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![i64::MAX, 1]))])
                    .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        let source = Arc::new(ParquetSource::open(path).unwrap());
        let runtime = runtime();
        let mut overflow = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(Arc::clone(&source))
                    .aggregate(vec![AggregateExpr::sum(0)])
                    .unwrap(),
                GenerationId(25),
            )
            .unwrap();
        assert!(format!("{:#}", overflow.next_batch().unwrap_err()).contains("overflow"));
        assert!(
            QueryEngine::new(&runtime)
                .execute(
                    &LogicalPlan::scan(source)
                        .aggregate(vec![AggregateExpr::sum(9)])
                        .unwrap(),
                    GenerationId(26),
                )
                .is_err()
        );
    }

    #[test]
    fn executes_direct_sort_with_deterministic_null_and_string_ordering() {
        let (_dir, source) = sort_source();
        let runtime = runtime();
        let mut ascending = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(Arc::clone(&source)).sort(
                    0,
                    SortDirection::Ascending,
                    NullOrder::Last,
                ),
                GenerationId(5),
            )
            .unwrap();
        let batch = ascending.next_batch().unwrap().unwrap().batch;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![Some(1), Some(2), Some(3), None]);

        let mut strings = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(source).sort(1, SortDirection::Descending, NullOrder::First),
                GenerationId(6),
            )
            .unwrap();
        let batch = strings.next_batch().unwrap().unwrap().batch;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(names, vec![None, Some("z"), Some("a"), Some("a")]);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![Some(1), Some(3), None, Some(2)]);
    }

    #[test]
    fn sorts_multiple_pages_and_applies_limit_after_ordering() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(source)
                    .sort(0, SortDirection::Descending, NullOrder::Last)
                    .limit(3),
                GenerationId(12),
            )
            .unwrap();
        let batch = execution.next_batch().unwrap().unwrap().batch;
        assert_eq!(ids(&batch), vec![ROWS - 1, ROWS - 2, ROWS - 3]);
        assert!(execution.next_batch().unwrap().is_none());
        assert_eq!(execution.scheduled_reads(), 1);
    }

    #[test]
    fn rejects_sort_inputs_over_the_configured_budget_without_temp_files() {
        let (directory, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::with_sort_budget(
            &runtime,
            SortBudget::new(10, SortBudget::DEFAULT_MAX_BYTES).unwrap(),
        )
        .execute(
            &LogicalPlan::scan(source).sort(0, SortDirection::Ascending, NullOrder::Last),
            GenerationId(13),
        )
        .unwrap();
        let error = execution.next_batch().unwrap_err();
        assert!(format!("{error:#}").contains("external sorting is unavailable"));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn sorting_preserves_cancellation_and_generation_handling() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(source).sort(0, SortDirection::Ascending, NullOrder::Last),
                GenerationId(14),
            )
            .unwrap();
        execution.cancel();
        assert!(
            execution
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    fn sorts_empty_and_single_row_inputs() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut empty = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(Arc::clone(&source))
                    .filter(Filter::parse("id > 999999").unwrap())
                    .sort(0, SortDirection::Ascending, NullOrder::Last),
                GenerationId(15),
            )
            .unwrap();
        assert!(empty.next_batch().unwrap().is_none());

        let mut single = QueryEngine::new(&runtime)
            .execute(
                &LogicalPlan::scan(source)
                    .filter(Filter::parse("id == 1").unwrap())
                    .sort(0, SortDirection::Ascending, NullOrder::Last),
                GenerationId(16),
            )
            .unwrap();
        assert_eq!(ids(&single.next_batch().unwrap().unwrap().batch), vec![1]);
        assert!(single.next_batch().unwrap().is_none());
    }

    #[test]
    fn polls_without_waiting_for_a_runtime_response() {
        let (_dir, source, _) = source();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&LogicalPlan::scan(source).limit(1), GenerationId(3))
            .unwrap();

        let mut batch = None;
        for _ in 0..100 {
            match execution.poll_next_batch().unwrap() {
                QueryPoll::Batch(result) => {
                    batch = Some(result);
                    break;
                }
                QueryPoll::Pending => std::thread::sleep(std::time::Duration::from_millis(1)),
                QueryPoll::Finished => break,
            }
        }

        assert_eq!(batch.unwrap().batch.num_rows(), 1);
    }
}
