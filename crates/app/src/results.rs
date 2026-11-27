//! The bounded window of filter, SQL and sort results shown in the grid.

use std::collections::VecDeque;

use std::ops::Range;
use std::sync::Arc;

use parquet_reader::{FilterExpr, ParquetSource, RowGroupInfo, SortSpec};
use pavi_query::{LogicalPlan, QueryExecution};
use pavi_runtime::{GenerationId, PageOutcome, PageTask, RuntimeHandle, SubmitError};

use crate::state::ColumnLayout;

/// Result batches kept for the grid; older batches are dropped as new ones arrive.
pub(crate) const MAX_FILTERED_BATCHES: usize = 8;

/// Result rows read above a row jumped to, so the rows just before it can be
/// scrolled to without another jump.
pub(crate) const JUMP_CONTEXT_ROWS: u64 = parquet_reader::PAGE_ROWS / 2;

/// Rows of consecutive row groups one count task reads, at least: smaller
/// chunks would leave workers waiting between the app's polls.
const MIN_COUNT_CHUNK_ROWS: u64 = 1 << 20;
/// Rows one count task reads, at most, so a cancel or a finished chunk frees
/// its worker soon.
const MAX_COUNT_CHUNK_ROWS: u64 = 64 << 20;
/// Workers left free while a count runs: for the grid's pages and the
/// result's own scan.
const COUNT_FREE_WORKERS: usize = 2;

/// The background count of a filter's matches in each row group (ADR 0012).
/// It gives the result's size, and where in the file each result row is, so
/// the grid can start reading anywhere in the result. The row groups are cut
/// into chunks counted on several workers at once (all but
/// [`COUNT_FREE_WORKERS`]); a chunk is submitted as another finishes.
pub(crate) struct MatchCount {
    work: Option<CountWork>,
    /// `prefix[g]` is the number of matches before row group `g`; the last
    /// entry is the total. One entry per row group, plus one.
    prefix: Option<Vec<u64>>,
    total: Option<u64>,
    pub(crate) error: Option<String>,
}

/// A count still running: the chunks, what was counted, what is in flight.
struct CountWork {
    runtime: RuntimeHandle,
    source: Arc<ParquetSource>,
    filter: FilterExpr,
    generation: GenerationId,
    /// Consecutive row groups per task, in file order.
    chunks: Vec<Range<usize>>,
    next_chunk: usize,
    /// At most this many chunks are counted at once.
    parallel: usize,
    /// Chunks being counted: the chunk, its first source row, its task.
    running: Vec<(Range<usize>, u64, PageTask)>,
    /// One count per row group of the file.
    counts: Vec<u64>,
    /// Source rows in the chunks counted so far.
    counted_rows: u64,
}

impl MatchCount {
    /// Starts counting `filter`'s matches in `source`. `None` when the runtime
    /// can't take a task now; the result then works without a count.
    pub(crate) fn start(
        runtime: RuntimeHandle,
        source: Arc<ParquetSource>,
        filter: FilterExpr,
        generation: GenerationId,
    ) -> Option<Self> {
        let parallel = runtime
            .worker_count()
            .saturating_sub(COUNT_FREE_WORKERS)
            .max(1);
        // About four chunks per worker, so the workers finish together.
        let chunk_rows = (source.row_count() / (parallel as u64 * 4))
            .clamp(MIN_COUNT_CHUNK_ROWS, MAX_COUNT_CHUNK_ROWS);
        Self::start_chunked(runtime, source, filter, generation, chunk_rows)
    }

    /// [`Self::start`] with chunks of about `chunk_rows` rows (at least one
    /// row group each).
    pub(crate) fn start_chunked(
        runtime: RuntimeHandle,
        source: Arc<ParquetSource>,
        filter: FilterExpr,
        generation: GenerationId,
        chunk_rows: u64,
    ) -> Option<Self> {
        let row_groups = source.row_groups();
        let parallel = runtime
            .worker_count()
            .saturating_sub(COUNT_FREE_WORKERS)
            .max(1);
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut rows = 0_u64;
        for (index, info) in row_groups.iter().enumerate() {
            rows += info.row_count;
            if rows >= chunk_rows {
                chunks.push(start..index + 1);
                start = index + 1;
                rows = 0;
            }
        }
        if start < row_groups.len() {
            chunks.push(start..row_groups.len());
        }
        let mut count = Self {
            work: Some(CountWork {
                runtime,
                counts: vec![0; row_groups.len()],
                source,
                filter,
                generation,
                parallel: parallel.min(chunks.len().max(1)),
                chunks,
                next_chunk: 0,
                running: Vec::new(),
                counted_rows: 0,
            }),
            prefix: None,
            total: None,
            error: None,
        };
        count.submit_more();
        // Nothing could be submitted: the runtime is full or gone.
        let started = count
            .work
            .as_ref()
            .is_none_or(|work| !work.running.is_empty());
        (started && count.error.is_none()).then_some(count)
    }

    /// A size known without counting (a sort of the whole file), with no
    /// positions to jump with.
    pub(crate) fn known(total: u64) -> Self {
        Self {
            work: None,
            prefix: None,
            total: Some(total),
            error: None,
        }
    }

    /// The number of matches, once counted.
    pub(crate) fn total(&self) -> Option<u64> {
        self.total
    }

    /// Whether the count is still running.
    pub(crate) fn running(&self) -> bool {
        self.work.is_some()
    }

    /// Source rows the count has read so far, all chunks together.
    pub(crate) fn scanned_rows(&self) -> Option<u64> {
        let work = self.work.as_ref()?;
        let row_groups = work.source.row_groups();
        let in_flight = work
            .running
            .iter()
            .map(|(chunk, first_row, task)| {
                let rows = chunk
                    .clone()
                    .filter_map(|index| row_groups.get(index))
                    .map(|info| info.row_count)
                    .sum::<u64>();
                task.progress()
                    .scanned_rows()
                    .saturating_sub(*first_row)
                    .min(rows)
            })
            .sum::<u64>();
        Some(work.counted_rows.saturating_add(in_flight))
    }

    /// Takes the counts that have arrived and submits the next chunks; the
    /// total is known once every chunk is counted.
    pub(crate) fn poll(&mut self) {
        let Some(work) = &mut self.work else {
            return;
        };
        let row_groups = work.source.row_groups();
        let mut failed = None;
        let mut index = 0;
        while index < work.running.len() {
            let outcome = match work.running[index].2.try_recv() {
                Ok(response) => response.outcome,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    index += 1;
                    continue;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    failed = Some("the count stopped without a result".to_owned());
                    break;
                }
            };
            let (chunk, _, _) = work.running.swap_remove(index);
            match outcome {
                PageOutcome::Counted(counts) if counts.len() == chunk.len() => {
                    for (group, count) in chunk.clone().zip(counts) {
                        if let Some(slot) = work.counts.get_mut(group) {
                            *slot = count;
                        }
                    }
                    work.counted_rows = work.counted_rows.saturating_add(
                        chunk
                            .filter_map(|group| row_groups.get(group))
                            .map(|info| info.row_count)
                            .sum::<u64>(),
                    );
                }
                PageOutcome::Cancelled => failed = Some("the count was cancelled".to_owned()),
                PageOutcome::ReadFailed(error) => failed = Some(format!("{error:#}")),
                _ => failed = Some("the count returned something else".to_owned()),
            }
            if failed.is_some() {
                break;
            }
        }
        if let Some(error) = failed {
            self.error = Some(error);
            self.cancel();
            self.work = None;
            return;
        }
        self.submit_more();
        if let Some(work) = &self.work
            && work.running.is_empty()
            && work.next_chunk >= work.chunks.len()
        {
            let prefix = prefix_sums(&work.counts);
            self.total = prefix.last().copied();
            self.prefix = Some(prefix);
            self.work = None;
        }
    }

    /// Submits chunks until `parallel` are running; a full queue is retried
    /// at the next poll.
    fn submit_more(&mut self) {
        let Some(work) = &mut self.work else {
            return;
        };
        while work.running.len() < work.parallel {
            let Some(chunk) = work.chunks.get(work.next_chunk).cloned() else {
                return;
            };
            let first_row = work
                .source
                .row_groups()
                .get(chunk.start)
                .map_or(0, |info| info.first_row);
            match work.runtime.submit_match_counts_in(
                Arc::clone(&work.source),
                work.filter.clone(),
                chunk.clone(),
                work.generation,
            ) {
                Ok(task) => {
                    work.running.push((chunk, first_row, task));
                    work.next_chunk += 1;
                }
                Err(SubmitError::QueueFull) => return,
                Err(error) => {
                    self.error = Some(format!("count the matches: {error}"));
                    return;
                }
            }
        }
    }

    /// Where to start reading to reach result row `row`: the first source row
    /// of the row group that holds it, and the number of matches before that
    /// group. `None` until counted, or past the end.
    pub(crate) fn entry_for(&self, row: u64, row_groups: &[RowGroupInfo]) -> Option<(u64, u64)> {
        let prefix = self.prefix.as_ref()?;
        let starts = prefix.get(..prefix.len().checked_sub(1)?)?;
        // The last group starting at or before `row`; empty groups before it
        // start at the same place, so they are passed over.
        let group = starts
            .partition_point(|before| *before <= row)
            .checked_sub(1)?;
        Some((row_groups.get(group)?.first_row, *starts.get(group)?))
    }

    fn cancel(&self) {
        if let Some(work) = &self.work {
            for (_, _, task) in &work.running {
                task.cancel();
            }
        }
    }
}

impl Drop for MatchCount {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn prefix_sums(counts: &[u64]) -> Vec<u64> {
    std::iter::once(0)
        .chain(counts.iter().scan(0_u64, |total, count| {
            *total = total.saturating_add(*count);
            Some(*total)
        }))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryKind {
    Filter,
    Sql,
    Sort,
}

impl QueryKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Filter => "Filter",
            Self::Sql => "SQL",
            Self::Sort => "Sort",
        }
    }

    pub(crate) fn empty_message(self) -> &'static str {
        match self {
            Self::Filter => "No rows match the filter.",
            Self::Sql => "The query returned no rows.",
            Self::Sort => "The dataset has no rows.",
        }
    }
}

pub(crate) struct FilteredGrid {
    pub(crate) kind: QueryKind,
    pub(crate) plan: LogicalPlan,
    /// Sort keys, most significant first; empty when the result is not sorted.
    pub(crate) sort: Vec<SortSpec>,
    /// The filter-box filter this result shows, kept so a cell menu can add to it.
    pub(crate) filter: Option<parquet_reader::FilterExpr>,
    /// The result's columns are not the file's: an aggregate, a join or a
    /// subquery (ADR 0009). They are shown as the result names them.
    pub(crate) aggregate: bool,
    pub(crate) projected_columns: Vec<usize>,
    pub(crate) display_columns: Vec<(usize, usize)>,
    pub(crate) execution: Option<QueryExecution>,
    pub(crate) batches: VecDeque<arrow_array::RecordBatch>,
    pub(crate) first_result: u64,
    pub(crate) rows: u64,
    pub(crate) target_result: u64,
    /// Result rows before this one are dropped as they arrive: the grid is
    /// reading on to a row it jumped to.
    pub(crate) skip_until: u64,
    /// A result row to show and select once it has arrived, and the column
    /// to select in it (the selection is cleared while the rows move).
    pub(crate) pending_jump: Option<u64>,
    pub(crate) pending_column: usize,
    /// How many rows match, for a filter (and a sort of a filtered file).
    pub(crate) count: Option<MatchCount>,
    /// The result reached its end at least once; a rerun to jump back
    /// doesn't add another history entry.
    pub(crate) completed_once: bool,
    pub(crate) finished: bool,
    pub(crate) error: Option<String>,
}

impl FilteredGrid {
    pub(crate) fn new(
        kind: QueryKind,
        plan: LogicalPlan,
        sort: Vec<SortSpec>,
        aggregate: bool,
        projected_columns: Vec<usize>,
        execution: QueryExecution,
    ) -> Self {
        Self {
            kind,
            plan,
            sort,
            filter: None,
            aggregate,
            projected_columns,
            display_columns: Vec::new(),
            execution: Some(execution),
            batches: VecDeque::new(),
            first_result: 0,
            rows: 0,
            target_result: parquet_reader::PAGE_ROWS,
            skip_until: 0,
            pending_jump: None,
            pending_column: 0,
            count: None,
            completed_once: false,
            finished: false,
            error: None,
        }
    }

    /// How many rows the result has, when known: counted, or read to the end.
    pub(crate) fn total(&self) -> Option<u64> {
        self.count
            .as_ref()
            .and_then(MatchCount::total)
            .or((self.finished && self.error.is_none()).then(|| self.received_rows()))
    }

    /// Whether result row `row` is in the rows held now.
    pub(crate) fn holds(&self, row: u64) -> bool {
        row >= self.first_result && row < self.received_rows()
    }

    /// Reads on with this execution, dropping rows until result row `start`.
    /// The workers drop them in large steps (`QueryExecution::skip_rows`);
    /// rows that still arrive before `start` are dropped by `push`.
    pub(crate) fn skip_forward(&mut self, start: u64) {
        self.note_skipped();
        let received = self.received_rows();
        self.batches.clear();
        self.first_result = received;
        self.rows = 0;
        self.skip_until = start.max(received);
        self.target_result = self.skip_until.saturating_add(parquet_reader::PAGE_ROWS);
        if let Some(execution) = &mut self.execution {
            execution.skip_rows(self.skip_until - received);
        }
    }

    /// Counts the rows the execution dropped for a jump ahead: they come right
    /// after the rows received, so the window starts after them.
    pub(crate) fn note_skipped(&mut self) {
        let Some(execution) = &mut self.execution else {
            return;
        };
        let skipped = execution.take_skipped_rows();
        if skipped == 0 {
            return;
        }
        let received = self.received_rows();
        self.batches.clear();
        self.rows = 0;
        self.first_result = received.saturating_add(skipped);
    }

    /// Starts reading the result again with `execution`, whose first row is
    /// result row `base`, dropping rows until result row `start`.
    pub(crate) fn restart(&mut self, execution: QueryExecution, base: u64, start: u64) {
        self.cancel_execution();
        self.execution = Some(execution);
        self.batches.clear();
        self.first_result = base;
        self.rows = 0;
        self.skip_until = start.max(base);
        self.target_result = self.skip_until.saturating_add(parquet_reader::PAGE_ROWS);
        self.finished = false;
        self.error = None;
        if let Some(execution) = &mut self.execution {
            execution.skip_rows(self.skip_until - base);
        }
    }

    pub(crate) fn received_rows(&self) -> u64 {
        self.first_result.saturating_add(self.rows)
    }

    pub(crate) fn needs_more(&self) -> bool {
        !self.finished && self.received_rows() < self.target_result
    }

    pub(crate) fn request_more(&mut self) {
        self.target_result = self.target_result.max(
            self.received_rows()
                .saturating_add(parquet_reader::PAGE_ROWS),
        );
    }

    pub(crate) fn push(&mut self, batch: arrow_array::RecordBatch) {
        // Rows before a jump target are dropped as they arrive.
        let start = self.received_rows();
        let rows = batch.num_rows() as u64;
        let batch = if start < self.skip_until {
            let drop = (self.skip_until - start).min(rows);
            self.first_result = self.first_result.saturating_add(drop);
            if drop == rows {
                return;
            }
            batch.slice(drop as usize, (rows - drop) as usize)
        } else {
            batch
        };
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        self.batches.push_back(batch);
        while self.batches.len() > MAX_FILTERED_BATCHES {
            if let Some(batch) = self.batches.pop_front() {
                let rows = batch.num_rows() as u64;
                self.first_result = self.first_result.saturating_add(rows);
                self.rows = self.rows.saturating_sub(rows);
            }
        }
    }

    pub(crate) fn cancel(&mut self) {
        self.cancel_execution();
        if let Some(count) = &self.count {
            count.cancel();
        }
    }

    fn cancel_execution(&mut self) {
        if let Some(execution) = &mut self.execution {
            execution.cancel();
        }
    }

    pub(crate) fn refresh_display_columns(&mut self, layout: &ColumnLayout) {
        self.display_columns = if self.aggregate {
            (0..self.projected_columns.len())
                .map(|column| (column, column))
                .collect()
        } else {
            layout
                .visible()
                .iter()
                .filter_map(|source| {
                    self.projected_columns
                        .iter()
                        .position(|column| column == source)
                        .map(|batch| (*source, batch))
                })
                .collect()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_where_each_result_row_starts_in_the_file() {
        let row_groups = [0_u64, 10, 20, 30]
            .iter()
            .enumerate()
            .map(|(index, first_row)| RowGroupInfo {
                index,
                first_row: *first_row,
                row_count: 10,
            })
            .collect::<Vec<_>>();
        // Matches per group: 5, none, 5, 2.
        let count = MatchCount {
            work: None,
            prefix: Some(prefix_sums(&[5, 0, 5, 2])),
            total: Some(12),
            error: None,
        };
        assert_eq!(count.total(), Some(12));
        assert_eq!(count.entry_for(0, &row_groups), Some((0, 0)));
        assert_eq!(count.entry_for(4, &row_groups), Some((0, 0)));
        // Row 5 is the first match of group 2; the empty group 1 is passed over.
        assert_eq!(count.entry_for(5, &row_groups), Some((20, 5)));
        assert_eq!(count.entry_for(11, &row_groups), Some((30, 10)));
        // Past the end it still names the last group; the reader finds nothing.
        assert_eq!(count.entry_for(99, &row_groups), Some((30, 10)));
        assert_eq!(MatchCount::known(7).entry_for(0, &row_groups), None);
        assert_eq!(MatchCount::known(7).total(), Some(7));
    }
}
