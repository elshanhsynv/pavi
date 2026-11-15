//! The bounded window of filter, SQL and sort results shown in the grid.

use std::collections::VecDeque;

use parquet_reader::{RowGroupInfo, SortSpec};
use pavi_query::{LogicalPlan, QueryExecution};
use pavi_runtime::{PageOutcome, PageTask};

use crate::state::ColumnLayout;

/// Result batches kept for the grid; older batches are dropped as new ones arrive.
pub(crate) const MAX_FILTERED_BATCHES: usize = 8;

/// Result rows read above a row jumped to, so the rows just before it can be
/// scrolled to without another jump.
pub(crate) const JUMP_CONTEXT_ROWS: u64 = parquet_reader::PAGE_ROWS / 2;

/// The background count of a filter's matches in each row group (ADR 0012).
/// It gives the result's size, and where in the file each result row is, so
/// the grid can start reading anywhere in the result.
pub(crate) struct MatchCount {
    task: Option<PageTask>,
    /// `prefix[g]` is the number of matches before row group `g`; the last
    /// entry is the total. One entry per row group, plus one.
    prefix: Option<Vec<u64>>,
    total: Option<u64>,
    pub(crate) error: Option<String>,
}

impl MatchCount {
    pub(crate) fn new(task: PageTask) -> Self {
        Self {
            task: Some(task),
            prefix: None,
            total: None,
            error: None,
        }
    }

    /// A size known without counting (a sort of the whole file), with no
    /// positions to jump with.
    pub(crate) fn known(total: u64) -> Self {
        Self {
            task: None,
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
        self.task.is_some()
    }

    /// Source rows the count has passed while it runs.
    pub(crate) fn scanned_rows(&self) -> Option<u64> {
        self.task
            .as_ref()
            .map(|task| task.progress().scanned_rows())
    }

    /// Takes the count when it has arrived.
    pub(crate) fn poll(&mut self) {
        let Some(task) = &self.task else {
            return;
        };
        let outcome = match task.try_recv() {
            Ok(response) => response.outcome,
            Err(std::sync::mpsc::TryRecvError::Empty) => return,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                self.task = None;
                self.error = Some("the count stopped without a result".to_owned());
                return;
            }
        };
        self.task = None;
        match outcome {
            PageOutcome::Counted(counts) => {
                let prefix = prefix_sums(&counts);
                self.total = prefix.last().copied();
                self.prefix = Some(prefix);
            }
            PageOutcome::Cancelled => self.error = Some("the count was cancelled".to_owned()),
            PageOutcome::ReadFailed(error) => self.error = Some(format!("{error:#}")),
            _ => self.error = Some("the count returned something else".to_owned()),
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
        if let Some(task) = &self.task {
            task.cancel();
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
    pub(crate) fn skip_forward(&mut self, start: u64) {
        let received = self.received_rows();
        self.batches.clear();
        self.first_result = received;
        self.rows = 0;
        self.skip_until = start.max(received);
        self.target_result = self.skip_until.saturating_add(parquet_reader::PAGE_ROWS);
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
            task: None,
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
