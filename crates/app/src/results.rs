//! The bounded window of filter, SQL and sort results shown in the grid.

use std::collections::VecDeque;

use parquet_reader::SortSpec;
use pavi_query::{LogicalPlan, QueryExecution};

use crate::state::ColumnLayout;

/// Result batches kept for the grid; older batches are dropped as new ones arrive.
pub(crate) const MAX_FILTERED_BATCHES: usize = 8;

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
            Self::Sort => "Sorted",
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
    pub(crate) sort: Option<SortSpec>,
    pub(crate) aggregate: bool,
    pub(crate) projected_columns: Vec<usize>,
    pub(crate) display_columns: Vec<(usize, usize)>,
    pub(crate) execution: Option<QueryExecution>,
    pub(crate) batches: VecDeque<arrow_array::RecordBatch>,
    pub(crate) first_result: u64,
    pub(crate) rows: u64,
    pub(crate) target_result: u64,
    pub(crate) finished: bool,
    pub(crate) error: Option<String>,
}

impl FilteredGrid {
    pub(crate) fn new(
        kind: QueryKind,
        plan: LogicalPlan,
        sort: Option<SortSpec>,
        aggregate: bool,
        projected_columns: Vec<usize>,
        execution: QueryExecution,
    ) -> Self {
        Self {
            kind,
            plan,
            sort,
            aggregate,
            projected_columns,
            display_columns: Vec::new(),
            execution: Some(execution),
            batches: VecDeque::new(),
            first_result: 0,
            rows: 0,
            target_result: parquet_reader::PAGE_ROWS,
            finished: false,
            error: None,
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
