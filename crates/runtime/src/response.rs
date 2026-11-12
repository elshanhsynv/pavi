use anyhow::Error;
use arrow_array::RecordBatch;
use parquet_reader::{DataPage, FilteredWindow, ParquetSource, SortMerge};
use std::sync::{Arc, Mutex};

use crate::{GenerationId, TaskId};

/// The terminal outcome of one requested Parquet page.
#[derive(Debug)]
pub enum PageOutcome {
    Loaded(DataPage),
    Batch(RecordBatch),
    Batches(Vec<RecordBatch>),
    /// Filtered rows plus the source row to resume scanning from.
    Filtered(FilteredWindow),
    /// A sort too large for memory spilled to disk; read its rows with
    /// `RuntimeHandle::submit_sort_merge`.
    SortSpilled(Arc<Mutex<SortMerge>>),
    /// Rows from a spilled sort, and whether it has handed out every row.
    Merged {
        batches: Vec<RecordBatch>,
        finished: bool,
    },
    Cancelled,
    ReadFailed(Error),
}

/// A page-read outcome tagged with the request and UI generation that created it.
#[derive(Debug)]
pub struct PageResponse {
    pub task_id: TaskId,
    pub generation_id: GenerationId,
    pub outcome: PageOutcome,
}

pub enum OpenOutcome {
    Opened(Arc<ParquetSource>),
    Cancelled,
    OpenFailed(Error),
}

pub struct OpenResponse {
    pub task_id: TaskId,
    pub generation_id: GenerationId,
    pub outcome: OpenOutcome,
}
