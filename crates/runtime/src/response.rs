use anyhow::Error;
use parquet_reader::DataPage;

use crate::{GenerationId, TaskId};

/// The terminal outcome of one requested Parquet page.
#[derive(Debug)]
pub enum PageOutcome {
    Loaded(DataPage),
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

impl PageResponse {
    pub fn is_stale_for(&self, current_generation: GenerationId) -> bool {
        self.generation_id != current_generation
    }
}
