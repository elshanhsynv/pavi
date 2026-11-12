use std::sync::mpsc::{Receiver, RecvError, TryRecvError};

use crate::{CancellationToken, OpenResponse, PageResponse, ScanProgress};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GenerationId(pub u64);

/// The caller-owned control and response endpoint for a submitted page read.
/// Its task and generation IDs arrive with the response.
pub struct PageTask {
    cancellation: CancellationToken,
    progress: ScanProgress,
    response: Receiver<PageResponse>,
}

/// The caller-owned control and response endpoint for a background source open.
/// Its task and generation IDs arrive with the response.
pub struct OpenTask {
    cancellation: CancellationToken,
    progress: ScanProgress,
    response: Receiver<OpenResponse>,
}

impl PageTask {
    pub(crate) fn new(
        cancellation: CancellationToken,
        progress: ScanProgress,
        response: Receiver<PageResponse>,
    ) -> Self {
        Self {
            cancellation,
            progress,
            response,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// How far this read has scanned; see [`ScanProgress::scanned_rows`].
    pub fn progress(&self) -> ScanProgress {
        self.progress.clone()
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn recv(self) -> Result<PageResponse, RecvError> {
        self.response.recv()
    }

    pub fn try_recv(&self) -> Result<PageResponse, TryRecvError> {
        self.response.try_recv()
    }
}

impl OpenTask {
    pub(crate) fn new(
        cancellation: CancellationToken,
        progress: ScanProgress,
        response: Receiver<OpenResponse>,
    ) -> Self {
        Self {
            cancellation,
            progress,
            response,
        }
    }

    /// How much of the open is done, from 0 to 1: the files of a folder opened,
    /// or the share of a CSV, JSON or Arrow file read. 0 for a Parquet file.
    pub fn progress(&self) -> f32 {
        (self.progress.scanned_rows() as f64 / crate::scheduler::OPEN_PROGRESS_SCALE).min(1.0)
            as f32
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn recv(self) -> Result<OpenResponse, RecvError> {
        self.response.recv()
    }

    pub fn try_recv(&self) -> Result<OpenResponse, TryRecvError> {
        self.response.try_recv()
    }
}
