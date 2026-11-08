use std::sync::mpsc::{Receiver, RecvError, TryRecvError};

use crate::{CancellationToken, OpenResponse, PageResponse};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GenerationId(pub u64);

/// The caller-owned control and response endpoint for a submitted page read.
/// Its task and generation IDs arrive with the response.
pub struct PageTask {
    cancellation: CancellationToken,
    response: Receiver<PageResponse>,
}

/// The caller-owned control and response endpoint for a background source open.
/// Its task and generation IDs arrive with the response.
pub struct OpenTask {
    cancellation: CancellationToken,
    response: Receiver<OpenResponse>,
}

impl PageTask {
    pub(crate) fn new(cancellation: CancellationToken, response: Receiver<PageResponse>) -> Self {
        Self {
            cancellation,
            response,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
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
    pub(crate) fn new(cancellation: CancellationToken, response: Receiver<OpenResponse>) -> Self {
        Self {
            cancellation,
            response,
        }
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
