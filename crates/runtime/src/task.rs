use std::sync::mpsc::{Receiver, RecvError, TryRecvError};

use crate::{CancellationToken, PageResponse};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GenerationId(pub u64);

/// The caller-owned control and response endpoint for a submitted page read.
pub struct PageTask {
    id: TaskId,
    generation_id: GenerationId,
    cancellation: CancellationToken,
    response: Receiver<PageResponse>,
}

impl PageTask {
    pub(crate) fn new(
        id: TaskId,
        generation_id: GenerationId,
        cancellation: CancellationToken,
        response: Receiver<PageResponse>,
    ) -> Self {
        Self {
            id,
            generation_id,
            cancellation,
            response,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn generation_id(&self) -> GenerationId {
        self.generation_id
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
