//! Bounded background page reads for PAVI.
//!
//! `Runtime` owns a fixed set of threads. Each submitted page read has one
//! bounded response channel, so neither queued requests nor undelivered
//! responses accumulate inside the runtime.

mod cancellation;
mod response;
mod scheduler;
mod task;

pub use cancellation::CancellationToken;
pub use response::{OpenOutcome, OpenResponse, PageOutcome, PageResponse};
pub use scheduler::{Runtime, RuntimeConfig, RuntimeConfigError, SubmitError};
pub use task::{GenerationId, OpenTask, PageTask, TaskId};
