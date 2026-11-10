use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// A cooperative cancellation flag shared by a task and its worker.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// How far a long scan has got, shared by a task and its worker. The worker
/// writes it between batches; the caller reads it whenever it likes.
#[derive(Clone, Debug, Default)]
pub struct ScanProgress(Arc<AtomicU64>);

impl ScanProgress {
    pub(crate) fn set_scanned_rows(&self, rows: u64) {
        self.0.store(rows, Ordering::Relaxed);
    }

    /// Source rows scanned so far: a position in the file, so row groups skipped
    /// by statistics count. Zero until the scan decodes its first batch, and for
    /// reads that are not scans (pages and windows).
    pub fn scanned_rows(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
