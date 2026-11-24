//! Noticing that the open tab's file changed on disk (ADR 0018).
//!
//! Every [`CHANGE_CHECK_INTERVAL`] the open tab asks a worker whether its file or
//! folder changed (`ParquetSource::check_changed`). Once one has, a bar above
//! the grid says so and offers Reload, and checks stop until the file is opened
//! again. Reload keeps the tab's filter and sort.

use super::*;
use pavi_runtime::{PageOutcome, PageTask};

/// Time between two checks of the open tab's file.
pub(crate) const CHANGE_CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// How soon a check in flight is looked at again; it takes a `stat` per file.
const CHECK_POLL: Duration = Duration::from_millis(100);

/// A tab's checks for changes on disk.
#[derive(Default)]
pub(crate) struct FileWatch {
    /// The check in flight, and the source it checks.
    task: Option<(PageTask, Arc<ParquetSource>)>,
    /// When the last check was sent.
    last: Option<Instant>,
    /// What changed, once a check found it.
    pub(crate) changed: Option<String>,
}

impl FileWatch {
    /// No check is in flight.
    #[cfg(test)]
    pub(crate) fn is_idle(&self) -> bool {
        self.task.is_none()
    }
}

impl PaviApp {
    /// Picks up the open tab's change check and sends the next one when it is
    /// due. Returns how long until the next one, so the frame loop can wake for it.
    pub(super) fn poll_file_watch(&mut self, now: Instant) -> Option<Duration> {
        let source = self
            .dataset
            .as_ref()
            .map(|dataset| Arc::clone(&dataset.source))?;
        if let Some((task, checked)) = &self.file_watch.task {
            let response = match task.try_recv() {
                Ok(response) => Some(response),
                Err(std::sync::mpsc::TryRecvError::Empty) => return Some(CHECK_POLL),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => None,
            };
            // A check of a file this tab no longer shows is dropped.
            let current = Arc::ptr_eq(checked, &source);
            self.file_watch.task = None;
            match response.map(|response| response.outcome) {
                Some(PageOutcome::Checked(Some(change))) if current => {
                    log::info!("{}", change.summary());
                    self.file_watch.changed = Some(change.summary().to_string());
                }
                Some(PageOutcome::ReadFailed(error)) => {
                    log::warn!("check for changes on disk: {error:#}");
                }
                _ => {}
            }
        }
        if self.file_watch.changed.is_some() {
            return None;
        }
        let due = self.file_watch.last.map_or(Duration::ZERO, |last| {
            CHANGE_CHECK_INTERVAL.saturating_sub(now.saturating_duration_since(last))
        });
        if !due.is_zero() {
            return Some(due);
        }
        let runtime = self.runtime.as_ref()?;
        // A full queue is tried again at the next interval.
        self.file_watch.last = Some(now);
        if let Ok(task) = runtime
            .handle()
            .submit_change_check(Arc::clone(&source), self.grid.generation)
        {
            self.file_watch.task = Some((task, source));
        }
        Some(CHANGE_CHECK_INTERVAL)
    }

    /// Opens the tab's file again, keeping its filter and sort.
    pub(super) fn reload_document(&mut self) {
        let Some(path) = self.dataset.as_ref().map(|dataset| dataset.path.clone()) else {
            return;
        };
        let view = self.active_tab_view();
        self.begin_open(path);
        self.pending_view = view;
    }
}
