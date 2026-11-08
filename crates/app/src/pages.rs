//! Pages of the source grid: background reads in flight, the small cache of loaded
//! pages, and cancelling reads for pages that scrolled out of view.

use std::collections::{BTreeSet, HashMap, VecDeque};

use parquet_reader::DataPage;
use pavi_runtime::{PageOutcome, PageTask};

use crate::state::GridState;

/// Loaded pages kept for the source grid (least recently loaded is evicted first).
pub(crate) const MAX_LOADED_PAGES: usize = 8;

#[derive(Default)]
pub(crate) struct SourcePages {
    pending: HashMap<u64, PageTask>,
    loaded: HashMap<u64, DataPage>,
    /// Loaded page indexes, oldest first.
    order: VecDeque<u64>,
    /// Pages submitted during the current frame; never cancelled as off-screen.
    frame_requests: BTreeSet<u64>,
    /// First and last page the source grid drew this frame.
    visible_range: Option<(u64, u64)>,
}

impl SourcePages {
    /// Call once at the start of each frame.
    pub(crate) fn begin_frame(&mut self) {
        self.frame_requests.clear();
    }

    pub(crate) fn get(&self, page: u64) -> Option<&DataPage> {
        self.loaded.get(&page)
    }

    pub(crate) fn loaded(&self) -> impl Iterator<Item = &DataPage> {
        self.loaded.values()
    }

    pub(crate) fn loaded_count(&self) -> usize {
        self.loaded.len()
    }

    pub(crate) fn is_loading(&self) -> bool {
        !self.pending.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_pending(&self, page: u64) -> bool {
        self.pending.contains_key(&page)
    }

    /// Pages with a read in flight, in ascending order.
    #[cfg(test)]
    pub(crate) fn pending_pages(&self) -> Vec<u64> {
        let mut pages = self.pending.keys().copied().collect::<Vec<_>>();
        pages.sort_unstable();
        pages
    }

    /// Tracks a submitted read for `page`.
    pub(crate) fn track(&mut self, page: u64, task: PageTask) {
        self.pending.insert(page, task);
        self.frame_requests.insert(page);
    }

    /// Collects finished reads. Loaded pages for the current generation are cached
    /// (evicting the oldest past [`MAX_LOADED_PAGES`]); anything else just clears the
    /// request. Returns a status message when a read failed.
    pub(crate) fn poll(&mut self, grid: &mut GridState) -> Option<String> {
        let mut completed = Vec::new();
        for (&page, task) in &self.pending {
            match task.try_recv() {
                Ok(response) => completed.push((page, Ok(response))),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    completed.push((page, Err("background page worker stopped".to_string())))
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
        }

        let mut status = None;
        for (page_index, result) in completed {
            self.pending.remove(&page_index);
            match result {
                Ok(response) if response.generation_id != grid.generation => {}
                Ok(response) => match response.outcome {
                    PageOutcome::Loaded(page)
                        if grid.accept_page(response.generation_id, page_index) =>
                    {
                        self.insert(grid, page_index, page);
                    }
                    PageOutcome::Cancelled => {
                        grid.requested.remove(&page_index);
                    }
                    PageOutcome::ReadFailed(error) => {
                        grid.requested.remove(&page_index);
                        status = Some(format!("load page {page_index}: {error:#}"));
                    }
                    PageOutcome::Batch(_)
                    | PageOutcome::Batches(_)
                    | PageOutcome::Filtered(_)
                    | PageOutcome::Loaded(_) => {
                        grid.requested.remove(&page_index);
                        status = Some(format!("unexpected response for page {page_index}"));
                    }
                },
                Err(error) => {
                    grid.requested.remove(&page_index);
                    status = Some(error);
                }
            }
        }
        status
    }

    fn insert(&mut self, grid: &mut GridState, page_index: u64, page: DataPage) {
        self.loaded.insert(page_index, page);
        self.order.retain(|index| *index != page_index);
        self.order.push_back(page_index);
        while self.order.len() > MAX_LOADED_PAGES {
            if let Some(oldest) = self.order.pop_front() {
                self.loaded.remove(&oldest);
                grid.loaded.remove(&oldest);
            }
        }
    }

    /// Records a page the source grid drew this frame.
    pub(crate) fn note_visible(&mut self, page: u64) {
        self.visible_range = Some(match self.visible_range {
            Some((first, last)) => (first.min(page), last.max(page)),
            None => (page, page),
        });
    }

    /// Forgets this frame's drawn pages (call before drawing the grid).
    pub(crate) fn clear_visible(&mut self) {
        self.visible_range = None;
    }

    /// Cancels pending reads more than one page away from what the grid drew this
    /// frame, so fast scrolling doesn't fill the runtime queue with pages that are no
    /// longer needed. Pages requested this frame (e.g. a jump target) are kept, and
    /// cancelled pages are requested again if they scroll back into view.
    pub(crate) fn cancel_offscreen(&mut self, grid: &mut GridState) {
        let Some((first, last)) = self.visible_range.take() else {
            return;
        };
        let keep = first.saturating_sub(1)..=last.saturating_add(1);
        let offscreen = self
            .pending
            .keys()
            .copied()
            .filter(|page| !keep.contains(page) && !self.frame_requests.contains(page))
            .collect::<Vec<_>>();
        for page in offscreen {
            if let Some(task) = self.pending.remove(&page) {
                task.cancel();
            }
            grid.requested.remove(&page);
        }
    }

    /// Cancels every read and drops every loaded page.
    pub(crate) fn clear(&mut self) {
        for task in self.pending.values() {
            task.cancel();
        }
        self.pending.clear();
        self.loaded.clear();
        self.order.clear();
    }
}
