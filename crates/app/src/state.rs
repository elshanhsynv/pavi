use std::collections::BTreeSet;

use parquet_reader::PAGE_ROWS;
use pavi_runtime::GenerationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoadState {
    Idle,
    Opening,
    Ready,
    Error(String),
}

pub struct GridState {
    pub generation: GenerationId,
    pub rows: u64,
    pub columns: usize,
    pub loading: LoadState,
    pub requested: BTreeSet<u64>,
    pub loaded: BTreeSet<u64>,
    pub selection: Option<(u64, usize)>,
}

impl Default for GridState {
    fn default() -> Self {
        Self {
            generation: GenerationId(0),
            rows: 0,
            columns: 0,
            loading: LoadState::Idle,
            requested: BTreeSet::new(),
            loaded: BTreeSet::new(),
            selection: None,
        }
    }
}

impl GridState {
    pub fn reset(&mut self) -> GenerationId {
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.rows = 0;
        self.columns = 0;
        self.loading = LoadState::Opening;
        self.requested.clear();
        self.loaded.clear();
        self.selection = None;
        self.generation
    }

    pub fn ready(&mut self, rows: u64, columns: usize) {
        self.rows = rows;
        self.columns = columns;
        self.loading = LoadState::Ready;
    }

    pub fn page_for_row(&self, row: u64) -> Option<u64> {
        (row < self.rows).then_some(row / PAGE_ROWS)
    }

    pub fn visible_pages(&self, first_row: u64, last_row: u64) -> Vec<u64> {
        let Some(first) = self.page_for_row(first_row) else {
            return Vec::new();
        };
        let last = self
            .page_for_row(last_row.min(self.rows.saturating_sub(1)))
            .unwrap_or(first);
        (first..=last).collect()
    }

    pub fn request_page(&mut self, page: u64) -> bool {
        !self.loaded.contains(&page) && self.requested.insert(page)
    }

    pub fn accept_page(&mut self, generation: GenerationId, page: u64) -> bool {
        if generation != self.generation {
            return false;
        }
        self.requested.remove(&page);
        self.loaded.insert(page);
        true
    }

    pub fn select(&mut self, row: u64, column: usize) -> bool {
        if row >= self.rows || column >= self.columns {
            return false;
        }
        self.selection = Some((row, column));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_visible_rows_to_logical_pages() {
        let mut state = GridState::default();
        state.ready(PAGE_ROWS * 3 + 2, 2);

        assert_eq!(state.visible_pages(4_095, 4_097), vec![0, 1]);
        assert_eq!(
            state.visible_pages(PAGE_ROWS * 3, PAGE_ROWS * 3 + 1),
            vec![3]
        );
    }

    #[test]
    fn deduplicates_page_requests_and_rejects_stale_results() {
        let mut state = GridState::default();
        let generation = state.reset();
        state.ready(10, 1);

        assert!(state.request_page(0));
        assert!(!state.request_page(0));
        assert!(!state.accept_page(GenerationId(generation.0 + 1), 0));
        assert!(state.accept_page(generation, 0));
        assert!(!state.request_page(0));
    }

    #[test]
    fn reset_clears_selection_and_loading_data() {
        let mut state = GridState::default();
        state.ready(3, 2);
        state.request_page(0);
        state.select(1, 1);

        state.reset();
        assert_eq!(state.loading, LoadState::Opening);
        assert!(state.selection.is_none());
        assert!(state.requested.is_empty());
        assert!(state.loaded.is_empty());
    }

    #[test]
    fn handles_zero_rows_and_selection_bounds() {
        let mut state = GridState::default();
        state.ready(0, 4);
        assert!(state.visible_pages(0, 10).is_empty());
        assert!(!state.select(0, 0));

        state.ready(2, 2);
        assert!(state.select(1, 1));
        assert!(!state.select(2, 1));
        assert!(!state.select(1, 2));
    }

    #[test]
    fn records_loading_and_error_states() {
        let mut state = GridState::default();
        state.reset();
        assert_eq!(state.loading, LoadState::Opening);
        state.loading = LoadState::Error("bad file".to_string());
        assert!(matches!(state.loading, LoadState::Error(_)));
    }
}
