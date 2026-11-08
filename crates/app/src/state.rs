use std::collections::BTreeSet;

use parquet_reader::PAGE_ROWS;
use pavi_runtime::GenerationId;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Workspace {
    #[default]
    Grid,
    Sql,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolbarCommand {
    Refresh,
    Filter,
    Sql,
    Charts,
    Export,
    Inspector,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ToolbarAvailability {
    pub has_dataset: bool,
    pub opening: bool,
}

impl ToolbarAvailability {
    pub fn enabled(self, command: ToolbarCommand) -> bool {
        match command {
            ToolbarCommand::Inspector => true,
            ToolbarCommand::Refresh => self.has_dataset && !self.opening,
            ToolbarCommand::Filter
            | ToolbarCommand::Sql
            | ToolbarCommand::Charts
            | ToolbarCommand::Export => self.has_dataset,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoadState {
    Idle,
    Opening,
    Ready,
    Error(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SelectionRange {
    pub anchor: (u64, usize),
    pub focus: (u64, usize),
}

impl SelectionRange {
    pub fn contains(self, row: u64, column: usize) -> bool {
        let (first_row, last_row) = if self.anchor.0 <= self.focus.0 {
            (self.anchor.0, self.focus.0)
        } else {
            (self.focus.0, self.anchor.0)
        };
        let (first_column, last_column) = if self.anchor.1 <= self.focus.1 {
            (self.anchor.1, self.focus.1)
        } else {
            (self.focus.1, self.anchor.1)
        };
        (first_row..=last_row).contains(&row) && (first_column..=last_column).contains(&column)
    }
}

/// Compact presentation-only mapping from display columns to source columns.
pub struct ColumnLayout {
    order: Vec<usize>,
    hidden: BTreeSet<usize>,
    widths: Vec<f32>,
    visible: Vec<usize>,
}

impl ColumnLayout {
    pub fn new(columns: usize, width: f32) -> Self {
        let mut layout = Self {
            order: (0..columns).collect(),
            hidden: BTreeSet::new(),
            widths: vec![width; columns],
            visible: Vec::new(),
        };
        layout.rebuild_visible();
        layout
    }

    pub fn visible(&self) -> &[usize] {
        &self.visible
    }

    pub fn display_for_source(&self, source: usize) -> Option<usize> {
        self.visible.iter().position(|column| *column == source)
    }

    pub fn is_hidden(&self, source: usize) -> bool {
        self.hidden.contains(&source)
    }

    pub fn set_hidden(&mut self, source: usize, hidden: bool) {
        if source >= self.order.len() {
            return;
        }
        if hidden {
            self.hidden.insert(source);
        } else {
            self.hidden.remove(&source);
        }
        self.rebuild_visible();
    }

    pub fn move_source(&mut self, source: usize, direction: isize) -> bool {
        let Some(visible_index) = self.display_for_source(source) else {
            return false;
        };
        let target = visible_index.saturating_add_signed(direction);
        let Some(other) = self.visible.get(target).copied() else {
            return false;
        };
        let Some(first) = self.order.iter().position(|column| *column == source) else {
            return false;
        };
        let Some(second) = self.order.iter().position(|column| *column == other) else {
            return false;
        };
        self.order.swap(first, second);
        self.rebuild_visible();
        true
    }

    pub fn reset(&mut self, width: f32) {
        *self = Self::new(self.order.len(), width);
    }

    pub fn width(&self, source: usize, fallback: f32) -> f32 {
        self.widths.get(source).copied().unwrap_or(fallback)
    }

    pub fn set_width(&mut self, source: usize, width: f32) {
        if let Some(stored) = self.widths.get_mut(source) {
            *stored = width.clamp(72.0, 480.0);
        }
    }

    pub fn order(&self) -> &[usize] {
        &self.order
    }

    pub fn hidden(&self) -> &BTreeSet<usize> {
        &self.hidden
    }

    pub fn restore(&mut self, order: Vec<usize>, hidden: BTreeSet<usize>, widths: Vec<f32>) {
        if order.len() != self.order.len()
            || widths.len() != self.widths.len()
            || order.iter().copied().collect::<BTreeSet<_>>().len() != order.len()
            || order.iter().any(|column| *column >= self.order.len())
        {
            return;
        }
        self.order = order;
        self.hidden = hidden
            .into_iter()
            .filter(|column| *column < self.order.len())
            .collect();
        self.widths = widths
            .into_iter()
            .map(|width| width.clamp(72.0, 480.0))
            .collect();
        self.rebuild_visible();
    }

    fn rebuild_visible(&mut self) {
        self.visible = self
            .order
            .iter()
            .copied()
            .filter(|column| !self.hidden.contains(column))
            .collect();
    }
}

/// Most rows the source grid's table holds at once. At 28 px per row this is
/// 14 million points, small enough for egui's `f32` layout to place every row
/// exactly. Larger files are shown through a window that moves with the view.
pub const GRID_WINDOW_ROWS: u64 = 500_000;

pub struct GridState {
    pub generation: GenerationId,
    /// Absolute row shown at the top of the table's window (0 unless windowed).
    pub window_base: u64,
    /// Absolute index of the first row in the grid; non-zero once a bounded
    /// result window has slid forward. Selections use absolute row indexes.
    pub first_row: u64,
    pub rows: u64,
    pub columns: usize,
    pub loading: LoadState,
    pub requested: BTreeSet<u64>,
    pub loaded: BTreeSet<u64>,
    pub selection: Option<(u64, usize)>,
    pub selection_range: Option<SelectionRange>,
}

impl Default for GridState {
    fn default() -> Self {
        Self {
            generation: GenerationId(0),
            window_base: 0,
            first_row: 0,
            rows: 0,
            columns: 0,
            loading: LoadState::Idle,
            requested: BTreeSet::new(),
            loaded: BTreeSet::new(),
            selection: None,
            selection_range: None,
        }
    }
}

impl GridState {
    pub fn reset(&mut self) -> GenerationId {
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.window_base = 0;
        self.first_row = 0;
        self.rows = 0;
        self.columns = 0;
        self.loading = LoadState::Opening;
        self.requested.clear();
        self.loaded.clear();
        self.selection = None;
        self.selection_range = None;
        self.generation
    }

    pub fn ready(&mut self, rows: u64, columns: usize) {
        self.rows = rows;
        self.columns = columns;
        self.loading = LoadState::Ready;
    }

    /// Whether the rows don't fit in one table window.
    pub fn is_windowed(&self) -> bool {
        self.rows > GRID_WINDOW_ROWS
    }

    /// Rows in the table: all of them, or one window.
    pub fn window_rows(&self) -> u64 {
        self.rows.min(GRID_WINDOW_ROWS)
    }

    /// Window start that puts `row` in the middle of the window (clamped to the data).
    pub fn window_base_for(&self, row: u64) -> u64 {
        if !self.is_windowed() {
            return 0;
        }
        row.saturating_sub(GRID_WINDOW_ROWS / 2)
            .min(self.rows - GRID_WINDOW_ROWS)
    }

    /// A new window start when the drawn rows `first..=last` come within a quarter
    /// window of an edge that isn't the start or end of the data; `None` otherwise.
    pub fn recentered_base(&self, first: u64, last: u64) -> Option<u64> {
        if !self.is_windowed() {
            return None;
        }
        let margin = GRID_WINDOW_ROWS / 4;
        let window_end = self.window_base + GRID_WINDOW_ROWS;
        let near_top = self.window_base > 0 && first < self.window_base + margin;
        let near_bottom = window_end < self.rows && last + margin >= window_end;
        if !near_top && !near_bottom {
            return None;
        }
        let base = self.window_base_for(first);
        (base != self.window_base).then_some(base)
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

    fn contains_row(&self, row: u64) -> bool {
        row.checked_sub(self.first_row)
            .is_some_and(|offset| offset < self.rows)
    }

    pub fn select(&mut self, row: u64, column: usize) -> bool {
        if !self.contains_row(row) || column >= self.columns {
            return false;
        }
        self.selection = Some((row, column));
        self.selection_range = Some(SelectionRange {
            anchor: (row, column),
            focus: (row, column),
        });
        true
    }

    pub fn extend_selection(&mut self, row: u64, column: usize) -> bool {
        if !self.contains_row(row) || column >= self.columns {
            return false;
        }
        let anchor = self
            .selection_range
            .map_or((row, column), |range| range.anchor);
        self.selection = Some((row, column));
        self.selection_range = Some(SelectionRange {
            anchor,
            focus: (row, column),
        });
        true
    }

    pub fn is_selected(&self, row: u64, column: usize) -> bool {
        self.selection_range
            .is_some_and(|range| range.contains(row, column))
    }

    pub fn move_selection(&mut self, row_delta: i64, column_delta: i64, extend: bool) -> bool {
        let Some((row, column)) = self.selection else {
            return self.select(0, 0);
        };
        let next_row = row
            .saturating_add_signed(row_delta)
            .min(self.rows.saturating_sub(1));
        let next_column = column
            .saturating_add_signed(column_delta as isize)
            .min(self.columns.saturating_sub(1));
        if extend {
            self.extend_selection(next_row, next_column)
        } else {
            self.select(next_row, next_column)
        }
    }

    pub fn jump_to_row(&mut self, row: u64) -> bool {
        let column = self.selection.map_or(0, |(_, column)| column);
        self.select(row.min(self.rows.saturating_sub(1)), column)
    }

    pub fn clear_pages(&mut self) {
        self.requested.clear();
        self.loaded.clear();
    }

    pub fn select_filtered(&mut self, first_result: u64, row: u64, column: usize) -> bool {
        if row >= self.rows || column >= self.columns {
            return false;
        }
        self.selection = Some((first_result.saturating_add(row), column));
        self.selection_range = Some(SelectionRange {
            anchor: (first_result.saturating_add(row), column),
            focus: (first_result.saturating_add(row), column),
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_large_row_counts_and_recenters_near_inner_edges() {
        let mut grid = GridState::default();
        grid.ready(GRID_WINDOW_ROWS, 2);
        assert!(!grid.is_windowed());
        assert_eq!(grid.window_base_for(400_000), 0);
        assert_eq!(grid.recentered_base(450_000, 450_030), None);

        grid.ready(1_000_000_000, 2);
        assert!(grid.is_windowed());
        assert_eq!(grid.window_rows(), GRID_WINDOW_ROWS);
        assert_eq!(grid.window_base_for(10), 0);
        assert_eq!(grid.window_base_for(600_000_000), 599_750_000);
        assert_eq!(
            grid.window_base_for(999_999_999),
            1_000_000_000 - GRID_WINDOW_ROWS
        );

        // At the start of the data nothing moves; near the window's lower quarter it does.
        assert_eq!(grid.recentered_base(0, 30), None);
        assert_eq!(grid.recentered_base(374_000, 374_030), None);
        assert_eq!(grid.recentered_base(380_000, 380_030), Some(130_000));
        grid.window_base = 130_000;
        assert_eq!(grid.recentered_base(380_000, 380_030), None);
        // Near the top edge of a window that doesn't start at row 0.
        assert_eq!(grid.recentered_base(140_000, 140_030), Some(0));

        // At the end of the data the window stays put.
        grid.window_base = 1_000_000_000 - GRID_WINDOW_ROWS;
        assert_eq!(grid.recentered_base(999_999_970, 999_999_999), None);

        grid.reset();
        assert_eq!(grid.window_base, 0);
    }

    #[test]
    fn selects_absolute_rows_inside_a_slid_window() {
        let mut grid = GridState::default();
        grid.ready(100, 3);
        grid.first_row = 40_960;

        assert!(grid.select_filtered(40_960, 10, 1));
        assert_eq!(grid.selection, Some((40_970, 1)));
        assert!(grid.select(40_971, 1));
        assert!(grid.extend_selection(41_000, 2));
        assert_eq!(grid.selection, Some((41_000, 2)));
        assert!(grid.is_selected(40_980, 1));

        assert!(!grid.select(40_959, 0), "row before the window");
        assert!(!grid.select(41_060, 0), "row after the window");
        assert!(
            !grid.extend_selection(10, 0),
            "relative index is not absolute"
        );

        grid.reset();
        assert_eq!(grid.first_row, 0);
    }

    #[test]
    fn tracks_workspace_and_toolbar_availability() {
        assert_eq!(Workspace::default(), Workspace::Grid);
        assert_ne!(Workspace::Grid, Workspace::Sql);

        let closed = ToolbarAvailability::default();
        assert!(closed.enabled(ToolbarCommand::Inspector));
        assert!(!closed.enabled(ToolbarCommand::Refresh));
        assert!(!closed.enabled(ToolbarCommand::Filter));

        let ready = ToolbarAvailability {
            has_dataset: true,
            opening: false,
        };
        assert!(ready.enabled(ToolbarCommand::Refresh));
        assert!(ready.enabled(ToolbarCommand::Sql));
        assert!(ready.enabled(ToolbarCommand::Export));
        assert!(
            !ToolbarAvailability {
                opening: true,
                ..ready
            }
            .enabled(ToolbarCommand::Refresh)
        );
    }

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
        assert!(state.selection_range.is_none());
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
    fn stores_filtered_selection_as_a_result_row() {
        let mut state = GridState::default();
        state.ready(2, 2);

        assert!(state.select_filtered(4_096, 1, 1));
        assert_eq!(state.selection, Some((4_097, 1)));
    }

    #[test]
    fn maps_layout_hiding_reordering_and_reset_without_batch_state() {
        let mut layout = ColumnLayout::new(4, 130.0);
        layout.set_hidden(1, true);
        layout.move_source(3, -1);
        layout.set_width(3, 320.0);
        assert_eq!(layout.visible(), &[0, 3, 2]);
        assert_eq!(layout.display_for_source(3), Some(1));
        assert_eq!(layout.width(3, 0.0), 320.0);

        layout.reset(130.0);
        assert_eq!(layout.visible(), &[0, 1, 2, 3]);
        assert_eq!(layout.width(3, 0.0), 130.0);
    }

    #[test]
    fn supports_ranges_keyboard_movement_and_row_jumps() {
        let mut state = GridState::default();
        state.ready(10, 4);
        assert!(state.select(2, 1));
        assert!(state.extend_selection(4, 3));
        assert!(state.is_selected(3, 2));
        assert!(!state.is_selected(5, 2));
        assert!(state.move_selection(1, -1, false));
        assert_eq!(state.selection, Some((5, 2)));
        assert!(state.jump_to_row(99));
        assert_eq!(state.selection, Some((9, 2)));
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
