//! Dataset tabs.
//!
//! The open tab's state lives in `PaviApp`'s own fields, so the rest of the app
//! works on one dataset at a time. Other tabs are parked as [`TabState`] and
//! swapped in when chosen. A parked tab keeps everything it had, results included;
//! its background reads finish into their own channels and are picked up when the
//! tab is shown again (each response has its own one-slot channel, so a worker
//! never waits on a parked tab).

use std::path::Path;

use super::*;
use crate::session::MAX_OPEN_FILES;

/// Tabs open at once. Each holds at most its bounded result window and footer
/// metadata, so this bounds what parked tabs keep in memory.
pub(crate) const MAX_TABS: usize = MAX_OPEN_FILES;

/// Everything that belongs to one tab: the dataset and what is done with it.
/// Each field mirrors the `PaviApp` field of the same name.
pub(super) struct TabState {
    tab_id: u64,
    grid: GridState,
    dataset: Option<Dataset>,
    opening: Option<OpenTask>,
    opening_path: Option<PathBuf>,
    pages: SourcePages,
    drawn_rows: Option<(u64, u64)>,
    grid_scroll_offset: Option<f32>,
    grid_scroll_x: Option<f32>,
    grid_view_x: (f32, f32),
    find: FindState,
    filtered: Option<FilteredGrid>,
    column_layout: ColumnLayout,
    column_search: String,
    jump_row_input: String,
    jump_target: Option<u64>,
    reveal_row: Option<u64>,
    export: ExportState,
    filter_input: String,
    filter_error: Option<String>,
    sql_input: String,
    sql_error: Option<String>,
    sql_completion: Option<SqlCompletion>,
    sql_replaced: Option<String>,
    chart: ChartState,
    profile: ProfileState,
    running_sql: Option<RunningSql>,
    show_safe_full_selection: bool,
    show_nested_selection: bool,
    workspace: Workspace,
    file_watch: watch::FileWatch,
    failed_path: Option<PathBuf>,
    pending_view: Option<TabView>,
}

impl TabState {
    /// An empty tab, as the app starts.
    pub(super) fn new(tab_id: u64, chart_visible: bool) -> Self {
        Self {
            tab_id,
            grid: GridState::default(),
            dataset: None,
            opening: None,
            opening_path: None,
            pages: SourcePages::default(),
            drawn_rows: None,
            grid_scroll_offset: None,
            grid_scroll_x: None,
            grid_view_x: (0.0, 0.0),
            find: FindState::default(),
            filtered: None,
            column_layout: ColumnLayout::new(0, INITIAL_COLUMN_WIDTH),
            column_search: String::new(),
            jump_row_input: String::new(),
            jump_target: None,
            reveal_row: None,
            export: ExportState::default(),
            filter_input: String::new(),
            filter_error: None,
            sql_input: String::new(),
            sql_error: None,
            sql_completion: None,
            sql_replaced: None,
            chart: ChartState {
                visible: chart_visible,
                ..ChartState::default()
            },
            profile: ProfileState::default(),
            running_sql: None,
            show_safe_full_selection: false,
            show_nested_selection: false,
            workspace: Workspace::default(),
            file_watch: watch::FileWatch::default(),
            failed_path: None,
            pending_view: None,
        }
    }

    fn path(&self) -> Option<&Path> {
        tab_path(self.dataset.as_ref(), self.opening_path.as_deref())
    }
}

/// The open tab's position and the parked tabs. `parked[active]` is always
/// `None`: that tab's state is in `PaviApp`.
pub(super) struct Tabs {
    pub(super) parked: Vec<Option<TabState>>,
    pub(super) active: usize,
    next_id: u64,
}

impl Default for Tabs {
    fn default() -> Self {
        Self {
            parked: vec![None],
            active: 0,
            // Tab 0 is the one the app starts with.
            next_id: 1,
        }
    }
}

/// One tab as the tab strip shows it.
pub(crate) struct TabLabel {
    /// Stays with the tab when tabs move, so a drag keeps hold of it.
    pub(crate) id: u64,
    pub(crate) title: String,
    /// The full path, for a tooltip.
    pub(crate) path: Option<PathBuf>,
    pub(crate) folder: bool,
    pub(crate) active: bool,
}

fn tab_path<'a>(dataset: Option<&'a Dataset>, opening: Option<&'a Path>) -> Option<&'a Path> {
    dataset.map(|dataset| dataset.path.as_path()).or(opening)
}

fn tab_title(path: Option<&Path>) -> String {
    path.map_or_else(
        || "New tab".to_string(),
        |path| {
            let path = parquet_reader::display_path(path);
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string())
        },
    )
}

/// A tab's filter and sort: from its result, or, while it is still opening,
/// the view waiting to be restored. `None` for neither, or a SQL result.
fn tab_view(
    path: PathBuf,
    dataset: Option<&Dataset>,
    filtered: Option<&FilteredGrid>,
    pending: Option<&TabView>,
) -> Option<TabView> {
    let view = match (dataset, filtered) {
        (Some(dataset), Some(filtered)) if filtered.kind != QueryKind::Sql => TabView {
            path,
            filter: filtered
                .filter
                .as_ref()
                .filter(|_| filtered.kind == QueryKind::Filter)
                .map(ToString::to_string)
                .unwrap_or_default(),
            sort: filtered
                .sort
                .iter()
                .filter_map(|key| {
                    Some(SavedSortKey {
                        column: dataset.column_names.get(key.column)?.clone(),
                        descending: key.direction == SortDirection::Descending,
                    })
                })
                .collect(),
        },
        (_, None) => pending.cloned()?,
        _ => return None,
    };
    (!view.is_empty()).then_some(view)
}

impl PaviApp {
    /// Swaps the open tab's state with `tab`.
    fn swap_tab(&mut self, tab: &mut TabState) {
        use std::mem::swap;
        swap(&mut self.tab_id, &mut tab.tab_id);
        swap(&mut self.grid, &mut tab.grid);
        swap(&mut self.dataset, &mut tab.dataset);
        swap(&mut self.opening, &mut tab.opening);
        swap(&mut self.opening_path, &mut tab.opening_path);
        swap(&mut self.pages, &mut tab.pages);
        swap(&mut self.drawn_rows, &mut tab.drawn_rows);
        swap(&mut self.grid_scroll_offset, &mut tab.grid_scroll_offset);
        swap(&mut self.grid_scroll_x, &mut tab.grid_scroll_x);
        swap(&mut self.grid_view_x, &mut tab.grid_view_x);
        swap(&mut self.find, &mut tab.find);
        swap(&mut self.filtered, &mut tab.filtered);
        swap(&mut self.column_layout, &mut tab.column_layout);
        swap(&mut self.column_search, &mut tab.column_search);
        swap(&mut self.jump_row_input, &mut tab.jump_row_input);
        swap(&mut self.jump_target, &mut tab.jump_target);
        swap(&mut self.reveal_row, &mut tab.reveal_row);
        swap(&mut self.export, &mut tab.export);
        swap(&mut self.filter_input, &mut tab.filter_input);
        swap(&mut self.filter_error, &mut tab.filter_error);
        swap(&mut self.sql_input, &mut tab.sql_input);
        swap(&mut self.sql_error, &mut tab.sql_error);
        swap(&mut self.sql_completion, &mut tab.sql_completion);
        swap(&mut self.sql_replaced, &mut tab.sql_replaced);
        swap(&mut self.chart, &mut tab.chart);
        swap(&mut self.profile, &mut tab.profile);
        swap(&mut self.running_sql, &mut tab.running_sql);
        swap(
            &mut self.show_safe_full_selection,
            &mut tab.show_safe_full_selection,
        );
        swap(
            &mut self.show_nested_selection,
            &mut tab.show_nested_selection,
        );
        swap(&mut self.workspace, &mut tab.workspace);
        swap(&mut self.file_watch, &mut tab.file_watch);
        swap(&mut self.failed_path, &mut tab.failed_path);
        swap(&mut self.pending_view, &mut tab.pending_view);
    }

    pub(super) fn tab_count(&self) -> usize {
        self.tabs.parked.len()
    }

    /// The file or folder shown (or being opened) in tab `index`.
    pub(super) fn tab_path_at(&self, index: usize) -> Option<&Path> {
        if index == self.tabs.active {
            tab_path(self.dataset.as_ref(), self.opening_path.as_deref())
        } else {
            self.tabs.parked.get(index)?.as_ref()?.path()
        }
    }

    pub(super) fn tab_labels(&self) -> Vec<TabLabel> {
        (0..self.tab_count())
            .map(|index| {
                let path = self.tab_path_at(index);
                let folder = if index == self.tabs.active {
                    self.dataset
                        .as_ref()
                        .is_some_and(|dataset| dataset.source.is_folder())
                } else {
                    self.tabs
                        .parked
                        .get(index)
                        .and_then(Option::as_ref)
                        .and_then(|tab| tab.dataset.as_ref())
                        .is_some_and(|dataset| dataset.source.is_folder())
                };
                let id = if index == self.tabs.active {
                    self.tab_id
                } else {
                    self.tabs
                        .parked
                        .get(index)
                        .and_then(Option::as_ref)
                        .map_or(u64::MAX, |tab| tab.tab_id)
                };
                TabLabel {
                    id,
                    title: tab_title(path),
                    path: path.map(parquet_reader::display_path),
                    folder,
                    active: index == self.tabs.active,
                }
            })
            .collect()
    }

    /// Files and folders of the open tabs, in tab order, for the session.
    pub(super) fn open_tab_paths(&self) -> (Vec<PathBuf>, usize) {
        let mut active = 0;
        let mut paths = Vec::new();
        for index in 0..self.tab_count() {
            if let Some(path) = self.tab_path_at(index) {
                if index == self.tabs.active {
                    active = paths.len();
                }
                paths.push(path.to_path_buf());
            }
        }
        (paths, active)
    }

    /// The filter and sort each open tab shows, for the session: from its result,
    /// or, while it is still opening, the view waiting to be restored. SQL
    /// results are not kept: their query is in the SQL history.
    pub(super) fn tab_views(&self) -> Vec<TabView> {
        (0..self.tab_count())
            .filter_map(|index| {
                let path = self.tab_path_at(index)?.to_path_buf();
                let (dataset, filtered, pending) = if index == self.tabs.active {
                    (
                        self.dataset.as_ref(),
                        self.filtered.as_ref(),
                        self.pending_view.as_ref(),
                    )
                } else {
                    let tab = self.tabs.parked.get(index)?.as_ref()?;
                    (
                        tab.dataset.as_ref(),
                        tab.filtered.as_ref(),
                        tab.pending_view.as_ref(),
                    )
                };
                tab_view(path, dataset, filtered, pending)
            })
            .collect()
    }

    /// The filter and sort the open tab shows, as [`Self::tab_views`] keeps them.
    pub(super) fn active_tab_view(&self) -> Option<TabView> {
        let path = self.dataset.as_ref()?.path.clone();
        tab_view(
            path,
            self.dataset.as_ref(),
            self.filtered.as_ref(),
            self.pending_view.as_ref(),
        )
    }

    /// Moves tab `from` to position `to` (both clamped to the tabs open); the
    /// tab shown stays shown.
    pub(super) fn move_tab(&mut self, from: usize, to: usize) {
        let count = self.tab_count();
        let to = to.min(count.saturating_sub(1));
        if from >= count || from == to {
            return;
        }
        let tab = self.tabs.parked.remove(from);
        self.tabs.parked.insert(to, tab);
        let active = self.tabs.active;
        self.tabs.active = if active == from {
            to
        } else if from < active && to >= active {
            active - 1
        } else if from > active && to <= active {
            active + 1
        } else {
            active
        };
        self.persist_session();
    }

    /// Shows tab `index`.
    pub(super) fn switch_tab(&mut self, index: usize) {
        if index == self.tabs.active {
            return;
        }
        let Some(mut tab) = self.tabs.parked.get_mut(index).and_then(Option::take) else {
            return;
        };
        self.swap_tab(&mut tab);
        let previous = self.tabs.active;
        if let Some(slot) = self.tabs.parked.get_mut(previous) {
            *slot = Some(tab);
        }
        self.tabs.active = index;
        self.path_input = self
            .tab_path_at(index)
            .map(|path| parquet_reader::display_path(path).display().to_string())
            .unwrap_or_default();
        self.status = match &self.dataset {
            Some(dataset) => format!(
                "{}: {} rows × {} columns",
                tab_title(Some(&dataset.path)),
                theme_number(dataset.source.row_count()),
                dataset.source.column_count()
            ),
            None if self.opening.is_some() => "Opening…".to_string(),
            None => "Choose a Parquet file or folder".to_string(),
        };
        self.persist_session();
    }

    /// Opens an empty tab after the others and shows it. `false` when
    /// [`MAX_TABS`] are already open.
    pub(super) fn new_tab(&mut self) -> bool {
        if self.tab_count() >= MAX_TABS {
            self.status = format!("At most {MAX_TABS} tabs can be open; close one first");
            return false;
        }
        let tab = TabState::new(self.tabs.next_id, self.chart.visible);
        self.tabs.next_id += 1;
        self.tabs.parked.push(Some(tab));
        self.switch_tab(self.tab_count() - 1);
        true
    }

    /// Stops everything the open tab is doing in the background.
    pub(super) fn cancel_tab_work(&mut self) {
        if let Some(task) = &self.opening {
            task.cancel();
        }
        self.opening = None;
        self.opening_path = None;
        self.cancel_page_work();
        self.cancel_export();
        self.record_sql_history(false, None);
        self.cancel_filter();
        self.find.cancel_search();
        self.chart.reset();
        self.profile.reset();
    }

    /// Closes tab `index`, cancelling its work. Closing the last tab leaves one
    /// empty tab.
    pub(super) fn close_tab(&mut self, index: usize) {
        if index >= self.tab_count() {
            return;
        }
        let previous = self.tabs.active;
        self.switch_tab(index);
        self.cancel_tab_work();
        let mut empty = TabState::new(self.tabs.next_id, self.chart.visible);
        self.tabs.next_id += 1;
        if self.tab_count() == 1 {
            // Replace the only tab with an empty one.
            self.swap_tab(&mut empty);
            self.grid.reset();
            self.path_input.clear();
            self.status = "Choose a Parquet file or folder".to_string();
            self.persist_session();
            return;
        }
        // Show a neighbour, then drop the closed tab's slot.
        let neighbour = if index + 1 < self.tab_count() {
            index + 1
        } else {
            index - 1
        };
        self.switch_tab(neighbour);
        self.tabs.parked.remove(index);
        if self.tabs.active > index {
            self.tabs.active -= 1;
        }
        // Go back to the tab that was open, unless it was the one closed.
        if previous != index {
            let previous = if previous > index {
                previous - 1
            } else {
                previous
            };
            self.switch_tab(previous);
        }
        self.persist_session();
    }

    /// Opens a file or folder the user chose: in the open tab when it is empty,
    /// otherwise in a new tab. A file already open in a tab is shown instead.
    pub(super) fn open_path(&mut self, path: PathBuf) {
        // A URL is kept as typed; a local path gets one spelling.
        let path = if parquet_reader::is_remote(&path) {
            path
        } else {
            std::path::absolute(&path).unwrap_or(path)
        };
        if let Some(index) =
            (0..self.tab_count()).find(|index| self.tab_path_at(*index) == Some(path.as_path()))
        {
            self.switch_tab(index);
            self.status = format!("{} is already open", tab_title(Some(&path)));
            return;
        }
        let empty = self.dataset.is_none() && self.opening.is_none();
        if !empty && !self.new_tab() {
            return;
        }
        self.begin_open(path);
    }
}

/// SQL table names, shared with the CLI (`pavi_query::table_name`).
pub(crate) use pavi_query::table_name;

impl PaviApp {
    /// The open tabs' datasets, in tab order, with the open one marked.
    fn tab_datasets(&self) -> Vec<(&Dataset, bool)> {
        (0..self.tab_count())
            .filter_map(|index| {
                if index == self.tabs.active {
                    self.dataset.as_ref().map(|dataset| (dataset, true))
                } else {
                    self.tabs
                        .parked
                        .get(index)
                        .and_then(Option::as_ref)
                        .and_then(|tab| tab.dataset.as_ref())
                        .map(|dataset| (dataset, false))
                }
            })
            .collect()
    }

    /// The tables SQL can name (ADR 0009): `dataset` for the open tab, and every
    /// open tab by a name from its file. `None` without an open dataset. The
    /// names are returned too, in tab order, for the editor.
    pub(super) fn sql_tables(&self) -> Option<(pavi_query::Tables, Vec<String>)> {
        let dataset = self.dataset.as_ref()?;
        let mut tables = pavi_query::Tables::new(Arc::clone(&dataset.source));
        let mut names = Vec::new();
        for (tab, _) in self.tab_datasets() {
            let name = table_name(&tab.path, &names);
            tables = tables.with(name.clone(), Arc::clone(&tab.source));
            names.push(name);
        }
        Some((tables, names))
    }

    /// Every open tab's dataset by its SQL table name, in tab order, with the
    /// open one marked. The names match [`Self::sql_tables`].
    pub(super) fn compare_tables(&self) -> Vec<(String, Arc<ParquetSource>, bool)> {
        let mut names = Vec::new();
        let mut tables = Vec::new();
        for (tab, open) in self.tab_datasets() {
            let name = table_name(&tab.path, &names);
            names.push(name.clone());
            tables.push((name, Arc::clone(&tab.source), open));
        }
        tables
    }

    /// Column names for SQL completions: the open tab's, then other tabs' that
    /// are not already listed.
    pub(super) fn sql_columns(&self) -> Vec<String> {
        let mut columns = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
            .unwrap_or_default();
        for (tab, open) in self.tab_datasets() {
            if open {
                continue;
            }
            for column in &tab.column_names {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }
}

fn theme_number(value: u64) -> String {
    ui::theme::number(value)
}
