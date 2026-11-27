#[path = "tabs.rs"]
mod tabs;
#[path = "ui/mod.rs"]
mod ui;
#[path = "watch.rs"]
mod watch;

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use eframe::egui::{self, Align, Layout, RichText};
use egui_extras::{Column, TableBuilder};
use parquet_reader::{
    Comparison, FilterExpr, FilterOp, MAX_SORT_KEYS, NullOrder, ParquetSource, Projection,
    SortDirection, SortSpec,
    types::is_comparable,
    value::{format_cell_with_limit, scalar_text},
};
use pavi_query::{Filter, LogicalPlan, Planner, QueryEngine, QueryPoll, SqlAst};
use pavi_runtime::{GenerationId, OpenOutcome, OpenTask, Runtime, RuntimeConfig};

use crate::chart::{
    AxisFormat, ChartAccumulator, ChartConfig, ChartKind, ChartModel, ChartSource, ChartState,
    ChartValues, MAX_INPUT_ROWS, MAX_SERIES,
};
use crate::compare::CompareState;
use crate::export::{ExportFormat, ExportInput, ExportState, ExportTask};
use crate::find::{FindOutcome, MAX_FIND_CELLS, Segment, find};
use crate::inspector::{CellDetails, column_summary, inspect_cell, nested_schema_preview};
use crate::pages::SourcePages;
use crate::profile::{
    FileProfileEvent, FileProfileTask, MAX_FILE_PROFILE_COLUMNS, ProfileEvent, ProfileState,
    ProfileTask,
};
use crate::results::{FilteredGrid, JUMP_CONTEXT_ROWS, MatchCount, QueryKind};
use crate::session::{
    GridLayoutPreference, MAX_LAYOUT_COLUMNS, MAX_ZOOM, MIN_ZOOM, SavedNumberFormat, SavedSortKey,
    SessionState, SessionStore, TabView, ThemeChoice,
};
use crate::state::{
    ColumnLayout, GridState, LoadState, MAX_COLUMN_WIDTH, MIN_COLUMN_WIDTH, ToolbarAvailability,
    ToolbarCommand, Workspace,
};

const ROW_HEIGHT: f32 = 24.0;
/// Rows Page Up and Page Down move before the grid has been drawn.
const DEFAULT_PAGE_ROWS: u64 = 20;
const INITIAL_COLUMN_WIDTH: f32 = 110.0;
const CELL_LIMIT: usize = 256;
/// Longest cell value "Filter to this value" accepts; longer ones are refused.
const MAX_FILTER_VALUE_BYTES: usize = 1024;
const MAX_CLIPBOARD_BYTES: usize = 16 * 1024 * 1024;
/// How far ahead of a filter's scan the next match must be, in source rows,
/// for the scan to move on to it: a scan run's worth, so a nearby match is
/// read on to rather than starting a new reader.
const COUNT_RESUME_GAP: u64 = 1 << 20;
/// How often the grid redraws while only a result's match count is running.
const COUNT_REPAINT: Duration = Duration::from_millis(250);

/// Starts counting a filter's matches per row group in the background, for a
/// result's size and to enter it anywhere (ADR 0012). `None` when the runtime
/// can't take the task now; the result then works without a count.
fn match_count(
    runtime: &Runtime,
    source: &Arc<ParquetSource>,
    filter: &FilterExpr,
    generation: GenerationId,
) -> Option<MatchCount> {
    MatchCount::start(
        runtime.handle(),
        Arc::clone(source),
        filter.clone(),
        generation,
    )
}

fn ui_row_count(rows: u64) -> usize {
    rows.min(usize::MAX as u64) as usize
}

struct Dataset {
    source: Arc<ParquetSource>,
    path: PathBuf,
    column_names: Vec<String>,
}

pub(crate) struct PaviApp {
    runtime: Option<Runtime>,
    /// Identifies the open tab; its widgets' state (scroll positions) is kept
    /// under this id. The fields from here to `workspace` belong to the open tab
    /// and are swapped with a parked one when another tab is shown (`tabs.rs`).
    tab_id: u64,
    /// The other tabs, parked.
    tabs: tabs::Tabs,
    /// The Compare window; it belongs to no one tab.
    compare: CompareState,
    grid: GridState,
    dataset: Option<Dataset>,
    opening: Option<OpenTask>,
    /// The path submitted with `opening`; the path box may be edited while it loads.
    opening_path: Option<PathBuf>,
    /// Source-grid page reads and the small cache of loaded pages.
    pages: SourcePages,
    /// First and last absolute row the source grid drew this frame. egui draws
    /// rows partly hidden at the top and one just below the view too.
    drawn_rows: Option<(u64, u64)>,
    /// First and last absolute row wholly in the source grid's view after this
    /// frame: what the scrollbar shows and what a moved selection is kept in.
    visible_rows: Option<(u64, u64)>,
    /// What the user asked of a grid header this frame; applied after the table is drawn.
    header_action: Option<HeaderAction>,
    /// What the user chose in a cell's right-click menu this frame.
    cell_action: Option<CellAction>,
    /// Vertical offset to apply to the source grid's table on the next frame.
    grid_scroll_offset: Option<f32>,
    /// Horizontal offset to apply to the grid on the next frame.
    grid_scroll_x: Option<f32>,
    /// The grid's horizontal scroll offset and visible width (right of the row
    /// numbers) last frame.
    grid_view_x: (f32, f32),
    /// The egui table ids of the file's grid and of a result's grid, keyed by
    /// their column widths (`ui::grid::column_table_id`).
    grid_table_ids: [Option<egui::Id>; 2],
    /// Rows of the grid on screen last frame, less one: what Page Up and Page
    /// Down move.
    grid_page_rows: u64,
    /// The find bar above the grid.
    find: FindState,
    filtered: Option<FilteredGrid>,
    column_layout: ColumnLayout,
    column_search: String,
    jump_row_input: String,
    jump_target: Option<u64>,
    /// A row to scroll into view, as little as needed (a keyboard move); a
    /// `jump_target` is centred instead.
    reveal_row: Option<u64>,
    export: ExportState,
    path_input: String,
    filter_input: String,
    filter_error: Option<String>,
    sql_input: String,
    sql_error: Option<String>,
    /// Completions open under the SQL editor's cursor.
    sql_completion: Option<SqlCompletion>,
    /// The editor's text before a saved or past query replaced it, until it is
    /// restored or the editor is changed.
    sql_replaced: Option<String>,
    /// The History menu is asking whether to clear the whole history.
    confirm_clear_history: bool,
    /// The saved query the Saved menu is asking whether to delete, by name.
    confirm_delete_saved: Option<String>,
    /// Name typed in the "Saved" menu for the query being saved.
    saved_query_name: String,
    chart: ChartState,
    profile: ProfileState,
    session_store: SessionStore,
    session: SessionState,
    running_sql: Option<RunningSql>,
    show_inspector: bool,
    /// The palette last applied (`true` for light); `None` before the first frame.
    light_applied: Option<bool>,
    show_safe_full_selection: bool,
    show_nested_selection: bool,
    workspace: Workspace,
    /// Checks for the open tab's file changing on disk.
    file_watch: watch::FileWatch,
    /// The file the open tab failed to open, to offer import options for it.
    failed_path: Option<PathBuf>,
    show_column_types: bool,
    freeze_header: bool,
    status: String,
    /// The interface zoom last applied; `None` before the first frame.
    zoom_applied: Option<f32>,
    /// The keyboard shortcuts window (F1).
    show_shortcuts: bool,
    /// The filter and sort to restore once the open tab's file has opened.
    pending_view: Option<TabView>,
    /// A message shown above the grid until dismissed (a crash last time).
    notice: Option<String>,
    /// The Import options window, while it is open (ADR 0020).
    import_dialog: Option<crate::import::ImportDialog>,
}

/// Installs PAVI's fonts and style. Call once, before the first frame: fonts
/// set during a frame only take effect on the next one.
pub(crate) fn apply_theme(ctx: &egui::Context) {
    ui::theme::apply(ctx);
}

/// Whether the window keeps the operating system's title bar and border. Only
/// on macOS: winit can't start a window resize there, so without the system
/// border the window could not be resized. Elsewhere PAVI draws its own title
/// bar (the top bar) with window buttons, and resize strips along the edges.
pub(crate) fn native_window_frame() -> bool {
    native_window_frame_on(std::env::consts::OS)
}

fn native_window_frame_on(os: &str) -> bool {
    os == "macos"
}

/// An action chosen on a grid column header.
#[derive(Clone, Debug, PartialEq)]
enum HeaderAction {
    /// Select the column at this display position.
    Select(usize),
    /// Sort by a source column.
    Sort(usize, SortDirection),
    /// Hide a source column.
    Hide(usize),
    /// Fit a source column's width to the loaded values.
    Fit(usize),
    /// Profile a source column in the Inspector.
    Profile(usize),
    /// Copy the column name.
    CopyName(String),
    /// Show a source column's numbers in this format.
    NumberFormat(usize, parquet_reader::NumberFormat),
    /// Count each value of a source column, as a SQL query.
    CountValues(usize),
    /// Add a source column as the next sort key.
    ThenSort(usize, SortDirection),
    /// Pin this many shown columns to the left (0 unpins).
    Pin(usize),
}

/// Completions for the word being typed in the SQL editor.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlCompletion {
    /// Where the word starts and where the cursor is, in characters.
    pub(crate) start: usize,
    pub(crate) cursor: usize,
    pub(crate) items: Vec<crate::sql_editor::Suggestion>,
    pub(crate) selected: usize,
    /// The user moved through the list with the arrow keys: Enter takes the
    /// completion then, and otherwise starts a new line.
    pub(crate) picked: bool,
}

/// The find bar: text to look for in the loaded rows.
#[derive(Default)]
pub(crate) struct FindState {
    pub(crate) open: bool,
    pub(crate) query: String,
    /// Give the find box keyboard focus on the next frame.
    pub(crate) focus: bool,
    /// What the last search found, in words.
    pub(crate) message: Option<String>,
    /// Where a search that ran out of its cell budget stopped.
    pub(crate) resume: Option<crate::find::Cell>,
    /// A search of the whole file in the background (ADR 0022).
    pub(crate) searching: Option<FileSearch>,
}

/// A whole-file find running on a worker.
pub(crate) struct FileSearch {
    pub(crate) task: pavi_runtime::PageTask,
    /// The source searched; a result for another one is dropped.
    pub(crate) source: Arc<ParquetSource>,
    pub(crate) query: String,
    pub(crate) backwards: bool,
    /// The searched columns' display order, as source columns.
    pub(crate) columns: Vec<usize>,
}

impl FindState {
    /// Stops a whole-file search, if one runs.
    pub(crate) fn cancel_search(&mut self) {
        if let Some(search) = self.searching.take() {
            search.task.cancel();
        }
    }
}

/// One row of the Inspector's Row section.
pub(crate) struct RowView {
    pub(crate) row: u64,
    pub(crate) batch: arrow_array::RecordBatch,
    pub(crate) offset: usize,
    /// Column name and batch column, in display order.
    pub(crate) fields: Vec<(String, usize)>,
}

/// An action chosen in a grid cell's right-click menu. It applies to the selected
/// cell: a right-click selects the cell first.
#[derive(Clone, Copy, Debug, PartialEq)]
enum CellAction {
    /// Keep only rows whose value in this column equals the cell's (or is null,
    /// for a null cell).
    FilterTo,
    /// Drop rows whose value in this column equals the cell's.
    Exclude,
    /// Like `FilterTo`, on top of the filter already applied.
    AndFilterTo,
    /// Like `Exclude`, on top of the filter already applied.
    AndExclude,
    CopyCell,
    CopyRow,
    /// Copy the selection in another format.
    CopyAs(CopyFormat),
}

/// How a copied selection is written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyFormat {
    /// Tab-separated, as spreadsheets paste it.
    Tsv,
    /// A Markdown table with the column names as its header.
    Markdown,
    /// A JSON array with one object per row, keyed by column name.
    Json,
    /// The distinct values as a SQL `IN (…)` list.
    SqlList,
}

struct RunningSql {
    text: String,
    started: Instant,
}

impl PaviApp {
    pub(crate) fn new(initial_path: Option<PathBuf>) -> Self {
        let mut app = Self::new_with_store(
            initial_path,
            SessionStore::new(SessionStore::default_path()),
        );
        app.report_previous_crash(&crate::logging::log_dir());
        app
    }

    /// Says so once when the last run ended in a crash that left a report.
    fn report_previous_crash(&mut self, log_dir: &std::path::Path) {
        if let Some(newest) = crate::logging::crash_since(log_dir, self.session.last_crash_report) {
            self.session.last_crash_report = newest;
            self.persist_session();
            self.notice = Some(
                "PAVI closed unexpectedly last time. A crash report was saved in the log folder."
                    .to_string(),
            );
        }
    }

    /// Opens the folder with the log and crash reports.
    pub(super) fn open_log_folder(&mut self) {
        let dir = crate::logging::log_dir();
        if let Err(error) = crate::logging::open_folder(&dir) {
            self.status = format!("{error:#}; the log folder is {}", dir.display());
        }
    }

    fn new_with_store(initial_path: Option<PathBuf>, session_store: SessionStore) -> Self {
        let runtime = Runtime::new(RuntimeConfig::default());
        let session = session_store.load();
        let initial_path_given = initial_path.clone();
        let reopened_path = initial_path.or_else(|| session.last_opened_file.clone());
        let show_inspector = session.preferences.inspector_visible;
        let chart_visible = session.preferences.chart_visible;
        let mut app = Self {
            runtime: runtime.ok(),
            tab_id: 0,
            tabs: tabs::Tabs::default(),
            compare: CompareState::default(),
            grid: GridState::default(),
            dataset: None,
            opening: None,
            opening_path: None,
            pages: SourcePages::default(),
            drawn_rows: None,
            visible_rows: None,
            header_action: None,
            cell_action: None,
            grid_scroll_offset: None,
            grid_scroll_x: None,
            grid_view_x: (0.0, 0.0),
            grid_table_ids: [None; 2],
            grid_page_rows: DEFAULT_PAGE_ROWS,
            find: FindState::default(),
            filtered: None,
            column_layout: ColumnLayout::new(0, INITIAL_COLUMN_WIDTH),
            column_search: String::new(),
            jump_row_input: String::new(),
            jump_target: None,
            reveal_row: None,
            export: ExportState::default(),
            path_input: reopened_path.as_ref().map_or_else(String::new, |path| {
                parquet_reader::display_path(path).display().to_string()
            }),
            filter_input: String::new(),
            filter_error: None,
            sql_input: session.sql_input.clone(),
            sql_error: None,
            sql_completion: None,
            sql_replaced: None,
            confirm_clear_history: false,
            confirm_delete_saved: None,
            saved_query_name: String::new(),
            chart: ChartState {
                visible: chart_visible,
                ..ChartState::default()
            },
            profile: ProfileState::default(),
            session_store,
            session,
            running_sql: None,
            show_inspector,
            light_applied: None,
            show_safe_full_selection: false,
            show_nested_selection: false,
            workspace: Workspace::default(),
            file_watch: watch::FileWatch::default(),
            failed_path: None,
            show_column_types: true,
            freeze_header: true,
            status: "Choose a Parquet file to begin".to_string(),
            zoom_applied: None,
            show_shortcuts: false,
            pending_view: None,
            notice: None,
            import_dialog: None,
        };
        if app.runtime.is_none() {
            app.grid.loading = LoadState::Error("start background runtime".to_string());
            app.status = "Unable to start the background runtime".to_string();
        } else if let Some(path) = initial_path_given {
            app.begin_open(path);
        } else if !app.session.open_files.is_empty() {
            // The tabs open when the app was last closed.
            let (paths, active) = (app.session.open_files.clone(), app.session.active_file);
            for (index, path) in paths.into_iter().enumerate() {
                if index == 0 || app.new_tab() {
                    // Its filter and sort come back once it has opened.
                    let view = app.session.tab_view(&path).cloned();
                    app.begin_open(path);
                    app.pending_view = view;
                }
            }
            app.switch_tab(active);
        } else if let Some(path) = reopened_path {
            app.begin_open(path);
        }
        app
    }

    /// Draws with the palette the user picked. With "System" it follows the
    /// operating system, also when that changes while PAVI runs.
    fn apply_theme_choice(&mut self, ctx: &egui::Context) {
        let system_light = ctx.system_theme().map(|theme| theme == egui::Theme::Light);
        let light = self.session.preferences.theme.is_light(system_light);
        if self.light_applied != Some(light) {
            ui::theme::set_light(ctx, light);
            self.light_applied = Some(light);
        }
    }

    pub(super) fn set_theme_choice(&mut self, choice: ThemeChoice) {
        if self.session.preferences.theme != choice {
            self.session.preferences.theme = choice;
            self.persist_session();
        }
    }

    /// Applies the saved zoom on the first frame, and keeps a zoom changed with
    /// the keyboard (egui's Ctrl+=, Ctrl+-, Ctrl+0) within bounds and saved.
    fn apply_zoom(&mut self, ctx: &egui::Context) {
        let Some(applied) = self.zoom_applied else {
            ctx.set_zoom_factor(self.session.preferences.zoom);
            self.zoom_applied = Some(self.session.preferences.zoom);
            return;
        };
        let now = ctx.zoom_factor();
        if (now - applied).abs() > 0.001 {
            self.set_zoom(ctx, now);
        }
    }

    /// Sets the interface zoom (1 is the normal size) and saves it.
    pub(super) fn set_zoom(&mut self, ctx: &egui::Context, zoom: f32) {
        let zoom = (zoom * 10.0).round() / 10.0;
        let zoom = if zoom.is_finite() {
            zoom.clamp(MIN_ZOOM, MAX_ZOOM)
        } else {
            1.0
        };
        ctx.set_zoom_factor(zoom);
        self.zoom_applied = Some(zoom);
        self.status = format!("Zoom {}%", (zoom * 100.0).round());
        if self.session.preferences.zoom != zoom {
            self.session.preferences.zoom = zoom;
            self.persist_session();
        }
    }

    pub(super) fn clear_recent_files(&mut self) {
        self.session.clear_recent_files();
        self.persist_session();
        self.status = "Recent files cleared".to_string();
    }

    fn persist_session(&mut self) {
        let (open_files, active_file) = self.open_tab_paths();
        self.session.set_open_files(open_files, active_file);
        let views = self.tab_views();
        self.session.set_tab_views(views);
        self.session.sql_input = self.sql_input.clone();
        self.session.preferences.inspector_visible = self.show_inspector;
        self.session.preferences.chart_visible = self.chart.visible;
        if let Err(error) = self.session_store.save(&self.session) {
            log::warn!("save session: {error}");
            self.status = format!("save session: {error}");
        }
    }

    fn toolbar_availability(&self) -> ToolbarAvailability {
        ToolbarAvailability {
            has_dataset: self.dataset.is_some(),
            opening: self.opening.is_some(),
        }
    }

    /// How much of the file the running filter, sort or SQL scan has passed, from
    /// 0 to 1. `None` when no scan is running.
    fn scan_fraction(&self) -> Option<f32> {
        // Reading a CSV, JSON or Arrow file, or the files of a folder.
        if let Some(task) = &self.opening {
            let done = task.progress();
            return (done > 0.0).then_some(done);
        }
        // Joins and subqueries being prepared (ADR 0009).
        if let Some(done) = self
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.execution.as_ref())
            .and_then(pavi_query::QueryExecution::preparing_progress)
        {
            return Some(done);
        }
        let scanned = self.filtered.as_ref()?.execution.as_ref()?.scanned_rows()?;
        let total = self.dataset.as_ref()?.source.row_count();
        (total > 0).then(|| (scanned as f64 / total as f64).min(1.0) as f32)
    }

    fn active_task_label(&self) -> Option<&'static str> {
        if self.opening.is_some() {
            Some("Opening")
        } else if self.export.is_running() {
            Some("Exporting")
        } else if self.chart.loading() {
            Some("Building chart")
        } else if self.profile.loading() {
            Some("Profiling")
        } else if self.running_sql.is_some() {
            Some("Running SQL")
        } else if let Some(filtered) = self
            .filtered
            .as_ref()
            .filter(|filtered| !filtered.finished && filtered.rows == 0)
        {
            // A scan that has not produced its first rows yet.
            Some(match filtered.kind {
                QueryKind::Filter => "Filtering",
                QueryKind::Sort => "Sorting",
                QueryKind::Sql => "Running SQL",
            })
        } else if self.pages.is_loading() {
            Some("Loading pages")
        } else {
            None
        }
    }

    fn handle_global_shortcuts(&mut self, ctx: &egui::Context) {
        // Matched on the key event's own modifiers, so a quick Ctrl+Shift+F is not
        // read as Ctrl+F once Shift is already up. Exactly: egui's `consume_key`
        // ignores an extra Shift or Alt, so Ctrl+Shift+W would close the tab.
        let find_key = |modifiers: egui::Modifiers, key: egui::Key| {
            ctx.input_mut(|input| consume_exact_key(input, modifiers, key))
        };
        if self.dataset.is_some()
            && find_key(
                egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
                egui::Key::F,
            )
        {
            self.open_find();
        }
        if find_key(egui::Modifiers::NONE, egui::Key::F1)
            || find_key(egui::Modifiers::COMMAND, egui::Key::Slash)
        {
            self.show_shortcuts = !self.show_shortcuts;
        }
        if self.find.open {
            if find_key(egui::Modifiers::SHIFT, egui::Key::F3) {
                self.find_next(true);
            } else if find_key(egui::Modifiers::NONE, egui::Key::F3) {
                self.find_next(false);
            }
        }
        let command = ctx.input(|input| input.modifiers.command);
        if !command {
            return;
        }
        if find_key(
            egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
            egui::Key::O,
        ) {
            self.pick_folder();
        }
        if find_key(egui::Modifiers::COMMAND, egui::Key::O) {
            self.pick_file();
        }
        if find_key(egui::Modifiers::COMMAND, egui::Key::T) {
            self.new_tab();
        }
        if find_key(egui::Modifiers::COMMAND, egui::Key::W) {
            self.close_tab(self.tabs.active);
        }
        if find_key(
            egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
            egui::Key::PageUp,
        ) && self.tabs.active > 0
        {
            self.move_tab(self.tabs.active, self.tabs.active - 1);
        }
        if find_key(
            egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
            egui::Key::PageDown,
        ) {
            self.move_tab(self.tabs.active, self.tabs.active + 1);
        }
        if self.tab_count() > 1 {
            let count = self.tab_count();
            if find_key(
                egui::Modifiers::COMMAND | egui::Modifiers::SHIFT,
                egui::Key::Tab,
            ) {
                self.switch_tab((self.tabs.active + count - 1) % count);
            } else if find_key(egui::Modifiers::COMMAND, egui::Key::Tab) {
                self.switch_tab((self.tabs.active + 1) % count);
            }
        }
        if find_key(egui::Modifiers::COMMAND, egui::Key::Num1) {
            self.workspace = Workspace::Grid;
        }
        if self.toolbar_availability().enabled(ToolbarCommand::Sql)
            && find_key(egui::Modifiers::COMMAND, egui::Key::Num2)
        {
            self.workspace = Workspace::Sql;
        }
        if find_key(egui::Modifiers::COMMAND, egui::Key::I) {
            self.show_inspector = !self.show_inspector;
            self.persist_session();
        }
        if self.toolbar_availability().enabled(ToolbarCommand::Refresh)
            && find_key(egui::Modifiers::COMMAND, egui::Key::R)
        {
            self.reload_document();
        }
        if self.toolbar_availability().enabled(ToolbarCommand::Filter)
            && find_key(egui::Modifiers::COMMAND, egui::Key::F)
        {
            self.workspace = Workspace::Grid;
            ctx.memory_mut(|memory| memory.request_focus(egui::Id::new("filter_search")));
        }
        if self.dataset.is_some() && find_key(egui::Modifiers::COMMAND, egui::Key::G) {
            ctx.memory_mut(|memory| memory.request_focus(ui::status::jump_box_id()));
        }
    }

    /// Rows Page Up and Page Down move: one screen of the grid, less a row so
    /// the last one stays in view.
    fn page_rows(&self) -> i64 {
        i64::try_from(self.grid_page_rows.max(1)).unwrap_or(1)
    }

    /// Opens each file or folder dropped on the window, in its own tab.
    fn open_dropped_files(&mut self, ctx: &egui::Context) {
        let dropped = ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .take(tabs::MAX_TABS)
                .collect::<Vec<_>>()
        });
        for path in dropped {
            self.open_path(path);
        }
    }

    fn schema_key(names: &[String]) -> Option<String> {
        (names.len() <= MAX_LAYOUT_COLUMNS).then(|| names.join("\u{1f}"))
    }

    fn restore_column_layout(&mut self) {
        let Some(names) = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
        else {
            return;
        };
        self.column_layout = ColumnLayout::new(names.len(), INITIAL_COLUMN_WIDTH);
        let Some(key) = Self::schema_key(&names) else {
            return;
        };
        let Some(saved) = self.session.grid_layout(&key) else {
            return;
        };
        let index_for = |name: &str| names.iter().position(|candidate| candidate == name);
        let mut order = saved
            .order
            .iter()
            .filter_map(|name| index_for(name))
            .collect::<Vec<_>>();
        let remaining = (0..names.len())
            .filter(|index| !order.contains(index))
            .collect::<Vec<_>>();
        order.extend(remaining);
        let hidden = saved
            .hidden
            .iter()
            .filter_map(|name| index_for(name))
            .collect();
        let widths = names
            .iter()
            .map(|name| {
                saved
                    .widths
                    .get(name)
                    .copied()
                    .map_or(INITIAL_COLUMN_WIDTH, f32::from)
            })
            .collect();
        self.column_layout.restore(order, hidden, widths);
        self.column_layout.set_pinned(saved.pinned);
        for (name, format) in &saved.number_formats {
            if let Some(source) = index_for(name) {
                self.column_layout.set_number_format(
                    source,
                    parquet_reader::NumberFormat {
                        decimals: format.decimals,
                        thousands: format.thousands,
                    },
                );
            }
        }
    }

    fn save_column_layout(&mut self) {
        let Some(dataset) = &self.dataset else {
            return;
        };
        let Some(schema_key) = Self::schema_key(&dataset.column_names) else {
            return;
        };
        let mut widths = std::collections::BTreeMap::new();
        for (source, name) in dataset.column_names.iter().enumerate() {
            widths.insert(
                name.clone(),
                self.column_layout
                    .width(source, INITIAL_COLUMN_WIDTH)
                    .round()
                    .clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH) as u16,
            );
        }
        self.session.save_grid_layout(GridLayoutPreference {
            schema_key,
            order: self
                .column_layout
                .order()
                .iter()
                .filter_map(|source| dataset.column_names.get(*source).cloned())
                .collect(),
            hidden: self
                .column_layout
                .hidden()
                .iter()
                .filter_map(|source| dataset.column_names.get(*source).cloned())
                .collect(),
            widths,
            pinned: self.column_layout.pinned(),
            number_formats: self
                .column_layout
                .number_formats()
                .iter()
                .filter_map(|(source, format)| {
                    Some((
                        dataset.column_names.get(*source)?.clone(),
                        SavedNumberFormat {
                            decimals: format.decimals,
                            thousands: format.thousands,
                        },
                    ))
                })
                .collect(),
        });
        self.persist_session();
    }

    fn refresh_grid_columns(&mut self) {
        if let Some(filtered) = &mut self.filtered {
            filtered.refresh_display_columns(&self.column_layout);
            self.grid.columns = filtered.display_columns.len();
        } else {
            self.grid.columns = self.column_layout.visible().len();
        }
    }

    fn apply_column_layout_change(&mut self) {
        self.cancel_page_work();
        self.grid.clear_pages();
        self.grid.selection = None;
        self.grid.selection_range = None;
        self.refresh_grid_columns();
        self.save_column_layout();
    }

    fn reset_column_layout(&mut self) {
        self.column_layout.reset(INITIAL_COLUMN_WIDTH);
        self.apply_column_layout_change();
    }

    fn auto_fit_column(&mut self, source: usize) {
        let Some(dataset) = &self.dataset else {
            return;
        };
        let mut width = dataset
            .column_names
            .get(source)
            .map_or(0.0, |name| name.chars().count() as f32 * 8.0 + 24.0);
        let mut measure = |batch: &arrow_array::RecordBatch, position: usize| {
            if let Some(array) = batch.columns().get(position) {
                for row in 0..batch.num_rows().min(64) {
                    width = width.max(
                        format_cell_with_limit(array.as_ref(), row, CELL_LIMIT)
                            .chars()
                            .count() as f32
                            * 8.0
                            + 24.0,
                    );
                }
            }
        };
        if let Some(filtered) = &self.filtered {
            let Some((_, position)) = filtered
                .display_columns
                .iter()
                .find(|(display_source, _)| *display_source == source)
            else {
                return;
            };
            for batch in &filtered.batches {
                measure(batch, *position);
            }
        } else {
            let Some(position) = self.column_layout.display_for_source(source) else {
                return;
            };
            for page in self.pages.loaded() {
                for batch in &page.batches {
                    measure(batch, position);
                }
            }
        }
        self.column_layout.set_width(source, width);
        self.save_column_layout();
    }

    fn record_sql_history(&mut self, success: bool, row_count: Option<u64>) {
        let Some(running) = self.running_sql.take() else {
            return;
        };
        let duration_ms = running.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        self.session
            .record_query(&running.text, duration_ms, success, row_count);
        self.persist_session();
    }

    fn record_sql_failure(&mut self, sql: &str) {
        self.session.record_query(sql, 0, false, None);
        self.persist_session();
    }

    fn begin_open(&mut self, path: PathBuf) {
        // One spelling per file, so a relative and an absolute path to the same
        // file do not become two recent entries. This does not touch the disk.
        // A URL is kept as typed; a local path gets one spelling.
        let path = if parquet_reader::is_remote(&path) {
            path
        } else {
            std::path::absolute(&path).unwrap_or(path)
        };
        self.cancel_tab_work();
        self.pending_view = None;
        self.show_safe_full_selection = false;
        self.show_nested_selection = false;
        self.workspace = Workspace::Grid;
        self.file_watch = watch::FileWatch::default();
        self.failed_path = None;
        self.dataset = None;
        self.filter_error = None;
        self.sql_error = None;
        let generation = self.grid.reset();
        let shown = parquet_reader::display_path(&path);
        self.path_input = shown.display().to_string();
        self.status = format!("Opening {}…", shown.display());

        let Some(runtime) = self.runtime.as_ref().map(Runtime::handle) else {
            self.open_error("background runtime is unavailable".to_string());
            return;
        };
        // Options the user chose for this file are used on every open of it.
        let choice = self
            .session
            .saved_import(&path)
            .map(crate::import::ImportChoice::from_saved)
            .unwrap_or_default();
        match runtime.submit_open_with(path.clone(), choice.format, choice.options, generation) {
            Ok(task) => {
                self.opening = Some(task);
                self.opening_path = Some(path);
            }
            Err(error) => self.open_error(format!("queue file open: {error}")),
        }
    }

    fn poll_open(&mut self) {
        let Some(task) = &self.opening else {
            return;
        };
        let response = match task.try_recv() {
            Ok(response) => response,
            Err(std::sync::mpsc::TryRecvError::Empty) => return,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                self.opening = None;
                self.opening_path = None;
                self.open_error("background file open worker stopped".to_string());
                return;
            }
        };
        self.opening = None;
        let opened_path = self.opening_path.take();
        if response.generation_id != self.grid.generation {
            return;
        }
        let path = opened_path.unwrap_or_else(|| PathBuf::from(&self.path_input));
        match response.outcome {
            OpenOutcome::Opened(source) => {
                let schema = source.schema();
                let column_names = schema
                    .fields()
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect();
                let rows = source.row_count();
                let columns = source.column_count();
                self.dataset = Some(Dataset {
                    source,
                    path: path.clone(),
                    column_names,
                });
                self.restore_column_layout();
                self.grid.ready(rows, self.column_layout.visible().len());
                self.session.record_recent_file(path.clone());
                self.persist_session();
                if let Some(chart_columns) =
                    self.dataset.as_ref().map(|dataset| &dataset.column_names)
                {
                    self.chart.config.x_column = chart_columns.first().cloned().unwrap_or_default();
                    self.chart.config.y_columns = vec![
                        chart_columns
                            .get(1)
                            .or_else(|| chart_columns.first())
                            .cloned()
                            .unwrap_or_default(),
                    ];
                }
                let files = self
                    .dataset
                    .as_ref()
                    .filter(|dataset| dataset.source.is_folder())
                    .map(|dataset| match dataset.source.file_count() {
                        1 => " from 1 file".to_string(),
                        count => format!(" from {count} files"),
                    })
                    .unwrap_or_default();
                self.status = if rows == 0 {
                    format!("Opened empty dataset{files}")
                } else {
                    format!("Ready: {rows} rows × {columns} columns{files}")
                };
                log::info!(
                    "opened {}: {rows} rows × {columns} columns{files}",
                    parquet_reader::display_path(&path).display()
                );
                if let Some(view) = self.pending_view.take() {
                    self.apply_tab_view(&view);
                }
            }
            OpenOutcome::Cancelled => {
                self.pending_view = None;
                self.status = "File open cancelled".to_string();
            }
            OpenOutcome::OpenFailed(error) => {
                self.pending_view = None;
                self.failed_path = Some(path.clone());
                self.session.remove_recent_file(&path);
                self.persist_session();
                self.open_error(format!("open file: {error:#}"));
            }
        }
    }

    /// Applies a tab's saved filter and sort to the file just opened. A column
    /// the file no longer has is left out of the sort; a filter that no longer
    /// parses shows its error, as typed.
    fn apply_tab_view(&mut self, view: &TabView) {
        let Some(names) = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
        else {
            return;
        };
        if !view.filter.trim().is_empty() {
            self.filter_input = view.filter.clone();
            self.apply_filter();
            if self.filter_error.is_some() {
                return;
            }
        }
        let keys = view
            .sort
            .iter()
            .filter_map(|key| {
                let column = names.iter().position(|name| *name == key.column)?;
                let direction = if key.descending {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                };
                Some(SortSpec::new(column, direction, NullOrder::Last))
            })
            .take(MAX_SORT_KEYS)
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            self.apply_sort(keys);
        }
    }

    /// Selects the cell of display column `column` in the current row, so the
    /// Inspector shows that column. Falls back to the first row in view.
    fn select_column(&mut self, column: usize) {
        let drawn = if self.filtered.is_none() {
            self.drawn_rows.map(|(first, _)| first)
        } else {
            None
        };
        let row = self
            .grid
            .selection
            .map(|(row, _)| row)
            .or(drawn)
            .unwrap_or(self.grid.first_row);
        if !self.grid.select(row, column) {
            self.grid.select(self.grid.first_row, column);
        }
    }

    /// Applies the header action chosen this frame, if any.
    fn apply_header_action(&mut self, ctx: &egui::Context) {
        match self.header_action.take() {
            Some(HeaderAction::Select(column)) => self.select_column(column),
            Some(HeaderAction::Sort(source, direction)) => self.sort_column(source, direction),
            Some(HeaderAction::ThenSort(source, direction)) => self.add_sort_key(source, direction),
            Some(HeaderAction::Hide(source)) => {
                self.column_layout.set_hidden(source, true);
                self.apply_column_layout_change();
            }
            Some(HeaderAction::Fit(source)) => self.auto_fit_column(source),
            Some(HeaderAction::Profile(source)) => {
                if !self.show_inspector {
                    self.show_inspector = true;
                    self.persist_session();
                }
                self.run_profile(source);
            }
            Some(HeaderAction::CopyName(name)) => {
                ctx.copy_text(name);
                self.status = "Copied the column name".to_string();
            }
            Some(HeaderAction::NumberFormat(source, format)) => {
                self.column_layout.set_number_format(source, format);
                self.save_column_layout();
            }
            Some(HeaderAction::CountValues(source)) => self.count_values(source),
            Some(HeaderAction::Pin(count)) => {
                self.column_layout.set_pinned(count);
                self.save_column_layout();
                self.status = match self.column_layout.pinned() {
                    0 => "Columns unpinned".to_string(),
                    1 => "1 column pinned".to_string(),
                    pinned => format!("{pinned} columns pinned"),
                };
            }
            None => {}
        }
    }

    /// Records an absolute row the source grid drew this frame.
    fn note_drawn_row(&mut self, row: u64) {
        self.drawn_rows = Some(match self.drawn_rows {
            Some((first, last)) => (first.min(row), last.max(row)),
            None => (row, row),
        });
    }

    /// Moves the source grid's table window to start at `base`, keeping absolute
    /// row `top` at the top of the view on the next frame.
    fn move_grid_window(&mut self, base: u64, top: u64, header_rows: usize) {
        self.grid.window_base = base;
        self.grid_scroll_offset =
            Some((top - base).saturating_add(header_rows as u64) as f32 * ROW_HEIGHT);
    }

    /// Scrolls the source grid so absolute row `top` is at the top of the view.
    fn scroll_grid_to(&mut self, top: u64, header_rows: usize) {
        let base = self.grid.window_base_for(top);
        self.move_grid_window(base, top, header_rows);
    }

    /// Cancels every page read and drops every loaded page.
    fn cancel_page_work(&mut self) {
        self.pages.clear();
    }

    fn cancel_export(&mut self) {
        if self.export.cancel() {
            self.status = "Export cancelled".to_string();
        }
    }

    fn export_input(&self, selected: bool) -> anyhow::Result<ExportInput> {
        if selected {
            let (batch, row) = self.selected_batch_row().ok_or_else(|| {
                anyhow::anyhow!("selected row is no longer in the bounded grid window")
            })?;
            let visible = self.visible_batch_columns();
            let batch = batch
                .project(&visible)
                .map_err(|error| anyhow::anyhow!("selected layout projection: {error}"))?;
            return Ok(ExportInput::Batch(batch.slice(row, 1)));
        }
        if let Some(filtered) = &self.filtered {
            return Ok(ExportInput::Plan(filtered.plan.clone()));
        }
        let dataset = self
            .dataset
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("open a dataset before exporting"))?;
        Ok(ExportInput::Plan(
            LogicalPlan::scan(Arc::clone(&dataset.source))
                .project((0..dataset.source.column_count()).collect::<Vec<_>>()),
        ))
    }

    fn start_export(&mut self, format: ExportFormat, selected: bool) {
        let Some(dataset) = &self.dataset else {
            self.status = "Open a dataset before exporting".to_string();
            return;
        };
        let suggested = parquet_reader::display_path(&dataset.path)
            .file_stem()
            .and_then(|name| name.to_str())
            .map_or_else(
                || "pavi-export".to_string(),
                |name| format!("{name}-export"),
            );
        let Some(path) = rfd::FileDialog::new()
            .add_filter(format.label(), &[format.extension()])
            .set_file_name(format!("{suggested}.{}", format.extension()))
            .save_file()
        else {
            return;
        };
        self.start_export_to(format, selected, path);
    }

    fn start_export_to(&mut self, format: ExportFormat, selected: bool, path: PathBuf) {
        let input = match self.export_input(selected) {
            Ok(input) => input,
            Err(error) => {
                self.status = format!("Export: {error:#}");
                return;
            }
        };
        let Some(runtime) = self.runtime.as_ref().map(Runtime::handle) else {
            self.status = "Export: background runtime is unavailable".to_string();
            return;
        };
        match self
            .export
            .start(|generation| ExportTask::start(input, path, format, runtime, generation))
        {
            Ok(()) => self.status = format!("Exporting {}…", format.label()),
            Err(error) => self.status = format!("Start export: {error}"),
        }
    }

    fn poll_export(&mut self) {
        if let Some(status) = self.export.poll() {
            // The outcome, not every progress step.
            if !status.starts_with("Exporting") {
                log::info!("{status}");
            }
            self.status = status;
        }
    }

    fn cancel_filter(&mut self) {
        let is_sql = self
            .filtered
            .as_ref()
            .is_some_and(|filtered| filtered.kind == QueryKind::Sql);
        if let Some(filtered) = &mut self.filtered {
            filtered.cancel();
        }
        self.filtered = None;
        if is_sql {
            self.record_sql_history(false, None);
        }
    }

    fn apply_filter(&mut self) {
        let expression = self.filter_input.trim().to_owned();
        match FilterExpr::parse(&expression) {
            Ok(filter) => self.start_filter(filter, &expression),
            Err(error) => self.filter_error = Some(format!("Invalid filter: {error}")),
        }
    }

    /// The filter applied from the filter box or a cell menu, if that is what the
    /// grid is showing. A sort on top of it keeps it.
    fn active_filter(&self) -> Option<&FilterExpr> {
        self.filtered
            .as_ref()
            .filter(|filtered| filtered.kind == QueryKind::Filter)
            .and_then(|filtered| filtered.filter.as_ref())
    }

    /// The test "selected column == (or !=) the selected cell's value", or
    /// `IS NULL` / `IS NOT NULL` for a null cell, or why that cell cannot be
    /// filtered by.
    fn selected_cell_filter(&self, exclude: bool) -> Result<FilterExpr, String> {
        let (row, display_column) = self.grid.selection.ok_or("Select a cell first")?;
        let dataset = self.dataset.as_ref().ok_or("Open a file first")?;
        let (source, batch_column) = match &self.filtered {
            Some(filtered) if filtered.aggregate => {
                return Err(
                    "Only rows with this file's own columns can be filtered by value; \
                     this result was aggregated or joined"
                        .to_owned(),
                );
            }
            Some(filtered) => *filtered
                .display_columns
                .get(display_column)
                .ok_or("That column is no longer shown")?,
            None => (
                *self
                    .column_layout
                    .visible()
                    .get(display_column)
                    .ok_or("That column is no longer shown")?,
                display_column,
            ),
        };
        let (batch, offset) = self.batch_row_at(row).ok_or("That cell is still loading")?;
        let array = batch
            .columns()
            .get(batch_column)
            .ok_or("That column is no longer shown")?;
        let name = dataset
            .column_names
            .get(source)
            .ok_or("That column is no longer shown")?;
        // Filters find a column by name before trying a position, so even a
        // column whose name is a number is written by name.
        let column = name.clone();
        if array.is_null(offset) {
            let op = if exclude {
                FilterOp::IsNotNull
            } else {
                FilterOp::IsNull
            };
            return Ok(FilterExpr::compare(column, op, ""));
        }
        let data_type = array.data_type();
        if !is_comparable(data_type) {
            return Err(format!("{data_type} columns cannot be filtered"));
        }
        let value = scalar_text(array.as_ref(), offset)
            .map_err(|error| format!("{error:#}"))?
            .ok_or("That cell has no value to filter by")?;
        if value.len() > MAX_FILTER_VALUE_BYTES {
            return Err(format!(
                "That value is longer than {MAX_FILTER_VALUE_BYTES} bytes; type a shorter filter"
            ));
        }
        let equals = Comparison {
            column,
            op: FilterOp::Eq,
            value,
        };
        // The filter is only offered when it finds the cell it was made from.
        let matches_itself = arrow_array::RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                name.as_str(),
                data_type.clone(),
                true,
            )])),
            vec![array.slice(offset, 1)],
        )
        .ok()
        .and_then(|cell| equals.evaluate_batch(&cell, 0).ok())
        .is_some_and(|mask| mask.len() == 1 && mask.value(0));
        if !matches_itself {
            return Err("That value cannot be matched exactly by a filter".to_owned());
        }
        Ok(FilterExpr::Compare(Comparison {
            op: if exclude { FilterOp::Ne } else { FilterOp::Eq },
            ..equals
        }))
    }

    /// Applies the action chosen in a cell's right-click menu, if any.
    fn apply_cell_action(&mut self, ctx: &egui::Context) {
        let Some(action) = self.cell_action.take() else {
            return;
        };
        match action {
            CellAction::CopyCell => self.copy_selection(ctx, false),
            CellAction::CopyRow => self.copy_selection(ctx, true),
            CellAction::CopyAs(format) => self.copy_selection_as(ctx, false, format),
            CellAction::FilterTo
            | CellAction::Exclude
            | CellAction::AndFilterTo
            | CellAction::AndExclude => {
                let exclude = matches!(action, CellAction::Exclude | CellAction::AndExclude);
                let add = matches!(action, CellAction::AndFilterTo | CellAction::AndExclude);
                match self.selected_cell_filter(exclude) {
                    Ok(test) => {
                        // "Add" narrows the filter already applied; without one it
                        // is a plain filter.
                        let filter = match self.active_filter().filter(|_| add) {
                            Some(active) => active.clone().and(test),
                            None => test,
                        };
                        let text = filter.to_string();
                        self.filter_input = text.clone();
                        self.workspace = Workspace::Grid;
                        self.start_filter(filter, &text);
                    }
                    Err(reason) => self.status = reason,
                }
            }
        }
    }

    /// Replaces the current result with the rows of the file that pass `filter`.
    /// `expression` is the filter as text, for the status bar.
    fn start_filter(&mut self, filter: FilterExpr, expression: &str) {
        let Some(dataset) = &self.dataset else {
            self.filter_error = Some("Open a dataset before applying a filter".to_string());
            return;
        };
        let source = Arc::clone(&dataset.source);
        let projected_columns = (0..source.column_count()).collect::<Vec<_>>();
        let plan = LogicalPlan::scan(source)
            .filter(Filter::new(filter.clone()))
            .project(projected_columns.clone());
        let sort = match Planner::plan(&plan) {
            Ok(plan) => plan.sort().to_vec(),
            Err(error) => {
                self.filter_error = Some(format!("Invalid filter: {error:#}"));
                return;
            }
        };
        self.cancel_page_work();
        self.cancel_filter();
        let generation = self.grid.reset();
        self.grid.ready(0, projected_columns.len());
        let Some(runtime) = &self.runtime else {
            self.filter_error = Some("Background runtime is unavailable".to_string());
            return;
        };
        match QueryEngine::new(runtime).execute(&plan, generation) {
            Ok(execution) => {
                let mut filtered = FilteredGrid::new(
                    QueryKind::Filter,
                    plan,
                    sort,
                    false,
                    projected_columns,
                    execution,
                );
                filtered.count = self
                    .dataset
                    .as_ref()
                    .and_then(|dataset| match_count(runtime, &dataset.source, &filter, generation));
                filtered.filter = Some(filter);
                self.filtered = Some(filtered);
                self.refresh_grid_columns();
                self.filter_error = None;
                self.status = format!("Filtering: {expression}");
                // The tab's filter is reopened at the next start.
                self.persist_session();
            }
            Err(error) => {
                self.filter_error = Some(format!("Start filter: {error:#}"));
                self.status = "Unable to start filter".to_string();
            }
        }
    }

    fn clear_filter(&mut self) {
        let filtering = self
            .filtered
            .as_ref()
            .is_some_and(|filtered| filtered.kind == QueryKind::Filter);
        if !filtering && self.filter_error.is_none() && self.filter_input.is_empty() {
            return;
        }
        self.filter_input.clear();
        self.filter_error = None;
        if !filtering {
            return;
        }
        self.cancel_filter();
        self.cancel_page_work();
        let generation = self.grid.reset();
        if let Some(dataset) = &self.dataset {
            self.grid.ready(
                dataset.source.row_count(),
                self.column_layout.visible().len(),
            );
            self.status = "Filter cleared".to_string();
        } else {
            self.grid.generation = generation;
        }
        self.persist_session();
    }

    fn run_sql(&mut self) {
        let sql = self.sql_input.trim().to_owned();
        if sql.is_empty() {
            self.sql_error = Some("Enter a SELECT query before running it".to_string());
            return;
        }
        let Some((tables, _)) = self.sql_tables() else {
            self.sql_error = Some("Open a dataset before running SQL".to_string());
            self.record_sql_failure(&sql);
            return;
        };
        let plan = match SqlAst::parse(&sql).and_then(|ast| ast.to_logical_plan_with(&tables)) {
            Ok(plan) => plan,
            Err(error) => {
                self.sql_error = Some(format!("SQL error: {error:#}"));
                self.record_sql_failure(&sql);
                return;
            }
        };
        // A join or subquery's columns are its own, not this tab's: they are
        // shown as the result names them, like an aggregate's (ADR 0009).
        let planned = if plan.is_prepared() {
            Planner::validate(&plan)
                .map(|schema| ((0..schema.fields().len()).collect(), Vec::new(), true))
        } else {
            Planner::plan(&plan).and_then(|plan| {
                let aggregate = plan.aggregate();
                // Aliases rename the result's columns: it is shown as it names
                // them, like an aggregate's.
                let own_names = aggregate.is_some() || plan.renames_columns();
                Ok((
                    if own_names {
                        (0..plan.output_schema()?.fields().len()).collect()
                    } else {
                        plan.projected_columns().to_vec()
                    },
                    plan.sort().to_vec(),
                    own_names,
                ))
            })
        };
        let (projected_columns, sort, aggregate): (Vec<usize>, _, _) = match planned {
            Ok(planned) => planned,
            Err(error) => {
                self.sql_error = Some(format!("SQL error: {error:#}"));
                self.record_sql_failure(&sql);
                return;
            }
        };
        if self.runtime.is_none() {
            self.sql_error = Some("Background runtime is unavailable".to_string());
            return;
        }
        self.cancel_page_work();
        self.record_sql_history(false, None);
        self.cancel_filter();
        let generation = self.grid.reset();
        self.grid.ready(0, projected_columns.len());
        let Some(runtime) = &self.runtime else {
            self.sql_error = Some("Background runtime is unavailable".to_string());
            self.status = "Unable to start SQL query".to_string();
            return;
        };
        let execution = QueryEngine::new(runtime).execute(&plan, generation);
        match execution {
            Ok(execution) => {
                self.filtered = Some(FilteredGrid::new(
                    QueryKind::Sql,
                    plan,
                    sort,
                    aggregate,
                    projected_columns,
                    execution,
                ));
                self.refresh_grid_columns();
                self.sql_error = None;
                self.running_sql = Some(RunningSql {
                    text: sql.to_owned(),
                    started: Instant::now(),
                });
                self.persist_session();
                self.status = "SQL: running".to_string();
            }
            Err(error) => {
                self.sql_error = Some(format!("Start SQL query: {error:#}"));
                self.status = "Unable to start SQL query".to_string();
                self.record_sql_failure(&sql);
            }
        }
    }

    fn cancel_sql(&mut self) {
        if !self
            .filtered
            .as_ref()
            .is_some_and(|filtered| filtered.kind == QueryKind::Sql)
        {
            return;
        }
        self.cancel_filter();
        self.cancel_page_work();
        self.record_sql_history(false, None);
        let generation = self.grid.reset();
        if let Some(dataset) = &self.dataset {
            self.grid.ready(
                dataset.source.row_count(),
                self.column_layout.visible().len(),
            );
            self.status = "SQL query cancelled".to_string();
        } else {
            self.grid.generation = generation;
        }
    }

    /// Sorts by `column`: ascending, or descending when it is already sorted ascending.
    /// The sort keys of the result on screen; empty when it is not sorted.
    fn sort_keys(&self) -> &[SortSpec] {
        self.filtered
            .as_ref()
            .map_or(&[], |filtered| filtered.sort.as_slice())
    }

    /// Sorts by `column` alone: ascending, or descending when it is already the
    /// only key, ascending.
    fn toggle_sort(&mut self, column: usize) {
        let direction = match self.sort_keys() {
            [key] if key.column == column && key.direction == SortDirection::Ascending => {
                SortDirection::Descending
            }
            _ => SortDirection::Ascending,
        };
        self.sort_column(column, direction);
    }

    /// Drops the sort and shows the rows in file order again: the whole file
    /// for a plain sort, the filter's matches for a sorted filter. A SQL
    /// result keeps its order, which its query decides.
    fn clear_sort(&mut self) {
        let (kind, filter) = match &self.filtered {
            Some(filtered) if !filtered.sort.is_empty() => (filtered.kind, filtered.filter.clone()),
            _ => return,
        };
        match kind {
            QueryKind::Sort => {
                self.cancel_filter();
                self.cancel_page_work();
                let generation = self.grid.reset();
                if let Some(dataset) = &self.dataset {
                    self.grid.ready(
                        dataset.source.row_count(),
                        self.column_layout.visible().len(),
                    );
                } else {
                    self.grid.generation = generation;
                }
                self.refresh_grid_columns();
                self.status = "Back to file order".to_string();
                self.persist_session();
            }
            QueryKind::Filter => {
                if let Some(filter) = filter {
                    let text = filter.to_string();
                    self.start_filter(filter, &text);
                }
            }
            QueryKind::Sql => {}
        }
    }

    /// Sorts by `column` alone.
    fn sort_column(&mut self, column: usize, direction: SortDirection) {
        self.apply_sort(vec![SortSpec::new(column, direction, NullOrder::Last)]);
    }

    /// Keeps the current sort and adds `column` as its least significant key (or
    /// changes its direction, if it is already a key).
    fn add_sort_key(&mut self, column: usize, direction: SortDirection) {
        let mut keys = self.sort_keys().to_vec();
        match keys.iter_mut().find(|key| key.column == column) {
            Some(key) => key.direction = direction,
            None => keys.push(SortSpec::new(column, direction, NullOrder::Last)),
        }
        if keys.len() > MAX_SORT_KEYS {
            self.status = format!("A sort can have at most {MAX_SORT_KEYS} columns");
            return;
        }
        self.apply_sort(keys);
    }

    /// Sorts the result on screen (or the file) by `keys`. The whole result is
    /// sorted; one too large for memory spills to disk (ADR 0005).
    fn apply_sort(&mut self, keys: Vec<SortSpec>) {
        let Some(dataset) = &self.dataset else {
            self.status = "Open a dataset before sorting".to_string();
            return;
        };
        let source = Arc::clone(&dataset.source);
        let column_count = source.column_count();
        let description = self.describe_sort(&keys);
        let (plan, kind) = if let Some(filtered) = &self.filtered {
            (filtered.plan.clone(), filtered.kind)
        } else {
            (
                LogicalPlan::scan(source).project((0..column_count).collect::<Vec<_>>()),
                QueryKind::Sort,
            )
        };
        let plan = plan.replace_sort(keys);
        // Sorting a filtered result keeps its filter, so a cell menu can still add to it.
        let active_filter = self
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.filter.clone());
        let (projected_columns, sort) = match Planner::plan(&plan) {
            Ok(planned) => (
                planned.projected_columns().to_vec(),
                planned.sort().to_vec(),
            ),
            Err(error) => {
                self.status = format!("Sort error: {error:#}");
                return;
            }
        };
        if self.runtime.is_none() {
            self.status = "Background runtime is unavailable".to_string();
            return;
        }

        self.cancel_page_work();
        self.cancel_filter();
        let generation = self.grid.reset();
        self.grid.ready(0, projected_columns.len());
        let Some(runtime) = &self.runtime else {
            self.status = "Background runtime is unavailable".to_string();
            return;
        };
        match QueryEngine::new(runtime).execute(&plan, generation) {
            Ok(execution) => {
                let mut filtered =
                    FilteredGrid::new(kind, plan, sort, false, projected_columns, execution);
                // A sort keeps every row it sorts: the filter's matches, or the file.
                filtered.count = match (&active_filter, kind, &self.dataset) {
                    (Some(filter), _, Some(dataset)) => {
                        match_count(runtime, &dataset.source, filter, generation)
                    }
                    (None, QueryKind::Sort, Some(dataset)) => {
                        Some(MatchCount::known(dataset.source.row_count()))
                    }
                    _ => None,
                };
                filtered.filter = active_filter;
                self.filtered = Some(filtered);
                self.refresh_grid_columns();
                self.status = format!("Sorting by {description} (nulls last)");
                self.persist_session();
            }
            Err(error) => self.status = format!("Start sort: {error:#}"),
        }
    }

    /// Sort keys as text: "DEP_DELAY ↓, FL_DATE ↑".
    fn describe_sort(&self, keys: &[SortSpec]) -> String {
        let names = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.as_slice())
            .unwrap_or_default();
        keys.iter()
            .map(|key| {
                let name = names.get(key.column).map_or("?", String::as_str);
                let arrow = match key.direction {
                    SortDirection::Ascending => "↑",
                    SortDirection::Descending => "↓",
                };
                format!("{name} {arrow}")
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn sort_heading(&self, column: usize, name: &str) -> String {
        match self.sort_keys().iter().find(|key| key.column == column) {
            Some(key) => format!(
                "{name} {}",
                match key.direction {
                    SortDirection::Ascending => "↑",
                    SortDirection::Descending => "↓",
                }
            ),
            None => name.to_string(),
        }
    }

    fn poll_filtered(&mut self) {
        let Some(filtered) = &mut self.filtered else {
            return;
        };
        let mut status = None;
        let mut sql_history = None;
        let mut retry_jump = None;
        if let Some(count) = &mut filtered.count {
            count.poll();
        }
        // A filter's count says where its results are (ADR 0012): once every
        // match has arrived the scan stops, and with the next match far ahead
        // it moves on to that row group instead of reading the rows between.
        if filtered.kind == QueryKind::Filter
            && filtered.sort.is_empty()
            && !filtered.finished
            && filtered.pending_jump.is_none()
            && let Some(count) = &filtered.count
            && let Some(total) = count.total()
        {
            let received = filtered.received_rows();
            if received >= total {
                if let Some(execution) = &mut filtered.execution {
                    execution.cancel();
                }
                filtered.execution = None;
                filtered.finished = true;
                filtered.completed_once = true;
                status = Some(if received == 0 {
                    filtered.kind.empty_message().to_string()
                } else {
                    format!(
                        "{} result: {} rows",
                        filtered.kind.label(),
                        ui::theme::number(received)
                    )
                });
            } else if let (Some(dataset), Some(execution)) =
                (&self.dataset, &mut filtered.execution)
                && let Some((source_row, _)) =
                    count.entry_for(received, dataset.source.row_groups())
                && execution
                    .scanned_rows()
                    .is_none_or(|at| source_row > at.saturating_add(COUNT_RESUME_GAP))
            {
                execution.resume_scan_at(source_row);
            }
        }
        while filtered.needs_more() {
            let Some(execution) = &mut filtered.execution else {
                break;
            };
            let polled = execution.poll_next_batch();
            // Rows the workers dropped for a jump ahead come before this poll's.
            filtered.note_skipped();
            match polled {
                Ok(QueryPoll::Pending) => break,
                Ok(QueryPoll::Finished) => {
                    filtered.execution = None;
                    filtered.finished = true;
                    status = Some(if filtered.received_rows() == 0 {
                        filtered.kind.empty_message().to_string()
                    } else {
                        format!(
                            "{} result: {} rows",
                            filtered.kind.label(),
                            ui::theme::number(filtered.received_rows())
                        )
                    });
                    // A rerun to jump back is not another query.
                    if filtered.kind == QueryKind::Sql && !filtered.completed_once {
                        sql_history = Some((true, Some(filtered.received_rows())));
                    }
                    filtered.completed_once = true;
                }
                Ok(QueryPoll::Batch(batch)) if batch.generation_id == self.grid.generation => {
                    if filtered.rows == 0 && filtered.pending_jump.is_none() {
                        // The scan-time message ("Sorting …") is no longer true.
                        status = Some(format!(
                            "{} result ready; more rows load as you scroll",
                            filtered.kind.label()
                        ));
                    }
                    filtered.push(batch.batch);
                    if self
                        .grid
                        .selection
                        .is_some_and(|(row, _)| row < filtered.first_result)
                    {
                        self.grid.selection = None;
                        self.grid.selection_range = None;
                    }
                }
                Ok(QueryPoll::Batch(_)) => {}
                Err(error) => {
                    let error = format!("{} execution: {error:#}", filtered.kind.label());
                    filtered.execution = None;
                    filtered.finished = true;
                    filtered.error = Some(error.clone());
                    status = Some(error);
                    if filtered.kind == QueryKind::Sql {
                        sql_history = Some((false, None));
                    }
                }
            }
        }
        self.grid.first_row = filtered.first_result;
        self.grid.rows = filtered.rows;
        // A row jumped to: show it once it has arrived, or say why it can't be.
        if let Some(target) = filtered.pending_jump {
            if filtered.holds(target) {
                filtered.pending_jump = None;
                self.grid.select(target, filtered.pending_column);
                self.jump_target = Some(target);
                status = Some(format!("Row {}", ui::theme::number(target + 1)));
            } else if target < filtered.first_result {
                // A skip already under way when a nearer jump came went past
                // it: read the result again up to it, as a jump back does.
                retry_jump = Some(target);
            } else if filtered.finished {
                filtered.pending_jump = None;
                if filtered.error.is_none() {
                    status = Some(format!(
                        "The result has {} rows",
                        ui::theme::number(filtered.received_rows())
                    ));
                }
            }
        }
        if let Some(status) = status {
            self.status = status;
        }
        if let Some(target) = retry_jump {
            self.jump_to_result(target);
        }
        if let Some((success, row_count)) = sql_history {
            self.record_sql_history(success, row_count);
        }
    }

    /// Shows result row `target` of a filter, SQL or sort result (ADR 0012):
    /// from the rows held, by reading on to it, or by reading the result again,
    /// from the row group that holds it once a filter's matches are counted.
    pub(super) fn jump_to_result(&mut self, target: u64) {
        let Some(filtered) = &mut self.filtered else {
            return;
        };
        let target = match filtered.total() {
            Some(0) => return,
            Some(total) => target.min(total - 1),
            None => target,
        };
        if filtered.holds(target) {
            filtered.pending_jump = None;
            self.grid.first_row = filtered.first_result;
            self.grid.rows = filtered.rows;
            let column = self.grid.selection.map_or(0, |(_, column)| column);
            self.grid.select(target, column);
            self.jump_target = Some(target);
            return;
        }
        let start = target.saturating_sub(JUMP_CONTEXT_ROWS);
        let row = ui::theme::number(target + 1);
        filtered.pending_column = self.grid.selection.map_or(0, |(_, column)| column);
        // A plain filter whose matches are counted can start at the row group
        // that holds `start`.
        let entry = self
            .dataset
            .as_ref()
            .filter(|_| filtered.kind == QueryKind::Filter && filtered.sort.is_empty())
            .and_then(|dataset| {
                filtered
                    .count
                    .as_ref()?
                    .entry_for(start, dataset.source.row_groups())
            });
        // Ahead of the rows read so far, reading on is cheaper unless that row
        // group starts even further ahead.
        let reads_on = target >= filtered.received_rows()
            && filtered.execution.is_some()
            && entry.is_none_or(|(_, before)| before <= filtered.received_rows());
        if reads_on {
            filtered.pending_jump = Some(target);
            filtered.skip_forward(start);
            self.grid.first_row = filtered.first_result;
            self.grid.rows = 0;
            self.status = format!("Reading on to row {row}…");
            return;
        }
        let Some(runtime) = &self.runtime else {
            self.status = "Background runtime is unavailable".to_string();
            return;
        };
        let generation = self.grid.next_generation();
        let engine = QueryEngine::new(runtime);
        let started = match entry {
            Some((source_row, before)) => engine
                .execute_from_row(&filtered.plan, source_row, generation)
                .map(|execution| (execution, before)),
            None => engine
                .execute(&filtered.plan, generation)
                .map(|execution| (execution, 0)),
        };
        match started {
            Ok((execution, base)) => {
                let from_start = base == 0 && start > 0;
                filtered.restart(execution, base, start);
                filtered.pending_jump = Some(target);
                self.grid.first_row = filtered.first_result;
                self.grid.rows = 0;
                self.status = if from_start {
                    format!("Reading the result again up to row {row}…")
                } else {
                    format!("Going to row {row}…")
                };
            }
            Err(error) => self.status = format!("Go to row {row}: {error:#}"),
        }
    }

    fn request_more_filtered(&mut self) {
        if let Some(filtered) = &mut self.filtered {
            filtered.request_more();
        }
    }

    fn request_page(&mut self, page_index: u64) {
        if !self.grid.request_page(page_index) {
            return;
        }
        let Some(dataset) = &self.dataset else {
            return;
        };
        let Some(runtime) = &self.runtime else {
            self.grid.requested.remove(&page_index);
            return;
        };
        let projection = match Projection::columns(
            self.column_layout.visible().to_vec(),
            dataset.source.column_count(),
        ) {
            Ok(projection) => projection,
            Err(error) => {
                self.grid.requested.remove(&page_index);
                self.status = format!("page projection: {error:#}");
                return;
            }
        };
        match runtime.submit_page(
            Arc::clone(&dataset.source),
            page_index,
            projection,
            self.grid.generation,
        ) {
            Ok(task) => {
                self.pages.track(page_index, task);
            }
            Err(error) => {
                self.grid.requested.remove(&page_index);
                self.status = format!("queue page {page_index}: {error}");
            }
        }
    }

    fn cell_text(&mut self, row: u64, display_column: usize) -> Option<String> {
        let page_index = self.grid.page_for_row(row)?;
        self.request_page(page_index);
        if row.is_multiple_of(parquet_reader::PAGE_ROWS)
            && page_index.saturating_add(1) * parquet_reader::PAGE_ROWS < self.grid.rows
        {
            self.request_page(page_index.saturating_add(1));
        }
        let page = self.pages.get(page_index)?;
        let mut offset = (row - page.window.first_row) as usize;
        for batch in &page.batches {
            if offset < batch.num_rows() {
                return Some(format_cell_with_limit(
                    batch.column(display_column).as_ref(),
                    offset,
                    CELL_LIMIT,
                ));
            }
            offset -= batch.num_rows();
        }
        None
    }

    fn open_error(&mut self, error: String) {
        log::warn!("{error}");
        self.grid.loading = LoadState::Error(error.clone());
        self.status = error;
    }

    fn run_chart(&mut self) {
        let accumulator = match ChartAccumulator::new(self.chart.config.clone()) {
            Ok(accumulator) => accumulator,
            Err(error) => {
                self.chart.error = Some(format!("Chart setup: {error:#}"));
                return;
            }
        };
        if self.dataset.is_none() {
            self.chart.error = Some("Open a dataset before running a chart".to_string());
            return;
        }
        self.chart.cancel();
        self.chart.model = None;
        self.chart.error = None;
        let generation = self.chart.next_generation();

        if self.chart.source == ChartSource::CurrentResult {
            let result = (|| -> anyhow::Result<ChartModel> {
                let filtered = self
                    .filtered
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("run a filter, SQL query, or sort first"))?;
                let mut accumulator = accumulator;
                for batch in &filtered.batches {
                    if !accumulator.push_batch(batch)? {
                        break;
                    }
                }
                Ok(accumulator.finish())
            })();
            match result {
                Ok(model) => {
                    self.chart.status = chart_status(&model, "current bounded result window");
                    self.chart.model = Some(model);
                }
                Err(error) => self.chart.error = Some(format!("Chart data: {error:#}")),
            }
            return;
        }

        let sql = self.chart.sql_input.trim().to_owned();
        let Some((tables, _)) = self.sql_tables() else {
            return;
        };
        let plan = match SqlAst::parse(&sql).and_then(|ast| ast.to_logical_plan_with(&tables)) {
            Ok(plan) => plan,
            Err(error) => {
                self.chart.error = Some(format!("Chart SQL: {error:#}"));
                return;
            }
        };
        let Some(runtime) = &self.runtime else {
            self.chart.error = Some("Background runtime is unavailable".to_string());
            return;
        };
        match QueryEngine::new(runtime).execute(&plan, generation) {
            Ok(execution) => {
                self.chart.accumulator = Some(accumulator);
                self.chart.execution = Some(execution);
                self.chart.status = "Chart query running…".to_string();
            }
            Err(error) => self.chart.error = Some(format!("Start chart query: {error:#}")),
        }
    }

    fn cancel_chart(&mut self) {
        if self.chart.loading() {
            self.chart.cancel();
            self.chart.generation = GenerationId(self.chart.generation.0.saturating_add(1));
            self.chart.status = "Chart query cancelled".to_string();
        }
    }

    fn finish_chart(&mut self, source: &str) {
        let Some(accumulator) = self.chart.accumulator.take() else {
            return;
        };
        let model = accumulator.finish();
        self.chart.status = chart_status(&model, source);
        self.chart.model = Some(model);
    }

    fn poll_chart(&mut self) {
        while self.chart.loading() {
            let Some(execution) = &mut self.chart.execution else {
                break;
            };
            let poll = execution.poll_next_batch();
            match poll {
                Ok(QueryPoll::Pending) => break,
                Ok(QueryPoll::Finished) => {
                    self.chart.execution = None;
                    self.finish_chart("query result");
                }
                Ok(QueryPoll::Batch(batch)) if batch.generation_id == self.chart.generation => {
                    let Some(accumulator) = &mut self.chart.accumulator else {
                        self.chart.execution = None;
                        self.chart.error =
                            Some("Chart query lost its bounded accumulator".to_string());
                        return;
                    };
                    let accepted = match accumulator.push_batch(&batch.batch) {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            self.chart.execution = None;
                            self.chart.accumulator = None;
                            self.chart.error = Some(format!("Chart data: {error:#}"));
                            return;
                        }
                    };
                    if !accepted {
                        if let Some(execution) = &mut self.chart.execution {
                            execution.cancel();
                        }
                        self.chart.execution = None;
                        self.finish_chart("query result sample");
                    }
                }
                Ok(QueryPoll::Batch(_)) => {
                    self.chart.cancel();
                    self.chart.status = "Ignored stale chart result".to_string();
                }
                Err(error) => {
                    self.chart.execution = None;
                    self.chart.accumulator = None;
                    self.chart.error = Some(format!("Chart query: {error:#}"));
                }
            }
        }
    }

    fn run_profile(&mut self, column: usize) {
        let Some(dataset) = &self.dataset else {
            self.profile.error = Some("Open a dataset before profiling".to_string());
            return;
        };
        let Some(runtime) = self.runtime.as_ref().map(Runtime::handle) else {
            self.profile.error = Some("Background runtime is unavailable".to_string());
            return;
        };
        self.profile.cancel();
        self.profile.result = None;
        self.profile.error = None;
        let generation = self.profile.next_generation();
        match ProfileTask::start(Arc::clone(&dataset.source), column, runtime, generation) {
            Ok(task) => {
                self.profile.task = Some(task);
                self.profile.column = Some(column);
                self.profile.status = "Profiling column…".to_string();
            }
            Err(error) => self.profile.error = Some(format!("Start profile: {error}")),
        }
    }

    fn cancel_profile(&mut self) {
        if self.profile.loading() {
            self.profile.cancel();
            self.profile.generation = GenerationId(self.profile.generation.0.saturating_add(1));
            self.profile.status = "Profile cancelled".to_string();
        }
    }

    /// Profiles every column (up to `MAX_FILE_PROFILE_COLUMNS`) in one scan and
    /// shows the result in the Profile window.
    fn run_file_profile(&mut self) {
        let Some(dataset) = &self.dataset else {
            return;
        };
        let Some(runtime) = self.runtime.as_ref().map(Runtime::handle) else {
            self.profile.file.error = Some("Background runtime is unavailable".to_string());
            return;
        };
        let count = dataset.source.column_count();
        let columns = (0..count.min(MAX_FILE_PROFILE_COLUMNS)).collect::<Vec<_>>();
        let source = Arc::clone(&dataset.source);
        if let Some(task) = &self.profile.file.task {
            task.cancel();
        }
        let generation = self.profile.next_generation();
        self.profile.file.visible = true;
        self.profile.file.result = None;
        self.profile.file.error = None;
        self.profile.file.skipped_columns = count - columns.len();
        match FileProfileTask::start(source, columns, runtime, generation) {
            Ok(task) => self.profile.file.task = Some(task),
            Err(error) => {
                self.profile.file.task = None;
                self.profile.file.error = Some(format!("start profile: {error}"));
            }
        }
    }

    fn poll_file_profile(&mut self) {
        let Some(task) = &self.profile.file.task else {
            return;
        };
        let event = match task.try_recv() {
            Ok(Some(event)) => event,
            Ok(None) => return,
            Err(error) => {
                self.profile.file.task = None;
                self.profile.file.error = Some(format!("{error:#}"));
                return;
            }
        };
        let generation = match &event {
            FileProfileEvent::Finished { generation, .. }
            | FileProfileEvent::Cancelled { generation }
            | FileProfileEvent::Failed { generation, .. } => *generation,
        };
        if generation != self.profile.generation {
            return;
        }
        self.profile.file.task = None;
        match event {
            FileProfileEvent::Finished { profiles, .. } => {
                self.status = format!("Profiled {} columns", profiles.len());
                self.profile.file.result = Some(profiles);
            }
            FileProfileEvent::Cancelled { .. } => {
                self.profile.file.error = Some("Profile cancelled".to_string());
            }
            FileProfileEvent::Failed { error, .. } => {
                self.profile.file.error = Some(format!("Profile failed: {error}"));
            }
        }
    }

    fn poll_profile(&mut self) {
        let Some(task) = &self.profile.task else {
            return;
        };
        let event = match task.try_recv() {
            Ok(Some(event)) => event,
            Ok(None) => return,
            Err(error) => {
                self.profile.task = None;
                self.profile.error = Some(format!("Profile worker: {error:#}"));
                return;
            }
        };
        self.profile.task = None;
        match event {
            ProfileEvent::Finished {
                generation,
                profile,
            } if generation == self.profile.generation => {
                self.profile.status = format!("Profiled {} rows", profile.row_count);
                self.profile.result = Some(*profile);
            }
            ProfileEvent::Cancelled { generation } if generation == self.profile.generation => {
                self.profile.status = "Profile cancelled".to_string();
            }
            ProfileEvent::Failed { generation, error } if generation == self.profile.generation => {
                self.profile.error = Some(format!("Profile: {error}"));
            }
            _ => self.profile.status = "Ignored stale profile result".to_string(),
        }
    }

    fn selected_source_column(&self) -> Option<usize> {
        let (_, display_column) = self.grid.selection?;
        if let Some(filtered) = &self.filtered {
            return (!filtered.aggregate)
                .then(|| {
                    filtered
                        .display_columns
                        .get(display_column)
                        .map(|(source, _)| *source)
                })
                .flatten();
        }
        self.column_layout.visible().get(display_column).copied()
    }

    fn selected_cell_details(&self) -> Option<CellDetails> {
        let (row, display_column) = self.grid.selection?;
        let dataset = self.dataset.as_ref()?;
        if let Some(filtered) = &self.filtered {
            let (_, column) = *filtered.display_columns.get(display_column)?;
            let mut batch_offset = row.checked_sub(filtered.first_result)? as usize;
            for batch in &filtered.batches {
                if batch_offset < batch.num_rows() {
                    return Some(inspect_cell(
                        row,
                        display_column,
                        batch.schema().field(column).name(),
                        batch.column(column),
                        batch_offset,
                        self.show_safe_full_selection,
                        self.show_nested_selection,
                    ));
                }
                batch_offset = batch_offset.saturating_sub(batch.num_rows());
            }
            return None;
        }

        let page_index = self.grid.page_for_row(row)?;
        let page = self.pages.get(page_index)?;
        let mut batch_offset = (row - page.window.first_row) as usize;
        for batch in &page.batches {
            if batch_offset < batch.num_rows() {
                return Some(inspect_cell(
                    row,
                    display_column,
                    dataset
                        .column_names
                        .get(*self.column_layout.visible().get(display_column)?)?,
                    batch.column(display_column),
                    batch_offset,
                    self.show_safe_full_selection,
                    self.show_nested_selection,
                ));
            }
            batch_offset = batch_offset.saturating_sub(batch.num_rows());
        }
        None
    }

    fn filtered_cell_text(&self, row: u64, column: usize) -> Option<String> {
        let filtered = self.filtered.as_ref()?;
        let mut offset = row as usize;
        for batch in &filtered.batches {
            if offset < batch.num_rows() {
                return Some(format_cell_with_limit(
                    batch.column(column).as_ref(),
                    offset,
                    CELL_LIMIT,
                ));
            }
            offset -= batch.num_rows();
        }
        None
    }

    fn selected_batch_row(&self) -> Option<(arrow_array::RecordBatch, usize)> {
        let (selected_row, _) = self.grid.selection?;
        self.batch_row_at(selected_row)
    }

    fn batch_row_at(&self, selected_row: u64) -> Option<(arrow_array::RecordBatch, usize)> {
        if let Some(filtered) = &self.filtered {
            let mut row = selected_row.checked_sub(filtered.first_result)? as usize;
            for batch in &filtered.batches {
                if row < batch.num_rows() {
                    return Some((batch.clone(), row));
                }
                row -= batch.num_rows();
            }
            return None;
        }
        let page = self.pages.get(self.grid.page_for_row(selected_row)?)?;
        let mut row = (selected_row - page.window.first_row) as usize;
        for batch in &page.batches {
            if row < batch.num_rows() {
                return Some((batch.clone(), row));
            }
            row -= batch.num_rows();
        }
        None
    }

    fn visible_batch_columns(&self) -> Vec<usize> {
        if let Some(filtered) = &self.filtered {
            return filtered
                .display_columns
                .iter()
                .map(|(_, batch_column)| *batch_column)
                .collect();
        }
        (0..self.column_layout.visible().len()).collect()
    }

    fn copy_selection(&mut self, ctx: &egui::Context, row: bool) {
        self.copy_selection_as(ctx, row, CopyFormat::Tsv);
    }

    fn copy_selection_as(&mut self, ctx: &egui::Context, row: bool, format: CopyFormat) {
        let Some((selected_grid_row, selected_display_column)) = self.grid.selection else {
            return;
        };
        let visible_columns = self.visible_batch_columns();
        let range = if row {
            crate::state::SelectionRange {
                anchor: (selected_grid_row, 0),
                focus: (selected_grid_row, visible_columns.len().saturating_sub(1)),
            }
        } else {
            self.grid
                .selection_range
                .unwrap_or(crate::state::SelectionRange {
                    anchor: (selected_grid_row, selected_display_column),
                    focus: (selected_grid_row, selected_display_column),
                })
        };
        let first_row = range.anchor.0.min(range.focus.0);
        let last_row = range.anchor.0.max(range.focus.0);
        let first_column = range.anchor.1.min(range.focus.1);
        let last_column = range.anchor.1.max(range.focus.1);
        if format != CopyFormat::Tsv {
            let mut copy = crate::copy::CopyBuilder::new(format, MAX_CLIPBOARD_BYTES);
            for row_index in first_row..=last_row {
                let Some((batch, batch_row)) = self.batch_row_at(row_index) else {
                    self.status =
                        "Part of the selection is no longer loaded; select it again".to_string();
                    return;
                };
                let mut cells = Vec::new();
                for display_column in first_column..=last_column {
                    let Some(column) = visible_columns.get(display_column).copied() else {
                        self.status = "Selected column is unavailable".to_string();
                        return;
                    };
                    if column >= batch.num_columns() {
                        self.status = "Selected column is unavailable".to_string();
                        return;
                    }
                    cells.push(column);
                }
                if let Err(error) = copy.add_row(&batch, batch_row, &cells) {
                    self.status = format!("{error:#}");
                    return;
                }
            }
            let (text, status) = copy.finish();
            ctx.copy_text(text);
            self.status = status;
            return;
        }
        let mut text = String::new();
        for row_index in first_row..=last_row {
            let Some((batch, batch_row)) = self.batch_row_at(row_index) else {
                self.status =
                    "Part of the selection is no longer loaded; select it again".to_string();
                return;
            };
            if row_index > first_row {
                text.push('\n');
            }
            for display_column in first_column..=last_column {
                let Some(array) = visible_columns
                    .get(display_column)
                    .and_then(|column| batch.columns().get(*column))
                else {
                    self.status = "Selected column is unavailable".to_string();
                    return;
                };
                if display_column > first_column {
                    text.push('\t');
                }
                let room = MAX_CLIPBOARD_BYTES.saturating_sub(text.len());
                match clipboard_field(array.as_ref(), batch_row, room) {
                    Some(field) => text.push_str(&field),
                    None => {
                        self.status = format!(
                            "The selection is larger than the {} MB clipboard limit; \
                             select fewer cells or export instead",
                            MAX_CLIPBOARD_BYTES / (1024 * 1024)
                        );
                        return;
                    }
                }
            }
        }
        let cells =
            (last_row - first_row + 1).saturating_mul((last_column - first_column + 1) as u64);
        ctx.copy_text(text);
        self.status = if row {
            "Selected row copied to clipboard".to_string()
        } else if cells == 1 {
            "Cell copied to clipboard".to_string()
        } else {
            format!("{} cells copied to clipboard", ui::theme::number(cells))
        };
    }

    /// Counts each value of source column `source`: a SQL query grouped by the
    /// column, most frequent first, run in the SQL workspace.
    fn count_values(&mut self, source: usize) {
        let Some(name) = self
            .dataset
            .as_ref()
            .and_then(|dataset| dataset.column_names.get(source))
            .cloned()
        else {
            return;
        };
        self.sql_input = count_values_sql(&name);
        self.sql_completion = None;
        self.workspace = Workspace::Sql;
        self.run_sql();
    }

    /// Shows the find bar and puts the cursor in it.
    fn open_find(&mut self) {
        self.find.open = true;
        self.find.focus = true;
    }

    /// The loaded rows, in row order, for find.
    fn loaded_segments(&self) -> Vec<Segment<'_>> {
        let mut segments = Vec::new();
        if let Some(filtered) = &self.filtered {
            let mut first_row = filtered.first_result;
            for batch in &filtered.batches {
                segments.push(Segment { first_row, batch });
                first_row += batch.num_rows() as u64;
            }
            return segments;
        }
        for page in self.pages.loaded() {
            let mut first_row = page.window.first_row;
            for batch in &page.batches {
                segments.push(Segment { first_row, batch });
                first_row += batch.num_rows() as u64;
            }
        }
        segments.sort_by_key(|segment| segment.first_row);
        segments
    }

    /// Moves the selection to the next (or previous) loaded cell containing the
    /// find text, and scrolls it into view.
    fn find_next(&mut self, backwards: bool) {
        let query = self.find.query.trim().to_owned();
        if query.is_empty() {
            self.find.message = None;
            return;
        }
        // A search of the file for this text in this direction is already on its way.
        if self
            .find
            .searching
            .as_ref()
            .is_some_and(|search| search.query == query && search.backwards == backwards)
        {
            return;
        }
        self.find.cancel_search();
        let columns = self.visible_batch_columns();
        let from = self.find.resume.take().or(self.grid.selection);
        let outcome = {
            let segments = self.loaded_segments();
            let loaded = segments.first().zip(segments.last()).map(|(first, last)| {
                (
                    first.first_row,
                    last.first_row + last.batch.num_rows() as u64,
                )
            });
            (
                find(&segments, &columns, &query, from, backwards, MAX_FIND_CELLS),
                loaded,
            )
        };
        self.find.message = Some(match outcome {
            (FindOutcome::Found((row, column)), _) => {
                self.grid.select(row, column);
                self.jump_target = Some(row);
                self.reveal_column(column);
                let name = self.display_column_name(column).unwrap_or_default();
                format!("Row {}, {name}", ui::theme::number(row + 1))
            }
            (FindOutcome::Stopped(at), _) => {
                self.find.resume = Some(at);
                format!(
                    "Searched {} cells; press Enter to keep going",
                    ui::theme::number(MAX_FIND_CELLS as u64)
                )
            }
            // Every row of the file is loaded and none holds it.
            (FindOutcome::NotFound, Some((0, end)))
                if self.filtered.is_none() && end >= self.grid.rows =>
            {
                "Not in the file".to_string()
            }
            // Not in the rows loaded: the grid shows the file, so look through all of it.
            (FindOutcome::NotFound, _) if self.filtered.is_none() && self.dataset.is_some() => {
                self.search_file(query, from, backwards);
                "Searching the whole file…".to_string()
            }
            (FindOutcome::NotFound, Some((first, end))) if end > first => format!(
                "Not in the loaded rows ({}–{}); Show matching rows looks through the file",
                ui::theme::number(first + 1),
                ui::theme::number(end)
            ),
            (FindOutcome::NotFound, _) => "No rows are loaded yet".to_string(),
        });
    }

    /// Starts looking for `query` through the whole file from `from`, on a worker.
    fn search_file(&mut self, query: String, from: Option<(u64, usize)>, backwards: bool) {
        let (Some(dataset), Some(runtime)) = (&self.dataset, &self.runtime) else {
            return;
        };
        let columns = self.column_layout.visible().to_vec();
        let task = runtime.handle().submit_find_text(
            Arc::clone(&dataset.source),
            parquet_reader::value::TextNeedle::new(&query),
            columns.clone(),
            from,
            backwards,
            self.grid.generation,
        );
        match task {
            Ok(task) => {
                self.find.searching = Some(FileSearch {
                    task,
                    source: Arc::clone(&dataset.source),
                    query,
                    backwards,
                    columns,
                });
            }
            Err(error) => self.find.message = Some(format!("Can't search the file now: {error}")),
        }
    }

    /// Picks up a whole-file search when it ends, and shows how far it is.
    pub(super) fn poll_find(&mut self) {
        let Some(search) = &self.find.searching else {
            return;
        };
        let current = self
            .dataset
            .as_ref()
            .is_some_and(|dataset| Arc::ptr_eq(&dataset.source, &search.source));
        let columns_same = search.columns == self.column_layout.visible();
        let response = match search.task.try_recv() {
            Ok(response) => response,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                let rows = search.source.row_count().max(1);
                let looked = search.task.progress().scanned_rows().min(rows);
                self.find.message = Some(format!(
                    "Searching the whole file… {}%",
                    looked * 100 / rows
                ));
                return;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                self.find.searching = None;
                return;
            }
        };
        self.find.searching = None;
        if !current || !columns_same {
            self.find.message = None;
            return;
        }
        self.find.message = Some(match response.outcome {
            pavi_runtime::PageOutcome::FoundText(Some((row, column))) => {
                self.grid.select(row, column);
                self.jump_target = Some(row);
                self.reveal_column(column);
                let name = self.display_column_name(column).unwrap_or_default();
                format!("Row {}, {name}", ui::theme::number(row + 1))
            }
            pavi_runtime::PageOutcome::FoundText(None) => "Not in the file".to_string(),
            pavi_runtime::PageOutcome::ReadFailed(error) => format!("Search failed: {error:#}"),
            _ => return,
        });
    }

    /// Shows the rows where any column contains the find text: the filter
    /// `* contains text`, on top of the filter already applied (ADR 0022).
    fn show_find_matches(&mut self) {
        let query = self.find.query.trim();
        if query.is_empty() || self.dataset.is_none() {
            return;
        }
        let any = FilterExpr::AnyContains(query.to_string());
        let filter = match self.active_filter() {
            Some(existing) => existing.clone().and(any),
            None => any,
        };
        self.find.cancel_search();
        self.filter_input = filter.to_string();
        self.workspace = Workspace::Grid;
        self.apply_filter();
    }

    /// Name of a display column of the grid on screen.
    fn display_column_name(&self, column: usize) -> Option<String> {
        if let Some(filtered) = &self.filtered {
            if filtered.aggregate {
                return filtered.batches.front().and_then(|batch| {
                    batch
                        .schema()
                        .fields()
                        .get(column)
                        .map(|field| field.name().clone())
                });
            }
            let (source, _) = filtered.display_columns.get(column)?;
            return self.dataset.as_ref()?.column_names.get(*source).cloned();
        }
        let source = self.column_layout.visible().get(column)?;
        self.dataset.as_ref()?.column_names.get(*source).cloned()
    }

    /// Scrolls the grid sideways on the next frame so display column `column` is
    /// in view, right of the pinned columns.
    fn reveal_column(&mut self, column: usize) {
        let widths = self.display_column_widths();
        let pinned = self.pinned_columns();
        let left = widths[..column.min(widths.len())].iter().sum::<f32>();
        let right = left + widths.get(column).copied().unwrap_or(INITIAL_COLUMN_WIDTH);
        let pinned_width = widths[..pinned.min(widths.len())].iter().sum::<f32>();
        let (offset, width) = self.grid_view_x;
        if column < pinned || width <= 0.0 {
            return;
        }
        // `offset` and `width` are measured right of the row numbers.
        if left - pinned_width < offset || right > offset + width {
            self.grid_scroll_x = Some((left - pinned_width).max(0.0));
        }
    }

    /// Widths of the grid's display columns, as laid out.
    fn display_column_widths(&self) -> Vec<f32> {
        match &self.filtered {
            Some(filtered) if filtered.aggregate => {
                vec![INITIAL_COLUMN_WIDTH; filtered.display_columns.len()]
            }
            Some(filtered) => filtered
                .display_columns
                .iter()
                .map(|(source, _)| self.column_layout.width(*source, INITIAL_COLUMN_WIDTH))
                .collect(),
            None => self
                .column_layout
                .visible()
                .iter()
                .map(|source| self.column_layout.width(*source, INITIAL_COLUMN_WIDTH))
                .collect(),
        }
    }

    /// Display columns pinned to the left of the grid on screen.
    fn pinned_columns(&self) -> usize {
        match &self.filtered {
            Some(filtered) if filtered.aggregate => 0,
            Some(filtered) => self
                .column_layout
                .pinned()
                .min(filtered.display_columns.len()),
            None => self.column_layout.pinned(),
        }
    }

    /// The selected row, every shown column, for the Inspector's Row section.
    fn selected_row_view(&self) -> Option<RowView> {
        let (row, _) = self.grid.selection?;
        let (batch, offset) = self.batch_row_at(row)?;
        let fields = self
            .visible_batch_columns()
            .into_iter()
            .enumerate()
            .map(|(display, column)| {
                (
                    self.display_column_name(display).unwrap_or_default(),
                    column,
                )
            })
            .collect();
        Some(RowView {
            row,
            batch,
            offset,
            fields,
        })
    }
}

/// The full display text of one cell for the clipboard, quoted as spreadsheets
/// expect when it holds a tab, line break or quote. `None` when it does not fit in
/// `room` bytes.
/// The query "Count values" runs: each value of `column` and how many rows
/// hold it, most frequent first.
fn count_values_sql(column: &str) -> String {
    let quoted = format!("\"{}\"", column.replace('"', "\"\""));
    format!(
        "SELECT {quoted}, COUNT(*) AS count\nFROM dataset\nGROUP BY {quoted}\nORDER BY count DESC"
    )
}

fn clipboard_field(array: &dyn arrow_array::Array, row: usize, room: usize) -> Option<String> {
    // Formatting stops just past `room`, so a huge value is never built in full.
    let text = format_cell_with_limit(array, row, room.saturating_add(1));
    let text = if text.contains(['\t', '\n', '\r', '"']) {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text
    };
    (text.len() <= room).then_some(text)
}

fn chart_status(model: &ChartModel, source: &str) -> String {
    let mut status = format!(
        "{} chart: {} values from {source}",
        model.kind.label(),
        model.input_rows
    );
    if model.input_capped {
        status.push_str(&format!(" (input capped at {MAX_INPUT_ROWS})"));
    }
    if model.reduced {
        status.push_str(&format!(" (reduced to {} points)", model.output_len()));
    }
    if model.skipped_rows > 0 {
        status.push_str(&format!(
            " ({} null/non-finite skipped)",
            model.skipped_rows
        ));
    }
    status
}

impl eframe::App for PaviApp {
    fn save(&mut self, _storage: &mut dyn eframe::Storage) {
        self.persist_session();
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.apply_theme_choice(ctx);
        self.apply_zoom(ctx);
        self.pages.begin_frame();
        self.handle_global_shortcuts(ctx);
        self.open_dropped_files(ctx);
        self.poll_open();
        if self.filtered.is_none()
            && let Some(status) = self.pages.poll(&mut self.grid)
        {
            self.status = status;
        }
        self.poll_filtered();
        self.poll_chart();
        self.poll_profile();
        self.poll_file_profile();
        self.poll_export();
        self.poll_find();
        let next_check = self.poll_file_watch(Instant::now());
        self.show_top_bar(ctx);
        ui::toolbar::window_resize_handles(ctx);
        self.show_chart_window(ctx);
        self.show_file_profile_window(ctx);
        self.show_compare_window(ctx);
        self.show_import_window(ctx);
        ui::shortcuts::window(ctx, &mut self.show_shortcuts);
        let compression = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.source.compression());
        let jump =
            ui::status::show(
                ctx,
                ui::status::StatusBar {
                    status: &self.status,
                    activity: self.active_task_label(),
                    progress: self.scan_fraction(),
                    has_error: matches!(self.grid.loading, LoadState::Error(_))
                        || self.filter_error.is_some()
                        || self.sql_error.is_some(),
                    selection: self.grid.selection.map(|(row, column)| {
                        let (rows, columns) = self
                            .grid
                            .selection_range
                            .map_or((1, 1), crate::state::SelectionRange::size);
                        ui::status::SelectionSummary {
                            row,
                            column: self
                                .display_column_name(column)
                                .unwrap_or_else(|| format!("column {}", column + 1)),
                            rows,
                            columns,
                        }
                    }),
                    dataset: self.dataset.as_ref().zip(compression).map(
                        |(dataset, compression)| ui::status::DatasetSummary {
                            rows: self.filtered.as_ref().map_or(self.grid.rows, |filtered| {
                                filtered.total().unwrap_or_else(|| filtered.received_rows())
                            }),
                            columns: self.grid.columns,
                            row_groups: dataset.source.row_groups().len(),
                            compression,
                        },
                    ),
                    path: self.dataset.as_ref().map(|dataset| dataset.path.as_path()),
                    jump_input: &mut self.jump_row_input,
                },
            );
        if jump {
            self.jump_to_input();
        }
        self.show_inspector_panel(ctx);
        egui::CentralPanel::default()
            .frame(egui::Frame::new().fill(ui::theme::background()))
            .show(ctx, |ui| {
                // Each tab keeps its own scroll positions.
                ui.push_id(("tab", self.tab_id), |ui| match self.workspace {
                    Workspace::Grid => self.show_grid(ui),
                    Workspace::Sql => self.show_sql_workspace(ui),
                });
            });

        if self.opening.is_some()
            || self.pages.is_loading()
            || self.filtered.as_ref().is_some_and(FilteredGrid::needs_more)
            || self.chart.loading()
            || self.profile.loading()
            || self.export.is_running()
            || self.find.searching.is_some()
        {
            ctx.request_repaint_after(Duration::from_millis(16));
        } else if self
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.count.as_ref())
            .is_some_and(MatchCount::running)
        {
            // Only the match count runs: pick it up, and show its progress,
            // without redrawing every frame.
            ctx.request_repaint_after(COUNT_REPAINT);
        } else if let Some(next_check) = next_check {
            ctx.request_repaint_after(next_check);
        }
    }
}

/// Consumes a press of `key` with exactly `modifiers` held (Ctrl or Cmd as
/// `command`), and says whether there was one.
fn consume_exact_key(
    input: &mut egui::InputState,
    modifiers: egui::Modifiers,
    key: egui::Key,
) -> bool {
    let mut found = false;
    input.events.retain(|event| {
        let hit = !found
            && matches!(
                event,
                egui::Event::Key { key: pressed, pressed: true, modifiers: held, .. }
                    if *pressed == key && held.matches_exact(modifiers)
            );
        found |= hit;
        !hit
    });
    found
}

#[cfg(test)]
#[path = "app_tests.rs"]
mod tests;
