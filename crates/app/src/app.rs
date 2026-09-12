#[path = "ui/mod.rs"]
mod ui;

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use eframe::egui::{self, Align, Layout, RichText};
use egui_extras::{Column, TableBuilder};
use parquet_reader::{
    DataPage, NullOrder, ParquetSource, Projection, SortDirection, SortSpec,
    value::format_cell_with_limit,
};
use pavi_query::{Filter, LogicalPlan, Planner, QueryEngine, QueryExecution, QueryPoll, SqlAst};
use pavi_runtime::{
    GenerationId, OpenOutcome, OpenTask, PageOutcome, PageTask, Runtime, RuntimeConfig,
};

use crate::chart::{
    ChartAccumulator, ChartConfig, ChartKind, ChartModel, ChartValues, MAX_INPUT_ROWS,
};
use crate::export::{ExportEvent, ExportFormat, ExportInput, ExportTask};
use crate::inspector::{CellDetails, column_summary, inspect_cell, nested_schema_preview};
use crate::profile::{ColumnProfile, ProfileEvent, ProfileTask};
use crate::session::{GridLayoutPreference, MAX_LAYOUT_COLUMNS, SessionState, SessionStore};
use crate::state::{
    ColumnLayout, GridState, LoadState, ToolbarAvailability, ToolbarCommand, Workspace,
};

const ROW_HEIGHT: f32 = 28.0;
const ROW_NUMBER_WIDTH: f32 = 58.0;
const INITIAL_COLUMN_WIDTH: f32 = 110.0;
const MAX_UI_PAGES: usize = 8;
const MAX_FILTERED_BATCHES: usize = 8;
const CELL_LIMIT: usize = 256;
const MAX_CLIPBOARD_BYTES: usize = 64 * 1024;

fn ui_row_count(rows: u64) -> usize {
    rows.min(usize::MAX as u64) as usize
}

struct Dataset {
    source: Arc<ParquetSource>,
    path: PathBuf,
    column_names: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryKind {
    Filter,
    Sql,
    Sort,
}

impl QueryKind {
    fn label(self) -> &'static str {
        match self {
            Self::Filter => "Filter",
            Self::Sql => "SQL",
            Self::Sort => "Sorted",
        }
    }

    fn empty_message(self) -> &'static str {
        match self {
            Self::Filter => "No rows match the filter.",
            Self::Sql => "The query returned no rows.",
            Self::Sort => "The dataset has no rows.",
        }
    }
}

struct FilteredGrid {
    kind: QueryKind,
    plan: LogicalPlan,
    sort: Option<SortSpec>,
    aggregate: bool,
    projected_columns: Vec<usize>,
    display_columns: Vec<(usize, usize)>,
    execution: Option<QueryExecution>,
    batches: VecDeque<arrow_array::RecordBatch>,
    first_result: u64,
    rows: u64,
    target_result: u64,
    finished: bool,
    error: Option<String>,
}

impl FilteredGrid {
    fn new(
        kind: QueryKind,
        plan: LogicalPlan,
        sort: Option<SortSpec>,
        aggregate: bool,
        projected_columns: Vec<usize>,
        execution: QueryExecution,
    ) -> Self {
        Self {
            kind,
            plan,
            sort,
            aggregate,
            projected_columns,
            display_columns: Vec::new(),
            execution: Some(execution),
            batches: VecDeque::new(),
            first_result: 0,
            rows: 0,
            target_result: parquet_reader::PAGE_ROWS,
            finished: false,
            error: None,
        }
    }

    fn received_rows(&self) -> u64 {
        self.first_result.saturating_add(self.rows)
    }

    fn needs_more(&self) -> bool {
        !self.finished && self.received_rows() < self.target_result
    }

    fn request_more(&mut self) {
        self.target_result = self.target_result.max(
            self.received_rows()
                .saturating_add(parquet_reader::PAGE_ROWS),
        );
    }

    fn push(&mut self, batch: arrow_array::RecordBatch) {
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        self.batches.push_back(batch);
        while self.batches.len() > MAX_FILTERED_BATCHES {
            if let Some(batch) = self.batches.pop_front() {
                let rows = batch.num_rows() as u64;
                self.first_result = self.first_result.saturating_add(rows);
                self.rows = self.rows.saturating_sub(rows);
            }
        }
    }

    fn cancel(&mut self) {
        if let Some(execution) = &mut self.execution {
            execution.cancel();
        }
    }

    fn refresh_display_columns(&mut self, layout: &ColumnLayout) {
        self.display_columns = if self.aggregate {
            (0..self.projected_columns.len())
                .map(|column| (column, column))
                .collect()
        } else {
            layout
                .visible()
                .iter()
                .filter_map(|source| {
                    self.projected_columns
                        .iter()
                        .position(|column| column == source)
                        .map(|batch| (*source, batch))
                })
                .collect()
        };
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChartSource {
    Sql,
    CurrentResult,
}

impl ChartSource {
    fn label(self) -> &'static str {
        match self {
            Self::Sql => "Chart SQL",
            Self::CurrentResult => "Current result window",
        }
    }
}

struct ChartState {
    visible: bool,
    source: ChartSource,
    sql_input: String,
    config: ChartConfig,
    generation: GenerationId,
    execution: Option<QueryExecution>,
    accumulator: Option<ChartAccumulator>,
    model: Option<ChartModel>,
    error: Option<String>,
    status: String,
}

struct ProfileState {
    generation: GenerationId,
    task: Option<ProfileTask>,
    column: Option<usize>,
    result: Option<ColumnProfile>,
    error: Option<String>,
    status: String,
}

impl Default for ProfileState {
    fn default() -> Self {
        Self {
            generation: GenerationId(0),
            task: None,
            column: None,
            result: None,
            error: None,
            status: "Select a source column to profile".to_string(),
        }
    }
}

impl ProfileState {
    fn loading(&self) -> bool {
        self.task.is_some()
    }

    fn cancel(&mut self) {
        if let Some(task) = &self.task {
            task.cancel();
        }
        self.task = None;
    }

    fn reset(&mut self) {
        self.cancel();
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.column = None;
        self.result = None;
        self.error = None;
        self.status = "Dataset changed; profile cleared".to_string();
    }

    fn next_generation(&mut self) -> GenerationId {
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.generation
    }
}

impl Default for ChartState {
    fn default() -> Self {
        Self {
            visible: false,
            source: ChartSource::Sql,
            sql_input: "SELECT * FROM dataset".to_string(),
            config: ChartConfig::default(),
            generation: GenerationId(0),
            execution: None,
            accumulator: None,
            model: None,
            error: None,
            status: "Choose a chart source and columns".to_string(),
        }
    }
}

impl ChartState {
    fn loading(&self) -> bool {
        self.execution.is_some()
    }

    fn cancel(&mut self) {
        if let Some(execution) = &mut self.execution {
            execution.cancel();
        }
        self.execution = None;
        self.accumulator = None;
    }

    fn reset(&mut self) {
        self.cancel();
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.model = None;
        self.error = None;
        self.status = "Dataset changed; chart results were cleared".to_string();
    }

    fn next_generation(&mut self) -> GenerationId {
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.generation
    }
}

pub(crate) struct PaviApp {
    runtime: Option<Runtime>,
    grid: GridState,
    dataset: Option<Dataset>,
    opening: Option<OpenTask>,
    pending_pages: HashMap<u64, PageTask>,
    pages: HashMap<u64, DataPage>,
    page_order: VecDeque<u64>,
    filtered: Option<FilteredGrid>,
    column_layout: ColumnLayout,
    column_search: String,
    jump_row_input: String,
    jump_target: Option<u64>,
    export: Option<ExportTask>,
    export_generation: GenerationId,
    path_input: String,
    filter_input: String,
    filter_error: Option<String>,
    sql_input: String,
    sql_error: Option<String>,
    chart: ChartState,
    profile: ProfileState,
    session_store: SessionStore,
    session: SessionState,
    running_sql: Option<RunningSql>,
    show_inspector: bool,
    show_safe_full_selection: bool,
    show_nested_selection: bool,
    workspace: Workspace,
    show_filter_controls: bool,
    ui_initialized: bool,
    show_column_types: bool,
    freeze_header: bool,
    show_tools: bool,
    status: String,
}

struct RunningSql {
    text: String,
    started: Instant,
}

impl PaviApp {
    pub(crate) fn new(initial_path: Option<PathBuf>) -> Self {
        Self::new_with_store(
            initial_path,
            SessionStore::new(SessionStore::default_path()),
        )
    }

    fn new_with_store(initial_path: Option<PathBuf>, session_store: SessionStore) -> Self {
        let runtime = Runtime::new(RuntimeConfig::default());
        let session = session_store.load();
        let reopened_path = initial_path.or_else(|| session.last_opened_file.clone());
        let show_inspector = session.preferences.inspector_visible;
        let chart_visible = session.preferences.chart_visible;
        let mut app = Self {
            runtime: runtime.ok(),
            grid: GridState::default(),
            dataset: None,
            opening: None,
            pending_pages: HashMap::new(),
            pages: HashMap::new(),
            page_order: VecDeque::new(),
            filtered: None,
            column_layout: ColumnLayout::new(0, INITIAL_COLUMN_WIDTH),
            column_search: String::new(),
            jump_row_input: String::new(),
            jump_target: None,
            export: None,
            export_generation: GenerationId(0),
            path_input: reopened_path
                .as_ref()
                .map_or_else(String::new, |path| path.display().to_string()),
            filter_input: String::new(),
            filter_error: None,
            sql_input: session.sql_input.clone(),
            sql_error: None,
            chart: ChartState {
                visible: chart_visible,
                ..ChartState::default()
            },
            profile: ProfileState::default(),
            session_store,
            session,
            running_sql: None,
            show_inspector,
            show_safe_full_selection: false,
            show_nested_selection: false,
            workspace: Workspace::default(),
            show_filter_controls: false,
            ui_initialized: false,
            show_column_types: true,
            freeze_header: true,
            show_tools: false,
            status: "Choose a Parquet file to begin".to_string(),
        };
        if app.runtime.is_none() {
            app.grid.loading = LoadState::Error("start background runtime".to_string());
            app.status = "Unable to start the background runtime".to_string();
        } else if let Some(path) = reopened_path {
            app.begin_open(path);
        }
        app
    }

    fn persist_session(&mut self) {
        self.session.sql_input = self.sql_input.clone();
        self.session.preferences.inspector_visible = self.show_inspector;
        self.session.preferences.chart_visible = self.chart.visible;
        if let Err(error) = self.session_store.save(&self.session) {
            self.status = format!("save session: {error}");
        }
    }

    fn toolbar_availability(&self) -> ToolbarAvailability {
        ToolbarAvailability {
            has_dataset: self.dataset.is_some(),
            opening: self.opening.is_some(),
        }
    }

    fn active_task_label(&self) -> Option<&'static str> {
        if self.opening.is_some() {
            Some("Opening")
        } else if self.export.is_some() {
            Some("Exporting")
        } else if self.chart.loading() {
            Some("Building chart")
        } else if self.profile.loading() {
            Some("Profiling")
        } else if self.running_sql.is_some() {
            Some("Running SQL")
        } else if !self.pending_pages.is_empty() {
            Some("Loading pages")
        } else {
            None
        }
    }

    fn refresh_document(&mut self) {
        if let Some(path) = self.dataset.as_ref().map(|dataset| dataset.path.clone()) {
            self.begin_open(path);
        }
    }

    fn handle_global_shortcuts(&mut self, ctx: &egui::Context) {
        let command = ctx.input(|input| input.modifiers.command);
        if !command {
            return;
        }
        if ctx.input(|input| input.key_pressed(egui::Key::O))
            && let Some(path) = rfd::FileDialog::new()
                .add_filter("Parquet", &["parquet"])
                .pick_file()
        {
            self.begin_open(path);
        }
        if ctx.input(|input| input.key_pressed(egui::Key::Num1)) {
            self.workspace = Workspace::Grid;
        }
        if self.toolbar_availability().enabled(ToolbarCommand::Sql)
            && ctx.input(|input| input.key_pressed(egui::Key::Num2))
        {
            self.workspace = Workspace::Sql;
        }
        if ctx.input(|input| input.key_pressed(egui::Key::I)) {
            self.show_inspector = !self.show_inspector;
            self.persist_session();
        }
        if self.toolbar_availability().enabled(ToolbarCommand::Refresh)
            && ctx.input(|input| input.key_pressed(egui::Key::R))
        {
            self.refresh_document();
        }
        if self.toolbar_availability().enabled(ToolbarCommand::Filter)
            && ctx.input(|input| input.key_pressed(egui::Key::F))
        {
            self.workspace = Workspace::Grid;
            self.show_filter_controls = true;
            ctx.memory_mut(|memory| memory.request_focus(egui::Id::new("filter_search")));
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
                    .clamp(72.0, 480.0) as u16,
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
            for page in self.pages.values() {
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
        if let Some(task) = &self.opening {
            task.cancel();
        }
        self.opening = None;
        self.cancel_page_work();
        self.cancel_export();
        self.record_sql_history(false, None);
        self.cancel_filter();
        self.chart.reset();
        self.profile.reset();
        self.show_safe_full_selection = false;
        self.show_nested_selection = false;
        self.workspace = Workspace::Grid;
        self.show_filter_controls = false;
        self.dataset = None;
        self.filter_error = None;
        self.sql_error = None;
        let generation = self.grid.reset();
        self.path_input = path.display().to_string();
        self.status = format!("Opening {}…", path.display());

        let Some(runtime) = self.runtime.as_ref().map(Runtime::handle) else {
            self.open_error("background runtime is unavailable".to_string());
            return;
        };
        match runtime.submit_open(path, generation) {
            Ok(task) => self.opening = Some(task),
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
                self.open_error("background file open worker stopped".to_string());
                return;
            }
        };
        self.opening = None;
        if response.generation_id != self.grid.generation {
            return;
        }
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
                    path: PathBuf::from(&self.path_input),
                    column_names,
                });
                self.restore_column_layout();
                self.grid.ready(rows, self.column_layout.visible().len());
                self.session
                    .record_recent_file(PathBuf::from(&self.path_input));
                self.persist_session();
                if let Some(chart_columns) =
                    self.dataset.as_ref().map(|dataset| &dataset.column_names)
                {
                    self.chart.config.x_column = chart_columns.first().cloned().unwrap_or_default();
                    self.chart.config.y_column = chart_columns
                        .get(1)
                        .or_else(|| chart_columns.first())
                        .cloned()
                        .unwrap_or_default();
                }
                self.status = if rows == 0 {
                    "Opened empty dataset".to_string()
                } else {
                    format!("Ready: {rows} rows × {columns} columns")
                };
            }
            OpenOutcome::Cancelled => self.status = "File open cancelled".to_string(),
            OpenOutcome::OpenFailed(error) => {
                self.session
                    .remove_recent_file(std::path::Path::new(&self.path_input));
                self.persist_session();
                self.open_error(format!("open file: {error:#}"));
            }
        }
    }

    fn poll_pages(&mut self) {
        if self.filtered.is_some() {
            return;
        }
        let mut completed = Vec::new();
        for (&page, task) in &self.pending_pages {
            match task.try_recv() {
                Ok(response) => completed.push((page, Ok(response))),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    completed.push((page, Err("background page worker stopped".to_string())))
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
        }

        for (page_index, result) in completed {
            self.pending_pages.remove(&page_index);
            match result {
                Ok(response) if response.generation_id != self.grid.generation => {}
                Ok(response) => match response.outcome {
                    PageOutcome::Loaded(page)
                        if self.grid.accept_page(response.generation_id, page_index) =>
                    {
                        self.insert_page(page_index, page);
                    }
                    PageOutcome::Cancelled => {
                        self.grid.requested.remove(&page_index);
                    }
                    PageOutcome::ReadFailed(error) => {
                        self.grid.requested.remove(&page_index);
                        self.status = format!("load page {page_index}: {error:#}");
                    }
                    PageOutcome::Batch(_) | PageOutcome::Batches(_) | PageOutcome::Loaded(_) => {
                        self.grid.requested.remove(&page_index);
                        self.status = format!("unexpected response for page {page_index}");
                    }
                },
                Err(error) => {
                    self.grid.requested.remove(&page_index);
                    self.status = error;
                }
            }
        }
    }

    fn insert_page(&mut self, page_index: u64, page: DataPage) {
        self.pages.insert(page_index, page);
        self.page_order.retain(|index| *index != page_index);
        self.page_order.push_back(page_index);
        while self.page_order.len() > MAX_UI_PAGES {
            if let Some(oldest) = self.page_order.pop_front() {
                self.pages.remove(&oldest);
                self.grid.loaded.remove(&oldest);
            }
        }
    }

    fn cancel_page_work(&mut self) {
        for task in self.pending_pages.values() {
            task.cancel();
        }
        self.pending_pages.clear();
        self.pages.clear();
        self.page_order.clear();
    }

    fn next_export_generation(&mut self) -> GenerationId {
        self.export_generation = GenerationId(self.export_generation.0.saturating_add(1));
        self.export_generation
    }

    fn cancel_export(&mut self) {
        if let Some(task) = &self.export {
            task.cancel();
            self.status = "Export cancelled".to_string();
        }
        self.export = None;
        self.next_export_generation();
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
        let suggested = dataset
            .path
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
        self.cancel_export();
        let generation = self.next_export_generation();
        match ExportTask::start(input, path, format, runtime, generation) {
            Ok(task) => {
                self.export = Some(task);
                self.status = format!("Exporting {}…", format.label());
            }
            Err(error) => self.status = format!("Start export: {error}"),
        }
    }

    fn poll_export(&mut self) {
        let Some(task) = &self.export else {
            return;
        };
        let event = match task.try_recv() {
            Ok(Some(event)) => event,
            Ok(None) => return,
            Err(error) => {
                self.export = None;
                self.status = format!("Export: {error:#}");
                return;
            }
        };
        let generation = match &event {
            ExportEvent::Progress { generation, .. }
            | ExportEvent::Finished { generation, .. }
            | ExportEvent::Cancelled { generation }
            | ExportEvent::Failed { generation, .. } => *generation,
        };
        if generation != self.export_generation {
            return;
        }
        match event {
            ExportEvent::Progress { rows, .. } => {
                self.status = format!("Exporting: {rows} rows written");
            }
            ExportEvent::Finished { rows, .. } => {
                let path = self
                    .export
                    .as_ref()
                    .map(|task| task.path().display().to_string())
                    .unwrap_or_default();
                self.export = None;
                self.status = format!("Export complete: {rows} rows written to {path}");
            }
            ExportEvent::Cancelled { .. } => {
                self.export = None;
                self.status = "Export cancelled".to_string();
            }
            ExportEvent::Failed { error, .. } => {
                self.export = None;
                self.status = format!("Export failed: {error}");
            }
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
        let filter = match Filter::parse(&expression) {
            Ok(filter) => filter,
            Err(error) => {
                self.filter_error = Some(format!("Invalid filter: {error}"));
                return;
            }
        };
        let Some(dataset) = &self.dataset else {
            self.filter_error = Some("Open a dataset before applying a filter".to_string());
            return;
        };
        let source = Arc::clone(&dataset.source);
        let projected_columns = (0..source.column_count()).collect::<Vec<_>>();
        let plan = LogicalPlan::scan(source)
            .filter(filter)
            .project(projected_columns.clone());
        let sort = match Planner::plan(&plan) {
            Ok(plan) => plan.sort(),
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
                self.filtered = Some(FilteredGrid::new(
                    QueryKind::Filter,
                    plan,
                    sort,
                    false,
                    projected_columns,
                    execution,
                ));
                self.refresh_grid_columns();
                self.filter_error = None;
                self.status = format!("Filtering: {expression}");
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
    }

    fn run_sql(&mut self) {
        let sql = self.sql_input.trim().to_owned();
        if sql.is_empty() {
            self.sql_error = Some("Enter a SELECT query before running it".to_string());
            return;
        }
        let Some(dataset) = &self.dataset else {
            self.sql_error = Some("Open a dataset before running SQL".to_string());
            self.record_sql_failure(&sql);
            return;
        };
        let source = Arc::clone(&dataset.source);
        let plan =
            match SqlAst::parse(&sql).and_then(|ast| ast.to_logical_plan(Arc::clone(&source))) {
                Ok(plan) => plan,
                Err(error) => {
                    self.sql_error = Some(format!("SQL error: {error:#}"));
                    self.record_sql_failure(&sql);
                    return;
                }
            };
        let (projected_columns, sort, aggregate) = match Planner::plan(&plan) {
            Ok(plan) => {
                let aggregate = plan.aggregate();
                (
                    if let Some(aggregate) = aggregate {
                        (0..aggregate.expressions().len()
                            + usize::from(aggregate.group_by().is_some()))
                            .collect()
                    } else {
                        plan.projected_columns().to_vec()
                    },
                    plan.sort(),
                    aggregate.is_some(),
                )
            }
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

    fn toggle_sort(&mut self, column: usize) {
        let Some(dataset) = &self.dataset else {
            self.status = "Open a dataset before sorting".to_string();
            return;
        };
        let source = Arc::clone(&dataset.source);
        let column_count = source.column_count();
        let column_name = dataset.column_names.get(column).cloned();
        let Some(column_name) = column_name else {
            self.status = format!("Sort column {column} is out of range");
            return;
        };
        let (plan, kind, previous) = if let Some(filtered) = &self.filtered {
            (filtered.plan.clone(), filtered.kind, filtered.sort)
        } else {
            (
                LogicalPlan::scan(source).project((0..column_count).collect::<Vec<_>>()),
                QueryKind::Sort,
                None,
            )
        };
        let direction = match previous {
            Some(sort) if sort.column == column && sort.direction == SortDirection::Ascending => {
                SortDirection::Descending
            }
            _ => SortDirection::Ascending,
        };
        let plan = plan.replace_sort(column, direction, NullOrder::Last);
        let (projected_columns, sort) = match Planner::plan(&plan) {
            Ok(plan) => (plan.projected_columns().to_vec(), plan.sort()),
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
                self.filtered = Some(FilteredGrid::new(
                    kind,
                    plan,
                    sort,
                    false,
                    projected_columns,
                    execution,
                ));
                self.refresh_grid_columns();
                let arrow = match direction {
                    SortDirection::Ascending => "↑",
                    SortDirection::Descending => "↓",
                };
                self.status = format!("Sorting {column_name} {arrow} (nulls last)");
            }
            Err(error) => self.status = format!("Start sort: {error:#}"),
        }
    }

    fn sort_heading(&self, column: usize, name: &str) -> String {
        let Some(sort) = self.filtered.as_ref().and_then(|filtered| filtered.sort) else {
            return name.to_string();
        };
        if sort.column != column {
            return name.to_string();
        }
        let arrow = match sort.direction {
            SortDirection::Ascending => " ↑",
            SortDirection::Descending => " ↓",
        };
        format!("{name}{arrow}")
    }

    fn poll_filtered(&mut self) {
        let Some(filtered) = &mut self.filtered else {
            return;
        };
        let mut status = None;
        let mut sql_history = None;
        while filtered.needs_more() {
            let Some(execution) = &mut filtered.execution else {
                break;
            };
            match execution.poll_next_batch() {
                Ok(QueryPoll::Pending) => break,
                Ok(QueryPoll::Finished) => {
                    filtered.execution = None;
                    filtered.finished = true;
                    status = Some(if filtered.rows == 0 {
                        filtered.kind.empty_message().to_string()
                    } else {
                        format!(
                            "{} results: {} rows loaded",
                            filtered.kind.label(),
                            filtered.received_rows()
                        )
                    });
                    if filtered.kind == QueryKind::Sql {
                        sql_history = Some((true, Some(filtered.received_rows())));
                    }
                }
                Ok(QueryPoll::Batch(batch)) if batch.generation_id == self.grid.generation => {
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
        self.grid.rows = filtered.rows;
        if let Some(status) = status {
            self.status = status;
        }
        if let Some((success, row_count)) = sql_history {
            self.record_sql_history(success, row_count);
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
                self.pending_pages.insert(page_index, task);
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
        let page = self.pages.get(&page_index)?;
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
        let Some(dataset) = &self.dataset else {
            self.chart.error = Some("Open a dataset before running a chart".to_string());
            return;
        };
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

        let sql = self.chart.sql_input.trim();
        let source = Arc::clone(&dataset.source);
        let plan = match SqlAst::parse(sql).and_then(|ast| ast.to_logical_plan(source)) {
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
        let page = self.pages.get(&page_index)?;
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
        let page = self.pages.get(&self.grid.page_for_row(selected_row)?)?;
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
        let Some((batch, selected_row)) = self.selected_batch_row() else {
            self.status = "Selected row is no longer in the bounded grid window".to_string();
            return;
        };
        let Some((selected_grid_row, selected_display_column)) = self.grid.selection else {
            return;
        };
        let text = if row {
            let mut text = String::new();
            for (display_column, column) in self.visible_batch_columns().into_iter().enumerate() {
                let Some(array) = batch.columns().get(column) else {
                    self.status = "Selected column is unavailable".to_string();
                    return;
                };
                let value = format_cell_with_limit(array.as_ref(), selected_row, CELL_LIMIT);
                if text
                    .len()
                    .saturating_add(value.len())
                    .saturating_add(usize::from(display_column > 0))
                    > MAX_CLIPBOARD_BYTES
                {
                    self.status = format!(
                        "Selected row exceeds the {MAX_CLIPBOARD_BYTES}-byte clipboard limit"
                    );
                    return;
                }
                if display_column > 0 {
                    text.push('\t');
                }
                text.push_str(&value);
            }
            text
        } else {
            let range = self
                .grid
                .selection_range
                .unwrap_or(crate::state::SelectionRange {
                    anchor: (selected_grid_row, selected_display_column),
                    focus: (selected_grid_row, selected_display_column),
                });
            let first_row = range.anchor.0.min(range.focus.0);
            let last_row = range.anchor.0.max(range.focus.0);
            let first_column = range.anchor.1.min(range.focus.1);
            let last_column = range.anchor.1.max(range.focus.1);
            let visible_columns = self.visible_batch_columns();
            let mut text = String::new();
            for row_index in first_row..=last_row {
                let Some((range_batch, batch_row)) = self.batch_row_at(row_index) else {
                    self.status =
                        "Selection is outside the current bounded grid window".to_string();
                    return;
                };
                for display_column in first_column..=last_column {
                    let Some(array) = visible_columns
                        .get(display_column)
                        .and_then(|column| range_batch.columns().get(*column))
                    else {
                        self.status = "Selected column is unavailable".to_string();
                        return;
                    };
                    let value = format_cell_with_limit(array.as_ref(), batch_row, CELL_LIMIT);
                    let separator = usize::from(display_column > first_column)
                        + usize::from(row_index > first_row && display_column == first_column);
                    if text
                        .len()
                        .saturating_add(value.len())
                        .saturating_add(separator)
                        > MAX_CLIPBOARD_BYTES
                    {
                        self.status = format!(
                            "Selection exceeds the {MAX_CLIPBOARD_BYTES}-byte clipboard limit"
                        );
                        return;
                    }
                    if display_column > first_column {
                        text.push('\t');
                    }
                    if row_index > first_row && display_column == first_column {
                        text.push('\n');
                    }
                    text.push_str(&value);
                }
            }
            text
        };
        ctx.copy_text(text);
        self.status = if row {
            "Selected row copied to clipboard".to_string()
        } else {
            "Selected cells copied to clipboard".to_string()
        };
    }
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
        if !self.ui_initialized {
            ui::theme::apply(ctx);
            self.ui_initialized = true;
        }
        self.handle_global_shortcuts(ctx);
        self.poll_open();
        self.poll_pages();
        self.poll_filtered();
        self.poll_chart();
        self.poll_profile();
        self.poll_export();
        self.show_top_bar(ctx);
        self.show_chart_window(ctx);
        ui::status::show(
            ctx,
            ui::status::StatusBar {
                status: &self.status,
                activity: self.active_task_label(),
                has_error: matches!(self.grid.loading, LoadState::Error(_))
                    || self.filter_error.is_some()
                    || self.sql_error.is_some(),
                selection: self.grid.selection,
                cached_pages: self.pages.len(),
                rows: self.grid.rows,
                path: self.dataset.as_ref().map(|dataset| dataset.path.as_path()),
                columns: self.dataset.as_ref().map(|_| self.grid.columns),
            },
        );
        self.show_inspector_panel(ctx);
        self.show_tools_window(ctx);
        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(ui::theme::BACKGROUND)
                    .inner_margin(ui::spacing::Spacing::Md.margin()),
            )
            .show(ctx, |ui| match self.workspace {
                Workspace::Grid => self.show_grid(ui),
                Workspace::Sql => self.show_sql_workspace(ui),
            });

        if self.opening.is_some()
            || !self.pending_pages.is_empty()
            || self.filtered.as_ref().is_some_and(FilteredGrid::needs_more)
            || self.chart.loading()
            || self.profile.loading()
            || self.export.is_some()
        {
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }
}

#[cfg(test)]
#[path = "app_tests.rs"]
mod tests;
