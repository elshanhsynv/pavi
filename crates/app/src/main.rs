mod chart;
mod export;
mod inspector;
mod session;
mod state;

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
use crate::inspector::{CellDetails, column_summary, inspect_cell};
use crate::session::{SessionState, SessionStore};
use crate::state::{GridState, LoadState};

const ROW_HEIGHT: f32 = 22.0;
const ROW_NUMBER_WIDTH: f32 = 72.0;
const INITIAL_COLUMN_WIDTH: f32 = 130.0;
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

struct PaviApp {
    runtime: Option<Runtime>,
    grid: GridState,
    dataset: Option<Dataset>,
    opening: Option<OpenTask>,
    pending_pages: HashMap<u64, PageTask>,
    pages: HashMap<u64, DataPage>,
    page_order: VecDeque<u64>,
    filtered: Option<FilteredGrid>,
    export: Option<ExportTask>,
    export_generation: GenerationId,
    path_input: String,
    filter_input: String,
    filter_error: Option<String>,
    sql_input: String,
    sql_error: Option<String>,
    chart: ChartState,
    session_store: SessionStore,
    session: SessionState,
    running_sql: Option<RunningSql>,
    show_inspector: bool,
    show_safe_full_selection: bool,
    status: String,
}

struct RunningSql {
    text: String,
    started: Instant,
}

impl PaviApp {
    fn new(initial_path: Option<PathBuf>) -> Self {
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
            session_store,
            session,
            running_sql: None,
            show_inspector,
            show_safe_full_selection: false,
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
        self.show_safe_full_selection = false;
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
                self.grid.ready(rows, columns);
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
            self.grid
                .ready(dataset.source.row_count(), dataset.source.column_count());
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
            self.grid
                .ready(dataset.source.row_count(), dataset.source.column_count());
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
        let projection = Projection::all(dataset.source.column_count());
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

    fn cell_text(&mut self, row: u64, column: usize) -> Option<String> {
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
                    batch.column(column).as_ref(),
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

    fn show_top_bar(&mut self, ctx: &egui::Context) {
        let mut open_recent = None;
        let mut rerun_history = None;
        let mut remove_history = None;
        let mut clear_history = false;
        let mut preferences_changed = false;
        let mut export_action = None;
        let mut copy_selection = None;
        let mut cancel_export = false;
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Open Parquet…").clicked()
                    && let Some(path) = rfd::FileDialog::new()
                        .add_filter("Parquet", &["parquet"])
                        .pick_file()
                {
                    self.begin_open(path);
                }
                ui.label("Path:");
                let response = ui.text_edit_singleline(&mut self.path_input);
                if (response.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)))
                    || ui.button("Open").clicked()
                {
                    let path = self.path_input.trim();
                    if !path.is_empty() {
                        self.begin_open(PathBuf::from(path));
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.label("Filter:");
                let response = ui.text_edit_singleline(&mut self.filter_input);
                let apply = (response.lost_focus()
                    && ui.input(|input| input.key_pressed(egui::Key::Enter)))
                    || ui.button("Apply").clicked();
                if apply {
                    self.apply_filter();
                }
                if ui.button("Clear").clicked() {
                    self.clear_filter();
                }
                ui.label("column == value, !=, contains, >, >=, <, <=");
                if let Some(error) = &self.filter_error {
                    ui.label(RichText::new(error).color(egui::Color32::RED));
                }
            });
            ui.collapsing("SQL", |ui| {
                ui.add(
                    egui::TextEdit::multiline(&mut self.sql_input)
                        .code_editor()
                        .desired_rows(3)
                        .desired_width(f32::INFINITY)
                        .hint_text("SELECT category, COUNT(*) FROM dataset GROUP BY category"),
                );
                ui.horizontal(|ui| {
                    if ui.button("Run SQL").clicked() {
                        self.run_sql();
                    }
                    if ui.button("Cancel SQL").clicked() {
                        self.cancel_sql();
                    }
                    ui.label("SELECT … FROM dataset [WHERE …] [GROUP BY column] [LIMIT n]");
                    if let Some(error) = &self.sql_error {
                        ui.label(RichText::new(error).color(egui::Color32::RED));
                    }
                });
            });
            ui.collapsing("Recent Files", |ui| {
                if self.session.recent_files.is_empty() {
                    ui.label("No recent available files.");
                }
                for path in &self.session.recent_files {
                    if ui.button(path.display().to_string()).clicked() {
                        open_recent = Some(path.clone());
                    }
                }
            });
            ui.collapsing("Query History", |ui| {
                if ui.button("Clear history").clicked() {
                    clear_history = true;
                }
                if self.session.query_history.is_empty() {
                    ui.label("No completed SQL queries.");
                }
                for (index, entry) in self.session.query_history.iter().enumerate() {
                    ui.horizontal(|ui| {
                        let result = if entry.success { "ok" } else { "failed" };
                        ui.label(format!(
                            "{result} · {} ms{}",
                            entry.duration_ms,
                            entry
                                .row_count
                                .map_or_else(String::new, |rows| format!(" · {rows} rows"))
                        ));
                        if ui.button("Run").clicked() {
                            rerun_history = Some(entry.sql.clone());
                        }
                        if ui.small_button("Remove").clicked() {
                            remove_history = Some(index);
                        }
                    });
                    ui.monospace(&entry.sql);
                    ui.separator();
                }
            });
            ui.horizontal(|ui| {
                ui.menu_button("Export", |ui| {
                    let available = self.dataset.is_some();
                    if ui
                        .add_enabled(available, egui::Button::new("Current result as CSV…"))
                        .clicked()
                    {
                        export_action = Some((ExportFormat::Csv, false));
                        ui.close_menu();
                    }
                    if ui
                        .add_enabled(available, egui::Button::new("Current result as Parquet…"))
                        .clicked()
                    {
                        export_action = Some((ExportFormat::Parquet, false));
                        ui.close_menu();
                    }
                    ui.separator();
                    if ui
                        .add_enabled(
                            self.selected_batch_row().is_some(),
                            egui::Button::new("Selected row as CSV…"),
                        )
                        .clicked()
                    {
                        export_action = Some((ExportFormat::Csv, true));
                        ui.close_menu();
                    }
                    if ui
                        .add_enabled(
                            self.selected_batch_row().is_some(),
                            egui::Button::new("Selected row as Parquet…"),
                        )
                        .clicked()
                    {
                        export_action = Some((ExportFormat::Parquet, true));
                        ui.close_menu();
                    }
                });
                if ui
                    .add_enabled(
                        self.selected_batch_row().is_some(),
                        egui::Button::new("Copy cell"),
                    )
                    .clicked()
                {
                    copy_selection = Some(false);
                }
                if ui
                    .add_enabled(
                        self.selected_batch_row().is_some(),
                        egui::Button::new("Copy row"),
                    )
                    .clicked()
                {
                    copy_selection = Some(true);
                }
                if self.export.is_some() {
                    ui.spinner();
                    if ui.button("Cancel export").clicked() {
                        cancel_export = true;
                    }
                }
            });
            ui.horizontal(|ui| {
                if ui
                    .button(if self.show_inspector {
                        "Hide Inspector"
                    } else {
                        "Show Inspector"
                    })
                    .clicked()
                {
                    self.show_inspector = !self.show_inspector;
                    preferences_changed = true;
                }
                if ui.button("Charts…").clicked() {
                    self.chart.visible = true;
                    preferences_changed = true;
                }
                if self.chart.loading() {
                    ui.spinner();
                    ui.label("Chart query loading");
                }
            });
        });
        if let Some(path) = open_recent {
            self.begin_open(path);
        }
        if let Some(sql) = rerun_history {
            self.sql_input = sql;
            self.run_sql();
        }
        if let Some(index) = remove_history {
            self.session.remove_history(index);
            self.persist_session();
        }
        if clear_history {
            self.session.clear_history();
            self.persist_session();
        }
        if preferences_changed {
            self.persist_session();
        }
        if let Some((format, selected)) = export_action {
            self.start_export(format, selected);
        }
        if let Some(row) = copy_selection {
            self.copy_selection(ctx, row);
        }
        if cancel_export {
            self.cancel_export();
        }
    }

    fn show_chart_window(&mut self, ctx: &egui::Context) {
        if !self.chart.visible {
            return;
        }
        let names = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
            .unwrap_or_default();
        let chart_was_visible = self.chart.visible;
        let mut open = chart_was_visible;
        let mut run = false;
        let mut cancel = false;
        egui::Window::new("Charts")
            .open(&mut open)
            .default_size([620.0, 520.0])
            .min_size([380.0, 280.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Source:");
                    egui::ComboBox::from_id_salt("chart_source")
                        .selected_text(self.chart.source.label())
                        .show_ui(ui, |ui| {
                            ui.selectable_value(
                                &mut self.chart.source,
                                ChartSource::Sql,
                                ChartSource::Sql.label(),
                            );
                            ui.selectable_value(
                                &mut self.chart.source,
                                ChartSource::CurrentResult,
                                ChartSource::CurrentResult.label(),
                            );
                        });
                    ui.label("Current result uses only its bounded grid window.");
                });
                if self.chart.source == ChartSource::Sql {
                    ui.add(
                        egui::TextEdit::multiline(&mut self.chart.sql_input)
                            .code_editor()
                            .desired_rows(3)
                            .desired_width(f32::INFINITY)
                            .hint_text("SELECT x, y FROM dataset WHERE …"),
                    );
                }
                ui.horizontal(|ui| {
                    ui.label("Type:");
                    egui::ComboBox::from_id_salt("chart_kind")
                        .selected_text(self.chart.config.kind.label())
                        .show_ui(ui, |ui| {
                            for kind in ChartKind::ALL {
                                ui.selectable_value(
                                    &mut self.chart.config.kind,
                                    kind,
                                    kind.label(),
                                );
                            }
                        });
                    ui.label(if self.chart.config.kind == ChartKind::Histogram {
                        "Value output:"
                    } else {
                        "X output:"
                    });
                    ui.text_edit_singleline(&mut self.chart.config.x_column);
                    egui::ComboBox::from_id_salt("chart_x_column")
                        .selected_text("Choose")
                        .show_ui(ui, |ui| {
                            for name in &names {
                                ui.selectable_value(
                                    &mut self.chart.config.x_column,
                                    name.clone(),
                                    name,
                                );
                            }
                        });
                });
                if self.chart.config.kind != ChartKind::Histogram {
                    ui.horizontal(|ui| {
                        ui.label("Y output:");
                        ui.text_edit_singleline(&mut self.chart.config.y_column);
                        egui::ComboBox::from_id_salt("chart_y_column")
                            .selected_text("Choose")
                            .show_ui(ui, |ui| {
                                for name in &names {
                                    ui.selectable_value(
                                        &mut self.chart.config.y_column,
                                        name.clone(),
                                        name,
                                    );
                                }
                            });
                    });
                }
                ui.horizontal(|ui| {
                    ui.label("Title:");
                    ui.text_edit_singleline(&mut self.chart.config.title);
                    ui.label("Point limit:");
                    ui.add(
                        egui::DragValue::new(&mut self.chart.config.point_limit)
                            .range(2..=MAX_INPUT_ROWS)
                            .speed(10),
                    );
                    if self.chart.config.kind == ChartKind::Histogram {
                        ui.label("Bins:");
                        ui.add(egui::DragValue::new(&mut self.chart.config.bins).range(1..=100));
                    }
                });
                ui.horizontal(|ui| {
                    if ui.button("Run / Refresh").clicked() {
                        run = true;
                    }
                    if ui
                        .add_enabled(self.chart.loading(), egui::Button::new("Cancel"))
                        .clicked()
                    {
                        cancel = true;
                    }
                    ui.label(&self.chart.status);
                });
                if let Some(error) = &self.chart.error {
                    ui.label(RichText::new(error).color(egui::Color32::RED));
                }
                if let Some(model) = &self.chart.model {
                    chart_notice(ui, model);
                    draw_chart(ui, model, &self.chart.config);
                } else if self.chart.loading() {
                    ui.centered_and_justified(|ui| ui.spinner());
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.label("Run a bounded SQL query or use the current result window.")
                    });
                }
            });
        self.chart.visible = open;
        if self.chart.visible != chart_was_visible {
            self.persist_session();
        }
        if cancel {
            self.cancel_chart();
        }
        if run {
            self.run_chart();
        }
    }

    fn selected_source_column(&self) -> Option<usize> {
        let (_, column) = self.grid.selection?;
        self.filtered.as_ref().map_or(Some(column), |filtered| {
            (!filtered.aggregate)
                .then(|| filtered.projected_columns.get(column).copied())
                .flatten()
        })
    }

    fn selected_cell_details(&self) -> Option<CellDetails> {
        let (row, column) = self.grid.selection?;
        let dataset = self.dataset.as_ref()?;
        if let Some(filtered) = &self.filtered {
            let mut batch_offset = row.checked_sub(filtered.first_result)? as usize;
            for batch in &filtered.batches {
                if batch_offset < batch.num_rows() {
                    return Some(inspect_cell(
                        row,
                        column,
                        batch.schema().field(column).name(),
                        batch.column(column),
                        batch_offset,
                        self.show_safe_full_selection,
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
                    column,
                    dataset.column_names.get(column)?,
                    batch.column(column),
                    batch_offset,
                    self.show_safe_full_selection,
                ));
            }
            batch_offset = batch_offset.saturating_sub(batch.num_rows());
        }
        None
    }

    fn show_metadata(&mut self, ctx: &egui::Context) {
        if !self.show_inspector {
            return;
        }
        let safe_value_was_visible = self.show_safe_full_selection;
        let selected_column = self.selected_source_column();
        let selected_cell = self.selected_cell_details();
        egui::SidePanel::left("inspector")
            .default_width(270.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("Inspector");
                if let Some(dataset) = &self.dataset {
                    let metadata = dataset.source.metadata();
                    ui.collapsing("Dataset", |ui| {
                        ui.label(dataset.path.display().to_string());
                        ui.label(format!("Rows: {}", metadata.row_count));
                        ui.label(format!("Columns: {}", metadata.column_count));
                        ui.label(format!("Row groups: {}", metadata.row_groups.len()));
                    });
                    ui.collapsing("Schema", |ui| {
                        egui::ScrollArea::vertical()
                            .id_salt("inspector_schema")
                            .max_height(180.0)
                            .show_rows(ui, 34.0, metadata.columns().len(), |ui, rows| {
                                for index in rows {
                                    let column = &metadata.columns()[index];
                                    ui.label(format!(
                                        "{}: {}\n  {:?} · {}",
                                        column.index,
                                        column.name,
                                        column.data_type,
                                        if column.nullable { "nullable" } else { "required" },
                                    ));
                                }
                            });
                    });
                    ui.collapsing("Column", |ui| {
                        let Some(index) = selected_column else {
                            ui.label("Select a source-grid cell to inspect its column.");
                            return;
                        };
                        let Some(column) = metadata.columns().get(index) else {
                            ui.label("Column metadata is unavailable for this result column.");
                            return;
                        };
                        let summary = column_summary(column);
                        ui.label(format!("{}: {}", summary.index, summary.name));
                        ui.label(&summary.data_type);
                        ui.label(if summary.nullable { "Nullable" } else { "Required" });
                        ui.label(format!(
                            "Statistics: {}/{} row groups",
                            summary.available_statistics, summary.row_groups
                        ));
                        match summary.null_count {
                            Some(count) => ui.label(format!("Null count: {count}")),
                            None => ui.label("Null count: unavailable"),
                        };
                        egui::ScrollArea::vertical()
                            .id_salt("inspector_statistics")
                            .max_height(150.0)
                            .show_rows(ui, 42.0, column.row_group_statistics.len(), |ui, rows| {
                                for row_group in rows {
                                    match &column.row_group_statistics[row_group] {
                                        Some(statistics) => ui.label(format!(
                                            "Group {row_group}: min {} · max {}\nnulls: {} · distinct: {}",
                                            statistics.min.as_deref().unwrap_or("unavailable"),
                                            statistics.max.as_deref().unwrap_or("unavailable"),
                                            statistics.null_count.map_or_else(|| "unavailable".to_string(), |count| count.to_string()),
                                            statistics.distinct_count.map_or_else(|| "unavailable".to_string(), |count| count.to_string()),
                                        )),
                                        None => ui.label(format!("Group {row_group}: statistics unavailable")),
                                    };
                                }
                            });
                    });
                    ui.collapsing("Row Groups", |ui| {
                        egui::ScrollArea::vertical()
                            .id_salt("inspector_row_groups")
                            .max_height(180.0)
                            .show_rows(ui, 20.0, metadata.row_groups.len(), |ui, rows| {
                                for index in rows {
                                    let group = &metadata.row_groups[index];
                                    ui.label(format!(
                                        "{}: rows {}–{} ({})",
                                        group.index,
                                        group.first_row + 1,
                                        group.first_row.saturating_add(group.row_count),
                                        group.row_count
                                    ));
                                }
                            });
                    });
                    ui.collapsing("Selection", |ui| {
                        ui.checkbox(
                            &mut self.show_safe_full_selection,
                            "Show safe full scalar value",
                        );
                        match selected_cell.as_ref() {
                            Some(cell) => {
                                ui.label(format!("Row {} · column {}", cell.row + 1, cell.column + 1));
                                ui.label(format!("{} · {}", cell.name, cell.data_type));
                                ui.label(if cell.is_null { "Null" } else { "Non-null" });
                                ui.label(format!("Value: {}", cell.value));
                                if self.show_safe_full_selection && cell.full_value.is_none() && !cell.is_null {
                                    ui.label("Full value is unavailable for variable or unsupported types.");
                                }
                                if let Some(value) = &cell.full_value {
                                    ui.label(format!("Full scalar: {value}"));
                                }
                            }
                            None if self.grid.selection.is_some() => {
                                ui.label("Selected value is outside the current bounded page/result window.");
                            }
                            None => {
                                ui.label("Select a cell to inspect it.");
                            }
                        }
                    });
                } else {
                    ui.label("No file open");
                }
            });
        if self.show_safe_full_selection != safe_value_was_visible {
            self.persist_session();
        }
    }

    fn show_grid(&mut self, ui: &mut egui::Ui) {
        if self.filtered.is_some() {
            self.show_filtered_grid(ui);
            return;
        }
        let Some(dataset) = &self.dataset else {
            match &self.grid.loading {
                LoadState::Opening => {
                    ui.centered_and_justified(|ui| ui.spinner());
                }
                LoadState::Error(error) => {
                    ui.centered_and_justified(|ui| {
                        ui.label(RichText::new(error).color(egui::Color32::RED))
                    });
                }
                _ => {
                    ui.centered_and_justified(|ui| ui.label("Open a Parquet file to explore it."));
                }
            }
            return;
        };
        if self.grid.rows == 0 {
            ui.centered_and_justified(|ui| ui.label("This dataset has no rows."));
            return;
        }

        let names = dataset.column_names.clone();
        let columns = names.len();
        egui::ScrollArea::horizontal()
            .id_salt("grid_horizontal")
            .show(ui, |ui| {
                ui.set_min_width(
                    (columns as f32 * INITIAL_COLUMN_WIDTH + ROW_NUMBER_WIDTH)
                        .max(ui.available_width()),
                );
                TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(Layout::left_to_right(Align::Center))
                    .column(Column::exact(ROW_NUMBER_WIDTH))
                    .columns(
                        Column::initial(INITIAL_COLUMN_WIDTH).at_least(72.0),
                        columns,
                    )
                    .header(ROW_HEIGHT, |mut header| {
                        header.col(|ui| {
                            ui.strong("Row");
                        });
                        for (column, name) in names.iter().enumerate() {
                            let heading = self.sort_heading(column, name);
                            header.col(|ui| {
                                if ui.button(heading).clicked() {
                                    self.toggle_sort(column);
                                }
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(ROW_HEIGHT, ui_row_count(self.grid.rows), |mut table_row| {
                            let row_index = table_row.index() as u64;
                            for page in self.grid.visible_pages(row_index, row_index) {
                                self.request_page(page);
                            }
                            table_row.col(|ui| {
                                ui.label((row_index + 1).to_string());
                            });
                            for column in 0..columns {
                                table_row.col(|ui| {
                                    let selected = self.grid.selection == Some((row_index, column));
                                    let text = self
                                        .cell_text(row_index, column)
                                        .unwrap_or_else(|| "…".to_string());
                                    if ui.selectable_label(selected, text).clicked() {
                                        self.grid.select(row_index, column);
                                    }
                                });
                            }
                        });
                    });
            });
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

    fn copy_selection(&mut self, ctx: &egui::Context, row: bool) {
        let Some((batch, selected_row)) = self.selected_batch_row() else {
            self.status = "Selected row is no longer in the bounded grid window".to_string();
            return;
        };
        let Some((_, selected_column)) = self.grid.selection else {
            return;
        };
        let text = if row {
            let mut text = String::new();
            for column in 0..batch.num_columns() {
                let value =
                    format_cell_with_limit(batch.column(column).as_ref(), selected_row, CELL_LIMIT);
                if text
                    .len()
                    .saturating_add(value.len())
                    .saturating_add(usize::from(column > 0))
                    > MAX_CLIPBOARD_BYTES
                {
                    self.status = format!(
                        "Selected row exceeds the {MAX_CLIPBOARD_BYTES}-byte clipboard limit"
                    );
                    return;
                }
                if column > 0 {
                    text.push('\t');
                }
                text.push_str(&value);
            }
            text
        } else {
            let Some(column) = batch.columns().get(selected_column) else {
                self.status = "Selected column is unavailable".to_string();
                return;
            };
            format_cell_with_limit(column.as_ref(), selected_row, CELL_LIMIT)
        };
        ctx.copy_text(text);
        self.status = if row {
            "Selected row copied to clipboard".to_string()
        } else {
            "Selected cell copied to clipboard".to_string()
        };
    }

    fn show_filtered_grid(&mut self, ui: &mut egui::Ui) {
        let Some(dataset) = &self.dataset else {
            return;
        };
        let Some(filtered) = self.filtered.as_ref() else {
            return;
        };
        let first_result = filtered.first_result;
        let rows = filtered.rows;
        let finished = filtered.finished;
        let error = filtered.error.clone();
        let kind = filtered.kind;
        let aggregate = filtered.aggregate;
        let projected_columns = filtered.projected_columns.clone();
        if rows == 0 {
            ui.centered_and_justified(|ui| {
                if let Some(error) = error {
                    ui.label(RichText::new(error).color(egui::Color32::RED));
                } else if finished {
                    ui.label(kind.empty_message());
                } else {
                    ui.spinner();
                    ui.label(format!("{} query running…", kind.label()));
                }
            });
            return;
        }

        let names = if aggregate {
            filtered
                .batches
                .front()
                .map(|batch| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().to_owned())
                        .collect()
                })
                .unwrap_or_default()
        } else {
            projected_columns
                .iter()
                .map(|column| dataset.column_names[*column].clone())
                .collect::<Vec<_>>()
        };
        let columns = names.len();
        if let Some(error) = error {
            ui.label(RichText::new(error).color(egui::Color32::RED));
        }
        ui.label(format!(
            "{} rows {}–{} (bounded window)",
            kind.label(),
            first_result + 1,
            first_result + rows
        ));
        egui::ScrollArea::horizontal()
            .id_salt("filtered_grid_horizontal")
            .show(ui, |ui| {
                ui.set_min_width(
                    (columns as f32 * INITIAL_COLUMN_WIDTH + ROW_NUMBER_WIDTH)
                        .max(ui.available_width()),
                );
                TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(Layout::left_to_right(Align::Center))
                    .column(Column::exact(ROW_NUMBER_WIDTH))
                    .columns(
                        Column::initial(INITIAL_COLUMN_WIDTH).at_least(72.0),
                        columns,
                    )
                    .header(ROW_HEIGHT, |mut header| {
                        header.col(|ui| {
                            ui.strong("Result");
                        });
                        for (column, name) in names.iter().enumerate() {
                            header.col(|ui| {
                                if aggregate {
                                    ui.strong(name);
                                } else {
                                    let source_column = projected_columns[column];
                                    let heading = self.sort_heading(source_column, name);
                                    if ui.button(heading).clicked() {
                                        self.toggle_sort(source_column);
                                    }
                                }
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(ROW_HEIGHT, ui_row_count(rows), |mut table_row| {
                            let row_index = table_row.index() as u64;
                            if row_index.saturating_add(1) == rows {
                                self.request_more_filtered();
                            }
                            let result_row = first_result.saturating_add(row_index);
                            table_row.col(|ui| {
                                ui.label((result_row + 1).to_string());
                            });
                            for column in 0..columns {
                                table_row.col(|ui| {
                                    let selected =
                                        self.grid.selection == Some((result_row, column));
                                    let text = self
                                        .filtered_cell_text(row_index, column)
                                        .unwrap_or_else(|| "…".to_string());
                                    if ui.selectable_label(selected, text).clicked() {
                                        self.grid.select_filtered(first_result, row_index, column);
                                    }
                                });
                            }
                        });
                    });
            });
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

fn chart_notice(ui: &mut egui::Ui, model: &ChartModel) {
    if model.is_empty() {
        ui.label("No chartable non-null values were returned.");
        return;
    }
    if model.input_capped || model.reduced {
        ui.label(format!(
            "Showing {} chart values from {} bounded input rows{}.",
            model.output_len(),
            model.input_rows,
            if model.input_capped {
                format!("; query sampling stopped at {MAX_INPUT_ROWS}")
            } else {
                String::new()
            }
        ));
    }
}

fn draw_chart(ui: &mut egui::Ui, model: &ChartModel, config: &ChartConfig) {
    let size = egui::vec2(
        ui.available_width().max(240.0),
        ui.available_height().max(220.0),
    );
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 4.0, ui.visuals().extreme_bg_color);
    if model.is_empty() {
        painter.text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            "No chartable values",
            egui::FontId::proportional(15.0),
            ui.visuals().weak_text_color(),
        );
        return;
    }
    let plot = rect.shrink2(egui::vec2(48.0, 28.0));
    painter.line_segment(
        [plot.left_bottom(), plot.left_top()],
        egui::Stroke::new(1.0_f32, ui.visuals().weak_text_color()),
    );
    painter.line_segment(
        [plot.left_bottom(), plot.right_bottom()],
        egui::Stroke::new(1.0_f32, ui.visuals().weak_text_color()),
    );
    let title = if config.title.trim().is_empty() {
        format!("{} chart", model.kind.label())
    } else {
        config.title.clone()
    };
    painter.text(
        rect.left_top() + egui::vec2(8.0, 6.0),
        egui::Align2::LEFT_TOP,
        title,
        egui::FontId::proportional(15.0),
        ui.visuals().text_color(),
    );

    let tooltip = match &model.values {
        ChartValues::Points(points) => {
            draw_points(&painter, plot, points, model.kind, response.hover_pos())
        }
        ChartValues::Bars(bars) => draw_bars(&painter, plot, bars, response.hover_pos()),
        ChartValues::Histogram(bins) => draw_histogram(&painter, plot, bins, response.hover_pos()),
    };
    painter.text(
        plot.left_bottom() + egui::vec2(0.0, 6.0),
        egui::Align2::LEFT_TOP,
        &config.x_column,
        egui::FontId::proportional(11.0),
        ui.visuals().weak_text_color(),
    );
    if model.kind != ChartKind::Histogram {
        painter.text(
            plot.left_top() - egui::vec2(42.0, 0.0),
            egui::Align2::LEFT_TOP,
            &config.y_column,
            egui::FontId::proportional(11.0),
            ui.visuals().weak_text_color(),
        );
    }
    if let Some(tooltip) = tooltip {
        response.on_hover_text_at_pointer(tooltip);
    } else {
        response.on_hover_text("Hover a plotted value for details");
    }
}

fn numeric_range(values: impl Iterator<Item = f64>) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in values {
        min = min.min(value);
        max = max.max(value);
    }
    if min == max {
        let pad = if min == 0.0 { 1.0 } else { min.abs() * 0.05 };
        (min - pad, max + pad)
    } else {
        (min, max)
    }
}

fn point_at(
    plot: egui::Rect,
    x: f64,
    y: f64,
    x_range: (f64, f64),
    y_range: (f64, f64),
) -> egui::Pos2 {
    let x = ((x - x_range.0) / (x_range.1 - x_range.0)) as f32;
    let y = ((y - y_range.0) / (y_range.1 - y_range.0)) as f32;
    egui::pos2(
        egui::lerp(plot.left()..=plot.right(), x),
        egui::lerp(plot.bottom()..=plot.top(), y),
    )
}

fn draw_points(
    painter: &egui::Painter,
    plot: egui::Rect,
    points: &[crate::chart::ChartPoint],
    kind: ChartKind,
    hover: Option<egui::Pos2>,
) -> Option<String> {
    let x_range = numeric_range(points.iter().map(|point| point.x));
    let y_range = numeric_range(points.iter().map(|point| point.y));
    let screen_points = points
        .iter()
        .map(|point| point_at(plot, point.x, point.y, x_range, y_range))
        .collect::<Vec<_>>();
    if kind == ChartKind::Line {
        for pair in screen_points.windows(2) {
            painter.line_segment(
                [pair[0], pair[1]],
                egui::Stroke::new(1.5_f32, egui::Color32::from_rgb(95, 165, 255)),
            );
        }
    }
    for point in &screen_points {
        painter.circle_filled(
            *point,
            if kind == ChartKind::Scatter { 2.5 } else { 2.0 },
            egui::Color32::from_rgb(95, 165, 255),
        );
    }
    hover.and_then(|hover| {
        screen_points
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.distance_sq(hover).total_cmp(&right.distance_sq(hover))
            })
            .and_then(|(index, point)| {
                (point.distance(hover) <= 14.0)
                    .then(|| format!("{}: {}\n{}: {}", "X", points[index].x, "Y", points[index].y))
            })
    })
}

fn draw_bars(
    painter: &egui::Painter,
    plot: egui::Rect,
    bars: &[crate::chart::ChartBar],
    hover: Option<egui::Pos2>,
) -> Option<String> {
    let (_, max) = numeric_range(bars.iter().map(|bar| bar.value));
    let min = bars.iter().map(|bar| bar.value).fold(0.0_f64, f64::min);
    let y_range = if min == max {
        (min - 1.0, max + 1.0)
    } else {
        (min, max)
    };
    let width = plot.width() / bars.len() as f32;
    let baseline = point_at(plot, 0.0, 0.0, (0.0, 1.0), y_range).y;
    let mut hovered = None;
    for (index, bar) in bars.iter().enumerate() {
        let left = plot.left() + index as f32 * width + 1.0;
        let y = point_at(plot, 0.0, bar.value, (0.0, 1.0), y_range).y;
        let bar_rect = egui::Rect::from_min_max(
            egui::pos2(left, y.min(baseline)),
            egui::pos2((left + width - 2.0).max(left + 1.0), y.max(baseline)),
        );
        painter.rect_filled(bar_rect, 1.0, egui::Color32::from_rgb(105, 190, 125));
        if hover.is_some_and(|hover| bar_rect.expand(3.0).contains(hover)) {
            hovered = Some(format!("{}: {}", bar.label, bar.value));
        }
    }
    hovered
}

fn draw_histogram(
    painter: &egui::Painter,
    plot: egui::Rect,
    bins: &[crate::chart::HistogramBin],
    hover: Option<egui::Pos2>,
) -> Option<String> {
    let max = bins.iter().map(|bin| bin.count).max().unwrap_or(1).max(1) as f32;
    let width = plot.width() / bins.len() as f32;
    let mut hovered = None;
    for (index, bin) in bins.iter().enumerate() {
        let left = plot.left() + index as f32 * width + 1.0;
        let height = plot.height() * bin.count as f32 / max;
        let bar_rect = egui::Rect::from_min_max(
            egui::pos2(left, plot.bottom() - height),
            egui::pos2((left + width - 2.0).max(left + 1.0), plot.bottom()),
        );
        painter.rect_filled(bar_rect, 1.0, egui::Color32::from_rgb(235, 165, 75));
        if hover.is_some_and(|hover| bar_rect.expand(3.0).contains(hover)) {
            hovered = Some(format!("{}–{}: {}", bin.start, bin.end, bin.count));
        }
    }
    hovered
}

impl eframe::App for PaviApp {
    fn save(&mut self, _storage: &mut dyn eframe::Storage) {
        self.persist_session();
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_open();
        self.poll_pages();
        self.poll_filtered();
        self.poll_chart();
        self.poll_export();
        self.show_top_bar(ctx);
        self.show_metadata(ctx);
        self.show_chart_window(ctx);
        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(&self.status);
                if let Some((row, column)) = self.grid.selection {
                    ui.separator();
                    ui.label(format!("Selected: row {}, column {}", row + 1, column + 1));
                }
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    ui.label(format!("{} cached pages", self.pages.len()));
                });
            });
        });
        egui::CentralPanel::default().show(ctx, |ui| self.show_grid(ui));

        if self.opening.is_some()
            || !self.pending_pages.is_empty()
            || self.filtered.as_ref().is_some_and(FilteredGrid::needs_more)
            || self.chart.loading()
            || self.export.is_some()
        {
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }
}

fn main() -> eframe::Result {
    let initial_path = std::env::args_os().nth(1).map(PathBuf::from);
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "PAVI",
        options,
        Box::new(move |_| Ok(Box::new(PaviApp::new(initial_path)))),
    )
}

#[cfg(test)]
mod tests {
    use std::{fs::File, thread, time::Duration};

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::TempDir;

    use crate::chart::DEFAULT_POINT_LIMIT;

    use super::*;

    fn source(rows: usize) -> (TempDir, Arc<ParquetSource>, PathBuf) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("filters.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1_000))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &arrow_array::RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..rows as i32)),
                        Arc::new(StringArray::from_iter_values(
                            (0..rows).map(|row| if row % 2 == 0 { "alpha" } else { "beta" }),
                        )),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        let source = Arc::new(ParquetSource::open(&path).unwrap());
        (directory, source, path)
    }

    fn make_app(source: Arc<ParquetSource>, path: PathBuf) -> PaviApp {
        let schema = source.schema();
        let column_names = schema
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect();
        let mut app =
            PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));
        app.dataset = Some(Dataset {
            source: Arc::clone(&source),
            path,
            column_names,
        });
        app.grid.ready(source.row_count(), source.column_count());
        app
    }

    fn poll_until_idle(app: &mut PaviApp) {
        for _ in 0..1_000 {
            app.poll_filtered();
            if app
                .filtered
                .as_ref()
                .is_none_or(|filtered| !filtered.needs_more())
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("filter did not become idle");
    }

    fn poll_until_opened(app: &mut PaviApp) {
        for _ in 0..1_000 {
            app.poll_open();
            if app.opening.is_none() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("open did not become idle");
    }

    fn poll_until_page_loaded(app: &mut PaviApp, page: u64) {
        for _ in 0..1_000 {
            app.poll_pages();
            if app.pages.contains_key(&page) {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("page did not load");
    }

    fn poll_chart_until_done(app: &mut PaviApp) {
        for _ in 0..2_000 {
            app.poll_chart();
            if !app.chart.loading() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("chart did not become idle");
    }

    fn filtered_ids(app: &PaviApp) -> Vec<String> {
        (0..app.grid.rows)
            .map(|row| app.filtered_cell_text(row, 0).unwrap())
            .collect()
    }

    #[test]
    fn applies_every_supported_operator_through_the_query_path() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);

        for filter in [
            "id == 2",
            "id != 2",
            "id > 2",
            "id >= 2",
            "id < 2",
            "id <= 2",
            "name contains alpha",
        ] {
            app.filter_input = filter.to_string();
            app.apply_filter();
            poll_until_idle(&mut app);
            assert!(app.filter_error.is_none(), "{filter}");
            assert!(app.grid.rows > 0, "{filter}");
        }
    }

    #[test]
    fn reports_invalid_and_incompatible_filters_without_replacing_the_grid() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        let rows = app.grid.rows;

        for filter in ["id", "missing == 1", "id contains two"] {
            app.filter_input = filter.to_string();
            app.apply_filter();
            assert!(app.filtered.is_none());
            assert!(app.filter_error.is_some());
            assert_eq!(app.grid.rows, rows);
        }
    }

    #[test]
    fn applies_and_clears_a_filter() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.filter_input = "id == 2".to_string();
        app.apply_filter();
        poll_until_idle(&mut app);
        assert_eq!(filtered_ids(&app), vec!["2"]);

        app.clear_filter();
        assert!(app.filtered.is_none());
        assert!(app.filter_input.is_empty());
        assert_eq!(app.grid.rows, 8);
    }

    #[test]
    fn distinguishes_zero_matches_and_execution_errors() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source.clone(), path.clone());
        app.filter_input = "id > 100".to_string();
        app.apply_filter();
        poll_until_idle(&mut app);
        assert_eq!(app.grid.rows, 0);
        assert!(app.status.contains("No rows match"));

        let mut failing = make_app(source, path.clone());
        std::fs::remove_file(path).unwrap();
        failing.filter_input = "id == 1".to_string();
        failing.apply_filter();
        poll_until_idle(&mut failing);
        assert!(
            failing
                .filtered
                .as_ref()
                .and_then(|filtered| filtered.error.as_ref())
                .is_some()
        );
    }

    #[test]
    fn streams_multiple_pages_with_a_bounded_result_window() {
        let rows = (parquet_reader::PAGE_ROWS * 9 + 1) as usize;
        let (_directory, source, path) = source(rows);
        let mut app = make_app(source, path);
        app.filter_input = "id >= 0".to_string();
        app.apply_filter();
        for _ in 0..9 {
            poll_until_idle(&mut app);
            app.request_more_filtered();
        }
        poll_until_idle(&mut app);

        let filtered = app.filtered.as_ref().unwrap();
        assert!(filtered.first_result > 0);
        assert!(filtered.batches.len() <= MAX_FILTERED_BATCHES);
        assert!(filtered.rows <= parquet_reader::PAGE_ROWS * MAX_FILTERED_BATCHES as u64);
        assert_eq!(app.grid.selection, None);
    }

    #[test]
    fn replaces_and_cancels_obsolete_filters() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.filter_input = "id >= 0".to_string();
        app.apply_filter();
        let cancellation = app
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.execution.as_ref())
            .and_then(QueryExecution::cancellation_token)
            .unwrap();

        app.filter_input = "id == 1".to_string();
        app.apply_filter();
        assert!(cancellation.is_cancelled());
        poll_until_idle(&mut app);
        assert_eq!(filtered_ids(&app), vec!["1"]);
        assert!(app.grid.generation.0 >= 2);
    }

    #[test]
    fn drops_stale_filtered_batches() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.filter_input = "id == 1".to_string();
        app.apply_filter();
        app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
        poll_until_idle(&mut app);

        assert_eq!(app.grid.rows, 0);
    }

    #[test]
    fn runs_sql_through_the_existing_bounded_result_grid() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.sql_input = "SELECT name FROM dataset WHERE id >= 2 LIMIT 2".to_string();

        app.run_sql();
        poll_until_idle(&mut app);

        assert!(app.sql_error.is_none());
        assert_eq!(app.grid.columns, 1);
        assert_eq!(app.grid.rows, 2);
        assert_eq!(filtered_ids(&app), vec!["alpha", "beta"]);
        assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sql);

        app.sql_input = "SELECT id FROM dataset ORDER BY id DESC LIMIT 2".to_string();
        app.run_sql();
        poll_until_idle(&mut app);
        assert_eq!(filtered_ids(&app), vec!["7", "6"]);
        assert_eq!(
            app.filtered.as_ref().and_then(|filtered| filtered.sort),
            Some(SortSpec::new(0, SortDirection::Descending, NullOrder::Last))
        );

        app.sql_input = "SELECT COUNT(*), SUM(id) FROM dataset".to_string();
        app.run_sql();
        poll_until_idle(&mut app);
        assert!(app.sql_error.is_none());
        assert_eq!(app.grid.columns, 2);
        assert_eq!(app.grid.rows, 1);
        assert_eq!(filtered_ids(&app), vec!["8"]);
        assert_eq!(app.filtered_cell_text(0, 1).as_deref(), Some("28"));
        assert!(app.filtered.as_ref().is_some_and(|grid| grid.aggregate));
        let fields = app
            .filtered
            .as_ref()
            .unwrap()
            .batches
            .front()
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(fields, vec!["COUNT(*)", "SUM(id)"]);
    }

    #[test]
    fn reports_sql_parse_and_type_errors_without_replacing_results() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        let rows = app.grid.rows;

        for sql in [
            "SELECT FROM dataset",
            "SELECT missing FROM dataset",
            "SELECT * FROM dataset WHERE id LIKE '%2%'",
        ] {
            app.sql_input = sql.to_string();
            app.run_sql();
            assert!(app.filtered.is_none(), "{sql}");
            assert!(app.sql_error.is_some(), "{sql}");
            assert_eq!(app.grid.rows, rows, "{sql}");
        }
        assert_eq!(app.session.query_history.len(), 3);
        assert!(app.session.query_history.iter().all(|entry| !entry.success));
    }

    #[test]
    fn replaces_and_cancels_obsolete_sql_queries() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        let first_sql = "SELECT id FROM dataset";
        app.sql_input = first_sql.to_string();
        app.run_sql();
        let cancellation = app
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.execution.as_ref())
            .and_then(QueryExecution::cancellation_token)
            .unwrap();

        app.sql_input = "SELECT id FROM dataset WHERE id = 1".to_string();
        app.run_sql();
        assert!(cancellation.is_cancelled());
        poll_until_idle(&mut app);
        assert_eq!(filtered_ids(&app), vec!["1"]);
        assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sql);
        assert_eq!(app.session.query_history.len(), 2);
        assert_eq!(app.session.query_history[1].sql, first_sql);
        assert!(!app.session.query_history[1].success);
        assert_eq!(
            app.session.query_history[0].sql,
            "SELECT id FROM dataset WHERE id = 1"
        );
        assert!(app.session.query_history[0].success);
    }

    #[test]
    fn restores_session_without_restoring_transient_work() {
        let (_directory, _source, path) = source(3);
        let store = SessionStore::new(path.with_extension("session.json"));
        let mut session = SessionState::default();
        session.record_recent_file(path.clone());
        session.sql_input = "SELECT id FROM dataset LIMIT 1".to_string();
        session.record_query(&session.sql_input.clone(), 5, true, Some(1));
        session.preferences.inspector_visible = false;
        session.preferences.chart_visible = true;
        store.save(&session).unwrap();

        let app = PaviApp::new_with_store(None, store);
        assert_eq!(app.path_input, path.display().to_string());
        assert_eq!(app.sql_input, "SELECT id FROM dataset LIMIT 1");
        assert!(!app.show_inspector);
        assert!(app.chart.visible);
        assert_eq!(app.session.query_history, session.query_history);
        assert!(app.opening.is_some());
        assert!(app.pending_pages.is_empty());
        assert!(app.pages.is_empty());
        assert!(app.filtered.is_none());
        assert!(app.running_sql.is_none());
        assert!(app.grid.selection.is_none());
    }

    #[test]
    fn rejects_stale_sql_results_and_keeps_sql_batches_bounded() {
        let rows = (parquet_reader::PAGE_ROWS * 9 + 1) as usize;
        let (_directory, source, path) = source(rows);
        let mut app = make_app(source, path);
        app.sql_input = "SELECT id FROM dataset".to_string();
        app.run_sql();
        app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
        poll_until_idle(&mut app);
        assert_eq!(app.grid.rows, 0);

        app.sql_input = "SELECT id FROM dataset".to_string();
        app.run_sql();
        for _ in 0..9 {
            poll_until_idle(&mut app);
            app.request_more_filtered();
        }
        poll_until_idle(&mut app);
        let sql = app.filtered.as_ref().unwrap();
        assert_eq!(sql.kind, QueryKind::Sql);
        assert!(sql.batches.len() <= MAX_FILTERED_BATCHES);
        assert!(sql.rows <= parquet_reader::PAGE_ROWS * MAX_FILTERED_BATCHES as u64);
    }

    #[test]
    fn cancels_sql_and_returns_to_the_source_grid() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.sql_input = "SELECT id FROM dataset".to_string();
        app.run_sql();

        app.cancel_sql();

        assert!(app.filtered.is_none());
        assert_eq!(app.grid.rows, 8);
        assert_eq!(app.grid.columns, 2);
        assert!(app.status.contains("cancelled"));
    }

    #[test]
    fn sorts_grid_headers_and_toggles_direction_without_a_second_renderer() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);

        app.toggle_sort(0);
        poll_until_idle(&mut app);
        assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sort);
        assert_eq!(app.sort_heading(0, "id"), "id ↑");
        assert_eq!(
            filtered_ids(&app),
            (0..8).map(|id| id.to_string()).collect::<Vec<_>>()
        );

        app.toggle_sort(0);
        poll_until_idle(&mut app);
        assert_eq!(app.sort_heading(0, "id"), "id ↓");
        assert_eq!(
            filtered_ids(&app),
            (0..8).rev().map(|id| id.to_string()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn sorts_an_active_filter_and_rejects_stale_sorted_results() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.filter_input = "id >= 2".to_string();
        app.apply_filter();
        let cancellation = app
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.execution.as_ref())
            .and_then(QueryExecution::cancellation_token)
            .unwrap();

        app.toggle_sort(0);
        assert!(cancellation.is_cancelled());
        poll_until_idle(&mut app);
        assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Filter);
        assert_eq!(filtered_ids(&app), vec!["2", "3", "4", "5", "6", "7"]);

        app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
        app.toggle_sort(0);
        app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
        poll_until_idle(&mut app);
        assert_eq!(app.grid.rows, 0);
    }

    #[test]
    fn opens_empty_and_replaces_active_document_work() {
        let (_first_directory, _first_source, first_path) = source(3);
        let (_second_directory, _second_source, second_path) = source(0);
        let mut app = PaviApp::new_with_store(
            None,
            SessionStore::new(first_path.with_extension("session.json")),
        );

        app.begin_open(first_path.clone());
        poll_until_opened(&mut app);
        assert_eq!(app.grid.rows, 3);
        app.request_page(0);
        poll_until_page_loaded(&mut app, 0);

        app.begin_open(second_path);
        assert!(app.pages.is_empty());
        assert!(app.pending_pages.is_empty());
        poll_until_opened(&mut app);
        assert_eq!(app.grid.rows, 0);
        assert!(matches!(app.grid.loading, LoadState::Ready));

        app.begin_open(first_path);
        poll_until_opened(&mut app);
        assert_eq!(app.grid.rows, 3);
    }

    #[test]
    fn reports_malformed_open_and_releases_its_task() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("malformed.parquet");
        std::fs::write(&path, b"not parquet").unwrap();
        let mut app =
            PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));

        app.begin_open(path);
        poll_until_opened(&mut app);

        assert!(app.dataset.is_none());
        assert!(app.opening.is_none());
        assert!(matches!(app.grid.loading, LoadState::Error(_)));
        assert!(app.status.contains("open file"));
    }

    #[test]
    fn inspector_selection_uses_cached_values_and_document_replacement_clears_it() {
        let (_first_directory, first_source, path) = source(3);
        let mut app = make_app(first_source, path);
        app.request_page(0);
        poll_until_page_loaded(&mut app, 0);
        assert!(app.grid.select(1, 0));
        let detail = app.selected_cell_details().unwrap();
        assert_eq!(detail.name, "id");
        assert_eq!(detail.value, "1");

        let (_second_directory, _second_source, second_path) = source(0);
        app.begin_open(second_path);
        assert!(app.grid.selection.is_none());
        assert!(app.selected_cell_details().is_none());
    }

    #[test]
    fn inspector_handles_empty_datasets_without_a_selection() {
        let (_directory, source, path) = source(0);
        let app = make_app(source, path);

        assert_eq!(app.dataset.as_ref().unwrap().source.metadata().row_count, 0);
        assert!(app.selected_cell_details().is_none());
        assert!(app.selected_source_column().is_none());
    }

    #[test]
    fn clamps_untrusted_row_counts_for_the_egui_row_api() {
        assert_eq!(ui_row_count(u64::MAX), usize::MAX);
    }

    #[test]
    fn charts_execute_sql_through_the_existing_runtime_and_reduce_results() {
        let (_directory, source, path) = source(32);
        let mut app = make_app(source, path);
        app.chart.config = ChartConfig {
            kind: ChartKind::Line,
            x_column: "id".to_string(),
            y_column: "id".to_string(),
            point_limit: 4,
            bins: 4,
            title: "IDs".to_string(),
        };
        app.chart.sql_input = "SELECT id FROM dataset".to_string();

        app.run_chart();
        poll_chart_until_done(&mut app);

        let model = app.chart.model.as_ref().unwrap();
        assert_eq!(model.kind, ChartKind::Line);
        assert_eq!(model.input_rows, 32);
        assert!(model.reduced);
        assert_eq!(model.output_len(), 4);
        assert!(app.chart.error.is_none());
        assert!(app.chart.status.contains("query result"));
    }

    #[test]
    fn charts_support_bar_scatter_and_histogram_query_results() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        for (kind, sql, x, y) in [
            (ChartKind::Bar, "SELECT name, id FROM dataset", "name", "id"),
            (ChartKind::Scatter, "SELECT id FROM dataset", "id", "id"),
            (ChartKind::Histogram, "SELECT id FROM dataset", "id", ""),
        ] {
            app.chart.config = ChartConfig {
                kind,
                x_column: x.to_string(),
                y_column: y.to_string(),
                point_limit: 8,
                bins: 4,
                title: String::new(),
            };
            app.chart.sql_input = sql.to_string();
            app.run_chart();
            poll_chart_until_done(&mut app);
            assert!(app.chart.error.is_none(), "{kind:?}");
            assert!(
                app.chart
                    .model
                    .as_ref()
                    .is_some_and(|model| !model.is_empty())
            );
        }
    }

    #[test]
    fn chart_current_result_and_errors_are_bounded_and_clear() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.sql_input = "SELECT id FROM dataset LIMIT 3".to_string();
        app.run_sql();
        poll_until_idle(&mut app);

        app.chart.source = ChartSource::CurrentResult;
        app.chart.config = ChartConfig {
            kind: ChartKind::Scatter,
            x_column: "id".to_string(),
            y_column: "id".to_string(),
            point_limit: 2,
            bins: 2,
            title: String::new(),
        };
        app.run_chart();
        assert_eq!(app.chart.model.as_ref().unwrap().input_rows, 3);
        assert!(app.chart.model.as_ref().unwrap().reduced);

        app.chart.source = ChartSource::Sql;
        app.chart.config.x_column = "missing".to_string();
        app.run_chart();
        poll_chart_until_done(&mut app);
        assert!(app.chart.model.is_none());
        assert!(
            app.chart
                .error
                .as_deref()
                .is_some_and(|error| error.contains("missing"))
        );
    }

    #[test]
    fn chart_replacement_cancellation_and_stale_results_are_safe() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.chart.config = ChartConfig {
            kind: ChartKind::Line,
            x_column: "id".to_string(),
            y_column: "id".to_string(),
            point_limit: 8,
            bins: 2,
            title: String::new(),
        };
        app.chart.sql_input = "SELECT id FROM dataset".to_string();
        app.run_chart();
        let cancellation = app
            .chart
            .execution
            .as_ref()
            .and_then(QueryExecution::cancellation_token)
            .unwrap();

        app.chart.sql_input = "SELECT id FROM dataset WHERE id = 1".to_string();
        app.run_chart();
        assert!(cancellation.is_cancelled());
        poll_chart_until_done(&mut app);
        assert_eq!(app.chart.model.as_ref().unwrap().input_rows, 1);

        app.chart.sql_input = "SELECT id FROM dataset".to_string();
        app.run_chart();
        app.chart.generation.0 = app.chart.generation.0.saturating_add(1);
        poll_chart_until_done(&mut app);
        assert!(app.chart.model.is_none());
        assert!(app.chart.status.contains("stale"));
    }

    #[test]
    fn chart_surfaces_runtime_read_failures() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path.clone());
        app.chart.config = ChartConfig {
            kind: ChartKind::Line,
            x_column: "id".to_string(),
            y_column: "id".to_string(),
            point_limit: 8,
            bins: 2,
            title: String::new(),
        };
        std::fs::remove_file(path).unwrap();
        app.run_chart();
        poll_chart_until_done(&mut app);

        assert!(app.chart.model.is_none());
        assert!(
            app.chart
                .error
                .as_deref()
                .is_some_and(|error| error.contains("Chart query"))
        );
    }

    #[test]
    fn chart_sampling_stops_at_the_configured_bounded_input_window() {
        let rows = MAX_INPUT_ROWS + parquet_reader::PAGE_ROWS as usize;
        let (_directory, source, path) = source(rows);
        let mut app = make_app(source, path);
        app.chart.config = ChartConfig {
            kind: ChartKind::Line,
            x_column: "id".to_string(),
            y_column: "id".to_string(),
            point_limit: DEFAULT_POINT_LIMIT,
            bins: 2,
            title: String::new(),
        };
        app.chart.sql_input = "SELECT id FROM dataset".to_string();
        app.run_chart();
        poll_chart_until_done(&mut app);

        let model = app.chart.model.as_ref().unwrap();
        assert_eq!(model.input_rows, MAX_INPUT_ROWS);
        assert!(model.input_capped);
        assert!(model.output_len() <= DEFAULT_POINT_LIMIT);
    }
}
