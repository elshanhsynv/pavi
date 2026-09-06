mod state;

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use eframe::egui::{self, Align, Layout, RichText};
use egui_extras::{Column, TableBuilder};
use parquet_reader::{
    DataPage, NullOrder, ParquetSource, Projection, SortDirection, SortSpec,
    value::format_cell_with_limit,
};
use pavi_query::{Filter, LogicalPlan, Planner, QueryEngine, QueryExecution, QueryPoll, SqlAst};
use pavi_runtime::{OpenOutcome, OpenTask, PageOutcome, PageTask, Runtime, RuntimeConfig};

use crate::state::{GridState, LoadState};

const ROW_HEIGHT: f32 = 22.0;
const ROW_NUMBER_WIDTH: f32 = 72.0;
const INITIAL_COLUMN_WIDTH: f32 = 130.0;
const MAX_UI_PAGES: usize = 8;
const MAX_FILTERED_BATCHES: usize = 8;
const CELL_LIMIT: usize = 256;

fn ui_row_count(rows: u64) -> usize {
    rows.min(usize::MAX as u64) as usize
}

struct Dataset {
    source: Arc<ParquetSource>,
    path: PathBuf,
    column_names: Vec<String>,
    column_types: Vec<String>,
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
        projected_columns: Vec<usize>,
        execution: QueryExecution,
    ) -> Self {
        Self {
            kind,
            plan,
            sort,
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

struct PaviApp {
    runtime: Option<Runtime>,
    grid: GridState,
    dataset: Option<Dataset>,
    opening: Option<OpenTask>,
    pending_pages: HashMap<u64, PageTask>,
    pages: HashMap<u64, DataPage>,
    page_order: VecDeque<u64>,
    filtered: Option<FilteredGrid>,
    path_input: String,
    filter_input: String,
    filter_error: Option<String>,
    sql_input: String,
    sql_error: Option<String>,
    status: String,
}

impl PaviApp {
    fn new(initial_path: Option<PathBuf>) -> Self {
        let runtime = Runtime::new(RuntimeConfig::default());
        let mut app = Self {
            runtime: runtime.ok(),
            grid: GridState::default(),
            dataset: None,
            opening: None,
            pending_pages: HashMap::new(),
            pages: HashMap::new(),
            page_order: VecDeque::new(),
            filtered: None,
            path_input: initial_path
                .as_ref()
                .map_or_else(String::new, |path| path.display().to_string()),
            filter_input: String::new(),
            filter_error: None,
            sql_input: String::new(),
            sql_error: None,
            status: "Choose a Parquet file to begin".to_string(),
        };
        if app.runtime.is_none() {
            app.grid.loading = LoadState::Error("start background runtime".to_string());
            app.status = "Unable to start the background runtime".to_string();
        } else if let Some(path) = initial_path {
            app.begin_open(path);
        }
        app
    }

    fn begin_open(&mut self, path: PathBuf) {
        if let Some(task) = &self.opening {
            task.cancel();
        }
        self.opening = None;
        self.cancel_page_work();
        self.cancel_filter();
        self.dataset = None;
        self.filter_error = None;
        self.sql_error = None;
        let generation = self.grid.reset();
        self.path_input = path.display().to_string();
        self.status = format!("Opening {}…", path.display());

        let Some(runtime) = &self.runtime else {
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
                let column_types = schema
                    .fields()
                    .iter()
                    .map(|field| format!("{:?}", field.data_type()))
                    .collect();
                let rows = source.row_count();
                let columns = source.column_count();
                self.dataset = Some(Dataset {
                    source,
                    path: PathBuf::from(&self.path_input),
                    column_names,
                    column_types,
                });
                self.grid.ready(rows, columns);
                self.status = if rows == 0 {
                    "Opened empty dataset".to_string()
                } else {
                    format!("Ready: {rows} rows × {columns} columns")
                };
            }
            OpenOutcome::Cancelled => self.status = "File open cancelled".to_string(),
            OpenOutcome::OpenFailed(error) => self.open_error(format!("open file: {error:#}")),
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

    fn cancel_filter(&mut self) {
        if let Some(filtered) = &mut self.filtered {
            filtered.cancel();
        }
        self.filtered = None;
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
        let sql = self.sql_input.trim();
        if sql.is_empty() {
            self.sql_error = Some("Enter a SELECT query before running it".to_string());
            return;
        }
        let Some(dataset) = &self.dataset else {
            self.sql_error = Some("Open a dataset before running SQL".to_string());
            return;
        };
        let source = Arc::clone(&dataset.source);
        let plan = match SqlAst::parse(sql).and_then(|ast| ast.to_logical_plan(Arc::clone(&source)))
        {
            Ok(plan) => plan,
            Err(error) => {
                self.sql_error = Some(format!("SQL error: {error:#}"));
                return;
            }
        };
        let (projected_columns, sort) = match Planner::plan(&plan) {
            Ok(plan) => (plan.projected_columns().to_vec(), plan.sort()),
            Err(error) => {
                self.sql_error = Some(format!("SQL error: {error:#}"));
                return;
            }
        };
        if self.runtime.is_none() {
            self.sql_error = Some("Background runtime is unavailable".to_string());
            return;
        }
        self.cancel_page_work();
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
                    projected_columns,
                    execution,
                ));
                self.sql_error = None;
                self.status = "SQL: running".to_string();
            }
            Err(error) => {
                self.sql_error = Some(format!("Start SQL query: {error:#}"));
                self.status = "Unable to start SQL query".to_string();
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
                }
            }
        }
        self.grid.rows = filtered.rows;
        if let Some(status) = status {
            self.status = status;
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

    fn show_top_bar(&mut self, ctx: &egui::Context) {
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
                        .hint_text(
                            "SELECT column FROM dataset WHERE column >= 10 ORDER BY column DESC LIMIT 100",
                        ),
                );
                ui.horizontal(|ui| {
                    if ui.button("Run SQL").clicked() {
                        self.run_sql();
                    }
                    if ui.button("Cancel SQL").clicked() {
                        self.cancel_sql();
                    }
                    ui.label("SELECT … FROM dataset [WHERE …] [ORDER BY column] [LIMIT n]");
                    if let Some(error) = &self.sql_error {
                        ui.label(RichText::new(error).color(egui::Color32::RED));
                    }
                });
            });
        });
    }

    fn show_metadata(&self, ctx: &egui::Context) {
        egui::SidePanel::left("metadata")
            .default_width(230.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("Dataset");
                if let Some(dataset) = &self.dataset {
                    ui.label(dataset.path.display().to_string());
                    ui.separator();
                    ui.label(format!("Rows: {}", self.grid.rows));
                    ui.label(format!("Columns: {}", self.grid.columns));
                    ui.label(format!("Row groups: {}", dataset.source.row_groups().len()));
                    ui.separator();
                    ui.heading("Schema");
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        for (index, (name, ty)) in dataset
                            .column_names
                            .iter()
                            .zip(&dataset.column_types)
                            .enumerate()
                        {
                            ui.label(format!("{index}: {name}\n  {ty}"));
                        }
                    });
                } else {
                    ui.label("No file open");
                }
            });
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

        let names = projected_columns
            .iter()
            .map(|column| dataset.column_names[*column].clone())
            .collect::<Vec<_>>();
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
                            let source_column = projected_columns[column];
                            let heading = self.sort_heading(source_column, name);
                            header.col(|ui| {
                                if ui.button(heading).clicked() {
                                    self.toggle_sort(source_column);
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

impl eframe::App for PaviApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_open();
        self.poll_pages();
        self.poll_filtered();
        self.show_top_bar(ctx);
        self.show_metadata(ctx);
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
        let column_types = schema
            .fields()
            .iter()
            .map(|field| format!("{:?}", field.data_type()))
            .collect();
        let mut app = PaviApp::new(None);
        app.dataset = Some(Dataset {
            source: Arc::clone(&source),
            path,
            column_names,
            column_types,
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
    }

    #[test]
    fn replaces_and_cancels_obsolete_sql_queries() {
        let (_directory, source, path) = source(8);
        let mut app = make_app(source, path);
        app.sql_input = "SELECT id FROM dataset".to_string();
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
        let mut app = PaviApp::new(None);

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
        let mut app = PaviApp::new(None);

        app.begin_open(path);
        poll_until_opened(&mut app);

        assert!(app.dataset.is_none());
        assert!(app.opening.is_none());
        assert!(matches!(app.grid.loading, LoadState::Error(_)));
        assert!(app.status.contains("open file"));
    }

    #[test]
    fn clamps_untrusted_row_counts_for_the_egui_row_api() {
        assert_eq!(ui_row_count(u64::MAX), usize::MAX);
    }
}
