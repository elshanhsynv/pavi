mod state;

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use eframe::egui::{self, Align, Layout, RichText};
use egui_extras::{Column, TableBuilder};
use parquet_reader::{DataPage, ParquetSource, Projection, value::format_cell_with_limit};
use pavi_runtime::{OpenOutcome, OpenTask, PageOutcome, PageTask, Runtime, RuntimeConfig};

use crate::state::{GridState, LoadState};

const ROW_HEIGHT: f32 = 22.0;
const ROW_NUMBER_WIDTH: f32 = 72.0;
const INITIAL_COLUMN_WIDTH: f32 = 130.0;
const MAX_UI_PAGES: usize = 8;
const CELL_LIMIT: usize = 256;

struct Dataset {
    source: Arc<ParquetSource>,
    path: PathBuf,
    column_names: Vec<String>,
    column_types: Vec<String>,
}

struct PaviApp {
    runtime: Option<Runtime>,
    grid: GridState,
    dataset: Option<Dataset>,
    opening: Option<OpenTask>,
    pending_pages: HashMap<u64, PageTask>,
    pages: HashMap<u64, DataPage>,
    page_order: VecDeque<u64>,
    path_input: String,
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
            path_input: initial_path
                .as_ref()
                .map_or_else(String::new, |path| path.display().to_string()),
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
        for task in self.pending_pages.values() {
            task.cancel();
        }
        self.pending_pages.clear();
        self.pages.clear();
        self.page_order.clear();
        self.dataset = None;
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
                    PageOutcome::Batch(_) | PageOutcome::Loaded(_) => {
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
        if row % parquet_reader::PAGE_ROWS == 0
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
                if ui.button("Open Parquet…").clicked() {
                    if let Some(path) = rfd::FileDialog::new()
                        .add_filter("Parquet", &["parquet"])
                        .pick_file()
                    {
                        self.begin_open(path);
                    }
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
                        for name in &names {
                            header.col(|ui| {
                                ui.strong(name);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(ROW_HEIGHT, self.grid.rows as usize, |mut table_row| {
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
}

impl eframe::App for PaviApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_open();
        self.poll_pages();
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

        if self.opening.is_some() || !self.pending_pages.is_empty() {
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
