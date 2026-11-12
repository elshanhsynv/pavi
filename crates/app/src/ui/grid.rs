use super::super::*;
use super::{
    inspector::type_label,
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

/// Recent files offered on the start screen.
const RECENT_ON_START: usize = 4;
/// Height of the pinned header row.
const HEADER_HEIGHT: f32 = 28.0;
/// Pinned columns stop once they would cover more than this share of the grid.
const MAX_PINNED_SHARE: f32 = 0.6;
/// Width of the find box.
const FIND_BOX_WIDTH: f32 = 240.0;
/// Width of the menu a right-click on a cell opens.
const CELL_MENU_WIDTH: f32 = 220.0;
/// Width of the menu a right-click on a header opens.
const HEADER_MENU_WIDTH: f32 = 180.0;

struct Header {
    source: Option<usize>,
    name: String,
    data_type: String,
    /// Color of the type label.
    color: egui::Color32,
    /// Values are right-aligned.
    numeric: bool,
}

impl Header {
    fn new(source: Option<usize>, name: &str, data_type: Option<&arrow_schema::DataType>) -> Self {
        Self {
            source,
            name: name.to_owned(),
            data_type: data_type.map(type_label).unwrap_or_default(),
            color: data_type.map_or(theme::MUTED, type_color),
            numeric: data_type.is_some_and(arrow_schema::DataType::is_numeric),
        }
    }
}

impl PaviApp {
    pub(in crate::app) fn show_grid(&mut self, ui: &mut egui::Ui) {
        if self.find.open && self.dataset.is_some() {
            self.show_find_bar(ui);
        }
        egui::Frame::new().fill(theme::BACKGROUND).show(ui, |ui| {
            ui.set_min_size(ui.available_size());
            ui.spacing_mut().item_spacing = Spacing::None.vec(Spacing::None);
            ui.spacing_mut().interact_size.y = ROW_HEIGHT;
            self.show_grid_content(ui);
        });
    }

    fn show_grid_content(&mut self, ui: &mut egui::Ui) {
        if self.filtered.is_some() {
            self.show_filtered_grid(ui);
            return;
        }
        if self.dataset.is_none() {
            self.show_closed_state(ui);
            return;
        }
        if self.grid.rows == 0 {
            widgets::placeholder(ui, |ui| {
                ui.heading("This file has no rows");
                ui.label(
                    RichText::new("Its schema and metadata are in the Inspector (Ctrl+I).")
                        .color(theme::MUTED),
                );
            });
            return;
        }

        self.handle_grid_keyboard(ui.ctx(), 0);

        let names = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
            .unwrap_or_default();
        let visible = self
            .column_layout
            .visible()
            .iter()
            .filter_map(|source| {
                names.get(*source).map(|name| {
                    (
                        *source,
                        name.clone(),
                        self.column_layout.width(*source, INITIAL_COLUMN_WIDTH),
                    )
                })
            })
            .collect::<Vec<_>>();
        let headers = visible
            .iter()
            .map(|(source, name, _)| {
                let schema = self.dataset.as_ref().map(|d| d.source.schema());
                Header::new(
                    Some(*source),
                    name,
                    schema
                        .as_ref()
                        .map(|schema| schema.field(*source).data_type()),
                )
            })
            .collect::<Vec<_>>();
        let columns = visible.len();
        if columns == 0 {
            widgets::placeholder(ui, |ui| {
                ui.heading("Every column is hidden");
                ui.label(
                    RichText::new("Show one from Columns in the top bar.").color(theme::MUTED),
                );
            });
            return;
        }
        let windowed = self.grid.is_windowed();
        let header_rows = usize::from(!self.freeze_header);
        let gutter = row_number_width(self.grid.rows);
        let selected_row = self.grid.selection.map(|(row, _)| row);
        let area = ui.available_rect_before_wrap();
        let mut jump_row = None;
        if let Some(row) = self.jump_target.take() {
            if windowed {
                // Move the window around the target and set the scroll offset
                // directly, which applies this frame (`scroll_to_row` lands a frame
                // or two later, after re-centering would already have run).
                let rows_in_view = ((area.height() - HEADER_HEIGHT) / ROW_HEIGHT).max(1.0) as u64;
                let top = row.saturating_sub(rows_in_view / 2);
                self.scroll_grid_to(top, header_rows);
            } else {
                jump_row = Some(ui_row_count(row));
            }
        }
        let base = self.grid.window_base;
        let scroll_offset = self.grid_scroll_offset.take();
        self.pages.clear_visible();
        self.drawn_rows = None;
        // Large files: the table holds one window of rows and PAVI draws a scrollbar
        // for the whole file in a strip on the right.
        let scrollbar = windowed.then(|| {
            egui::Rect::from_min_max(
                egui::pos2(area.right() - SCROLLBAR_WIDTH, area.top()),
                area.max,
            )
        });
        let table_area = if windowed {
            area.with_max_x(area.right() - SCROLLBAR_WIDTH - 2.0)
        } else {
            area
        };
        let pinned = self.pinned_columns();
        let scroll_x = self.grid_scroll_x.take();
        let mut frame = PinnedFrame::default();
        // The table's vertical scroll offset after this frame (egui applies wheel
        // movement at the end of the frame, after the rows were drawn).
        let (body, offset_x, viewport) = ui
            .scope_builder(egui::UiBuilder::new().max_rect(table_area), |ui| {
                let mut sideways = egui::ScrollArea::horizontal().id_salt("grid_horizontal");
                if let Some(x) = scroll_x {
                    sideways = sideways.horizontal_scroll_offset(x);
                }
                let output = sideways.show(ui, |ui| {
                    ui.set_min_width(
                        (visible.iter().map(|(_, _, width)| width).sum::<f32>() + gutter)
                            .max(ui.available_width()),
                    );
                    let mut table = TableBuilder::new(ui)
                        .striped(true)
                        .resizable(true)
                        .cell_layout(Layout::left_to_right(Align::Center))
                        .column(Column::exact(gutter));
                    for (_, _, width) in &visible {
                        table = table.column(Column::initial(*width).at_least(72.0));
                    }
                    // An empty last column takes the leftover width, so data
                    // columns keep their own widths.
                    table = table.column(Column::remainder());
                    if windowed {
                        table = table.scroll_bar_visibility(
                            egui::scroll_area::ScrollBarVisibility::AlwaysHidden,
                        );
                    }
                    if let Some(offset) = scroll_offset {
                        table = table.vertical_scroll_offset(offset);
                    }
                    if let Some(row) = jump_row {
                        table = table
                            .scroll_to_row(row.saturating_add(header_rows), Some(Align::Center));
                    }
                    table
                        .header(
                            if self.freeze_header {
                                HEADER_HEIGHT
                            } else {
                                0.0
                            },
                            |header| {
                                if self.freeze_header {
                                    frame.header = Some(self.render_headers(header, &headers));
                                }
                            },
                        )
                        .body(|body| {
                            body.rows(
                                ROW_HEIGHT,
                                ui_row_count(self.grid.window_rows()).saturating_add(header_rows),
                                |mut table_row| {
                                    let index = table_row.index();
                                    if index < header_rows {
                                        frame.header =
                                            Some(self.render_headers(table_row, &headers));
                                        frame.header_in_body = true;
                                        return;
                                    }
                                    let row_index = base + (index - header_rows) as u64;
                                    self.note_drawn_row(row_index);
                                    let current_row = selected_row == Some(row_index);
                                    for page in self.grid.visible_pages(row_index, row_index) {
                                        self.pages.note_visible(page);
                                        self.request_page(page);
                                    }
                                    let (gutter_rect, _) =
                                        table_row.col(|ui| row_number(ui, row_index, current_row));
                                    let mut cells = Vec::new();
                                    for (column, header) in headers.iter().enumerate() {
                                        let look = CellLook {
                                            selected: self.grid.is_selected(row_index, column),
                                            current_row,
                                            numeric: header.numeric,
                                        };
                                        let text = self
                                            .cell_text(row_index, column)
                                            .unwrap_or_else(|| "…".to_string());
                                        let (rect, _) = table_row.col(|ui| {
                                            self.data_cell(ui, row_index, column, &text, look);
                                        });
                                        if column < pinned {
                                            cells.push(PinnedCell { rect, text, look });
                                        }
                                    }
                                    table_row.col(|_| {});
                                    frame.rows.push(PinnedRow {
                                        row: row_index,
                                        stripe: index % 2 == 0,
                                        current: current_row,
                                        gutter: gutter_rect,
                                        cells,
                                    });
                                },
                            );
                        })
                });
                (output.inner, output.state.offset.x, output.inner_rect)
            })
            .inner;
        let table_offset = body.state.offset.y;
        self.grid_view_x = (offset_x, viewport.width() - gutter);
        self.draw_pinned(ui, frame, &headers, table_area, body.inner_rect);
        if let (Some(bar), Some((first, last))) = (scrollbar, self.drawn_rows) {
            if let Some(top) = row_scrollbar(ui, bar, self.grid.rows, first, last) {
                self.scroll_grid_to(top, header_rows);
                ui.ctx().request_repaint();
            } else if let Some(new_base) = self.grid.recentered_base(first, last) {
                // Keep the exact view: shift the offset by however far the window moved.
                let shift = (new_base as f64 - base as f64) * f64::from(ROW_HEIGHT);
                self.grid.window_base = new_base;
                self.grid_scroll_offset = Some((f64::from(table_offset) - shift) as f32);
                ui.ctx().request_repaint();
            }
        }
        self.pages.cancel_offscreen(&mut self.grid);
        self.apply_header_action(ui.ctx());
        self.apply_cell_action(ui.ctx());
    }

    pub(in crate::app) fn show_filtered_grid(&mut self, ui: &mut egui::Ui) {
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
        let scanned = self.scan_fraction();
        // Joins and subqueries are prepared before the query runs (ADR 0009).
        let preparing = filtered
            .execution
            .as_ref()
            .is_some_and(|execution| execution.preparing_progress().is_some());
        let display_columns = filtered.display_columns.clone();
        if rows == 0 {
            widgets::placeholder(ui, |ui| {
                if let Some(error) = error {
                    widgets::error(ui, &error);
                } else if finished {
                    ui.heading(kind.empty_message());
                } else {
                    ui.add(egui::Spinner::new().color(theme::ACCENT));
                    ui.label(
                        RichText::new(match scanned {
                            Some(fraction) if preparing => format!(
                                "{} query running… joining tables, {} done",
                                kind.label(),
                                super::status::percent(fraction)
                            ),
                            Some(fraction) => format!(
                                "{} query running… {} of the file scanned",
                                kind.label(),
                                super::status::percent(fraction)
                            ),
                            None => format!("{} query running…", kind.label()),
                        })
                        .color(theme::MUTED),
                    );
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
            display_columns
                .iter()
                .filter_map(|(source, _)| dataset.column_names.get(*source).cloned())
                .collect::<Vec<_>>()
        };
        self.handle_grid_keyboard(ui.ctx(), first_result);
        let batch_schema = self
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.batches.front())
            .map(|batch| batch.schema());
        let headers = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                Header::new(
                    (!aggregate).then_some(display_columns[index].0),
                    name,
                    batch_schema
                        .as_ref()
                        .map(|schema| schema.field(display_columns[index].1).data_type()),
                )
            })
            .collect::<Vec<_>>();
        let columns = names.len();
        let jump_target = self
            .jump_target
            .take()
            .and_then(|row| row.checked_sub(first_result))
            .map(ui_row_count);
        egui::Frame::new()
            .fill(theme::PANEL)
            .inner_margin(Spacing::Md.symmetric(Spacing::Sm))
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                if let Some(error) = error {
                    widgets::error(ui, &error);
                }
                ui.label(
                    RichText::new(format!(
                        "{} result · rows {}–{} loaded",
                        kind.label(),
                        theme::number(first_result + 1),
                        theme::number(first_result + rows),
                    ))
                    .font(theme::small())
                    .color(theme::MUTED),
                );
            });
        let header_rows = usize::from(!self.freeze_header);
        let gutter = row_number_width(first_result.saturating_add(rows));
        let selected_row = self.grid.selection.map(|(row, _)| row);
        let pinned = self.pinned_columns();
        let scroll_x = self.grid_scroll_x.take();
        let mut frame = PinnedFrame::default();
        let area = ui.available_rect_before_wrap();
        let mut sideways = egui::ScrollArea::horizontal().id_salt("filtered_grid_horizontal");
        if let Some(x) = scroll_x {
            sideways = sideways.horizontal_scroll_offset(x);
        }
        let output = sideways.show(ui, |ui| {
            ui.set_min_width(
                ((0..columns)
                    .map(|column| {
                        if aggregate {
                            INITIAL_COLUMN_WIDTH
                        } else {
                            self.column_layout
                                .width(display_columns[column].0, INITIAL_COLUMN_WIDTH)
                        }
                    })
                    .sum::<f32>()
                    + gutter)
                    .max(ui.available_width()),
            );
            let mut table = TableBuilder::new(ui)
                .striped(true)
                .resizable(true)
                .cell_layout(Layout::left_to_right(Align::Center))
                .column(Column::exact(gutter));
            for (source, _) in &display_columns {
                let width = if aggregate {
                    INITIAL_COLUMN_WIDTH
                } else {
                    self.column_layout.width(*source, INITIAL_COLUMN_WIDTH)
                };
                table = table.column(Column::initial(width).at_least(72.0));
            }
            table = table.column(Column::remainder());
            if let Some(row) = jump_target {
                table = table.scroll_to_row(row.saturating_add(header_rows), Some(Align::Center));
            }
            table
                .header(
                    if self.freeze_header {
                        HEADER_HEIGHT
                    } else {
                        0.0
                    },
                    |header| {
                        if self.freeze_header {
                            frame.header = Some(self.render_headers(header, &headers));
                        }
                    },
                )
                .body(|body| {
                    body.rows(
                        ROW_HEIGHT,
                        ui_row_count(rows).saturating_add(header_rows),
                        |mut table_row| {
                            let index = table_row.index();
                            if index < header_rows {
                                frame.header = Some(self.render_headers(table_row, &headers));
                                frame.header_in_body = true;
                                return;
                            }
                            let row_index = (index - header_rows) as u64;
                            if row_index.saturating_add(1) == rows {
                                self.request_more_filtered();
                            }
                            let result_row = first_result.saturating_add(row_index);
                            let current_row = selected_row == Some(result_row);
                            let (gutter_rect, _) =
                                table_row.col(|ui| row_number(ui, result_row, current_row));
                            let mut cells = Vec::new();
                            for (column, (_, batch_column)) in display_columns.iter().enumerate() {
                                let look = CellLook {
                                    selected: self.grid.is_selected(result_row, column),
                                    current_row,
                                    numeric: headers[column].numeric,
                                };
                                let text = self
                                    .filtered_cell_text(row_index, *batch_column)
                                    .unwrap_or_else(|| "…".to_string());
                                let (rect, _) = table_row.col(|ui| {
                                    self.data_cell(ui, result_row, column, &text, look);
                                });
                                if column < pinned {
                                    cells.push(PinnedCell { rect, text, look });
                                }
                            }
                            table_row.col(|_| {});
                            frame.rows.push(PinnedRow {
                                row: result_row,
                                stripe: index % 2 == 0,
                                current: current_row,
                                gutter: gutter_rect,
                                cells,
                            });
                        },
                    );
                })
        });
        self.grid_view_x = (output.state.offset.x, output.inner_rect.width() - gutter);
        self.draw_pinned(ui, frame, &headers, area, output.inner.inner_rect);
        self.apply_header_action(ui.ctx());
        self.apply_cell_action(ui.ctx());
    }

    /// The right-click menu of a data cell. It acts on the selected cell, which a
    /// right-click has just made this one.
    fn cell_menu(&mut self, response: &egui::Response) {
        response.context_menu(|ui| {
            ui.set_width(CELL_MENU_WIDTH);
            let unavailable = self.selected_cell_filter(false).err();
            let mut chosen = None;
            ui.add_enabled_ui(unavailable.is_none(), |ui| {
                if widgets::menu_item(ui, "Filter to this value", "").clicked() {
                    chosen = Some(CellAction::FilterTo);
                }
                if widgets::menu_item(ui, "Exclude this value", "").clicked() {
                    chosen = Some(CellAction::Exclude);
                }
                if self.active_filter().is_some() {
                    if widgets::menu_item(ui, "Add to filter: this value", "").clicked() {
                        chosen = Some(CellAction::AndFilterTo);
                    }
                    if widgets::menu_item(ui, "Add to filter: not this value", "").clicked() {
                        chosen = Some(CellAction::AndExclude);
                    }
                }
            });
            if let Some(reason) = &unavailable {
                widgets::menu_caption(ui, reason);
            }
            ui.separator();
            if widgets::menu_item(ui, "Copy cell", "Ctrl+C").clicked() {
                chosen = Some(CellAction::CopyCell);
            }
            if widgets::menu_item(ui, "Copy row", "").clicked() {
                chosen = Some(CellAction::CopyRow);
            }
            if chosen.is_some() {
                self.cell_action = chosen;
                ui.close_menu();
            }
        });
    }

    /// No dataset: opening, failed to open, or nothing chosen yet.
    fn show_closed_state(&mut self, ui: &mut egui::Ui) {
        let mut browse = false;
        let mut browse_folder = false;
        let mut open = None;
        widgets::placeholder(ui, |ui| match &self.grid.loading {
            LoadState::Opening => {
                ui.add(egui::Spinner::new().color(theme::ACCENT));
                ui.heading("Opening…");
                let format = self
                    .opening_path
                    .as_deref()
                    .map_or(parquet_reader::FileFormat::Parquet, |path| {
                        parquet_reader::FileFormat::from_path(path)
                    });
                let detail = match format {
                    parquet_reader::FileFormat::Parquet => {
                        "Reading the Parquet footers.".to_string()
                    }
                    format => format!(
                        "Reading the {} file into a temporary Parquet file, so it can be \
                         filtered and sorted. The status bar shows how far it has got.",
                        format.name()
                    ),
                };
                ui.label(RichText::new(detail).color(theme::MUTED));
            }
            LoadState::Error(error) => {
                ui.heading("This could not be opened");
                widgets::error(ui, error);
                browse = widgets::primary_button(ui, Some(Icon::Folder), "Choose another file")
                    .clicked();
            }
            _ => {
                let (mark, _) =
                    ui.allocate_exact_size(egui::vec2(40.0, 36.0), egui::Sense::hover());
                widgets::logo(ui, mark.center(), 2.2);
                ui.heading("Open a data file or folder");
                ui.label(
                    RichText::new(
                        "Parquet, CSV, JSON or Arrow/Feather. Drop files or folders on this \
                         window; a folder of Parquet files opens as one table.",
                    )
                    .color(theme::MUTED),
                );
                ui.horizontal(|ui| {
                    browse = widgets::primary_button(ui, Some(Icon::Columns), "Open file…")
                        .on_hover_text("Ctrl+O")
                        .clicked();
                    browse_folder =
                        widgets::tool_button(ui, Some(Icon::Folder), "Open folder…", false)
                            .on_hover_text(
                                "Every Parquet file in a folder as one table (Ctrl+Shift+O)",
                            )
                            .clicked();
                });
                for path in self.session.recent_files.iter().take(RECENT_ON_START) {
                    let name = path.file_name().unwrap_or_default().to_string_lossy();
                    if widgets::tool_button(ui, Some(Icon::History), &name, false)
                        .on_hover_text(path.display().to_string())
                        .clicked()
                    {
                        open = Some(path.clone());
                    }
                }
            }
        });
        if browse {
            self.pick_file();
        }
        if browse_folder {
            self.pick_folder();
        }
        if let Some(path) = open {
            self.open_path(path);
        }
    }

    pub(in crate::app) fn handle_grid_keyboard(&mut self, ctx: &egui::Context, first_row: u64) {
        if ctx.wants_keyboard_input() || self.grid.rows == 0 || self.grid.columns == 0 {
            return;
        }
        let (row_delta, column_delta, home, end, copy) = ctx.input(|input| {
            let row_delta = if input.key_pressed(egui::Key::ArrowUp) {
                -1
            } else if input.key_pressed(egui::Key::ArrowDown) {
                1
            } else if input.key_pressed(egui::Key::PageUp) {
                -20
            } else if input.key_pressed(egui::Key::PageDown) {
                20
            } else {
                0
            };
            let column_delta = if input.key_pressed(egui::Key::ArrowLeft) {
                -1
            } else if input.key_pressed(egui::Key::ArrowRight) {
                1
            } else {
                0
            };
            (
                row_delta,
                column_delta,
                input.key_pressed(egui::Key::Home),
                input.key_pressed(egui::Key::End),
                input.modifiers.command && input.key_pressed(egui::Key::C),
            )
        });
        if copy {
            self.copy_selection(ctx, false);
            return;
        }
        if row_delta == 0 && column_delta == 0 && !home && !end {
            return;
        }
        if self.filtered.is_none() && !home && !end {
            self.grid.move_selection(
                row_delta,
                column_delta,
                ctx.input(|input| input.modifiers.shift),
            );
            if let Some((row, _)) = self.grid.selection
                && let Some(page) = self.grid.page_for_row(row)
            {
                self.request_page(page);
            }
            return;
        }
        let (row, column) = self.grid.selection.unwrap_or((first_row, 0));
        let last_row = first_row.saturating_add(self.grid.rows.saturating_sub(1));
        let target_row = if home {
            first_row
        } else if end {
            last_row
        } else {
            row.saturating_add_signed(row_delta)
                .clamp(first_row, last_row)
        };
        let target_column = column
            .saturating_add_signed(column_delta as isize)
            .min(self.grid.columns.saturating_sub(1));
        let extend = ctx.input(|input| input.modifiers.shift);
        if extend {
            self.grid.extend_selection(target_row, target_column);
        } else {
            self.grid.select(target_row, target_column);
        }
        if self.filtered.is_none()
            && let Some(page) = self.grid.page_for_row(target_row)
        {
            self.request_page(page);
        }
    }
    /// Where `column` is among the sort keys (0 = most significant), and its direction.
    fn sort_direction(&self, column: usize) -> Option<(usize, SortDirection)> {
        self.sort_keys()
            .iter()
            .enumerate()
            .find(|(_, key)| key.column == column)
            .map(|(index, key)| (index, key.direction))
    }

    /// Draws one header row and returns where its cells went.
    fn render_headers(
        &mut self,
        mut row: egui_extras::TableRow<'_, '_>,
        headers: &[Header],
    ) -> HeaderRects {
        let (gutter, _) = row.col(|ui| {
            let (rect, _) = ui.allocate_exact_size(ui.max_rect().size(), egui::Sense::hover());
            header_fill(ui, rect, false);
        });
        let selected_column = self.grid.selection.map(|(_, column)| column);
        let cells = headers
            .iter()
            .enumerate()
            .map(|(column, header)| {
                row.col(|ui| self.header_cell(ui, column, header, selected_column))
                    .0
            })
            .collect();
        row.col(|ui| header_fill(ui, ui.max_rect(), false));
        HeaderRects { gutter, cells }
    }

    /// One column header: name, sort arrow and type; click selects the column,
    /// right-click opens its menu.
    fn header_cell(
        &mut self,
        ui: &mut egui::Ui,
        column: usize,
        header: &Header,
        selected_column: Option<usize>,
    ) {
        let (rect, response) = ui.allocate_exact_size(ui.max_rect().size(), egui::Sense::click());
        response.widget_info(|| {
            egui::WidgetInfo::labeled(egui::WidgetType::Button, ui.is_enabled(), &header.name)
        });
        header_fill(ui, rect, response.hovered());
        if selected_column == Some(column) {
            // Marks the column the Inspector describes.
            ui.painter().hline(
                rect.x_range(),
                rect.bottom() - 1.0,
                egui::Stroke::new(2.0_f32, theme::ACCENT),
            );
        }
        let pad = Spacing::Md.px();
        let name = ui.painter_at(rect.shrink2(egui::vec2(pad, 0.0))).text(
            egui::pos2(rect.left() + pad, rect.center().y),
            egui::Align2::LEFT_CENTER,
            &header.name,
            theme::strong(12.5),
            theme::TEXT,
        );
        let mut used = name.right();
        if let Some((order, direction)) =
            header.source.and_then(|source| self.sort_direction(source))
            && used + 18.0 < rect.right()
        {
            widgets::icon(
                ui,
                egui::pos2(used + 10.0, rect.center().y),
                match direction {
                    SortDirection::Ascending => Icon::ArrowUp,
                    SortDirection::Descending => Icon::ArrowDown,
                },
                theme::ACCENT,
            );
            used += 18.0;
            // With several keys, a small number says which one this is.
            if self.sort_keys().len() > 1 && used + 10.0 < rect.right() {
                let number = ui.painter().text(
                    egui::pos2(used - 2.0, rect.center().y),
                    egui::Align2::LEFT_CENTER,
                    (order + 1).to_string(),
                    theme::small(),
                    theme::ACCENT,
                );
                used = number.right() + 2.0;
            }
        }
        if self.show_column_types {
            // The name wins: the type label is cut on the left when space is short.
            let room = rect.with_min_x(used + pad).with_max_x(rect.right() - pad);
            let label =
                ui.painter()
                    .layout_no_wrap(header.data_type.clone(), theme::small(), header.color);
            // A cut-off label is noise; the tooltip still names the type.
            if label.size().x <= room.width() {
                ui.painter().galley(
                    egui::pos2(
                        room.right() - label.size().x,
                        room.center().y - label.size().y / 2.0,
                    ),
                    label,
                    header.color,
                );
            }
        }
        let response = response.on_hover_text(format!(
            "{}\n{}\nClick to select, right-click for more",
            header.name, header.data_type,
        ));
        if response.clicked() {
            self.header_action = Some(HeaderAction::Select(column));
        }
        response.context_menu(|ui| {
            ui.set_width(HEADER_MENU_WIDTH);
            let mut chosen = None;
            if let Some(source) = header.source {
                for (label, direction) in [
                    ("Sort ascending", SortDirection::Ascending),
                    ("Sort descending", SortDirection::Descending),
                ] {
                    if widgets::menu_item(ui, label, "").clicked() {
                        chosen = Some(HeaderAction::Sort(source, direction));
                    }
                }
                // Once something is sorted, this column can break its ties.
                let keys = self.sort_keys();
                if !keys.is_empty() && !(keys.len() == 1 && keys[0].column == source) {
                    for (label, direction) in [
                        ("Then by ascending", SortDirection::Ascending),
                        ("Then by descending", SortDirection::Descending),
                    ] {
                        if widgets::menu_item(ui, label, "").clicked() {
                            chosen = Some(HeaderAction::ThenSort(source, direction));
                        }
                    }
                }
                ui.separator();
                if widgets::menu_item(ui, "Profile column", "").clicked() {
                    chosen = Some(HeaderAction::Profile(source));
                }
                if widgets::menu_item(ui, "Fit width", "").clicked() {
                    chosen = Some(HeaderAction::Fit(source));
                }
                if widgets::menu_item(ui, "Hide column", "").clicked() {
                    chosen = Some(HeaderAction::Hide(source));
                }
                ui.separator();
                if widgets::menu_item(ui, "Pin columns up to here", "").clicked() {
                    chosen = Some(HeaderAction::Pin(column + 1));
                }
                if self.pinned_columns() > 0
                    && widgets::menu_item(ui, "Unpin columns", "").clicked()
                {
                    chosen = Some(HeaderAction::Pin(0));
                }
                ui.separator();
            }
            if widgets::menu_item(ui, "Copy name", "").clicked() {
                chosen = Some(HeaderAction::CopyName(header.name.clone()));
            }
            if chosen.is_some() {
                self.header_action = chosen;
                ui.close_menu();
            }
        });
    }

    /// One data cell: draws it and handles clicks and its right-click menu.
    fn data_cell(
        &mut self,
        ui: &mut egui::Ui,
        row: u64,
        column: usize,
        text: &str,
        look: CellLook,
    ) {
        let response = cell_value(ui, text, look);
        if response.clicked() && ui.input(|input| input.modifiers.shift) {
            self.grid.extend_selection(row, column);
        } else if response.clicked() || response.secondary_clicked() {
            self.grid.select(row, column);
        }
        self.cell_menu(&response);
    }

    /// Draws the row numbers and pinned columns again at the left edge of the
    /// grid, over the columns that scrolled under them. Nothing is drawn while the
    /// grid is not scrolled sideways: the real cells are already in place.
    fn draw_pinned(
        &mut self,
        ui: &mut egui::Ui,
        frame: PinnedFrame,
        headers: &[Header],
        area: egui::Rect,
        body: egui::Rect,
    ) {
        let Some(gutter_left) = frame
            .rows
            .first()
            .map(|row| row.gutter.left())
            .or_else(|| frame.header.as_ref().map(|header| header.gutter.left()))
        else {
            return;
        };
        let shift = egui::vec2(area.left() - gutter_left, 0.0);
        if shift.x < 0.5 {
            return;
        }
        // Pinned columns never cover most of the grid.
        let limit = area.left() + area.width() * MAX_PINNED_SHARE;
        let fits = |rect: &egui::Rect| rect.right() + shift.x <= limit;
        let body_clip = egui::Rect::from_x_y_ranges(area.x_range(), body.y_range());
        let mut right = area.left();
        // The edge line stops at the last drawn row, not at the bottom of the area.
        let mut bottom = frame
            .header
            .as_ref()
            .map_or(area.top(), |header| header.gutter.bottom());
        for row in frame.rows {
            let gutter = row.gutter.translate(shift);
            bottom = bottom.max(gutter.bottom());
            let cells = row
                .cells
                .into_iter()
                .take_while(|cell| fits(&cell.rect))
                .collect::<Vec<_>>();
            let strip_right = cells
                .last()
                .map_or(gutter.right(), |cell| cell.rect.right() + shift.x);
            right = right.max(strip_right);
            ui.painter_at(body_clip).rect_filled(
                egui::Rect::from_x_y_ranges(gutter.left()..=strip_right, gutter.y_range()),
                0.0,
                if row.stripe {
                    theme::STRIPE
                } else {
                    theme::BACKGROUND
                },
            );
            pinned_scope(ui, gutter, body_clip, ("pinned_gutter", row.row), |ui| {
                row_number(ui, row.row, row.current);
            });
            for (column, cell) in cells.iter().enumerate() {
                let rect = cell.rect.translate(shift);
                pinned_scope(
                    ui,
                    rect,
                    body_clip,
                    ("pinned_cell", row.row, column),
                    |ui| {
                        self.data_cell(ui, row.row, column, &cell.text, cell.look);
                    },
                );
            }
        }
        if let Some(header) = frame.header {
            let clip = if frame.header_in_body {
                body_clip
            } else {
                egui::Rect::from_x_y_ranges(area.x_range(), header.gutter.y_range())
            };
            let gutter = header.gutter.translate(shift);
            pinned_scope(ui, gutter, clip, "pinned_header_gutter", |ui| {
                header_fill(ui, gutter, false);
            });
            let selected_column = self.grid.selection.map(|(_, column)| column);
            let pinned = self.pinned_columns();
            for (column, rect) in header.cells.iter().enumerate().take(pinned) {
                if !fits(rect) {
                    break;
                }
                right = right.max(rect.right() + shift.x);
                pinned_scope(
                    ui,
                    rect.translate(shift),
                    clip,
                    ("pinned_header", column),
                    |ui| {
                        self.header_cell(ui, column, &headers[column], selected_column);
                    },
                );
            }
        }
        // The edge the other columns slide under.
        ui.painter().vline(
            right,
            area.top()..=bottom.min(body.bottom()),
            egui::Stroke::new(1.0_f32, theme::BORDER),
        );
    }

    /// The find bar above the grid.
    fn show_find_bar(&mut self, ui: &mut egui::Ui) {
        let mut search = None;
        let mut close = false;
        egui::Frame::new()
            .fill(theme::PANEL)
            .inner_margin(Spacing::Md.symmetric(Spacing::Sm))
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = Spacing::Sm.px();
                    let response = ui.add(
                        egui::TextEdit::singleline(&mut self.find.query)
                            .id(find_box_id())
                            .desired_width(FIND_BOX_WIDTH)
                            .hint_text("Find in loaded rows"),
                    );
                    if std::mem::take(&mut self.find.focus) {
                        response.request_focus();
                    }
                    if response.changed() {
                        self.find.resume = None;
                        self.find.message = None;
                    }
                    if response.lost_focus() {
                        if ui.input(|input| input.key_pressed(egui::Key::Enter)) {
                            search = Some(ui.input(|input| input.modifiers.shift));
                            // Keep typing or pressing Enter for the next match.
                            response.request_focus();
                        } else if ui.input(|input| input.key_pressed(egui::Key::Escape)) {
                            close = true;
                        }
                    }
                    if widgets::tool_button(ui, Some(Icon::ArrowUp), "", false)
                        .on_hover_text("Previous match (Shift+Enter)")
                        .clicked()
                    {
                        search = Some(true);
                    }
                    if widgets::tool_button(ui, Some(Icon::ArrowDown), "", false)
                        .on_hover_text("Next match (Enter, F3)")
                        .clicked()
                    {
                        search = Some(false);
                    }
                    if let Some(message) = &self.find.message {
                        ui.label(
                            RichText::new(message)
                                .font(theme::small())
                                .color(theme::TEXT),
                        );
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if widgets::tool_button(ui, Some(Icon::Close), "", false)
                            .on_hover_text("Close (Esc)")
                            .clicked()
                        {
                            close = true;
                        }
                        ui.add(
                            egui::Label::new(
                                RichText::new(
                                    "Looks only at the rows loaded in memory; filter to search the whole file.",
                                )
                                .font(theme::small())
                                .color(theme::MUTED),
                            )
                            .truncate(),
                        );
                    });
                });
            });
        if let Some(backwards) = search {
            self.find_next(backwards);
        }
        if close {
            self.find = Default::default();
        }
    }
}

const SCROLLBAR_WIDTH: f32 = 12.0;

/// Id of the find box, so Ctrl+Shift+F can focus it.
pub fn find_box_id() -> egui::Id {
    egui::Id::new("find_box")
}

/// Where this frame's header cells were drawn.
struct HeaderRects {
    gutter: egui::Rect,
    cells: Vec<egui::Rect>,
}

/// What was drawn this frame in the columns that stay put, to draw again over
/// the scrolled columns.
#[derive(Default)]
struct PinnedFrame {
    header: Option<HeaderRects>,
    /// The header is a scrolling row (not pinned to the top).
    header_in_body: bool,
    rows: Vec<PinnedRow>,
}

struct PinnedRow {
    row: u64,
    /// egui striped this row.
    stripe: bool,
    current: bool,
    gutter: egui::Rect,
    cells: Vec<PinnedCell>,
}

struct PinnedCell {
    rect: egui::Rect,
    text: String,
    look: CellLook,
}

/// Runs `body` in a child Ui covering `rect`, clipped to `clip`, with a stable id.
fn pinned_scope(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    clip: egui::Rect,
    id: impl std::hash::Hash,
    body: impl FnOnce(&mut egui::Ui),
) {
    ui.scope_builder(egui::UiBuilder::new().max_rect(rect).id_salt(id), |ui| {
        ui.set_clip_rect(clip.intersect(rect));
        body(ui);
    });
}

/// PAVI's scrollbar for windowed grids. It shows the drawn rows `first..=last`
/// within all `total` rows and, when clicked or dragged, returns the row to show
/// at the top of the view.
pub(in crate::app) fn row_scrollbar(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    total: u64,
    first: u64,
    last: u64,
) -> Option<u64> {
    let response = ui.interact(
        rect,
        ui.id().with("pavi_row_scrollbar"),
        egui::Sense::click_and_drag(),
    );
    let visible = (last.saturating_sub(first) + 1).clamp(1, total.max(1));
    let max_top = total.saturating_sub(visible);
    let track = rect.shrink2(egui::vec2(2.0, 2.0));
    let thumb_height = (track.height() * (visible as f64 / total.max(1) as f64) as f32)
        .max(24.0)
        .min(track.height());
    let travel = (track.height() - thumb_height).max(1.0);
    let position = if max_top == 0 {
        0.0
    } else {
        first.min(max_top) as f64 / max_top as f64
    };
    let thumb = egui::Rect::from_min_size(
        egui::pos2(track.left(), track.top() + travel * position as f32),
        egui::vec2(track.width(), thumb_height),
    );
    let painter = ui.painter_at(rect);
    let active = response.hovered() || response.dragged();
    painter.rect_filled(
        thumb,
        4.0,
        if active { theme::MUTED } else { theme::BORDER },
    );
    let response = response.on_hover_text(format!(
        "Rows {}–{} of {}",
        theme::number(first + 1),
        theme::number(last + 1),
        theme::number(total)
    ));
    if !(response.dragged() || response.clicked()) {
        return None;
    }
    let pointer = response.interact_pointer_pos()?;
    let fraction = ((pointer.y - track.top() - thumb_height / 2.0) / travel).clamp(0.0, 1.0);
    Some((f64::from(fraction) * max_top as f64).round() as u64)
}

/// Width of the row-number gutter: wide enough for the last row number.
fn row_number_width(rows: u64) -> f32 {
    let digits = rows.max(1).ilog10() + 1;
    (digits as f32 * 7.0 + 20.0).max(44.0)
}

/// Text color of a type label, by type family.
fn type_color(data_type: &arrow_schema::DataType) -> egui::Color32 {
    super::inspector::type_color(data_type)
}

fn header_fill(ui: &egui::Ui, rect: egui::Rect, hovered: bool) {
    let painter = ui.painter();
    painter.rect_filled(
        rect,
        0.0,
        if hovered { theme::HOVER } else { theme::HEADER },
    );
    let line = egui::Stroke::new(1.0_f32, theme::BORDER);
    painter.vline(rect.right(), rect.y_range(), line);
    painter.hline(rect.x_range(), rect.bottom(), line);
}

/// The row-number gutter cell.
fn row_number(ui: &mut egui::Ui, row: u64, current: bool) {
    // Allocated, not only painted, so the table reports where the cell is.
    let (rect, _) = ui.allocate_exact_size(ui.max_rect().size(), egui::Sense::hover());
    let painter = ui.painter_at(rect);
    if current {
        painter.rect_filled(rect, 0.0, theme::HEADER);
    }
    painter.vline(
        rect.right(),
        rect.y_range(),
        egui::Stroke::new(1.0_f32, theme::BORDER),
    );
    painter.text(
        egui::pos2(rect.right() - Spacing::Md.px(), rect.center().y),
        egui::Align2::RIGHT_CENTER,
        (row + 1).to_string(),
        theme::small(),
        if current { theme::TEXT } else { theme::MUTED },
    );
}

/// How one data cell is drawn.
#[derive(Clone, Copy)]
struct CellLook {
    /// The cell is part of the selection.
    selected: bool,
    /// The cell is in the row of the selected cell.
    current_row: bool,
    /// Right-align, so digits line up.
    numeric: bool,
}

fn cell_value(ui: &mut egui::Ui, text: &str, look: CellLook) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(ui.max_rect().size(), egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::SelectableLabel,
            ui.is_enabled(),
            look.selected,
            text,
        )
    });
    let painter = ui.painter_at(rect);
    if look.selected {
        painter.rect_filled(rect, 0.0, theme::SELECTED);
    } else if look.current_row {
        painter.rect_filled(rect, 0.0, theme::HEADER);
    }
    painter.vline(
        rect.right(),
        rect.y_range(),
        egui::Stroke::new(1.0_f32, theme::GRID_LINE),
    );
    if look.selected {
        painter.rect_stroke(
            rect,
            0.0,
            egui::Stroke::new(1.0_f32, theme::ACCENT),
            egui::StrokeKind::Inside,
        );
    }
    let pad = Spacing::Md.px();
    let color = match text {
        "null" => theme::MUTED,
        "…" => theme::DISABLED,
        _ => theme::TEXT,
    };
    let drawn = if look.numeric {
        painter.text(
            egui::pos2(rect.right() - pad, rect.center().y),
            egui::Align2::RIGHT_CENTER,
            text,
            theme::body(),
            color,
        )
    } else {
        painter.text(
            egui::pos2(rect.left() + pad, rect.center().y),
            egui::Align2::LEFT_CENTER,
            text,
            theme::body(),
            color,
        )
    };
    if drawn.width() + pad * 2.0 > rect.width() {
        response.on_hover_text(text)
    } else {
        response
    }
}
