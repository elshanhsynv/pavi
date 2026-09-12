use super::super::*;
use super::{inspector::type_label, spacing::Spacing, theme};

struct Header {
    source: Option<usize>,
    name: String,
    data_type: String,
}

impl PaviApp {
    pub(in crate::app) fn show_grid(&mut self, ui: &mut egui::Ui) {
        egui::Frame::new()
            .fill(super::theme::PANEL)
            .stroke(egui::Stroke::new(1.0_f32, theme::GRID_LINE))
            .corner_radius(5.0)
            .show(ui, |ui| {
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
            match &self.grid.loading {
                LoadState::Opening => {
                    ui.centered_and_justified(|ui| {
                        ui.vertical_centered(|ui| {
                            ui.spinner();
                            ui.strong("Opening dataset…");
                            ui.label("Reading Parquet metadata on a background worker.");
                        });
                    });
                }
                LoadState::Error(error) => {
                    ui.centered_and_justified(|ui| {
                        ui.vertical_centered(|ui| {
                            ui.heading("Couldn’t open dataset");
                            ui.label(RichText::new(error).color(egui::Color32::RED));
                            ui.label("Choose Open… to try another Parquet file.");
                        });
                    });
                }
                _ => {
                    ui.centered_and_justified(|ui| {
                        ui.vertical_centered(|ui| {
                            ui.heading("Open a Parquet file");
                            ui.label("Use Open… or Ctrl+O to start exploring.");
                        });
                    });
                }
            }
            return;
        }
        if self.grid.rows == 0 {
            ui.centered_and_justified(|ui| {
                ui.vertical_centered(|ui| {
                    ui.heading("Dataset is empty");
                    ui.label("The schema and metadata are still available in Inspector.");
                });
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
            .map(|(source, name, _)| Header {
                source: Some(*source),
                name: name.clone(),
                data_type: self
                    .dataset
                    .as_ref()
                    .map(|d| type_label(d.source.schema().field(*source).data_type()))
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        let columns = visible.len();
        if columns == 0 {
            ui.centered_and_justified(|ui| {
                ui.label("All columns are hidden. Use Columns to show one.")
            });
            return;
        }
        let jump_target = self.jump_target.take().map(ui_row_count);
        let header_rows = if self.freeze_header {
            0
        } else if self.show_column_types {
            2
        } else {
            1
        };
        let header_height = if self.show_column_types { 52.0 } else { 32.0 };
        egui::ScrollArea::horizontal()
            .id_salt("grid_horizontal")
            .show(ui, |ui| {
                ui.set_min_width(
                    (visible.iter().map(|(_, _, width)| width).sum::<f32>() + ROW_NUMBER_WIDTH)
                        .max(ui.available_width()),
                );
                let mut table = TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(Layout::left_to_right(Align::Center))
                    .column(Column::exact(ROW_NUMBER_WIDTH));
                for (index, (_, _, width)) in visible.iter().enumerate() {
                    table = table.column(if index + 1 == columns {
                        Column::remainder().at_least(*width)
                    } else {
                        Column::initial(*width).at_least(72.0)
                    });
                }
                if let Some(row) = jump_target {
                    table =
                        table.scroll_to_row(row.saturating_add(header_rows), Some(Align::Center));
                }
                table
                    .header(
                        if self.freeze_header {
                            header_height
                        } else {
                            0.0
                        },
                        |header| {
                            if self.freeze_header {
                                self.render_headers(header, &headers, false, false);
                            }
                        },
                    )
                    .body(|body| {
                        body.rows(
                            ROW_HEIGHT,
                            ui_row_count(self.grid.rows).saturating_add(header_rows),
                            |mut table_row| {
                                let index = table_row.index();
                                if index < header_rows {
                                    self.render_headers(table_row, &headers, true, index == 1);
                                    return;
                                }
                                let row_index = (index - header_rows) as u64;
                                table_row.set_selected(
                                    self.grid.selection.is_some_and(|(row, _)| row == row_index),
                                );
                                for page in self.grid.visible_pages(row_index, row_index) {
                                    self.request_page(page);
                                }
                                table_row.col(|ui| {
                                    decorate_cell(ui, false);
                                    ui.label((row_index + 1).to_string());
                                });
                                for column in 0..columns {
                                    table_row.col(|ui| {
                                        decorate_cell(ui, false);
                                        let selected = self.grid.is_selected(row_index, column);
                                        let text = self
                                            .cell_text(row_index, column)
                                            .unwrap_or_else(|| "…".to_string());
                                        if cell_value(ui, &text, selected).clicked() {
                                            if ui.input(|input| input.modifiers.shift) {
                                                self.grid.extend_selection(row_index, column);
                                            } else {
                                                self.grid.select(row_index, column);
                                            }
                                        }
                                    });
                                }
                            },
                        );
                    });
            });
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
        let display_columns = filtered.display_columns.clone();
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
            display_columns
                .iter()
                .filter_map(|(source, _)| dataset.column_names.get(*source).cloned())
                .collect::<Vec<_>>()
        };
        self.handle_grid_keyboard(ui.ctx(), first_result);
        let headers = names
            .iter()
            .enumerate()
            .map(|(index, name)| Header {
                source: (!aggregate).then_some(display_columns[index].0),
                name: name.clone(),
                data_type: self
                    .filtered
                    .as_ref()
                    .and_then(|f| f.batches.front())
                    .map(|batch| {
                        type_label(batch.schema().field(display_columns[index].1).data_type())
                    })
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        let columns = names.len();
        let jump_target = self
            .jump_target
            .take()
            .and_then(|row| row.checked_sub(first_result))
            .map(ui_row_count);
        if let Some(error) = error {
            ui.label(RichText::new(error).color(egui::Color32::RED));
        }
        ui.label(format!(
            "{} rows {}–{} (bounded window)",
            kind.label(),
            first_result + 1,
            first_result + rows
        ));
        let header_rows = if self.freeze_header {
            0
        } else if self.show_column_types {
            2
        } else {
            1
        };
        let header_height = if self.show_column_types { 52.0 } else { 32.0 };
        egui::ScrollArea::horizontal()
            .id_salt("filtered_grid_horizontal")
            .show(ui, |ui| {
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
                        + ROW_NUMBER_WIDTH)
                        .max(ui.available_width()),
                );
                let mut table = TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(Layout::left_to_right(Align::Center))
                    .column(Column::exact(ROW_NUMBER_WIDTH));
                for (index, (source, _)) in display_columns.iter().enumerate() {
                    let width = if aggregate {
                        INITIAL_COLUMN_WIDTH
                    } else {
                        self.column_layout.width(*source, INITIAL_COLUMN_WIDTH)
                    };
                    table = table.column(if index + 1 == columns {
                        Column::remainder().at_least(width)
                    } else {
                        Column::initial(width).at_least(72.0)
                    });
                }
                if let Some(row) = jump_target {
                    table =
                        table.scroll_to_row(row.saturating_add(header_rows), Some(Align::Center));
                }
                table
                    .header(
                        if self.freeze_header {
                            header_height
                        } else {
                            0.0
                        },
                        |header| {
                            if self.freeze_header {
                                self.render_headers(header, &headers, false, false);
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
                                    self.render_headers(table_row, &headers, true, index == 1);
                                    return;
                                }
                                let row_index = (index - header_rows) as u64;
                                if row_index.saturating_add(1) == rows {
                                    self.request_more_filtered();
                                }
                                let result_row = first_result.saturating_add(row_index);
                                table_row.set_selected(
                                    self.grid
                                        .selection
                                        .is_some_and(|(row, _)| row == result_row),
                                );
                                table_row.col(|ui| {
                                    decorate_cell(ui, false);
                                    ui.label((result_row + 1).to_string());
                                });
                                for (column, (_, batch_column)) in
                                    display_columns.iter().enumerate()
                                {
                                    table_row.col(|ui| {
                                        decorate_cell(ui, false);
                                        let selected = self.grid.is_selected(result_row, column);
                                        let text = self
                                            .filtered_cell_text(row_index, *batch_column)
                                            .unwrap_or_else(|| "…".to_string());
                                        if cell_value(ui, &text, selected).clicked() {
                                            if ui.input(|input| input.modifiers.shift) {
                                                self.grid.extend_selection(result_row, column);
                                            } else {
                                                self.grid.select_filtered(
                                                    first_result,
                                                    row_index,
                                                    column,
                                                );
                                            }
                                        }
                                    });
                                }
                            },
                        );
                    });
            });
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
    fn render_headers(
        &mut self,
        mut row: egui_extras::TableRow<'_, '_>,
        headers: &[Header],
        single_line: bool,
        types_only: bool,
    ) {
        row.col(|ui| {
            decorate_cell(ui, true);
            if !types_only {
                ui.label("#");
            }
        });
        for header in headers {
            row.col(|ui| {
                let rect = ui.max_rect();
                ui.painter().rect_filled(rect, 0.0, theme::HEADER);
                ui.painter().vline(
                    rect.right(),
                    rect.y_range(),
                    egui::Stroke::new(1.0_f32, theme::GRID_LINE),
                );
                let (rect, response) = ui.allocate_exact_size(
                    rect.size(),
                    if header.source.is_some() && !types_only {
                        egui::Sense::click()
                    } else {
                        egui::Sense::hover()
                    },
                );
                response.widget_info(|| {
                    egui::WidgetInfo::labeled(
                        egui::WidgetType::Button,
                        ui.is_enabled(),
                        &header.name,
                    )
                });
                let x = rect.left() + Spacing::Lg.px();
                let painter = ui.painter_at(rect);
                if !types_only {
                    let heading = header.source.map_or_else(
                        || header.name.clone(),
                        |source| self.sort_heading(source, &header.name),
                    );
                    painter.text(
                        egui::pos2(
                            x,
                            if single_line || !self.show_column_types {
                                rect.center().y
                            } else {
                                rect.top() + 16.0
                            },
                        ),
                        egui::Align2::LEFT_CENTER,
                        heading,
                        egui::FontId::proportional(13.0),
                        theme::TEXT,
                    );
                    if header.source.is_some() {
                        let x = rect.right() - Spacing::Md.px();
                        let y = rect.top() + 14.0;
                        for sign in [-1.0, 1.0] {
                            painter.add(egui::Shape::line(
                                vec![
                                    egui::pos2(x - 3.0, y + sign * 3.0),
                                    egui::pos2(x, y + sign * 6.0),
                                    egui::pos2(x + 3.0, y + sign * 3.0),
                                ],
                                egui::Stroke::new(1.0_f32, theme::ACCENT),
                            ));
                        }
                    }
                }
                if types_only || (!single_line && self.show_column_types) {
                    painter.text(
                        egui::pos2(
                            x,
                            if types_only {
                                rect.center().y
                            } else {
                                rect.top() + 36.0
                            },
                        ),
                        egui::Align2::LEFT_CENTER,
                        &header.data_type,
                        egui::FontId::proportional(12.0),
                        theme::MUTED,
                    );
                }
                if response
                    .on_hover_text(format!("{} · {}", header.name, header.data_type))
                    .clicked()
                    && let Some(source) = header.source
                {
                    self.toggle_sort(source);
                }
            });
        }
    }
}

fn decorate_cell(ui: &mut egui::Ui, header: bool) {
    let rect = ui.max_rect();
    if header {
        ui.painter().rect_filled(rect, 0.0, super::theme::HEADER);
    }
    ui.painter().vline(
        rect.right(),
        rect.y_range(),
        egui::Stroke::new(1.0_f32, theme::GRID_LINE),
    );
    if header {
        ui.painter().hline(
            rect.x_range(),
            rect.bottom(),
            egui::Stroke::new(1.0_f32, theme::GRID_LINE),
        );
    }
    ui.add_space(Spacing::Lg.px());
}
fn cell_value(ui: &mut egui::Ui, text: &str, selected: bool) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), ROW_HEIGHT),
        egui::Sense::click(),
    );
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::SelectableLabel,
            ui.is_enabled(),
            selected,
            text,
        )
    });
    if selected {
        ui.painter().rect_stroke(
            rect.shrink(1.0),
            1.0,
            egui::Stroke::new(1.0_f32, super::theme::ACCENT),
            egui::StrokeKind::Inside,
        );
    }
    ui.painter_at(rect).text(
        rect.left_center(),
        egui::Align2::LEFT_CENTER,
        text,
        egui::FontId::proportional(13.0),
        super::theme::TEXT,
    );
    response.on_hover_text(text)
}
