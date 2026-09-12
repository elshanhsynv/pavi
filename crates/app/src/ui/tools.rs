use super::super::*;
use super::widgets::{self, Icon};

impl PaviApp {
    pub(in crate::app) fn show_tools_window(&mut self, ctx: &egui::Context) {
        if !self.show_tools {
            return;
        }
        let mut open = true;
        let mut open_recent = None;
        let mut rerun_history = None;
        let mut remove_history = None;
        let mut clear_history = false;
        let mut toggle_column = None;
        let mut move_column = None;
        let mut resize_column = None;
        let mut auto_fit_column = None;
        let mut reset_columns = false;
        let column_entries = self.dataset.as_ref().map_or_else(Vec::new, |dataset| {
            let search = self.column_search.to_lowercase();
            dataset
                .column_names
                .iter()
                .enumerate()
                .filter(|(_, name)| search.is_empty() || name.to_lowercase().contains(&search))
                .take(128)
                .map(|(source, name)| (source, name.clone()))
                .collect::<Vec<_>>()
        });

        egui::Window::new("Dataset tools")
            .open(&mut open)
            .default_width(420.0)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .max_height(600.0)
                    .show(ui, |ui| {
                        {
                            widgets::section(ui, "Quick SQL", Icon::Sql, |ui| {
                                ui.add(
                                egui::TextEdit::multiline(&mut self.sql_input)
                                    .code_editor()
                                    .desired_rows(3)
                                    .desired_width(f32::INFINITY)
                                    .hint_text(
                                        "SELECT category, COUNT(*) FROM dataset GROUP BY category",
                                    ),
                            );
                                ui.horizontal_wrapped(|ui| {
                                    if ui.button("Run SQL").clicked() {
                                        self.run_sql();
                                    }
                                    if ui.button("Cancel SQL").clicked() {
                                        self.cancel_sql();
                                    }
                                    ui.label(
                                    "SELECT … FROM dataset [WHERE …] [GROUP BY column] [LIMIT n]",
                                );
                                    if let Some(error) = &self.sql_error {
                                        ui.label(RichText::new(error).color(egui::Color32::RED));
                                    }
                                });
                            });
                        }
                        widgets::section(ui, "Recent Files", Icon::Clock, |ui| {
                            if self.session.recent_files.is_empty() {
                                ui.label("No recent available files.");
                            }
                            for path in &self.session.recent_files {
                                if ui
                                    .add(egui::Button::new(path.display().to_string()).truncate())
                                    .clicked()
                                {
                                    open_recent = Some(path.clone());
                                }
                            }
                        });
                        widgets::section(ui, "Query History", Icon::History, |ui| {
                            if ui.button("Clear history").clicked() {
                                clear_history = true;
                            }
                            if self.session.query_history.is_empty() {
                                ui.label("No completed SQL queries.");
                            }
                            for (index, entry) in self.session.query_history.iter().enumerate() {
                                ui.horizontal_wrapped(|ui| {
                                    let result = if entry.success { "ok" } else { "failed" };
                                    ui.label(format!(
                                        "{result} · {} ms{}",
                                        entry.duration_ms,
                                        entry.row_count.map_or_else(String::new, |rows| format!(
                                            " · {rows} rows"
                                        ))
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
                        widgets::section(ui, "Columns", Icon::Columns, |ui| {
                            ui.vertical(|ui| {
                                ui.label("Search:");
                                ui.text_edit_singleline(&mut self.column_search);
                                if ui.button("Reset layout").clicked() {
                                    reset_columns = true;
                                }
                            });
                            if column_entries.is_empty() {
                                ui.label("No matching columns.");
                            } else {
                                egui::ScrollArea::vertical()
                                    .id_salt("column_layout")
                                    .max_height(190.0)
                                    .show(ui, |ui| {
                                        for (source, name) in &column_entries {
                                            ui.vertical(|ui| {
                                                let mut shown =
                                                    !self.column_layout.is_hidden(*source);
                                                if ui.checkbox(&mut shown, name).changed() {
                                                    toggle_column = Some((*source, shown));
                                                }
                                                if ui.small_button("↑").clicked() {
                                                    move_column = Some((*source, -1));
                                                }
                                                if ui.small_button("↓").clicked() {
                                                    move_column = Some((*source, 1));
                                                }
                                                let mut width = self
                                                    .column_layout
                                                    .width(*source, INITIAL_COLUMN_WIDTH);
                                                if ui
                                                    .add(
                                                        egui::Slider::new(&mut width, 72.0..=480.0)
                                                            .suffix(" px"),
                                                    )
                                                    .changed()
                                                {
                                                    resize_column = Some((*source, width));
                                                }
                                                if ui.small_button("Fit").clicked() {
                                                    auto_fit_column = Some(*source);
                                                }
                                            });
                                        }
                                    });
                                if column_entries.len() == 128 {
                                    ui.label(
                                    "Showing the first 128 matching columns; narrow the search.",
                                );
                                }
                            }
                        });
                    });
            });
        self.show_tools = open;
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
        if let Some((source, shown)) = toggle_column {
            self.column_layout.set_hidden(source, !shown);
            self.apply_column_layout_change();
        }
        if let Some((source, direction)) = move_column
            && self.column_layout.move_source(source, direction)
        {
            self.apply_column_layout_change();
        }
        if let Some((source, width)) = resize_column {
            self.column_layout.set_width(source, width);
            self.save_column_layout();
        }
        if let Some(source) = auto_fit_column {
            self.auto_fit_column(source);
        }
        if reset_columns {
            self.reset_column_layout();
        }
    }
}
