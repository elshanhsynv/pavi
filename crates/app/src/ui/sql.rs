use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

/// Lines the SQL editor shows before it scrolls.
const EDITOR_ROWS: usize = 6;

impl PaviApp {
    pub(in crate::app) fn show_sql_workspace(&mut self, ui: &mut egui::Ui) {
        let mut run = false;
        ui.spacing_mut().item_spacing.y = 0.0;
        egui::Frame::new()
            .fill(theme::PANEL)
            .inner_margin(Spacing::Md.margin())
            .show(ui, |ui| {
                let editor = ui.add(
                    egui::TextEdit::multiline(&mut self.sql_input)
                        .code_editor()
                        .desired_rows(EDITOR_ROWS)
                        .desired_width(f32::INFINITY)
                        .margin(Spacing::Md.margin())
                        .hint_text("SELECT column, COUNT(*) FROM dataset WHERE … GROUP BY column LIMIT 100"),
                );
                run = editor.has_focus()
                    && ui.input(|input| {
                        input.modifiers.command && input.key_pressed(egui::Key::Enter)
                    });
                ui.add_space(Spacing::Xs.px());
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled_ui(self.dataset.is_some(), |ui| {
                            widgets::primary_button(ui, Some(Icon::Play), "Run")
                        })
                        .inner
                        .on_hover_text("Run the query (Ctrl+Enter in the editor)")
                        .clicked()
                    {
                        run = true;
                    }
                    if self.running_sql.is_some() {
                        if widgets::tool_button(ui, Some(Icon::Close), "Cancel", false).clicked() {
                            self.cancel_sql();
                        }
                        ui.add(egui::Spinner::new().size(12.0).color(theme::ACCENT));
                        ui.label(RichText::new("Running…").color(theme::MUTED));
                    }
                    self.history_menu(ui);
                    if let Some(error) = &self.sql_error {
                        widgets::error(ui, error);
                    } else if self.running_sql.is_none() {
                        ui.label(
                            RichText::new("The table is always named dataset.")
                                .font(theme::small())
                                .color(theme::MUTED),
                        );
                    }
                });
            });
        if run {
            self.run_sql();
        }
        self.show_grid(ui);
    }

    fn history_menu(&mut self, ui: &mut egui::Ui) {
        let mut rerun = None;
        let mut remove = None;
        let mut clear = false;
        widgets::menu(
            ui,
            "history",
            Some(Icon::History),
            "History",
            false,
            460.0,
            |ui| {
                if self.session.query_history.is_empty() {
                    widgets::menu_caption(ui, "Queries you run are kept here.");
                    return;
                }
                egui::ScrollArea::vertical()
                    .max_height(360.0)
                    .show(ui, |ui| {
                        for (index, entry) in self.session.query_history.iter().enumerate() {
                            let outcome = if entry.success {
                                entry.row_count.map_or_else(
                                    || format!("{} ms", entry.duration_ms),
                                    |rows| {
                                        format!(
                                            "{} rows, {} ms",
                                            theme::number(rows),
                                            entry.duration_ms
                                        )
                                    },
                                )
                            } else {
                                "failed".to_owned()
                            };
                            ui.horizontal(|ui| {
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    if widgets::tool_button(ui, Some(Icon::Close), "", false)
                                        .on_hover_text("Remove from history")
                                        .clicked()
                                    {
                                        remove = Some(index);
                                    }
                                    ui.with_layout(Layout::top_down_justified(Align::Min), |ui| {
                                        if widgets::menu_item(ui, &entry.sql, &outcome)
                                            .on_hover_text(&entry.sql)
                                            .clicked()
                                        {
                                            rerun = Some(entry.sql.clone());
                                        }
                                    });
                                });
                            });
                        }
                    });
                ui.separator();
                clear = widgets::menu_item(ui, "Clear history", "").clicked();
                if rerun.is_some() || clear {
                    widgets::close_menu(ui);
                }
            },
        );
        if let Some(sql) = rerun {
            self.sql_input = sql;
            self.run_sql();
        }
        if let Some(index) = remove {
            self.session.remove_history(index);
            self.persist_session();
        }
        if clear {
            self.session.clear_history();
            self.persist_session();
        }
    }
}
