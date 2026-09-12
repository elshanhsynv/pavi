use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

impl PaviApp {
    pub(in crate::app) fn show_dataset_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("dataset_bar")
            .frame(
                egui::Frame::new()
                    .fill(theme::PANEL)
                    .inner_margin(Spacing::Lg.symmetric(Spacing::Sm)),
            )
            .show(ctx, |ui| {
                ui.horizontal_wrapped(|ui| {
                    let path_width = (ui.available_width() * 0.40).clamp(180.0, 600.0);
                    let response = ui.add_sized(
                        [path_width, 32.0],
                        egui::TextEdit::singleline(&mut self.path_input)
                            .margin(egui::Margin {
                                left: 32,
                                ..Spacing::Sm.margin()
                            })
                            .hint_text("Enter a Parquet file path…"),
                    );
                    widgets::icon(
                        ui,
                        response.rect.left_center() - Spacing::Xl.vec(Spacing::None),
                        Icon::Folder,
                        theme::ACCENT,
                    );
                    response
                        .clone()
                        .on_hover_text("Enter a path and press Enter to open");
                    if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        let path = self.path_input.trim();
                        if !path.is_empty() {
                            self.begin_open(PathBuf::from(path));
                        }
                    }
                    if let Some(dataset) = &self.dataset {
                        let (rect, _) =
                            ui.allocate_exact_size(egui::vec2(24.0, 28.0), egui::Sense::hover());
                        widgets::icon(ui, rect.center(), Icon::Database, theme::ACCENT);
                        ui.add(
                            egui::Label::new(
                                dataset
                                    .path
                                    .file_name()
                                    .unwrap_or_default()
                                    .to_string_lossy(),
                            )
                            .truncate(),
                        )
                        .on_hover_text(dataset.path.display().to_string());
                        ui.separator();
                        ui.label(
                            RichText::new(format!(
                                "{} rows",
                                theme::number(dataset.source.row_count())
                            ))
                            .color(theme::MUTED),
                        );
                        ui.separator();
                        ui.label(
                            RichText::new(format!("{} columns", dataset.column_names.len()))
                                .color(theme::MUTED),
                        );
                        ui.separator();
                        ui.label(
                            RichText::new(format!(
                                "{} row groups",
                                dataset.source.row_groups().len()
                            ))
                            .color(theme::MUTED),
                        );
                        ui.separator();
                        ui.label(RichText::new(dataset.source.compression()).color(theme::MUTED));
                    }
                    ui.separator();
                    ui.label("Jump to row:");
                    let jump = ui.add_sized(
                        [68.0, 28.0],
                        egui::TextEdit::singleline(&mut self.jump_row_input)
                            .hint_text("1")
                            .margin(Spacing::Sm.vec(Spacing::Sm)),
                    );
                    if ui
                        .add_sized([48.0, 28.0], egui::Button::new("Go").fill(theme::SELECTED))
                        .clicked()
                        || (jump.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                    {
                        self.jump_to_input();
                    }
                });
            });
    }
    pub(in crate::app) fn jump_to_input(&mut self) {
        match self.jump_row_input.trim().parse::<u64>() {
            Ok(one_based) if one_based > 0 => {
                let row = one_based - 1;
                if let Some(filtered) = &self.filtered {
                    if row < filtered.first_result
                        || row >= filtered.first_result.saturating_add(filtered.rows)
                    {
                        self.status =
                            "That result row is outside the current bounded window".to_string();
                        return;
                    }
                    self.grid.select_filtered(
                        filtered.first_result,
                        row - filtered.first_result,
                        0,
                    );
                } else if row < self.grid.rows {
                    self.grid.jump_to_row(row);
                } else {
                    self.status = "Enter a row number within the current result set".to_string();
                    return;
                }
                self.jump_target = Some(row);
                if self.filtered.is_none()
                    && let Some(page) = self.grid.page_for_row(row)
                {
                    self.request_page(page);
                }
                self.status = format!("Jumped to row {one_based}");
            }
            _ => self.status = "Enter a row number within the current result set".to_string(),
        }
    }
}
