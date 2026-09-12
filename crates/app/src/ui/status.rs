use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};
use eframe::egui::{self, Align, Layout, RichText};

pub struct StatusBar<'a> {
    pub status: &'a str,
    pub activity: Option<&'static str>,
    pub has_error: bool,
    pub selection: Option<(u64, usize)>,
    pub cached_pages: usize,
    pub columns: Option<usize>,
    pub rows: u64,
    pub path: Option<&'a std::path::Path>,
}

pub fn show(ctx: &egui::Context, status: StatusBar<'_>) {
    egui::TopBottomPanel::bottom("status_bar")
        .exact_height(40.0)
        .resizable(false)
        .frame(
            egui::Frame::new()
                .fill(theme::PANEL)
                .inner_margin(Spacing::Lg.symmetric(Spacing::Sm))
                .stroke(egui::Stroke::new(1.0_f32, theme::BORDER)),
        )
        .show(ctx, |ui| {
            ui.horizontal_centered(|ui| {
                if status.activity.is_some() {
                    ui.spinner();
                } else {
                    let (rect, _) =
                        ui.allocate_exact_size(egui::vec2(18.0, 18.0), egui::Sense::hover());
                    ui.painter().circle_filled(
                        rect.center(),
                        8.0,
                        if status.has_error {
                            egui::Color32::LIGHT_RED
                        } else {
                            egui::Color32::from_rgb(25, 193, 123)
                        },
                    );
                }
                ui.label(status.activity.unwrap_or(if status.has_error {
                    "Error"
                } else {
                    "Ready"
                }));
                ui.separator();
                let right_width = if status.columns.is_some() {
                    350.0
                } else {
                    150.0
                };
                let message = if status.status.starts_with("Ready:") {
                    status.path.map_or_else(
                        || status.status.to_string(),
                        |path| format!("Loaded {}", path.display()),
                    )
                } else {
                    status.status.to_string()
                };
                ui.allocate_ui_with_layout(
                    egui::vec2((ui.available_width() - right_width).max(70.0), 28.0),
                    Layout::left_to_right(Align::Center),
                    |ui| {
                        ui.add(
                            egui::Label::new(RichText::new(&message).color(if status.has_error {
                                egui::Color32::LIGHT_RED
                            } else {
                                theme::MUTED
                            }))
                            .truncate(),
                        )
                        .on_hover_text(&message);
                    },
                );
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    ui.label(
                        RichText::new(format!("{} cached pages", status.cached_pages))
                            .color(theme::MUTED),
                    );
                    let (rect, _) =
                        ui.allocate_exact_size(egui::vec2(25.0, 24.0), egui::Sense::hover());
                    widgets::icon(ui, rect.center(), Icon::Database, theme::MUTED);
                    ui.separator();
                    if let Some(columns) = status.columns {
                        let response = ui.label(
                            RichText::new(format!(
                                "{} rows × {} columns",
                                theme::number(status.rows),
                                columns
                            ))
                            .color(theme::MUTED),
                        );
                        if let Some((row, column)) = status.selection {
                            response.on_hover_text(format!(
                                "Selected: row {}, column {}",
                                row + 1,
                                column + 1
                            ));
                        }
                    }
                });
            });
        });
}
