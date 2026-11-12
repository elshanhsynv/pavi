use super::{spacing::Spacing, theme, widgets};
use eframe::egui::{self, Align, Layout, RichText};

/// Facts about the open file shown on the right of the status bar.
pub struct DatasetSummary<'a> {
    pub rows: u64,
    pub columns: usize,
    pub row_groups: usize,
    pub compression: &'a str,
}

pub struct StatusBar<'a> {
    pub status: &'a str,
    pub activity: Option<&'static str>,
    /// How far the running scan has got, from 0 to 1.
    pub progress: Option<f32>,
    pub has_error: bool,
    pub selection: Option<(u64, usize)>,
    pub dataset: Option<DatasetSummary<'a>>,
    pub path: Option<&'a std::path::Path>,
    pub jump_input: &'a mut String,
}

/// A 0–1 fraction as a whole percentage, e.g. "42%".
pub fn percent(fraction: f32) -> String {
    format!("{:.0}%", (fraction.clamp(0.0, 1.0) * 100.0).floor())
}

/// Id of the "go to row" box, so Ctrl+G can focus it.
pub fn jump_box_id() -> egui::Id {
    egui::Id::new("jump_row")
}

/// Draws the status bar. Returns `true` when the user asked to jump to the typed row.
pub fn show(ctx: &egui::Context, status: StatusBar<'_>) -> bool {
    let mut jump = false;
    egui::TopBottomPanel::bottom("status_bar")
        .exact_height(theme::STATUS_HEIGHT)
        .resizable(false)
        .frame(
            egui::Frame::new()
                .fill(theme::PANEL)
                .inner_margin(Spacing::Lg.symmetric(Spacing::None)),
        )
        .show(ctx, |ui| {
            ui.horizontal_centered(|ui| {
                ui.spacing_mut().item_spacing.x = Spacing::Md.px();
                ui.spacing_mut().interact_size.y = 20.0;
                let small = |text: String| RichText::new(text).font(theme::small());
                if status.activity.is_some() {
                    ui.add(egui::Spinner::new().size(11.0).color(theme::ACCENT));
                } else {
                    let (rect, _) =
                        ui.allocate_exact_size(egui::vec2(8.0, 8.0), egui::Sense::hover());
                    ui.painter().circle_filled(
                        rect.center(),
                        3.5,
                        if status.has_error {
                            theme::ERROR
                        } else {
                            theme::SUCCESS
                        },
                    );
                }
                let message = if status.status.starts_with("Ready:") {
                    status.path.map_or_else(
                        || status.status.to_string(),
                        |path| format!("Loaded {}", parquet_reader::display_path(path).display()),
                    )
                } else {
                    status.status.to_string()
                };
                // "Sorting" next to "Sorting DEP_DELAY …" says the same thing twice.
                if let Some(activity) = status.activity
                    && !message.starts_with(activity)
                {
                    ui.label(small(activity.to_owned()).color(theme::TEXT));
                }
                if let Some(progress) = status.progress {
                    ui.label(small(percent(progress)).color(theme::TEXT))
                        .on_hover_text("Share of the file scanned, or of the joins done, so far");
                }
                let right_width = if status.dataset.is_some() { 560.0 } else { 0.0 };
                ui.allocate_ui_with_layout(
                    egui::vec2(
                        (ui.available_width() - right_width).max(60.0),
                        theme::STATUS_HEIGHT,
                    ),
                    Layout::left_to_right(Align::Center),
                    |ui| {
                        ui.add(
                            egui::Label::new(small(message.clone()).color(if status.has_error {
                                theme::ERROR
                            } else {
                                theme::MUTED
                            }))
                            .truncate(),
                        )
                        .on_hover_text(&message);
                    },
                );
                let Some(dataset) = status.dataset else {
                    return;
                };
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let response = ui.add(
                        egui::TextEdit::singleline(status.jump_input)
                            .id(jump_box_id())
                            .desired_width(96.0)
                            .font(theme::small())
                            .vertical_align(Align::Center)
                            .margin(Spacing::Sm.symmetric(Spacing::None))
                            .hint_text("Go to row"),
                    );
                    if response.lost_focus()
                        && ui.input(|input| input.key_pressed(egui::Key::Enter))
                    {
                        jump = true;
                    }
                    response.on_hover_text("Type a row number and press Enter (Ctrl+G)");
                    for fact in [
                        dataset.compression.to_owned(),
                        format!(
                            "{} row group{}",
                            theme::number(dataset.row_groups as u64),
                            if dataset.row_groups == 1 { "" } else { "s" }
                        ),
                        format!("{} columns", dataset.columns),
                        format!("{} rows", theme::number(dataset.rows)),
                    ] {
                        widgets::divider(ui);
                        ui.label(small(fact).color(theme::MUTED));
                    }
                    if let Some((row, column)) = status.selection {
                        widgets::divider(ui);
                        ui.label(
                            small(format!(
                                "Row {}, column {}",
                                theme::number(row + 1),
                                column + 1
                            ))
                            .color(theme::TEXT),
                        );
                    }
                });
            });
        });
    jump
}

#[cfg(test)]
mod tests {
    #[test]
    fn formats_scan_progress_as_a_whole_percentage() {
        for (fraction, expected) in [
            (0.0, "0%"),
            (0.004, "0%"),
            (0.425, "42%"),
            (0.999, "99%"),
            (1.0, "100%"),
            (7.0, "100%"),
            (-1.0, "0%"),
        ] {
            assert_eq!(super::percent(fraction), expected);
        }
    }
}
