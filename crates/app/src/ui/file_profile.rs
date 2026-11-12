use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};
use crate::profile::ColumnProfile;

/// Height of one profile table row.
const ROW_HEIGHT: f32 = 22.0;
/// Characters of a min or max value shown in the table; the tooltip has it all.
const VALUE_CHARS: usize = 28;

impl PaviApp {
    /// The whole-file profile: one row per column, in its own window.
    pub(in crate::app) fn show_file_profile_window(&mut self, ctx: &egui::Context) {
        if !self.profile.file.visible {
            return;
        }
        let mut open = true;
        let mut again = false;
        let mut cancel = false;
        let rows = self
            .dataset
            .as_ref()
            .map_or(0, |dataset| dataset.source.row_count());
        egui::Window::new("Profile")
            .open(&mut open)
            .collapsible(false)
            .default_size([760.0, 480.0])
            .min_size([480.0, 240.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.spacing_mut().item_spacing = Spacing::Md.vec(Spacing::Sm);
                ui.horizontal(|ui| {
                    if let Some(task) = &self.profile.file.task {
                        ui.add(egui::Spinner::new().size(12.0).color(theme::accent()));
                        let done = if rows == 0 {
                            String::new()
                        } else {
                            format!(
                                " {}",
                                super::status::percent(
                                    (task.scanned_rows() as f64 / rows as f64).min(1.0) as f32
                                )
                            )
                        };
                        ui.label(
                            RichText::new(format!("Scanning every column…{done}"))
                                .color(theme::muted()),
                        );
                        cancel =
                            widgets::tool_button(ui, Some(Icon::Close), "Cancel", false).clicked();
                    } else {
                        again =
                            widgets::tool_button(ui, Some(Icon::Refresh), "Profile again", false)
                                .clicked();
                    }
                    if self.profile.file.skipped_columns > 0 {
                        ui.label(
                            RichText::new(format!(
                                "The first {} columns; {} more are not profiled.",
                                crate::profile::MAX_FILE_PROFILE_COLUMNS,
                                self.profile.file.skipped_columns
                            ))
                            .font(theme::small())
                            .color(theme::muted()),
                        );
                    }
                });
                if let Some(error) = &self.profile.file.error {
                    widgets::error(ui, error);
                }
                if let Some(profiles) = &self.profile.file.result {
                    profile_table(ui, profiles);
                }
            });
        self.profile.file.visible = open;
        if !open && let Some(task) = &self.profile.file.task {
            task.cancel();
        }
        if cancel && let Some(task) = &self.profile.file.task {
            task.cancel();
        }
        if again {
            self.run_file_profile();
        }
    }
}

fn shorten(text: &str) -> String {
    if text.chars().count() > VALUE_CHARS {
        format!(
            "{}…",
            text.chars().take(VALUE_CHARS - 1).collect::<String>()
        )
    } else {
        text.to_owned()
    }
}

fn profile_table(ui: &mut egui::Ui, profiles: &[ColumnProfile]) {
    let muted = |text: String| RichText::new(text).color(theme::muted());
    egui_extras::TableBuilder::new(ui)
        .striped(true)
        .resizable(true)
        .cell_layout(Layout::left_to_right(Align::Center))
        .column(Column::initial(150.0).at_least(80.0))
        .column(Column::initial(90.0).at_least(50.0))
        .column(Column::initial(80.0).at_least(50.0))
        .column(Column::initial(80.0).at_least(50.0))
        .column(Column::initial(130.0).at_least(60.0))
        .column(Column::initial(130.0).at_least(60.0))
        .column(Column::remainder().at_least(60.0))
        .header(ROW_HEIGHT, |mut header| {
            for title in ["Column", "Type", "Nulls", "Distinct", "Min", "Max", "Mean"] {
                header.col(|ui| {
                    ui.label(RichText::new(title).font(theme::strong(12.5)));
                });
            }
        })
        .body(|body| {
            body.rows(ROW_HEIGHT, profiles.len(), |mut row| {
                let profile = &profiles[row.index()];
                row.col(|ui| {
                    ui.label(&profile.name);
                });
                row.col(|ui| {
                    ui.label(
                        RichText::new(super::inspector::type_label(&profile.data_type))
                            .font(theme::small())
                            .color(super::inspector::type_color(&profile.data_type)),
                    );
                });
                row.col(|ui| {
                    let share = if profile.row_count == 0 {
                        String::new()
                    } else {
                        format!(
                            " ({})",
                            super::status::percent(
                                profile.null_count as f32 / profile.row_count as f32
                            )
                        )
                    };
                    ui.label(format!("{}{share}", theme::number(profile.null_count)));
                });
                row.col(|ui| {
                    let comparable = parquet_reader::types::is_comparable(&profile.data_type);
                    if !comparable {
                        ui.label(muted("—".into()));
                    } else if profile.distinct_capped {
                        ui.label(format!(
                            "> {}",
                            theme::number(profile.distinct_count as u64)
                        ))
                        .on_hover_text("More distinct values than a whole-file profile counts");
                    } else {
                        ui.label(theme::number(profile.distinct_count as u64));
                    }
                });
                for value in [&profile.min, &profile.max] {
                    row.col(|ui| match value {
                        Some(value) => {
                            ui.label(shorten(value)).on_hover_text(value);
                        }
                        None => {
                            ui.label(muted("—".into()));
                        }
                    });
                }
                row.col(|ui| match profile.mean {
                    Some(mean) => {
                        ui.label(parquet_reader::value::axis_number(mean, 0.001));
                    }
                    None => {
                        ui.label(muted("—".into()));
                    }
                });
            });
        });
}
