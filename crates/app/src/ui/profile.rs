use eframe::egui::{self, RichText};

use super::{spacing::Spacing, theme, widgets};
use crate::{
    chart::{ChartConfig, ChartKind, ChartModel, MAX_INPUT_ROWS},
    profile::ColumnProfile,
};

/// Bins of the distribution chart under a profile.
const DISTRIBUTION_BINS: usize = 20;

pub fn show(
    ui: &mut egui::Ui,
    profile: &ColumnProfile,
    draw_chart: impl FnOnce(&mut egui::Ui, &ChartModel, &ChartConfig),
) {
    let caption = |ui: &mut egui::Ui, text: &str| {
        ui.add_space(Spacing::Sm.px());
        ui.label(RichText::new(text).font(theme::small()).color(theme::MUTED));
    };
    caption(ui, &format!("Profile of {}", profile.name));
    widgets::detail(ui, "Rows", theme::number(profile.row_count));
    widgets::detail(
        ui,
        "Nulls",
        format!(
            "{} ({:.2}%)",
            theme::number(profile.null_count),
            if profile.row_count == 0 {
                0.0
            } else {
                profile.null_count as f64 * 100.0 / profile.row_count as f64
            }
        ),
    );
    widgets::detail(ui, "Non-null", theme::number(profile.non_null_count()));
    if let Some(null_count) = profile.metadata_null_count {
        widgets::detail(ui, "Nulls in footer", theme::number(null_count));
    }
    widgets::detail(
        ui,
        "Distinct",
        format!(
            "{} (exact, up to the profile limit)",
            profile.distinct_count
        ),
    );
    widgets::detail(ui, "Min", profile.min.as_deref().unwrap_or("none"));
    widgets::detail(ui, "Max", profile.max.as_deref().unwrap_or("none"));
    if let Some(mean) = profile.mean {
        widgets::detail(ui, "Mean", format!("{mean:.4}"));
    }
    if let Some(lengths) = &profile.string_lengths {
        widgets::detail(
            ui,
            "Length",
            format!(
                "{} to {}, mean {:.2}",
                lengths.min, lengths.max, lengths.mean
            ),
        );
    }
    if profile.non_finite_count > 0 {
        widgets::detail(
            ui,
            "Non-finite",
            format!(
                "{} left out of mean and distribution",
                theme::number(profile.non_finite_count)
            ),
        );
    }
    if !profile.frequent_values.is_empty() {
        caption(ui, "Most frequent values");
        for value in &profile.frequent_values {
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(RichText::new(value.count.to_string()).color(theme::MUTED));
                    ui.with_layout(egui::Layout::left_to_right(egui::Align::Center), |ui| {
                        ui.add(egui::Label::new(&value.value).truncate())
                            .on_hover_text(&value.value);
                    });
                });
            });
        }
    }
    if let Some(distribution) = &profile.distribution {
        caption(
            ui,
            if profile.distribution_sampled {
                "Distribution of the first values read"
            } else {
                "Distribution of all finite values"
            },
        );
        let config = ChartConfig {
            kind: ChartKind::Histogram,
            x_column: profile.name.clone(),
            y_column: String::new(),
            point_limit: MAX_INPUT_ROWS,
            bins: DISTRIBUTION_BINS,
            title: String::new(),
        };
        ui.allocate_ui(egui::vec2(ui.available_width(), 150.0), |ui| {
            draw_chart(ui, distribution, &config);
        });
    }
}
