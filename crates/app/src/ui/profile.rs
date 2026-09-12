use eframe::egui;

use crate::{
    chart::{ChartConfig, ChartKind, ChartModel, MAX_INPUT_ROWS},
    profile::ColumnProfile,
};

pub fn show(
    ui: &mut egui::Ui,
    profile: &ColumnProfile,
    draw_chart: impl FnOnce(&mut egui::Ui, &ChartModel, &ChartConfig),
) {
    ui.separator();
    ui.strong(format!("{} ({:?})", profile.name, profile.data_type));
    ui.label(format!(
        "Rows: {} · null: {} ({:.2}%) · non-null: {}",
        profile.row_count,
        profile.null_count,
        if profile.row_count == 0 {
            0.0
        } else {
            profile.null_count as f64 * 100.0 / profile.row_count as f64
        },
        profile.non_null_count(),
    ));
    if let Some(null_count) = profile.metadata_null_count {
        ui.label(format!("Parquet metadata null count: {null_count}"));
    }
    ui.label(format!(
        "Distinct: {} (exact; within bounded profile limit)",
        profile.distinct_count
    ));
    ui.label(format!(
        "Min: {} · max: {}",
        profile.min.as_deref().unwrap_or("none"),
        profile.max.as_deref().unwrap_or("none")
    ));
    if let Some(mean) = profile.mean {
        ui.label(format!("Mean: {mean:.4}"));
    }
    if let Some(lengths) = &profile.string_lengths {
        ui.label(format!(
            "String length: min {} · max {} · mean {:.2}",
            lengths.min, lengths.max, lengths.mean
        ));
    }
    if profile.non_finite_count > 0 {
        ui.label(format!(
            "{} non-finite numeric values excluded from mean/distribution",
            profile.non_finite_count
        ));
    }
    if !profile.frequent_values.is_empty() {
        ui.label("Most frequent values:");
        for value in &profile.frequent_values {
            ui.label(format!("{} · {}", value.value, value.count));
        }
    }
    if let Some(distribution) = &profile.distribution {
        ui.label(if profile.distribution_sampled {
            "Distribution: sampled first bounded numeric values"
        } else {
            "Distribution: all finite numeric values"
        });
        let config = ChartConfig {
            kind: ChartKind::Histogram,
            x_column: profile.name.clone(),
            y_column: String::new(),
            point_limit: MAX_INPUT_ROWS,
            bins: 20,
            title: format!("{} distribution", profile.name),
        };
        ui.allocate_ui(egui::vec2(ui.available_width(), 170.0), |ui| {
            draw_chart(ui, distribution, &config);
        });
    }
}
