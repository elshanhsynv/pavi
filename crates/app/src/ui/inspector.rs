use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};
use arrow_schema::DataType;

impl PaviApp {
    pub(in crate::app) fn show_inspector_panel(&mut self, ctx: &egui::Context) {
        if !self.show_inspector {
            return;
        }
        egui::SidePanel::right("inspector")
            .exact_width(280.0)
            .resizable(false)
            .frame(
                egui::Frame::new()
                    .fill(theme::BACKGROUND)
                    .inner_margin(Spacing::Md.margin()),
            )
            .show(ctx, |ui| {
                ui.spacing_mut().interact_size.y = 20.0;
                egui::ScrollArea::vertical()
                    .id_salt("inspector_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| self.show_metadata(ui));
            });
    }

    fn show_metadata(&mut self, ui: &mut egui::Ui) {
        let selected_column = self.selected_source_column();
        let selected_cell = self.selected_cell_details();
        let mut start_profile = None;
        let mut cancel_profile = false;
        if let Some(dataset) = &self.dataset {
            let metadata = dataset.source.metadata();
            widgets::card(ui, "Inspector", Icon::File, true, |ui| {
                widgets::button(
                    ui,
                    "Dataset",
                    Icon::Database,
                    false,
                    egui::vec2(ui.available_width(), 28.0),
                );
                let file = dataset
                    .path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                widgets::detail(ui, "File", file.to_string());
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [84.0, 20.0],
                        egui::Label::new("Path").halign(egui::Align::Min),
                    );
                    ui.add_sized(
                        [(ui.available_width() - 36.0).max(32.0), 20.0],
                        egui::Label::new(dataset.path.display().to_string()).truncate(),
                    )
                    .on_hover_text(dataset.path.display().to_string());
                    if widgets::button(ui, "", Icon::Copy, false, egui::vec2(28.0, 24.0))
                        .on_hover_text("Copy file path")
                        .clicked()
                    {
                        ui.ctx().copy_text(dataset.path.display().to_string());
                    }
                });
                widgets::detail(
                    ui,
                    "File size",
                    format_bytes(dataset.source.file_info().len()),
                );
                widgets::detail(ui, "Rows", theme::number(metadata.row_count));
                widgets::detail(ui, "Columns", metadata.column_count.to_string());
                widgets::detail(ui, "Row groups", metadata.row_groups.len().to_string());
                widgets::detail(ui, "Compression", dataset.source.compression());
                widgets::detail(
                    ui,
                    "Created (UTC)",
                    format_time(dataset.source.file_info().created()),
                );
                widgets::detail(
                    ui,
                    "Modified (UTC)",
                    format_time(dataset.source.file_info().modified()),
                );
            });
            widgets::card(
                ui,
                &format!("Schema ({} columns)", metadata.column_count),
                Icon::Grid,
                true,
                |ui| {
                    egui::ScrollArea::vertical()
                        .id_salt("schema_rows")
                        .max_height(240.0)
                        .show_rows(ui, 20.0, metadata.columns().len(), |ui, rows| {
                            for index in rows {
                                let column = &metadata.columns()[index];
                                ui.horizontal(|ui| {
                                    ui.add_sized(
                                        [118.0, 20.0],
                                        egui::Label::new(&column.name)
                                            .truncate()
                                            .halign(egui::Align::Min),
                                    )
                                    .on_hover_text(&column.name);
                                    ui.add(
                                        egui::Label::new(type_label(&column.data_type)).truncate(),
                                    )
                                    .on_hover_text(
                                        nested_schema_preview(&column.name, &column.data_type),
                                    );
                                });
                            }
                        });
                },
            );
            widgets::card(ui, "Selection", Icon::Cursor, true, |ui| {
                if let Some(cell) = &selected_cell {
                    widgets::detail(ui, "Row", theme::number(cell.row + 1));
                    widgets::detail(ui, "Column", &cell.name);
                    widgets::detail(ui, "Value", &cell.value);
                    ui.horizontal(|ui| {
                        if ui.small_button("Copy cell").clicked() {
                            ui.ctx().copy_text(cell.value.clone());
                        }
                        ui.checkbox(&mut self.show_safe_full_selection, "Full value");
                        ui.checkbox(&mut self.show_nested_selection, "Nested");
                    });
                    if let Some(value) = &cell.full_value {
                        ui.label(value);
                    }
                    if let Some(value) = &cell.expanded_value {
                        ui.label(value);
                    }
                } else {
                    ui.label("Select a cell to inspect its value.");
                }
            });
            widgets::card(ui, "Cache", Icon::Cache, true, |ui| {
                match dataset.source.cache_stats() {
                    Ok(stats) => {
                        widgets::detail(ui, "Status", format!("{} cached pages", stats.entries));
                        widgets::detail(ui, "Cache size", format_bytes(stats.bytes as u64));
                        widgets::detail(
                            ui,
                            "Pages cached",
                            format!(
                                "{} / {}",
                                stats.entries,
                                theme::number(
                                    metadata.row_count.div_ceil(parquet_reader::PAGE_ROWS)
                                )
                            ),
                        );
                        widgets::detail(
                            ui,
                            "Hits / misses",
                            format!("{} / {}", stats.hits, stats.misses),
                        );
                    }
                    Err(error) => {
                        ui.label(error.to_string());
                    }
                }
            });
            widgets::card(ui, "More details", Icon::Info, false, |ui| {
                widgets::section(ui, "Dataset Profile", Icon::Chart, |ui| {
                    ui.label(format!(
                        "Exact row count: {} (Parquet metadata)",
                        metadata.row_count
                    ));
                    ui.label("Column scans run only when Profile selected column is requested.");
                });
                widgets::section(ui, "Column", Icon::Schema, |ui| {
                    let Some(index) = selected_column else {
                        ui.label("Select a source-grid cell to inspect its column.");
                        return;
                    };
                    let Some(column) = metadata.columns().get(index) else {
                        ui.label("Column metadata is unavailable for this result column.");
                        return;
                    };
                    let summary = column_summary(column);
                    ui.label(format!("{}: {}", summary.index, summary.name));
                    ui.label(&summary.data_type);
                    ui.label(if summary.nullable {
                        "Nullable"
                    } else {
                        "Required"
                    });
                    ui.label(format!(
                        "Statistics: {}/{} row groups",
                        summary.available_statistics, summary.row_groups
                    ));
                    match summary.null_count {
                        Some(count) => ui.label(format!("Null count: {count}")),
                        None => ui.label("Null count: unavailable"),
                    };
                    egui::ScrollArea::vertical()
                        .id_salt("inspector_statistics")
                        .max_height(150.0)
                        .show_rows(ui, 42.0, column.row_group_statistics.len(), |ui, rows| {
                            for row_group in rows {
                                match &column.row_group_statistics[row_group] {
                                Some(statistics) => ui.label(format!(
                                    "Group {row_group}: min {} · max {}\nnulls: {} · distinct: {}",
                                    statistics.min.as_deref().unwrap_or("unavailable"),
                                    statistics.max.as_deref().unwrap_or("unavailable"),
                                    statistics.null_count.map_or_else(
                                        || "unavailable".to_string(),
                                        |count| count.to_string()
                                    ),
                                    statistics.distinct_count.map_or_else(
                                        || "unavailable".to_string(),
                                        |count| count.to_string()
                                    ),
                                )),
                                None => {
                                    ui.label(format!("Group {row_group}: statistics unavailable"))
                                }
                            };
                            }
                        });
                });
                widgets::section(ui, "Row Groups", Icon::Database, |ui| {
                    egui::ScrollArea::vertical()
                        .id_salt("inspector_row_groups")
                        .max_height(180.0)
                        .show_rows(ui, 20.0, metadata.row_groups.len(), |ui, rows| {
                            for index in rows {
                                let group = &metadata.row_groups[index];
                                ui.label(format!(
                                    "{}: rows {}–{} ({})",
                                    group.index,
                                    group.first_row + 1,
                                    group.first_row.saturating_add(group.row_count),
                                    group.row_count
                                ));
                            }
                        });
                });
                widgets::section(ui, "Profile", Icon::Chart, |ui| {
                    if let Some(index) = selected_column {
                        if self.profile.loading() && self.profile.column == Some(index) {
                            ui.spinner();
                            ui.label(&self.profile.status);
                            if ui.button("Cancel profile").clicked() {
                                cancel_profile = true;
                            }
                        } else if ui.button("Profile selected column").clicked() {
                            start_profile = Some(index);
                        }
                    } else {
                        ui.label("Select a source column to profile it on demand.");
                    }
                    if let Some(error) = &self.profile.error {
                        ui.label(RichText::new(error).color(egui::Color32::RED));
                    }
                    if let Some(profile) = &self.profile.result {
                        super::profile::show(ui, profile, super::charts::draw_chart);
                    }
                });
            });
        } else {
            widgets::card(ui, "Inspector", Icon::File, true, |ui| {
                ui.label("Open a Parquet file to view its metadata.");
            });
        }
        if cancel_profile {
            self.cancel_profile();
        }
        if let Some(column) = start_profile {
            self.run_profile(column);
        }
    }
}

pub fn type_label(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "i8".into(),
        DataType::Int16 => "i16".into(),
        DataType::Int32 => "i32".into(),
        DataType::Int64 => "i64".into(),
        DataType::UInt8 => "u8".into(),
        DataType::UInt16 => "u16".into(),
        DataType::UInt32 => "u32".into(),
        DataType::UInt64 => "u64".into(),
        DataType::Float16 => "f16".into(),
        DataType::Float32 => "f32".into(),
        DataType::Float64 => "f64".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "str".into(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("list<{}>", type_label(field.data_type()))
        }
        _ => data_type.to_string(),
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes} B")
    }
}
fn format_time(time: std::io::Result<std::time::SystemTime>) -> String {
    time.map(|time| {
        chrono::DateTime::<chrono::Utc>::from(time)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    })
    .unwrap_or_else(|_| "Unavailable".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn formats_real_metadata_and_nested_types() {
        assert_eq!(format_bytes(12_400_000_000), "12.40 GB");
        assert_eq!(
            format_time(Ok(std::time::UNIX_EPOCH)),
            "1970-01-01 00:00:00"
        );
        assert_eq!(
            type_label(&DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Utf8,
                true
            )))),
            "list<str>"
        );
    }
}
