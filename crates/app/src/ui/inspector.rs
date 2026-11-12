use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};
use arrow_schema::DataType;

/// Width the Inspector opens at; the user can drag it within `INSPECTOR_WIDTHS`.
const INSPECTOR_WIDTH: f32 = 280.0;
const INSPECTOR_WIDTHS: std::ops::RangeInclusive<f32> = 240.0..=460.0;
/// Most height the nested-value tree takes before it scrolls.
const TREE_HEIGHT: f32 = 260.0;
/// Height of one line of the Row section.
const FIELD_HEIGHT: f32 = 20.0;
/// Most height the Row section takes before it scrolls.
const ROW_SECTION_HEIGHT: f32 = 240.0;
/// Narrowest the column-name part of a Row line gets.
const FIELD_NAME_MIN_WIDTH: f32 = 90.0;

impl PaviApp {
    pub(in crate::app) fn show_inspector_panel(&mut self, ctx: &egui::Context) {
        if !self.show_inspector {
            return;
        }
        egui::SidePanel::right("inspector")
            .default_width(INSPECTOR_WIDTH)
            .width_range(INSPECTOR_WIDTHS)
            .resizable(true)
            .frame(egui::Frame::new().fill(theme::PANEL))
            .show(ctx, |ui| {
                ui.spacing_mut().item_spacing = Spacing::None.vec(Spacing::None);
                egui::ScrollArea::vertical()
                    .id_salt("inspector_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| self.show_metadata(ui));
            });
    }

    fn show_metadata(&mut self, ui: &mut egui::Ui) {
        let selected_column = self.selected_source_column();
        let selected_cell = self.selected_cell_details();
        let row_view = self.selected_row_view();
        let mut select_field = None;
        let mut start_profile = None;
        let mut cancel_profile = false;
        let Some(dataset) = &self.dataset else {
            widgets::placeholder(ui, |ui| {
                ui.label(
                    RichText::new("Open a file to see its schema and metadata.")
                        .color(theme::MUTED),
                );
            });
            return;
        };
        let metadata = dataset.source.metadata();

        widgets::section(ui, "Cell", "", true, |ui| {
            let Some(cell) = &selected_cell else {
                ui.label(RichText::new("Click a cell to see its full value.").color(theme::MUTED));
                return;
            };
            widgets::detail(ui, "Row", theme::number(cell.row + 1));
            widgets::detail(ui, "Column", &cell.name);
            ui.add_space(Spacing::Xs.px());
            egui::Frame::new()
                .fill(theme::BACKGROUND)
                .stroke(egui::Stroke::new(1.0_f32, theme::BORDER))
                .corner_radius(theme::RADIUS)
                .inner_margin(Spacing::Md.symmetric(Spacing::Sm))
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.add(egui::Label::new(&cell.value).wrap());
                });
            ui.add_space(Spacing::Xs.px());
            ui.horizontal(|ui| {
                if widgets::tool_button(ui, Some(Icon::Copy), "Copy", false).clicked() {
                    ui.ctx().copy_text(cell.value.clone());
                }
                ui.checkbox(&mut self.show_safe_full_selection, "Full value");
                if cell.tree.is_some() {
                    ui.checkbox(&mut self.show_nested_selection, "As text");
                }
            });
            if let Some(tree) = &cell.tree {
                ui.add_space(Spacing::Xs.px());
                let id = ui.make_persistent_id(("cell_tree", cell.row, cell.column));
                egui::ScrollArea::vertical()
                    .id_salt("cell_tree_scroll")
                    .max_height(TREE_HEIGHT)
                    .auto_shrink([false, true])
                    .show(ui, |ui| value_tree_ui(ui, tree, id, true));
            }
            for extra in [&cell.full_value, &cell.expanded_value]
                .into_iter()
                .flatten()
            {
                ui.add(egui::Label::new(extra).wrap());
            }
        });

        let row_note = row_view
            .as_ref()
            .map(|view| format!("{} columns", view.fields.len()))
            .unwrap_or_default();
        widgets::section(ui, "Row", &row_note, true, |ui| {
            let Some(view) = &row_view else {
                ui.label(
                    RichText::new("Click a cell to see its whole row, one column per line.")
                        .color(theme::MUTED),
                );
                return;
            };
            let selected = self.grid.selection.map(|(_, column)| column);
            egui::ScrollArea::vertical()
                .id_salt("inspector_row")
                .max_height(ROW_SECTION_HEIGHT)
                .auto_shrink([false, true])
                .show_rows(ui, FIELD_HEIGHT, view.fields.len(), |ui, range| {
                    for column in range {
                        let (name, batch_column) = &view.fields[column];
                        let value = view.batch.column(*batch_column);
                        let text = format_cell_with_limit(value.as_ref(), view.offset, CELL_LIMIT);
                        if field_row(ui, name, &text, selected == Some(column)).clicked() {
                            select_field = Some((view.row, column));
                        }
                    }
                });
        });

        widgets::section(ui, "Column", "", true, |ui| {
            let Some(index) = selected_column else {
                ui.label(
                    RichText::new("Click a cell to see its column's statistics.")
                        .color(theme::MUTED),
                );
                return;
            };
            let Some(column) = metadata.columns().get(index) else {
                ui.label(
                    RichText::new("This result column has no file metadata.").color(theme::MUTED),
                );
                return;
            };
            let summary = column_summary(column);
            widgets::detail(ui, "Name", &summary.name);
            widgets::detail(ui, "Position", (summary.index + 1).to_string());
            widgets::detail(ui, "Type", &summary.data_type);
            widgets::detail(
                ui,
                "Nulls",
                match (summary.nullable, summary.null_count) {
                    (false, _) => "Not allowed".to_owned(),
                    (true, Some(count)) => theme::number(count),
                    (true, None) => "Allowed, count not stored".to_owned(),
                },
            );
            widgets::detail(
                ui,
                "Statistics",
                format!(
                    "In {} of {} row groups",
                    summary.available_statistics, summary.row_groups
                ),
            );
            ui.add_space(Spacing::Sm.px());
            if self.profile.loading() && self.profile.column == Some(index) {
                ui.horizontal(|ui| {
                    ui.add(egui::Spinner::new().size(12.0).color(theme::ACCENT));
                    ui.label(RichText::new(&self.profile.status).color(theme::MUTED));
                    if ui.button("Cancel").clicked() {
                        cancel_profile = true;
                    }
                });
            } else if !parquet_reader::types::is_comparable(&column.data_type) {
                ui.label(
                    RichText::new("Profiling needs a column of plain values.")
                        .font(theme::small())
                        .color(theme::MUTED),
                );
            } else if widgets::tool_button(ui, Some(Icon::Chart), "Profile this column", false)
                .on_hover_text("Scan the column for nulls, distinct values and distribution")
                .clicked()
            {
                start_profile = Some(index);
            }
            if let Some(error) = &self.profile.error {
                widgets::error(ui, error);
            }
            if let Some(profile) = &self.profile.result {
                super::profile::show(ui, profile, super::charts::draw_chart);
            }
            if parquet_reader::value::is_nested(&column.data_type) {
                ui.add_space(Spacing::Sm.px());
                ui.label(
                    RichText::new("A nested column has no single min and max per row group.")
                        .font(theme::small())
                        .color(theme::MUTED),
                );
            } else if !column.row_group_statistics.is_empty() {
                ui.add_space(Spacing::Sm.px());
                ui.label(
                    RichText::new("Min and max per row group")
                        .font(theme::small())
                        .color(theme::MUTED),
                );
                egui::ScrollArea::vertical()
                    .id_salt("inspector_statistics")
                    .max_height(132.0)
                    .show_rows(ui, 20.0, column.row_group_statistics.len(), |ui, rows| {
                        for row_group in rows {
                            let text = column.row_group_statistics[row_group].as_ref().map_or_else(
                                || "not stored".to_owned(),
                                |statistics| {
                                    format!(
                                        "{} to {}",
                                        statistics.min.as_deref().unwrap_or("?"),
                                        statistics.max.as_deref().unwrap_or("?"),
                                    )
                                },
                            );
                            widgets::detail(ui, &format!("Group {row_group}"), text);
                        }
                    });
            }
        });

        widgets::section(ui, "File", "", true, |ui| {
            let file = dataset
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            widgets::detail(ui, "Name", file.to_string());
            widgets::detail(ui, "Folder", {
                dataset
                    .path
                    .parent()
                    .map(|parent| parent.display().to_string())
                    .unwrap_or_default()
            });
            widgets::detail(ui, "Size", format_bytes(dataset.source.file_info().len()));
            widgets::detail(ui, "Rows", theme::number(metadata.row_count));
            widgets::detail(ui, "Columns", metadata.column_count.to_string());
            widgets::detail(ui, "Row groups", metadata.row_groups.len().to_string());
            widgets::detail(ui, "Compression", dataset.source.compression());
            widgets::detail(
                ui,
                "Created",
                format_time(dataset.source.file_info().created()),
            );
            widgets::detail(
                ui,
                "Modified",
                format_time(dataset.source.file_info().modified()),
            );
            if widgets::tool_button(ui, Some(Icon::Copy), "Copy path", false).clicked() {
                ui.ctx().copy_text(dataset.path.display().to_string());
            }
        });

        widgets::section(
            ui,
            "Schema",
            &format!("{} columns", metadata.column_count),
            true,
            |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("schema_rows")
                    .max_height(260.0)
                    .auto_shrink([false, true])
                    .show_rows(ui, 20.0, metadata.columns().len(), |ui, rows| {
                        for index in rows {
                            let column = &metadata.columns()[index];
                            let (rect, response) = ui.allocate_exact_size(
                                egui::vec2(ui.available_width(), 20.0),
                                egui::Sense::hover(),
                            );
                            let label = type_label(&column.data_type);
                            let kind = ui.painter_at(rect).text(
                                rect.right_center(),
                                egui::Align2::RIGHT_CENTER,
                                &label,
                                theme::small(),
                                type_color(&column.data_type),
                            );
                            ui.painter_at(rect.with_max_x(kind.left() - Spacing::Md.px()))
                                .text(
                                    rect.left_center(),
                                    egui::Align2::LEFT_CENTER,
                                    &column.name,
                                    theme::body(),
                                    theme::TEXT,
                                );
                            response.on_hover_text(nested_schema_preview(
                                &column.name,
                                &column.data_type,
                            ));
                        }
                    });
            },
        );

        widgets::section(
            ui,
            "Row groups",
            &metadata.row_groups.len().to_string(),
            false,
            |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("inspector_row_groups")
                    .max_height(180.0)
                    .show_rows(ui, 20.0, metadata.row_groups.len(), |ui, rows| {
                        for index in rows {
                            let group = &metadata.row_groups[index];
                            widgets::detail(
                                ui,
                                &format!("Group {}", group.index),
                                format!(
                                    "rows {}–{}",
                                    theme::number(group.first_row + 1),
                                    theme::number(group.first_row.saturating_add(group.row_count)),
                                ),
                            );
                        }
                    });
            },
        );

        widgets::section(ui, "Page cache", "", false, |ui| {
            match dataset.source.cache_stats() {
                Ok(stats) => {
                    widgets::detail(
                        ui,
                        "Pages",
                        format!(
                            "{} of {}",
                            stats.entries,
                            theme::number(metadata.row_count.div_ceil(parquet_reader::PAGE_ROWS))
                        ),
                    );
                    widgets::detail(ui, "Memory", format_bytes(stats.bytes as u64));
                    widgets::detail(ui, "Hits", theme::number(stats.hits));
                    widgets::detail(ui, "Misses", theme::number(stats.misses));
                }
                Err(error) => {
                    widgets::error(ui, &error.to_string());
                }
            }
        });

        if let Some((row, column)) = select_field {
            self.grid.select(row, column);
            self.reveal_column(column);
        }
        if cancel_profile {
            self.cancel_profile();
        }
        if let Some(column) = start_profile {
            self.run_profile(column);
        }
    }
}

/// A nested value as collapsible lines: one per list item, struct field or map
/// entry. Only the first level starts open.
fn value_tree_ui(
    ui: &mut egui::Ui,
    node: &parquet_reader::value::ValueNode,
    id: egui::Id,
    open: bool,
) {
    if node.children.is_empty() && node.hidden == 0 {
        widgets::detail(ui, &node.label, &node.text);
        return;
    }
    egui::collapsing_header::CollapsingState::load_with_default_open(ui.ctx(), id, open)
        .show_header(ui, |ui| {
            ui.label(RichText::new(&node.label).color(theme::TEXT));
            ui.label(
                RichText::new(&node.text)
                    .font(theme::small())
                    .color(theme::MUTED),
            );
        })
        .body(|ui| {
            for (index, child) in node.children.iter().enumerate() {
                value_tree_ui(ui, child, id.with(index), false);
            }
            if node.hidden > 0 {
                ui.label(
                    RichText::new(format!(
                        "… {} more not shown",
                        theme::number(node.hidden as u64)
                    ))
                    .font(theme::small())
                    .color(theme::MUTED),
                );
            }
        });
}

/// One "column  value" line of the Row section; click it to select that cell.
fn field_row(ui: &mut egui::Ui, name: &str, value: &str, selected: bool) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), FIELD_HEIGHT),
        egui::Sense::click(),
    );
    response.widget_info(|| {
        egui::WidgetInfo::selected(egui::WidgetType::Button, ui.is_enabled(), selected, name)
    });
    if selected {
        ui.painter()
            .rect_filled(rect, theme::RADIUS, theme::SELECTED);
    } else if response.hovered() {
        ui.painter().rect_filled(rect, theme::RADIUS, theme::HOVER);
    }
    let split = rect.left() + (rect.width() * 0.42).max(FIELD_NAME_MIN_WIDTH);
    let pad = Spacing::Sm.px();
    ui.painter_at(rect.with_max_x(split - pad)).text(
        egui::pos2(rect.left() + pad, rect.center().y),
        egui::Align2::LEFT_CENTER,
        name,
        theme::body(),
        theme::MUTED,
    );
    let color = if value == "null" {
        theme::DISABLED
    } else {
        theme::TEXT
    };
    let drawn = ui.painter_at(rect.with_min_x(split)).text(
        egui::pos2(split, rect.center().y),
        egui::Align2::LEFT_CENTER,
        value,
        theme::body(),
        color,
    );
    if drawn.right() > rect.right() || name.len() > 24 {
        response.on_hover_text(format!("{name}\n{value}"))
    } else {
        response
    }
}

/// Text color of a type label, by type family.
pub fn type_color(data_type: &DataType) -> egui::Color32 {
    if data_type.is_numeric() {
        theme::TYPE_NUMBER
    } else if data_type.is_temporal() {
        theme::TYPE_TEMPORAL
    } else if matches!(data_type, DataType::Boolean) {
        theme::TYPE_BOOL
    } else {
        theme::MUTED
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
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("dec({precision},{scale})"),
        DataType::Time32(_) | DataType::Time64(_) => "time".into(),
        DataType::Struct(fields) => format!("struct<{}>", fields.len()),
        DataType::Map(_, _) => "map".into(),
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
        assert_eq!(type_label(&DataType::Decimal128(10, 2)), "dec(10,2)");
        assert_eq!(
            type_label(&DataType::Time64(arrow_schema::TimeUnit::Microsecond)),
            "time"
        );
    }
}
