use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

/// Width of the label column of the chart form.
const FIELD_LABEL_WIDTH: f32 = 76.0;
/// Width of a column-name box in the chart form.
const COLUMN_BOX_WIDTH: f32 = 220.0;
/// Height kept free for the plot before a chart is drawn.
const EMPTY_PLOT_HEIGHT: f32 = 200.0;
/// Columns listed in a column picker; type the name to reach the rest.
const PICKER_COLUMNS: usize = 128;

impl PaviApp {
    pub(in crate::app) fn show_chart_window(&mut self, ctx: &egui::Context) {
        if !self.chart.visible {
            return;
        }
        let names = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
            .unwrap_or_default();
        let chart_was_visible = self.chart.visible;
        let mut open = chart_was_visible;
        let mut run = false;
        let mut cancel = false;
        egui::Window::new("Charts")
            .open(&mut open)
            .collapsible(false)
            .default_size([640.0, 540.0])
            .min_size([420.0, 320.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.spacing_mut().item_spacing = Spacing::Md.vec(Spacing::Sm);
                let histogram = self.chart.config.kind == ChartKind::Histogram;
                field(ui, "Source", |ui| {
                    if let Some(source) = widgets::segmented(
                        ui,
                        self.chart.source,
                        &[
                            (ChartSource::Sql, ChartSource::Sql.label(), true),
                            (
                                ChartSource::CurrentResult,
                                ChartSource::CurrentResult.label(),
                                true,
                            ),
                        ],
                    ) {
                        self.chart.source = source;
                    }
                    if self.chart.source == ChartSource::CurrentResult {
                        caption(ui, "Charts only the rows loaded in the grid.");
                    }
                });
                if self.chart.source == ChartSource::Sql {
                    ui.add(
                        egui::TextEdit::multiline(&mut self.chart.sql_input)
                            .code_editor()
                            .desired_rows(3)
                            .desired_width(f32::INFINITY)
                            .margin(Spacing::Md.margin())
                            .hint_text("SELECT x, y FROM dataset WHERE …"),
                    );
                }
                field(ui, "Type", |ui| {
                    let kinds = ChartKind::ALL.map(|kind| (kind, kind.label(), true));
                    if let Some(kind) = widgets::segmented(ui, self.chart.config.kind, &kinds) {
                        self.chart.config.kind = kind;
                    }
                });
                column_field(
                    ui,
                    if histogram { "Values" } else { "X column" },
                    "chart_x",
                    &mut self.chart.config.x_column,
                    &names,
                );
                if !histogram {
                    column_field(
                        ui,
                        "Y column",
                        "chart_y",
                        &mut self.chart.config.y_column,
                        &names,
                    );
                }
                field(ui, "Title", |ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.chart.config.title)
                            .desired_width(COLUMN_BOX_WIDTH)
                            .hint_text("Optional"),
                    );
                });
                field(ui, "Limits", |ui| {
                    ui.add(
                        egui::DragValue::new(&mut self.chart.config.point_limit)
                            .range(2..=MAX_INPUT_ROWS)
                            .speed(10)
                            .suffix(" points"),
                    )
                    .on_hover_text("Most points the chart draws; more are thinned out");
                    if histogram {
                        ui.add(
                            egui::DragValue::new(&mut self.chart.config.bins)
                                .range(1..=100)
                                .suffix(" bins"),
                        );
                    }
                });
                ui.add_space(Spacing::Xs.px());
                ui.horizontal(|ui| {
                    if widgets::primary_button(ui, Some(Icon::Play), "Draw chart").clicked() {
                        run = true;
                    }
                    if self.chart.loading() {
                        if widgets::tool_button(ui, Some(Icon::Close), "Cancel", false).clicked() {
                            cancel = true;
                        }
                        ui.add(egui::Spinner::new().size(12.0).color(theme::ACCENT));
                    }
                    ui.add(
                        egui::Label::new(
                            RichText::new(&self.chart.status)
                                .font(theme::small())
                                .color(theme::MUTED),
                        )
                        .truncate(),
                    )
                    .on_hover_text(&self.chart.status);
                });
                if let Some(error) = &self.chart.error {
                    widgets::error(ui, error);
                }
                if let Some(model) = &self.chart.model {
                    chart_notice(ui, model);
                    draw_chart(ui, model, &self.chart.config);
                } else {
                    ui.set_min_height(EMPTY_PLOT_HEIGHT);
                    widgets::placeholder(ui, |ui| {
                        if self.chart.loading() {
                            ui.add(egui::Spinner::new().color(theme::ACCENT));
                        } else {
                            ui.label(
                                RichText::new("Pick a source and columns, then draw the chart.")
                                    .color(theme::MUTED),
                            );
                        }
                    });
                }
            });
        self.chart.visible = open;
        if self.chart.visible != chart_was_visible {
            self.persist_session();
        }
        if cancel {
            self.cancel_chart();
        }
        if run {
            self.run_chart();
        }
    }
}

/// One "label  control" row of the chart form.
fn field(ui: &mut egui::Ui, label: &str, body: impl FnOnce(&mut egui::Ui)) {
    ui.horizontal(|ui| {
        let (rect, _) = ui.allocate_exact_size(
            egui::vec2(FIELD_LABEL_WIDTH, theme::CONTROL_HEIGHT),
            egui::Sense::hover(),
        );
        ui.painter().text(
            rect.left_center(),
            egui::Align2::LEFT_CENTER,
            label,
            theme::body(),
            theme::MUTED,
        );
        body(ui);
    });
}

/// A column name: typed, because a chart query can name columns the file does
/// not have, or picked from the file's columns.
fn column_field(
    ui: &mut egui::Ui,
    label: &str,
    id_salt: &str,
    value: &mut String,
    names: &[String],
) {
    field(ui, label, |ui| {
        ui.add(
            egui::TextEdit::singleline(value)
                .desired_width(COLUMN_BOX_WIDTH)
                .hint_text("Column name"),
        );
        widgets::menu(ui, id_salt, Some(Icon::Columns), "", false, 260.0, |ui| {
            if names.is_empty() {
                widgets::menu_caption(ui, "Open a file to list its columns.");
            }
            egui::ScrollArea::vertical()
                .max_height(280.0)
                .show(ui, |ui| {
                    for name in names.iter().take(PICKER_COLUMNS) {
                        if widgets::menu_item(ui, name, "").clicked() {
                            value.clone_from(name);
                            widgets::close_menu(ui);
                        }
                    }
                });
            if names.len() > PICKER_COLUMNS {
                widgets::menu_caption(
                    ui,
                    &format!("First {PICKER_COLUMNS} columns. Type the name for the rest."),
                );
            }
        })
        .on_hover_text("Pick a column of the file");
    });
}

fn caption(ui: &mut egui::Ui, text: &str) {
    ui.label(RichText::new(text).font(theme::small()).color(theme::MUTED));
}

fn chart_notice(ui: &mut egui::Ui, model: &ChartModel) {
    if model.is_empty() {
        caption(ui, "No chartable non-null values were returned.");
        return;
    }
    if model.input_capped || model.reduced {
        caption(
            ui,
            &format!(
                "Showing {} chart values from {} bounded input rows{}.",
                model.output_len(),
                model.input_rows,
                if model.input_capped {
                    format!("; query sampling stopped at {MAX_INPUT_ROWS}")
                } else {
                    String::new()
                }
            ),
        );
    }
}

pub(in crate::app) fn draw_chart(ui: &mut egui::Ui, model: &ChartModel, config: &ChartConfig) {
    let size = egui::vec2(
        ui.available_width().max(240.0),
        ui.available_height().max(220.0),
    );
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 4.0, theme::BACKGROUND);
    if model.is_empty() {
        painter.text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            "No chartable values",
            theme::strong(13.0),
            theme::MUTED,
        );
        return;
    }
    let plot = rect.shrink2(egui::vec2(48.0, 28.0));
    painter.line_segment(
        [plot.left_bottom(), plot.left_top()],
        egui::Stroke::new(1.0_f32, theme::MUTED),
    );
    painter.line_segment(
        [plot.left_bottom(), plot.right_bottom()],
        egui::Stroke::new(1.0_f32, theme::MUTED),
    );
    let title = if config.title.trim().is_empty() {
        format!("{} chart", model.kind.label())
    } else {
        config.title.clone()
    };
    painter.text(
        rect.left_top() + egui::vec2(8.0, 6.0),
        egui::Align2::LEFT_TOP,
        title,
        theme::strong(13.0),
        theme::TEXT,
    );

    let tooltip = match &model.values {
        ChartValues::Points(points) => {
            draw_points(&painter, plot, points, model.kind, response.hover_pos())
        }
        ChartValues::Bars(bars) => draw_bars(&painter, plot, bars, response.hover_pos()),
        ChartValues::Histogram(bins) => draw_histogram(&painter, plot, bins, response.hover_pos()),
    };
    painter.text(
        plot.left_bottom() + egui::vec2(0.0, 6.0),
        egui::Align2::LEFT_TOP,
        &config.x_column,
        theme::small(),
        theme::MUTED,
    );
    if model.kind != ChartKind::Histogram {
        painter.text(
            plot.left_top() - egui::vec2(42.0, 0.0),
            egui::Align2::LEFT_TOP,
            &config.y_column,
            theme::small(),
            theme::MUTED,
        );
    }
    if let Some(tooltip) = tooltip {
        response.on_hover_text_at_pointer(tooltip);
    } else {
        response.on_hover_text("Hover a plotted value for details");
    }
}

fn numeric_range(values: impl Iterator<Item = f64>) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in values {
        min = min.min(value);
        max = max.max(value);
    }
    if min == max {
        let pad = if min == 0.0 { 1.0 } else { min.abs() * 0.05 };
        (min - pad, max + pad)
    } else {
        (min, max)
    }
}

fn point_at(
    plot: egui::Rect,
    x: f64,
    y: f64,
    x_range: (f64, f64),
    y_range: (f64, f64),
) -> egui::Pos2 {
    let x = ((x - x_range.0) / (x_range.1 - x_range.0)) as f32;
    let y = ((y - y_range.0) / (y_range.1 - y_range.0)) as f32;
    egui::pos2(
        egui::lerp(plot.left()..=plot.right(), x),
        egui::lerp(plot.bottom()..=plot.top(), y),
    )
}

fn draw_points(
    painter: &egui::Painter,
    plot: egui::Rect,
    points: &[crate::chart::ChartPoint],
    kind: ChartKind,
    hover: Option<egui::Pos2>,
) -> Option<String> {
    let x_range = numeric_range(points.iter().map(|point| point.x));
    let y_range = numeric_range(points.iter().map(|point| point.y));
    let screen_points = points
        .iter()
        .map(|point| point_at(plot, point.x, point.y, x_range, y_range))
        .collect::<Vec<_>>();
    if kind == ChartKind::Line {
        for pair in screen_points.windows(2) {
            painter.line_segment(
                [pair[0], pair[1]],
                egui::Stroke::new(1.5_f32, theme::CHART_POINT),
            );
        }
    }
    for point in &screen_points {
        painter.circle_filled(
            *point,
            if kind == ChartKind::Scatter { 2.5 } else { 2.0 },
            theme::CHART_POINT,
        );
    }
    hover.and_then(|hover| {
        screen_points
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.distance_sq(hover).total_cmp(&right.distance_sq(hover))
            })
            .and_then(|(index, point)| {
                (point.distance(hover) <= 14.0)
                    .then(|| format!("{}: {}\n{}: {}", "X", points[index].x, "Y", points[index].y))
            })
    })
}

fn draw_bars(
    painter: &egui::Painter,
    plot: egui::Rect,
    bars: &[crate::chart::ChartBar],
    hover: Option<egui::Pos2>,
) -> Option<String> {
    let (_, max) = numeric_range(bars.iter().map(|bar| bar.value));
    let min = bars.iter().map(|bar| bar.value).fold(0.0_f64, f64::min);
    let y_range = if min == max {
        (min - 1.0, max + 1.0)
    } else {
        (min, max)
    };
    let width = plot.width() / bars.len() as f32;
    let baseline = point_at(plot, 0.0, 0.0, (0.0, 1.0), y_range).y;
    let mut hovered = None;
    for (index, bar) in bars.iter().enumerate() {
        let left = plot.left() + index as f32 * width + 1.0;
        let y = point_at(plot, 0.0, bar.value, (0.0, 1.0), y_range).y;
        let bar_rect = egui::Rect::from_min_max(
            egui::pos2(left, y.min(baseline)),
            egui::pos2((left + width - 2.0).max(left + 1.0), y.max(baseline)),
        );
        painter.rect_filled(bar_rect, 1.0, theme::CHART_BAR);
        if hover.is_some_and(|hover| bar_rect.expand(3.0).contains(hover)) {
            hovered = Some(format!("{}: {}", bar.label, bar.value));
        }
    }
    hovered
}

fn draw_histogram(
    painter: &egui::Painter,
    plot: egui::Rect,
    bins: &[crate::chart::HistogramBin],
    hover: Option<egui::Pos2>,
) -> Option<String> {
    let max = bins.iter().map(|bin| bin.count).max().unwrap_or(1).max(1) as f32;
    let width = plot.width() / bins.len() as f32;
    let mut hovered = None;
    for (index, bin) in bins.iter().enumerate() {
        let left = plot.left() + index as f32 * width + 1.0;
        let height = plot.height() * bin.count as f32 / max;
        let bar_rect = egui::Rect::from_min_max(
            egui::pos2(left, plot.bottom() - height),
            egui::pos2((left + width - 2.0).max(left + 1.0), plot.bottom()),
        );
        painter.rect_filled(bar_rect, 1.0, theme::CHART_BAR);
        if hover.is_some_and(|hover| bar_rect.expand(3.0).contains(hover)) {
            hovered = Some(format!("{}–{}: {}", bin.start, bin.end, bin.count));
        }
    }
    hovered
}
