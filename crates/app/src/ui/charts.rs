use super::super::*;

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
            .default_size([620.0, 520.0])
            .min_size([380.0, 280.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Source:");
                    egui::ComboBox::from_id_salt("chart_source")
                        .selected_text(self.chart.source.label())
                        .show_ui(ui, |ui| {
                            ui.selectable_value(
                                &mut self.chart.source,
                                ChartSource::Sql,
                                ChartSource::Sql.label(),
                            );
                            ui.selectable_value(
                                &mut self.chart.source,
                                ChartSource::CurrentResult,
                                ChartSource::CurrentResult.label(),
                            );
                        });
                    ui.label("Current result uses only its bounded grid window.");
                });
                if self.chart.source == ChartSource::Sql {
                    ui.add(
                        egui::TextEdit::multiline(&mut self.chart.sql_input)
                            .code_editor()
                            .desired_rows(3)
                            .desired_width(f32::INFINITY)
                            .hint_text("SELECT x, y FROM dataset WHERE …"),
                    );
                }
                ui.horizontal(|ui| {
                    ui.label("Type:");
                    egui::ComboBox::from_id_salt("chart_kind")
                        .selected_text(self.chart.config.kind.label())
                        .show_ui(ui, |ui| {
                            for kind in ChartKind::ALL {
                                ui.selectable_value(
                                    &mut self.chart.config.kind,
                                    kind,
                                    kind.label(),
                                );
                            }
                        });
                    ui.label(if self.chart.config.kind == ChartKind::Histogram {
                        "Value output:"
                    } else {
                        "X output:"
                    });
                    ui.text_edit_singleline(&mut self.chart.config.x_column);
                    egui::ComboBox::from_id_salt("chart_x_column")
                        .selected_text("Choose")
                        .show_ui(ui, |ui| {
                            for name in &names {
                                ui.selectable_value(
                                    &mut self.chart.config.x_column,
                                    name.clone(),
                                    name,
                                );
                            }
                        });
                });
                if self.chart.config.kind != ChartKind::Histogram {
                    ui.horizontal(|ui| {
                        ui.label("Y output:");
                        ui.text_edit_singleline(&mut self.chart.config.y_column);
                        egui::ComboBox::from_id_salt("chart_y_column")
                            .selected_text("Choose")
                            .show_ui(ui, |ui| {
                                for name in &names {
                                    ui.selectable_value(
                                        &mut self.chart.config.y_column,
                                        name.clone(),
                                        name,
                                    );
                                }
                            });
                    });
                }
                ui.horizontal(|ui| {
                    ui.label("Title:");
                    ui.text_edit_singleline(&mut self.chart.config.title);
                    ui.label("Point limit:");
                    ui.add(
                        egui::DragValue::new(&mut self.chart.config.point_limit)
                            .range(2..=MAX_INPUT_ROWS)
                            .speed(10),
                    );
                    if self.chart.config.kind == ChartKind::Histogram {
                        ui.label("Bins:");
                        ui.add(egui::DragValue::new(&mut self.chart.config.bins).range(1..=100));
                    }
                });
                ui.horizontal(|ui| {
                    if ui.button("Run / Refresh").clicked() {
                        run = true;
                    }
                    if ui
                        .add_enabled(self.chart.loading(), egui::Button::new("Cancel"))
                        .clicked()
                    {
                        cancel = true;
                    }
                    ui.label(&self.chart.status);
                });
                if let Some(error) = &self.chart.error {
                    ui.label(RichText::new(error).color(egui::Color32::RED));
                }
                if let Some(model) = &self.chart.model {
                    chart_notice(ui, model);
                    draw_chart(ui, model, &self.chart.config);
                } else if self.chart.loading() {
                    ui.centered_and_justified(|ui| ui.spinner());
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.label("Run a bounded SQL query or use the current result window.")
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

fn chart_notice(ui: &mut egui::Ui, model: &ChartModel) {
    if model.is_empty() {
        ui.label("No chartable non-null values were returned.");
        return;
    }
    if model.input_capped || model.reduced {
        ui.label(format!(
            "Showing {} chart values from {} bounded input rows{}.",
            model.output_len(),
            model.input_rows,
            if model.input_capped {
                format!("; query sampling stopped at {MAX_INPUT_ROWS}")
            } else {
                String::new()
            }
        ));
    }
}

pub(in crate::app) fn draw_chart(ui: &mut egui::Ui, model: &ChartModel, config: &ChartConfig) {
    let size = egui::vec2(
        ui.available_width().max(240.0),
        ui.available_height().max(220.0),
    );
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 4.0, ui.visuals().extreme_bg_color);
    if model.is_empty() {
        painter.text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            "No chartable values",
            egui::FontId::proportional(15.0),
            ui.visuals().weak_text_color(),
        );
        return;
    }
    let plot = rect.shrink2(egui::vec2(48.0, 28.0));
    painter.line_segment(
        [plot.left_bottom(), plot.left_top()],
        egui::Stroke::new(1.0_f32, ui.visuals().weak_text_color()),
    );
    painter.line_segment(
        [plot.left_bottom(), plot.right_bottom()],
        egui::Stroke::new(1.0_f32, ui.visuals().weak_text_color()),
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
        egui::FontId::proportional(15.0),
        ui.visuals().text_color(),
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
        egui::FontId::proportional(11.0),
        ui.visuals().weak_text_color(),
    );
    if model.kind != ChartKind::Histogram {
        painter.text(
            plot.left_top() - egui::vec2(42.0, 0.0),
            egui::Align2::LEFT_TOP,
            &config.y_column,
            egui::FontId::proportional(11.0),
            ui.visuals().weak_text_color(),
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
                egui::Stroke::new(1.5_f32, egui::Color32::from_rgb(95, 165, 255)),
            );
        }
    }
    for point in &screen_points {
        painter.circle_filled(
            *point,
            if kind == ChartKind::Scatter { 2.5 } else { 2.0 },
            egui::Color32::from_rgb(95, 165, 255),
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
        painter.rect_filled(bar_rect, 1.0, egui::Color32::from_rgb(105, 190, 125));
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
        painter.rect_filled(bar_rect, 1.0, egui::Color32::from_rgb(235, 165, 75));
        if hover.is_some_and(|hover| bar_rect.expand(3.0).contains(hover)) {
            hovered = Some(format!("{}–{}: {}", bin.start, bin.end, bin.count));
        }
    }
    hovered
}
