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
        self.poll_chart_image(ctx);
        let chart_was_visible = self.chart.visible;
        let mut open = chart_was_visible;
        let mut run = false;
        let mut cancel = false;
        let mut save_png = false;
        let mut save_svg = false;
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
                    // One Y column per series, each with its colour.
                    let count = self.chart.config.y_columns.len();
                    let mut remove = None;
                    for index in 0..count {
                        let label = if count == 1 {
                            "Y column".to_owned()
                        } else {
                            format!("Series {}", index + 1)
                        };
                        let salt = format!("chart_y_{index}");
                        let removed = series_field(
                            ui,
                            &label,
                            &salt,
                            &mut self.chart.config.y_columns[index],
                            &names,
                            index,
                            count > 1,
                        );
                        if removed {
                            remove = Some(index);
                        }
                    }
                    if let Some(index) = remove {
                        self.chart.config.y_columns.remove(index);
                    }
                    if self.chart.config.y_columns.len() < MAX_SERIES {
                        field(ui, "", |ui| {
                            if widgets::tool_button(ui, Some(Icon::Plus), "Add series", false)
                                .on_hover_text(format!(
                                    "Draw another Y column (up to {MAX_SERIES})"
                                ))
                                .clicked()
                            {
                                self.chart.config.y_columns.push(String::new());
                            }
                        });
                    }
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
                        ui.add(egui::Spinner::new().size(12.0).color(theme::accent()));
                    }
                    ui.add_enabled_ui(self.chart.model.is_some(), |ui| {
                        widgets::menu(
                            ui,
                            "chart_save",
                            Some(Icon::Export),
                            "Save image",
                            false,
                            200.0,
                            |ui| {
                                if widgets::menu_item(ui, "PNG…", "as on screen").clicked() {
                                    save_png = true;
                                    widgets::close_menu(ui);
                                }
                                if widgets::menu_item(ui, "SVG…", "960 × 540").clicked() {
                                    save_svg = true;
                                    widgets::close_menu(ui);
                                }
                            },
                        )
                        .on_hover_text("Save the chart as an image");
                    });
                    ui.add(
                        egui::Label::new(
                            RichText::new(&self.chart.status)
                                .font(theme::small())
                                .color(theme::muted()),
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
                    self.chart.drawn_at = Some(draw_chart(ui, model, &self.chart.config));
                } else {
                    ui.set_min_height(EMPTY_PLOT_HEIGHT);
                    widgets::placeholder(ui, |ui| {
                        if self.chart.loading() {
                            ui.add(egui::Spinner::new().color(theme::accent()));
                        } else {
                            ui.label(
                                RichText::new("Pick a source and columns, then draw the chart.")
                                    .color(theme::muted()),
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
        if save_svg {
            self.save_chart_svg();
        }
        if save_png {
            self.save_chart_png(ctx);
        }
    }

    fn save_chart_svg(&mut self) {
        let Some(model) = &self.chart.model else {
            return;
        };
        let Some(path) = rfd::FileDialog::new()
            .add_filter("SVG image", &["svg"])
            .set_file_name("chart.svg")
            .save_file()
        else {
            return;
        };
        let svg = chart_svg(model, &self.chart.config);
        self.chart.save_image(path, move || Ok(svg.into_bytes()));
    }

    /// Asks for a screenshot of the window; the chart is cut out of it when it
    /// arrives (`poll_chart_image`).
    fn save_chart_png(&mut self, ctx: &egui::Context) {
        let Some(path) = rfd::FileDialog::new()
            .add_filter("PNG image", &["png"])
            .set_file_name("chart.png")
            .save_file()
        else {
            return;
        };
        self.chart.pending_png = Some(path);
        ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot(egui::UserData::default()));
    }

    fn poll_chart_image(&mut self, ctx: &egui::Context) {
        if let Some(path) = self.chart.pending_png.clone() {
            let screenshot = ctx.input(|input| {
                input.events.iter().find_map(|event| match event {
                    egui::Event::Screenshot { image, .. } => Some(Arc::clone(image)),
                    _ => None,
                })
            });
            if let (Some(image), Some(rect)) = (screenshot, self.chart.drawn_at) {
                self.chart.pending_png = None;
                let region = image.region(&rect, Some(ctx.pixels_per_point()));
                self.chart
                    .save_image(path, move || crate::chart::encode_png(&region));
            }
        }
        if let Some(status) = self.chart.poll_save() {
            self.chart.status = status;
        }
    }
}

/// A Y column box with its series colour and, when there are several, a ×
/// that removes it. Returns `true` when × was clicked.
fn series_field(
    ui: &mut egui::Ui,
    label: &str,
    id_salt: &str,
    value: &mut String,
    names: &[String],
    index: usize,
    removable: bool,
) -> bool {
    let mut removed = false;
    ui.horizontal(|ui| {
        column_field(ui, label, id_salt, value, names);
        let (swatch, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
        ui.painter().rect_filled(
            swatch,
            2.0,
            theme::chart_series()[index % theme::chart_series().len()],
        );
        if removable
            && widgets::tool_button(ui, Some(Icon::Close), "", false)
                .on_hover_text("Remove this series")
                .clicked()
        {
            removed = true;
        }
    });
    removed
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
            theme::muted(),
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
    ui.label(
        RichText::new(text)
            .font(theme::small())
            .color(theme::muted()),
    );
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

/// Size of a chart saved as SVG, in pixels.
pub(in crate::app) const SVG_SIZE: egui::Vec2 = egui::vec2(960.0, 540.0);
/// Ticks aimed for on each axis.
const TICKS: usize = 5;
/// Approximate width of one character of small text, for laying out labels the
/// same way on screen and in SVG.
const CHAR_WIDTH: f32 = 6.5;

/// The two text sizes a chart uses.
#[derive(Clone, Copy)]
pub(in crate::app) enum ChartText {
    Small,
    Title,
}

/// Where a chart is drawn: the screen or an SVG document.
pub(in crate::app) trait Canvas {
    fn line(&mut self, points: &[egui::Pos2], color: egui::Color32, width: f32);
    fn rect(&mut self, rect: egui::Rect, color: egui::Color32);
    fn circle(&mut self, center: egui::Pos2, radius: f32, color: egui::Color32);
    fn text(
        &mut self,
        at: egui::Pos2,
        anchor: egui::Align2,
        text: &str,
        style: ChartText,
        color: egui::Color32,
    );
}

struct PainterCanvas<'a> {
    painter: &'a egui::Painter,
}

impl Canvas for PainterCanvas<'_> {
    fn line(&mut self, points: &[egui::Pos2], color: egui::Color32, width: f32) {
        self.painter.add(egui::Shape::line(
            points.to_vec(),
            egui::Stroke::new(width, color),
        ));
    }

    fn rect(&mut self, rect: egui::Rect, color: egui::Color32) {
        self.painter.rect_filled(rect, 1.0, color);
    }

    fn circle(&mut self, center: egui::Pos2, radius: f32, color: egui::Color32) {
        self.painter.circle_filled(center, radius, color);
    }

    fn text(
        &mut self,
        at: egui::Pos2,
        anchor: egui::Align2,
        text: &str,
        style: ChartText,
        color: egui::Color32,
    ) {
        let font = match style {
            ChartText::Small => theme::small(),
            ChartText::Title => theme::strong(13.0),
        };
        self.painter.text(at, anchor, text, font, color);
    }
}

/// Builds an SVG document from chart shapes.
pub(in crate::app) struct SvgCanvas {
    out: String,
}

impl SvgCanvas {
    pub(in crate::app) fn new(size: egui::Vec2) -> Self {
        Self {
            out: format!(
                "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w}\" height=\"{h}\" \
                 viewBox=\"0 0 {w} {h}\" font-family=\"Segoe UI, Helvetica, Arial, sans-serif\">\n",
                w = size.x,
                h = size.y
            ),
        }
    }

    pub(in crate::app) fn finish(mut self) -> String {
        self.out.push_str("</svg>\n");
        self.out
    }
}

fn svg_color(color: egui::Color32) -> String {
    format!("#{:02x}{:02x}{:02x}", color.r(), color.g(), color.b())
}

fn svg_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

impl Canvas for SvgCanvas {
    fn line(&mut self, points: &[egui::Pos2], color: egui::Color32, width: f32) {
        let points = points
            .iter()
            .map(|point| format!("{:.1},{:.1}", point.x, point.y))
            .collect::<Vec<_>>()
            .join(" ");
        self.out.push_str(&format!(
            "<polyline points=\"{points}\" fill=\"none\" stroke=\"{}\" stroke-width=\"{width}\"/>\n",
            svg_color(color)
        ));
    }

    fn rect(&mut self, rect: egui::Rect, color: egui::Color32) {
        self.out.push_str(&format!(
            "<rect x=\"{:.1}\" y=\"{:.1}\" width=\"{:.1}\" height=\"{:.1}\" fill=\"{}\"/>\n",
            rect.left(),
            rect.top(),
            rect.width(),
            rect.height(),
            svg_color(color)
        ));
    }

    fn circle(&mut self, center: egui::Pos2, radius: f32, color: egui::Color32) {
        self.out.push_str(&format!(
            "<circle cx=\"{:.1}\" cy=\"{:.1}\" r=\"{radius}\" fill=\"{}\"/>\n",
            center.x,
            center.y,
            svg_color(color)
        ));
    }

    fn text(
        &mut self,
        at: egui::Pos2,
        anchor: egui::Align2,
        text: &str,
        style: ChartText,
        color: egui::Color32,
    ) {
        let horizontal = match anchor.x() {
            egui::Align::Min => "start",
            egui::Align::Center => "middle",
            egui::Align::Max => "end",
        };
        let (size, weight) = match style {
            ChartText::Small => (11.5, "normal"),
            ChartText::Title => (13.0, "600"),
        };
        // SVG places text by its baseline; move it to match the anchor.
        let y = match anchor.y() {
            egui::Align::Min => at.y + size,
            egui::Align::Center => at.y + size * 0.35,
            egui::Align::Max => at.y,
        };
        self.out.push_str(&format!(
            "<text x=\"{:.1}\" y=\"{y:.1}\" font-size=\"{size}\" font-weight=\"{weight}\" \
             text-anchor=\"{horizontal}\" fill=\"{}\">{}</text>\n",
            at.x,
            svg_color(color),
            svg_escape(text)
        ));
    }
}

/// Draws a chart in the window: the plot, and a tooltip for the value under
/// the pointer. Returns where it was drawn, for saving it as an image.
pub(in crate::app) fn draw_chart(
    ui: &mut egui::Ui,
    model: &ChartModel,
    config: &ChartConfig,
) -> egui::Rect {
    let size = egui::vec2(
        ui.available_width().max(240.0),
        ui.available_height().max(220.0),
    );
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::hover());
    let painter = ui.painter_at(rect);
    let targets = paint_chart(
        &mut PainterCanvas { painter: &painter },
        rect,
        model,
        config,
    );
    let hovered = response.hover_pos().and_then(|hover| {
        targets
            .iter()
            .filter(|(target, _)| target.expand(3.0).contains(hover))
            .min_by(|(left, _), (right, _)| {
                left.center()
                    .distance_sq(hover)
                    .total_cmp(&right.center().distance_sq(hover))
            })
            .map(|(_, text)| text.clone())
    });
    match hovered {
        Some(text) => {
            response.on_hover_text_at_pointer(text);
        }
        None => {
            response.on_hover_text("Hover a plotted value for details");
        }
    }
    rect
}

/// The chart as an SVG document.
pub(in crate::app) fn chart_svg(model: &ChartModel, config: &ChartConfig) -> String {
    let mut canvas = SvgCanvas::new(SVG_SIZE);
    paint_chart(
        &mut canvas,
        egui::Rect::from_min_size(egui::Pos2::ZERO, SVG_SIZE),
        model,
        config,
    );
    canvas.finish()
}

/// Round tick values between `min` and `max` (1, 2 or 5 times a power of ten
/// apart) and the step between them.
pub(in crate::app) fn nice_ticks(min: f64, max: f64, count: usize, whole: bool) -> (Vec<f64>, f64) {
    let range = (max - min).abs();
    if !range.is_finite() || range == 0.0 {
        return (vec![min], 1.0);
    }
    let raw = range / count.max(1) as f64;
    let magnitude = 10_f64.powf(raw.log10().floor());
    let normalized = raw / magnitude;
    let nice = if normalized < 1.5 {
        1.0
    } else if normalized < 3.0 {
        2.0
    } else if normalized < 7.0 {
        5.0
    } else {
        10.0
    };
    let mut step = nice * magnitude;
    if whole && step < 1.0 {
        step = 1.0;
    }
    let mut ticks = Vec::new();
    let mut tick = (min / step).ceil() * step;
    while tick <= max + step * 1e-9 && ticks.len() < 50 {
        ticks.push(if tick.abs() < step * 1e-9 { 0.0 } else { tick });
        tick += step;
    }
    (ticks, step)
}

fn numeric_range(values: impl Iterator<Item = f64>) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in values {
        min = min.min(value);
        max = max.max(value);
    }
    if !min.is_finite() || !max.is_finite() {
        return (0.0, 1.0);
    }
    if min == max {
        let pad = if min == 0.0 { 1.0 } else { min.abs() * 0.05 };
        (min - pad, max + pad)
    } else {
        (min, max)
    }
}

/// Maps data values into a plot rectangle.
struct Scale {
    plot: egui::Rect,
    x: (f64, f64),
    y: (f64, f64),
}

impl Scale {
    fn x(&self, value: f64) -> f32 {
        let t = ((value - self.x.0) / (self.x.1 - self.x.0)) as f32;
        egui::lerp(self.plot.left()..=self.plot.right(), t)
    }

    fn y(&self, value: f64) -> f32 {
        let t = ((value - self.y.0) / (self.y.1 - self.y.0)) as f32;
        egui::lerp(self.plot.bottom()..=self.plot.top(), t)
    }
}

fn series_color(index: usize) -> egui::Color32 {
    theme::chart_series()[index % theme::chart_series().len()]
}

/// Up to 4 significant digits, for tooltips.
fn tip(axis: &AxisFormat, value: f64) -> String {
    let step = if value == 0.0 {
        1.0
    } else {
        10_f64.powf(value.abs().log10().floor() - 3.0)
    };
    axis.label(value, step)
}

/// Draws the chart into `rect` and returns hover targets with their text.
pub(in crate::app) fn paint_chart(
    canvas: &mut dyn Canvas,
    rect: egui::Rect,
    model: &ChartModel,
    config: &ChartConfig,
) -> Vec<(egui::Rect, String)> {
    canvas.rect(rect, theme::background());
    let title = if config.title.trim().is_empty() {
        format!("{} chart", model.kind.label())
    } else {
        config.title.clone()
    };
    canvas.text(
        rect.left_top() + egui::vec2(10.0, 8.0),
        egui::Align2::LEFT_TOP,
        &title,
        ChartText::Title,
        theme::text(),
    );
    if model.is_empty() {
        canvas.text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            "No chartable values",
            ChartText::Title,
            theme::muted(),
        );
        return Vec::new();
    }

    // A legend when there is more than one series, right-aligned on the title line.
    let names = model.series_names();
    if names.len() > 1 {
        let mut right = rect.right() - 10.0;
        for (index, name) in names.iter().enumerate().rev() {
            let width = name.chars().count() as f32 * CHAR_WIDTH;
            canvas.text(
                egui::pos2(right, rect.top() + 10.0),
                egui::Align2::RIGHT_TOP,
                name,
                ChartText::Small,
                theme::text(),
            );
            right -= width + 6.0;
            canvas.rect(
                egui::Rect::from_min_size(
                    egui::pos2(right - 10.0, rect.top() + 12.0),
                    egui::vec2(10.0, 10.0),
                ),
                series_color(index),
            );
            right -= 22.0;
        }
    }

    // Room above the plot for the title line, then the Y axis name.
    let plot = egui::Rect::from_min_max(
        rect.left_top() + egui::vec2(60.0, 52.0),
        rect.right_bottom() - egui::vec2(18.0, 44.0),
    );
    if plot.width() < 20.0 || plot.height() < 20.0 {
        return Vec::new();
    }
    let mut targets = Vec::new();
    let y_label = (names.len() == 1).then(|| names[0].to_owned());
    match &model.values {
        ChartValues::Points(series) => {
            let x = numeric_range(
                series
                    .iter()
                    .flat_map(|series| series.points.iter().map(|point| point.x)),
            );
            let y = numeric_range(
                series
                    .iter()
                    .flat_map(|series| series.points.iter().map(|point| point.y)),
            );
            let scale = Scale { plot, x, y };
            axes(
                canvas,
                &scale,
                Some(&model.x_axis),
                &config.x_column,
                y_label.as_deref(),
            );
            for (index, series) in series.iter().enumerate() {
                let color = series_color(index);
                let points = series
                    .points
                    .iter()
                    .map(|point| egui::pos2(scale.x(point.x), scale.y(point.y)))
                    .collect::<Vec<_>>();
                if model.kind == ChartKind::Line && points.len() > 1 {
                    canvas.line(&points, color, 1.5);
                }
                let radius = if model.kind == ChartKind::Scatter {
                    2.5
                } else {
                    2.0
                };
                for (point, value) in points.iter().zip(&series.points) {
                    canvas.circle(*point, radius, color);
                    targets.push((
                        egui::Rect::from_center_size(*point, egui::vec2(8.0, 8.0)),
                        format!(
                            "{}: {}\n{}: {}",
                            config.x_column,
                            tip(&model.x_axis, value.x),
                            series.name,
                            tip(&AxisFormat::Number, value.y)
                        ),
                    ));
                }
            }
        }
        ChartValues::Bars { labels, series } => {
            let values = series
                .iter()
                .flat_map(|(_, values)| values.iter().flatten().copied());
            let (low, high) = numeric_range(values.chain([0.0]));
            let scale = Scale {
                plot,
                x: (0.0, labels.len() as f64),
                y: (low, high),
            };
            axes(canvas, &scale, None, &config.x_column, y_label.as_deref());
            let group = plot.width() / labels.len().max(1) as f32;
            let bar = (group * 0.8 / series.len().max(1) as f32).max(1.0);
            let baseline = scale.y(0.0);
            // Category labels, thinned out so they don't overlap.
            let widest = labels
                .iter()
                .map(|label| label.chars().count().min(14))
                .max()
                .unwrap_or(1) as f32
                * CHAR_WIDTH;
            let every = ((widest + 8.0) / group).ceil().max(1.0) as usize;
            for (index, label) in labels.iter().enumerate() {
                let left = plot.left() + index as f32 * group + group * 0.1;
                for (number, (name, values)) in series.iter().enumerate() {
                    let Some(value) = values.get(index).copied().flatten() else {
                        continue;
                    };
                    let top = scale.y(value);
                    let x = left + number as f32 * bar;
                    let shape = egui::Rect::from_min_max(
                        egui::pos2(x, top.min(baseline)),
                        egui::pos2((x + bar - 1.0).max(x + 1.0), top.max(baseline)),
                    );
                    canvas.rect(shape, series_color(number));
                    targets.push((
                        shape,
                        format!("{label}\n{name}: {}", tip(&AxisFormat::Number, value)),
                    ));
                }
                if index % every == 0 {
                    let short = if label.chars().count() > 14 {
                        format!("{}…", label.chars().take(13).collect::<String>())
                    } else {
                        label.clone()
                    };
                    canvas.text(
                        egui::pos2(left + group * 0.4, plot.bottom() + 6.0),
                        egui::Align2::CENTER_TOP,
                        &short,
                        ChartText::Small,
                        theme::muted(),
                    );
                }
            }
        }
        ChartValues::Histogram(bins) => {
            let x = numeric_range(bins.iter().flat_map(|bin| [bin.start, bin.end]));
            let most = bins.iter().map(|bin| bin.count).max().unwrap_or(1).max(1) as f64;
            let scale = Scale {
                plot,
                x,
                y: (0.0, most),
            };
            axes(
                canvas,
                &scale,
                Some(&model.x_axis),
                &config.x_column,
                Some("count"),
            );
            let width = plot.width() / bins.len().max(1) as f32;
            for (index, bin) in bins.iter().enumerate() {
                let left = plot.left() + index as f32 * width + 1.0;
                let shape = egui::Rect::from_min_max(
                    egui::pos2(left, scale.y(bin.count as f64)),
                    egui::pos2((left + width - 2.0).max(left + 1.0), plot.bottom()),
                );
                canvas.rect(shape, theme::chart_bar());
                targets.push((
                    shape,
                    format!(
                        "{} – {}: {}",
                        tip(&model.x_axis, bin.start),
                        tip(&model.x_axis, bin.end),
                        bin.count
                    ),
                ));
            }
        }
    }
    targets
}

/// Grid lines, tick labels, axis lines and names. `x_axis` is `None` for
/// categories, whose labels the bars draw.
fn axes(
    canvas: &mut dyn Canvas,
    scale: &Scale,
    x_axis: Option<&AxisFormat>,
    x_name: &str,
    y_name: Option<&str>,
) {
    let plot = scale.plot;
    let (y_ticks, y_step) = nice_ticks(scale.y.0, scale.y.1, TICKS, false);
    for tick in y_ticks {
        let y = scale.y(tick);
        canvas.line(
            &[egui::pos2(plot.left(), y), egui::pos2(plot.right(), y)],
            theme::chart_grid(),
            1.0,
        );
        canvas.text(
            egui::pos2(plot.left() - 6.0, y),
            egui::Align2::RIGHT_CENTER,
            &AxisFormat::Number.label(tick, y_step),
            ChartText::Small,
            theme::muted(),
        );
    }
    if let Some(format) = x_axis {
        let whole = matches!(format, AxisFormat::Temporal(arrow_schema::DataType::Date32));
        let label_width = format.label(scale.x.1, 1.0).chars().count() as f32 * CHAR_WIDTH;
        let count = ((plot.width() / (label_width + 24.0)).floor() as usize).clamp(2, TICKS + 2);
        let (x_ticks, x_step) = nice_ticks(scale.x.0, scale.x.1, count, whole);
        for tick in x_ticks {
            let x = scale.x(tick);
            canvas.line(
                &[egui::pos2(x, plot.top()), egui::pos2(x, plot.bottom())],
                theme::chart_grid(),
                1.0,
            );
            canvas.text(
                egui::pos2(x, plot.bottom() + 6.0),
                egui::Align2::CENTER_TOP,
                &format.label(tick, x_step),
                ChartText::Small,
                theme::muted(),
            );
        }
    }
    canvas.line(
        &[plot.left_top(), plot.left_bottom(), plot.right_bottom()],
        theme::muted(),
        1.0,
    );
    canvas.text(
        egui::pos2(plot.center().x, plot.bottom() + 24.0),
        egui::Align2::CENTER_TOP,
        x_name,
        ChartText::Small,
        theme::text(),
    );
    if let Some(y_name) = y_name {
        canvas.text(
            egui::pos2(plot.left(), plot.top() - 6.0),
            egui::Align2::LEFT_BOTTOM,
            y_name,
            ChartText::Small,
            theme::text(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::{ChartPoint, ChartSeries};

    #[test]
    fn picks_round_ticks() {
        let (ticks, step) = nice_ticks(0.0, 100.0, 5, false);
        assert_eq!(step, 20.0);
        assert_eq!(ticks, [0.0, 20.0, 40.0, 60.0, 80.0, 100.0]);
        let (ticks, step) = nice_ticks(-0.3, 0.7, 5, false);
        assert!((step - 0.2).abs() < 1e-12);
        assert_eq!(ticks.len(), 5);
        assert!(ticks.contains(&0.0));
        // Counts never get fractional ticks.
        assert_eq!(nice_ticks(0.0, 3.0, 8, true).1, 1.0);
        // A flat range has one tick.
        assert_eq!(nice_ticks(5.0, 5.0, 5, false).0, [5.0]);
    }

    #[test]
    fn writes_every_series_to_svg() {
        let model = ChartModel {
            kind: ChartKind::Line,
            values: ChartValues::Points(vec![
                ChartSeries {
                    name: "low".to_owned(),
                    points: vec![ChartPoint { x: 0.0, y: 1.0 }, ChartPoint { x: 1.0, y: 2.0 }],
                },
                ChartSeries {
                    name: "high <&>".to_owned(),
                    points: vec![ChartPoint { x: 0.0, y: 5.0 }, ChartPoint { x: 1.0, y: 6.0 }],
                },
            ]),
            x_axis: AxisFormat::Number,
            input_rows: 2,
            skipped_rows: 0,
            input_capped: false,
            reduced: false,
        };
        let config = ChartConfig {
            kind: ChartKind::Line,
            x_column: "day".to_owned(),
            y_columns: vec!["low".to_owned(), "high <&>".to_owned()],
            title: "Week".to_owned(),
            ..ChartConfig::default()
        };
        let svg = chart_svg(&model, &config);
        assert!(svg.starts_with("<svg"), "{svg}");
        assert!(svg.trim_end().ends_with("</svg>"));
        assert!(svg.contains(">Week<"));
        // The legend names both series, escaped.
        assert!(svg.contains(">low<"));
        assert!(svg.contains("high &lt;&amp;&gt;"));
        assert!(!svg.contains("<&>"));
        // The first two series colours, from whichever palette was in use (a
        // test elsewhere switches it for a moment).
        let hex =
            |color: egui::Color32| format!("#{:02x}{:02x}{:02x}", color.r(), color.g(), color.b());
        for (dark, light) in [
            (theme::DARK.type_number, theme::LIGHT.type_number),
            (theme::DARK.accent, theme::LIGHT.accent),
        ] {
            assert!(
                svg.contains(&hex(dark)) || svg.contains(&hex(light)),
                "{} missing",
                hex(dark)
            );
        }
    }
}
