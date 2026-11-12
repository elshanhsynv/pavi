use super::{spacing::Spacing, theme};
use eframe::egui::{self, Color32, Pos2, Response, Stroke, Vec2};
use std::sync::Arc;

/// Painted line icons, drawn in a 14 px box around a centre point.
#[derive(Clone, Copy)]
pub enum Icon {
    Search,
    Sort,
    Folder,
    Refresh,
    Chart,
    History,
    Columns,
    Export,
    Copy,
    Panel,
    Play,
    Close,
    Minimize,
    Maximize,
    ArrowUp,
    ArrowDown,
    Bookmark,
    Plus,
}

const ICON_STROKE: f32 = 1.3;

pub fn icon(ui: &egui::Ui, center: Pos2, glyph: Icon, color: Color32) {
    let painter = ui.painter();
    let stroke = Stroke::new(ICON_STROKE, color);
    let p = |x: f32, y: f32| center + egui::vec2(x, y);
    let line = |points: &[(f32, f32)]| {
        painter.add(egui::Shape::line(
            points.iter().map(|&(x, y)| p(x, y)).collect(),
            stroke,
        ));
    };
    let rect = |x: f32, y: f32, w: f32, h: f32| {
        painter.rect_stroke(
            egui::Rect::from_min_size(p(x, y), egui::vec2(w, h)),
            1.5,
            stroke,
            egui::StrokeKind::Middle,
        );
    };
    match glyph {
        Icon::Search => {
            painter.circle_stroke(p(-1.5, -1.5), 4.5, stroke);
            line(&[(1.8, 1.8), (6.0, 6.0)]);
        }
        Icon::Sort => {
            line(&[(-3.5, 6.0), (-3.5, -6.0)]);
            line(&[(-6.5, -3.0), (-3.5, -6.0), (-0.5, -3.0)]);
            line(&[(3.5, -6.0), (3.5, 6.0)]);
            line(&[(0.5, 3.0), (3.5, 6.0), (6.5, 3.0)]);
        }
        Icon::Folder => line(&[
            (-6.5, -5.0),
            (-6.5, 5.5),
            (6.5, 5.5),
            (6.5, -2.5),
            (0.0, -2.5),
            (-2.0, -5.0),
            (-6.5, -5.0),
        ]),
        Icon::Refresh => {
            let arc = (0..=20)
                .map(|step| {
                    let angle = (20.0 + step as f32 * 15.0).to_radians();
                    p(angle.cos() * 5.5, angle.sin() * 5.5)
                })
                .collect();
            painter.add(egui::Shape::line(arc, stroke));
            line(&[(6.3, -6.5), (6.3, -2.6), (2.4, -2.6)]);
        }
        Icon::Chart => {
            for (x, height) in [(-5.0, 6.0), (0.0, 11.5), (5.0, 8.5)] {
                painter.rect_filled(
                    egui::Rect::from_min_size(p(x - 1.4, 6.0 - height), egui::vec2(2.8, height)),
                    0.5,
                    color,
                );
            }
        }
        Icon::History => {
            painter.circle_stroke(center, 6.0, stroke);
            line(&[(0.0, -3.5), (0.0, 0.0), (2.6, 1.6)]);
        }
        Icon::Columns => {
            rect(-6.5, -5.5, 13.0, 11.0);
            line(&[(-2.2, -5.5), (-2.2, 5.5)]);
            line(&[(2.2, -5.5), (2.2, 5.5)]);
        }
        Icon::Export => {
            line(&[(-6.0, 2.0), (-6.0, 6.0), (6.0, 6.0), (6.0, 2.0)]);
            line(&[(0.0, 2.5), (0.0, -6.5)]);
            line(&[(-3.2, -3.3), (0.0, -6.5), (3.2, -3.3)]);
        }
        Icon::Copy => {
            rect(-6.0, -3.0, 8.5, 9.5);
            line(&[
                (-2.5, -3.0),
                (-2.5, -6.5),
                (6.0, -6.5),
                (6.0, 3.0),
                (2.5, 3.0),
            ]);
        }
        Icon::Panel => {
            rect(-6.5, -5.5, 13.0, 11.0);
            line(&[(2.0, -5.5), (2.0, 5.5)]);
        }
        Icon::Play => {
            painter.add(egui::Shape::convex_polygon(
                vec![p(-3.5, -5.5), p(5.5, 0.0), p(-3.5, 5.5)],
                color,
                Stroke::NONE,
            ));
        }
        Icon::Close => {
            line(&[(-4.5, -4.5), (4.5, 4.5)]);
            line(&[(4.5, -4.5), (-4.5, 4.5)]);
        }
        Icon::Minimize => line(&[(-5.0, 0.0), (5.0, 0.0)]),
        Icon::Maximize => rect(-4.5, -4.5, 9.0, 9.0),
        Icon::ArrowUp => {
            line(&[(0.0, 5.0), (0.0, -5.0)]);
            line(&[(-3.5, -1.5), (0.0, -5.0), (3.5, -1.5)]);
        }
        Icon::ArrowDown => {
            line(&[(0.0, -5.0), (0.0, 5.0)]);
            line(&[(-3.5, 1.5), (0.0, 5.0), (3.5, 1.5)]);
        }
        Icon::Plus => {
            line(&[(0.0, -5.5), (0.0, 5.5)]);
            line(&[(-5.5, 0.0), (5.5, 0.0)]);
        }
        Icon::Bookmark => line(&[
            (-4.5, -6.0),
            (4.5, -6.0),
            (4.5, 6.0),
            (0.0, 2.5),
            (-4.5, 6.0),
            (-4.5, -6.0),
        ]),
    }
}

/// The PAVI mark: four planks of a herringbone parquet floor.
pub fn logo(ui: &egui::Ui, center: Pos2, scale: f32) {
    for (side, color) in [
        (-1.0_f32, theme::ACCENT),
        (1.0_f32, theme::ACCENT.gamma_multiply(0.55)),
    ] {
        for top in [-3.5_f32, 3.0] {
            let p = |x: f32, y: f32| center + egui::vec2(side * x, y) * scale;
            ui.painter().add(egui::Shape::convex_polygon(
                vec![
                    p(7.0, top - 4.0),
                    p(0.6, top),
                    p(0.6, top + 4.0),
                    p(7.0, top),
                ],
                color,
                Stroke::NONE,
            ));
        }
    }
}

fn chevron(ui: &egui::Ui, center: Pos2, down: bool, color: Color32) {
    let points = if down {
        [(-3.5, -1.8), (0.0, 1.8), (3.5, -1.8)]
    } else {
        [(-1.8, -3.5), (1.8, 0.0), (-1.8, 3.5)]
    };
    ui.painter().add(egui::Shape::line(
        points
            .iter()
            .map(|&(x, y)| center + egui::vec2(x, y))
            .collect(),
        Stroke::new(ICON_STROKE, color),
    ));
}

/// One line of text, cut with "…" when wider than `max_width`.
/// `text` laid out on one line, cut with "…" past `max_width`.
pub fn elided(
    ui: &egui::Ui,
    text: &str,
    font: egui::FontId,
    color: Color32,
    max_width: f32,
) -> Arc<egui::Galley> {
    let mut job = egui::text::LayoutJob::simple_singleline(text.to_owned(), font, color);
    job.wrap = egui::text::TextWrapping::truncate_at_width(max_width.max(0.0));
    ui.fonts(|fonts| fonts.layout_job(job))
}

fn tool(
    ui: &mut egui::Ui,
    glyph: Option<Icon>,
    label: &str,
    selected: bool,
    has_menu: bool,
) -> Response {
    const ICON: f32 = 16.0;
    let text = (!label.is_empty()).then(|| elided(ui, label, theme::body(), theme::TEXT, 240.0));
    let icon_only = text.is_none() && !has_menu;
    let padding = if icon_only {
        (theme::CONTROL_HEIGHT - ICON) / 2.0
    } else {
        Spacing::Md.px()
    };
    let mut width = padding * 2.0;
    if glyph.is_some() {
        width += ICON;
    }
    if let Some(text) = &text {
        width += text.size().x
            + if glyph.is_some() {
                Spacing::Sm.px()
            } else {
                0.0
            };
    }
    if has_menu {
        width += 14.0;
    }
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(width, theme::CONTROL_HEIGHT),
        egui::Sense::click(),
    );
    response.widget_info(|| {
        egui::WidgetInfo::selected(egui::WidgetType::Button, ui.is_enabled(), selected, label)
    });
    let hovered = response.hovered();
    let fill = if selected {
        theme::SELECTED
    } else if response.is_pointer_button_down_on() {
        theme::HEADER
    } else if hovered {
        theme::HOVER
    } else {
        Color32::TRANSPARENT
    };
    ui.painter().rect_filled(rect, theme::RADIUS, fill);
    let quiet = if selected {
        theme::ACCENT
    } else if hovered {
        theme::TEXT
    } else {
        theme::MUTED
    };
    let mut x = rect.left() + padding;
    if let Some(glyph) = glyph {
        icon(
            ui,
            egui::pos2(x + ICON / 2.0, rect.center().y),
            glyph,
            quiet,
        );
        x += ICON + Spacing::Sm.px();
    }
    if let Some(text) = text {
        let position = egui::pos2(x, rect.center().y - text.size().y / 2.0);
        ui.painter().galley(position, text, theme::TEXT);
    }
    if has_menu {
        chevron(
            ui,
            egui::pos2(rect.right() - padding - 3.0, rect.center().y),
            true,
            quiet,
        );
    }
    response
}

/// A flat toolbar button: icon, label, or both. `selected` marks a switched-on tool.
pub fn tool_button(
    ui: &mut egui::Ui,
    glyph: Option<Icon>,
    label: &str,
    selected: bool,
) -> Response {
    tool(ui, glyph, label, selected, false)
}

/// A toolbar button that opens a popup below it. Close it with `close_menu`.
pub fn menu(
    ui: &mut egui::Ui,
    id_salt: &str,
    glyph: Option<Icon>,
    label: &str,
    selected: bool,
    width: f32,
    body: impl FnOnce(&mut egui::Ui),
) -> Response {
    let id = egui::Id::new(("menu", id_salt));
    let response = tool(ui, glyph, label, selected, true);
    if response.clicked() {
        ui.memory_mut(|memory| memory.toggle_popup(id));
    }
    egui::popup_below_widget(
        ui,
        id,
        &response,
        egui::PopupCloseBehavior::CloseOnClickOutside,
        |ui| {
            ui.set_width(width);
            ui.spacing_mut().item_spacing = Spacing::Sm.vec(Spacing::Xs);
            body(ui);
        },
    );
    response
}

pub fn close_menu(ui: &egui::Ui) {
    ui.memory_mut(|memory| memory.close_popup());
}

/// A full-width row inside a menu, with an optional right-aligned hint
/// (a shortcut or a folder).
pub fn menu_item(ui: &mut egui::Ui, label: &str, hint: &str) -> Response {
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), theme::CONTROL_HEIGHT),
        egui::Sense::click(),
    );
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, ui.is_enabled(), label)
    });
    if response.hovered() {
        ui.painter().rect_filled(rect, theme::RADIUS, theme::HOVER);
    }
    let inner = rect.shrink2(Spacing::Md.vec(Spacing::None));
    let hint = (!hint.is_empty())
        .then(|| elided(ui, hint, theme::small(), theme::MUTED, inner.width() * 0.5));
    let hint_width = hint
        .as_ref()
        .map_or(0.0, |hint| hint.size().x + Spacing::Lg.px());
    let text = elided(
        ui,
        label,
        theme::body(),
        theme::TEXT,
        inner.width() - hint_width,
    );
    ui.painter().galley(
        egui::pos2(inner.left(), rect.center().y - text.size().y / 2.0),
        text,
        theme::TEXT,
    );
    if let Some(hint) = hint {
        ui.painter().galley(
            egui::pos2(
                inner.right() - hint.size().x,
                rect.center().y - hint.size().y / 2.0,
            ),
            hint,
            theme::MUTED,
        );
    }
    response
}

/// A small muted caption above a group of menu rows.
pub fn menu_caption(ui: &mut egui::Ui, text: &str) {
    ui.add_space(Spacing::Xs.px());
    ui.horizontal(|ui| {
        ui.add_space(Spacing::Md.px());
        ui.label(
            egui::RichText::new(text)
                .font(theme::small())
                .color(theme::MUTED),
        );
    });
}

/// A segmented switch. Returns the option that was clicked, if any.
pub fn segmented<T: Copy + PartialEq>(
    ui: &mut egui::Ui,
    current: T,
    options: &[(T, &str, bool)],
) -> Option<T> {
    let texts = options
        .iter()
        .map(|(_, label, _)| elided(ui, label, theme::body(), theme::TEXT, 160.0))
        .collect::<Vec<_>>();
    let widths = texts
        .iter()
        .map(|text| text.size().x + Spacing::Lg.px() * 2.0)
        .collect::<Vec<_>>();
    let (rect, _) = ui.allocate_exact_size(
        egui::vec2(widths.iter().sum(), theme::CONTROL_HEIGHT),
        egui::Sense::hover(),
    );
    ui.painter()
        .rect_filled(rect, theme::RADIUS, theme::BACKGROUND);
    ui.painter().rect_stroke(
        rect,
        theme::RADIUS,
        Stroke::new(1.0_f32, theme::BORDER),
        egui::StrokeKind::Inside,
    );
    let mut clicked = None;
    let mut left = rect.left();
    for (index, ((value, label, enabled), text)) in options.iter().zip(texts).enumerate() {
        let segment = egui::Rect::from_min_size(
            egui::pos2(left, rect.top()),
            egui::vec2(widths[index], rect.height()),
        );
        left = segment.right();
        let selected = *value == current;
        let response = ui.interact(
            segment,
            // `unique_id`, not `id`: sibling layouts share an `id`, so two switches
            // in one form would otherwise collide.
            ui.unique_id().with(("segment", index)),
            if *enabled {
                egui::Sense::click()
            } else {
                egui::Sense::hover()
            },
        );
        response.widget_info(|| {
            egui::WidgetInfo::selected(egui::WidgetType::RadioButton, *enabled, selected, *label)
        });
        if selected {
            ui.painter()
                .rect_filled(segment.shrink(2.0), theme::RADIUS - 1, theme::HOVER);
        }
        let color = if !*enabled {
            theme::DISABLED
        } else if selected || response.hovered() {
            theme::TEXT
        } else {
            theme::MUTED
        };
        ui.painter().galley_with_override_text_color(
            segment.center() - text.size() / 2.0,
            text,
            color,
        );
        if response.clicked() {
            clicked = Some(*value);
        }
    }
    clicked
}

/// A thin vertical rule between groups of bar controls.
pub fn divider(ui: &mut egui::Ui) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(9.0, 16.0), egui::Sense::hover());
    ui.painter().vline(
        rect.center().x,
        rect.y_range(),
        Stroke::new(1.0_f32, theme::BORDER),
    );
}

/// Minimize, maximize or close, drawn into the top bar.
pub fn window_button(ui: &mut egui::Ui, glyph: Icon, label: &str, danger: bool) -> Response {
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(44.0, theme::BAR_HEIGHT), egui::Sense::click());
    response.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, label));
    let hovered = response.hovered();
    if hovered {
        ui.painter().rect_filled(
            rect,
            0.0,
            if danger {
                theme::CLOSE_HOVER
            } else {
                theme::HOVER
            },
        );
    }
    icon(
        ui,
        rect.center(),
        glyph,
        if hovered { theme::TEXT } else { theme::MUTED },
    );
    response.on_hover_text(label)
}

/// The one filled button per view: the main action.
pub fn primary_button(ui: &mut egui::Ui, glyph: Option<Icon>, label: &str) -> Response {
    let text = elided(ui, label, theme::strong(13.0), theme::ON_ACCENT, 240.0);
    let icon_width = if glyph.is_some() { 20.0 } else { 0.0 };
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(
            text.size().x + icon_width + Spacing::Lg.px() * 2.0,
            theme::CONTROL_HEIGHT,
        ),
        egui::Sense::click(),
    );
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, ui.is_enabled(), label)
    });
    ui.painter().rect_filled(
        rect,
        theme::RADIUS,
        if response.hovered() {
            theme::ACCENT.gamma_multiply(0.9)
        } else {
            theme::ACCENT
        },
    );
    let left = rect.left() + Spacing::Lg.px();
    if let Some(glyph) = glyph {
        icon(
            ui,
            egui::pos2(left + 6.0, rect.center().y),
            glyph,
            theme::ON_ACCENT,
        );
    }
    ui.painter().galley(
        egui::pos2(left + icon_width, rect.center().y - text.size().y / 2.0),
        text,
        theme::ON_ACCENT,
    );
    response
}

/// A collapsible Inspector section: a title row, then `body` when open.
pub fn section(
    ui: &mut egui::Ui,
    title: &str,
    note: &str,
    default_open: bool,
    body: impl FnOnce(&mut egui::Ui),
) {
    let id = ui.make_persistent_id(("section", title));
    let mut open = ui.data_mut(|data| data.get_temp::<bool>(id).unwrap_or(default_open));
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(ui.available_width(), 28.0), egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::CollapsingHeader, ui.is_enabled(), title)
    });
    if response.hovered() {
        ui.painter().rect_filled(rect, 0.0, theme::HEADER);
    }
    chevron(
        ui,
        egui::pos2(rect.left() + 14.0, rect.center().y),
        open,
        theme::MUTED,
    );
    ui.painter().text(
        egui::pos2(rect.left() + 26.0, rect.center().y),
        egui::Align2::LEFT_CENTER,
        title,
        theme::strong(12.5),
        theme::TEXT,
    );
    if !note.is_empty() {
        ui.painter().text(
            egui::pos2(rect.right() - Spacing::Lg.px(), rect.center().y),
            egui::Align2::RIGHT_CENTER,
            note,
            theme::small(),
            theme::MUTED,
        );
    }
    if response.clicked() {
        open = !open;
        ui.data_mut(|data| data.insert_temp(id, open));
    }
    if open {
        egui::Frame::new()
            .inner_margin(egui::Margin {
                left: 12,
                right: 10,
                top: 2,
                bottom: 10,
            })
            .show(ui, |ui| {
                ui.spacing_mut().item_spacing = Spacing::Sm.vec(Spacing::Xs);
                ui.spacing_mut().interact_size.y = 20.0;
                body(ui);
            });
    }
    let line = ui.available_rect_before_wrap();
    ui.painter().hline(
        line.x_range(),
        line.top(),
        Stroke::new(1.0_f32, theme::BORDER),
    );
    ui.add_space(1.0);
}

/// A "label  value" row. Long values are cut and shown in full on hover.
pub fn detail(ui: &mut egui::Ui, label: &str, value: impl Into<String>) {
    let value = value.into();
    ui.horizontal(|ui| {
        let (rect, _) = ui.allocate_exact_size(egui::vec2(88.0, 20.0), egui::Sense::hover());
        ui.painter().text(
            rect.left_center(),
            egui::Align2::LEFT_CENTER,
            label,
            theme::body(),
            theme::MUTED,
        );
        ui.add(egui::Label::new(&value).truncate())
            .on_hover_text(&value);
    });
}

/// Error text in the one error color.
pub fn error(ui: &mut egui::Ui, text: &str) -> Response {
    ui.label(egui::RichText::new(text).color(theme::ERROR))
}

/// Centres a short column of content (an empty or loading state) in the free space.
pub fn placeholder(ui: &mut egui::Ui, body: impl FnOnce(&mut egui::Ui)) {
    let area = ui.available_rect_before_wrap();
    let size = Vec2::new(area.width().min(420.0), area.height().min(220.0));
    let rect = egui::Rect::from_center_size(area.center(), size);
    ui.scope_builder(
        egui::UiBuilder::new()
            .max_rect(rect)
            .layout(egui::Layout::top_down(egui::Align::Center)),
        |ui| {
            ui.spacing_mut().item_spacing = Spacing::Md.vec(Spacing::Md);
            body(ui);
        },
    );
}
