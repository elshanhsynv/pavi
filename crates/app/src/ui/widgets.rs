use super::{spacing::Spacing, theme};
use eframe::egui::{self, Color32, Pos2, Response, Stroke, Vec2};

#[derive(Clone, Copy)]
pub enum Icon {
    Search,
    Sort,
    Freeze,
    Cache,
    Database,
    Folder,
    Refresh,
    Grid,
    Sql,
    Filter,
    File,
    Chart,
    Clock,
    History,
    Columns,
    Export,
    Copy,
    Info,
    Schema,
    Cursor,
}

pub fn icon(ui: &egui::Ui, center: Pos2, glyph: Icon, color: Color32) {
    let painter = ui.painter();
    let stroke = Stroke::new(1.6_f32, color);
    let p = |x, y| center + egui::vec2(x, y);
    let line = |points: &[(f32, f32)]| {
        painter.add(egui::Shape::line(
            points.iter().map(|&(x, y)| p(x, y)).collect(),
            stroke,
        ));
    };
    let rect = |x, y, w, h| {
        painter.rect_stroke(
            egui::Rect::from_min_size(p(x, y), egui::vec2(w, h)),
            1.5,
            stroke,
            egui::StrokeKind::Inside,
        );
    };
    match glyph {
        Icon::Search => {
            painter.circle_stroke(center - egui::vec2(2.0, 2.0), 6.0, stroke);
            line(&[(2.0, 2.0), (8.0, 8.0)]);
        }
        Icon::Sort => {
            line(&[(-5.0, -8.0), (-5.0, 8.0), (-9.0, 3.0)]);
            line(&[(5.0, 8.0), (5.0, -8.0), (9.0, -3.0)]);
        }
        Icon::Freeze => {
            rect(-8.0, -8.0, 16.0, 16.0);
            line(&[(-8.0, -2.0), (8.0, -2.0)]);
            line(&[(-3.0, -8.0), (-3.0, 8.0)]);
            line(&[(3.0, -8.0), (3.0, 8.0)]);
        }
        Icon::Cache => {
            painter.circle_stroke(center, 5.0, stroke);
            for i in 0..8 {
                let a = i as f32 * std::f32::consts::TAU / 8.0;
                line(&[
                    (a.cos() * 6.0, a.sin() * 6.0),
                    (a.cos() * 9.0, a.sin() * 9.0),
                ]);
            }
        }

        Icon::Database => {
            for y in [-6.0, 0.0, 6.0] {
                let points = (0..=24)
                    .map(|i| {
                        let a = i as f32 * std::f32::consts::TAU / 24.0;
                        p(a.cos() * 8.0, y + a.sin() * 3.0)
                    })
                    .collect();
                painter.add(egui::Shape::closed_line(points, stroke));
            }
            line(&[(-8.0, -6.0), (-8.0, 6.0)]);
            line(&[(8.0, -6.0), (8.0, 6.0)]);
        }
        Icon::Folder => {
            rect(-8.0, -5.0, 16.0, 12.0);
            line(&[(-7.0, -5.0), (-7.0, -8.0), (-1.0, -8.0), (2.0, -5.0)]);
        }
        Icon::Grid | Icon::Columns => {
            rect(-8.0, -8.0, 16.0, 16.0);
            line(&[(-8.0, -2.0), (8.0, -2.0)]);
            line(&[(0.0, -8.0), (0.0, 8.0)]);
            if matches!(glyph, Icon::Grid) {
                line(&[(-8.0, 3.0), (8.0, 3.0)]);
            }
        }
        Icon::Sql => {
            line(&[(-7.0, -6.0), (-1.0, 0.0), (-7.0, 6.0)]);
            line(&[(1.0, 6.0), (9.0, 6.0)]);
        }
        Icon::Filter => line(&[
            (-8.0, -8.0),
            (8.0, -8.0),
            (2.0, 0.0),
            (2.0, 8.0),
            (-2.0, 5.0),
            (-2.0, 0.0),
            (-8.0, -8.0),
        ]),
        Icon::File => {
            line(&[
                (-6.0, -9.0),
                (2.0, -9.0),
                (7.0, -4.0),
                (7.0, 9.0),
                (-6.0, 9.0),
                (-6.0, -9.0),
            ]);
            line(&[(2.0, -9.0), (2.0, -4.0), (7.0, -4.0)]);
            for y in [0.0, 4.0] {
                line(&[(-3.0, y), (4.0, y)]);
            }
        }
        Icon::Chart => {
            for (x, h) in [(-7.0, 8.0), (0.0, 17.0), (7.0, 12.0)] {
                painter.rect_filled(
                    egui::Rect::from_min_size(p(x - 1.5, 8.0 - h), egui::vec2(3.0, h)),
                    1.0,
                    color,
                );
            }
        }
        Icon::Clock | Icon::History | Icon::Refresh => {
            painter.circle_stroke(center, 8.0, stroke);
            if matches!(glyph, Icon::Refresh) {
                line(&[(3.0, -4.0), (8.0, -4.0), (8.0, -9.0)]);
            } else {
                line(&[(0.0, -5.0), (0.0, 0.0), (4.0, 2.0)]);
            }
            if matches!(glyph, Icon::History) {
                line(&[(-11.0, -1.0), (-7.0, 2.0), (-4.0, -2.0)]);
            }
        }
        Icon::Copy => {
            rect(-7.0, -4.0, 12.0, 12.0);
            rect(-3.0, -8.0, 12.0, 12.0);
        }
        Icon::Export => {
            line(&[(-7.0, 2.0), (-7.0, 8.0), (7.0, 8.0), (7.0, 2.0)]);
            line(&[(0.0, 4.0), (0.0, -9.0)]);
            line(&[(-4.0, -5.0), (0.0, -9.0), (4.0, -5.0)]);
        }
        Icon::Info => {
            painter.circle_filled(center, 9.0, color);
            painter.text(
                center,
                egui::Align2::CENTER_CENTER,
                "i",
                egui::FontId::proportional(15.0),
                theme::PANEL,
            );
        }
        Icon::Schema => {
            line(&[(0.0, -6.0), (0.0, 0.0), (-7.0, 6.0)]);
            line(&[(0.0, 0.0), (7.0, 6.0)]);
            for (x, y) in [(0.0, -7.0), (-7.0, 7.0), (7.0, 7.0)] {
                painter.circle_filled(p(x, y), 3.0, theme::PANEL);
                painter.circle_stroke(p(x, y), 3.0, stroke);
            }
        }
        Icon::Cursor => line(&[
            (-6.0, -9.0),
            (-6.0, 8.0),
            (-1.0, 4.0),
            (3.0, 10.0),
            (6.0, 8.0),
            (2.0, 2.0),
            (8.0, 1.0),
            (-6.0, -9.0),
        ]),
    }
}

pub fn button(ui: &mut egui::Ui, label: &str, glyph: Icon, selected: bool, size: Vec2) -> Response {
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::selected(egui::WidgetType::Button, ui.is_enabled(), selected, label)
    });
    {
        ui.painter().rect_filled(
            rect,
            5.0,
            if selected {
                theme::SELECTED
            } else {
                theme::HEADER
            },
        );
        ui.painter().rect_stroke(
            rect,
            5.0,
            Stroke::new(
                1.0_f32,
                if selected {
                    theme::ACCENT
                } else {
                    theme::BORDER
                },
            ),
            egui::StrokeKind::Inside,
        );
    }
    let color = if ui.is_enabled() {
        theme::TEXT
    } else {
        theme::DISABLED
    };
    icon(
        ui,
        egui::pos2(rect.left() + 16.0, rect.center().y),
        glyph,
        if selected {
            theme::ACCENT
        } else if ui.is_enabled() {
            theme::MUTED
        } else {
            theme::DISABLED
        },
    );
    ui.painter().text(
        egui::pos2(rect.left() + 34.0, rect.center().y),
        egui::Align2::LEFT_CENTER,
        label,
        egui::FontId::proportional(13.0),
        color,
    );
    response
}

pub fn section(ui: &mut egui::Ui, label: &str, glyph: Icon, body: impl FnOnce(&mut egui::Ui)) {
    let id = ui.make_persistent_id(label);
    let mut open = ui.data_mut(|data| data.get_temp::<bool>(id).unwrap_or(false));
    let response = button(
        ui,
        label,
        glyph,
        open || label == "Dataset",
        egui::vec2(ui.available_width(), 32.0),
    );
    if response.clicked() {
        open = !open;
        ui.data_mut(|data| data.insert_temp(id, open));
    }
    if matches!(
        label,
        "Quick SQL" | "Recent Files" | "Query History" | "Columns"
    ) {
        chevron(
            ui,
            response.rect.right_center() - Spacing::Lg.vec(Spacing::None),
            open,
        );
    }
    if open {
        ui.indent(id, body);
        ui.add_space(Spacing::Sm.px());
    }
}

pub fn menu(
    ui: &mut egui::Ui,
    label: &str,
    glyph: Icon,
    selected: bool,
    body: impl FnOnce(&mut egui::Ui),
) {
    let width = ui.fonts(|fonts| {
        fonts
            .layout_no_wrap(label.into(), egui::FontId::proportional(13.0), theme::TEXT)
            .size()
            .x
    }) + 52.0;
    let response = button(ui, label, glyph, selected, egui::vec2(width, 36.0));
    let id = ui.make_persistent_id(label);
    if response.clicked() {
        ui.memory_mut(|memory| memory.toggle_popup(id));
    }
    chevron(
        ui,
        response.rect.right_center() - Spacing::Lg.vec(Spacing::None),
        false,
    );
    egui::popup_below_widget(
        ui,
        id,
        &response,
        egui::PopupCloseBehavior::CloseOnClick,
        |ui| {
            ui.set_min_width(260.0);
            ui.set_max_width(420.0);
            body(ui);
        },
    );
}

fn chevron(ui: &egui::Ui, center: Pos2, up: bool) {
    let sign = if up { -1.0 } else { 1.0 };
    ui.painter().add(egui::Shape::line(
        vec![
            center + egui::vec2(-4.0, -2.0 * sign),
            center + egui::vec2(0.0, 2.0 * sign),
            center + egui::vec2(4.0, -2.0 * sign),
        ],
        Stroke::new(1.5_f32, theme::ACCENT),
    ));
}

pub fn card(
    ui: &mut egui::Ui,
    title: &str,
    glyph: Icon,
    default_open: bool,
    body: impl FnOnce(&mut egui::Ui),
) {
    let id = ui.make_persistent_id(title);
    let mut open = ui.data_mut(|data| data.get_temp::<bool>(id).unwrap_or(default_open));
    egui::Frame::new()
        .fill(theme::PANEL)
        .stroke(Stroke::new(1.0_f32, theme::BORDER))
        .corner_radius(5.0)
        .inner_margin(Spacing::Md.margin())
        .show(ui, |ui| {
            let (rect, response) = ui
                .allocate_exact_size(egui::vec2(ui.available_width(), 28.0), egui::Sense::click());
            response.widget_info(|| {
                egui::WidgetInfo::labeled(
                    egui::WidgetType::CollapsingHeader,
                    ui.is_enabled(),
                    title,
                )
            });
            icon(
                ui,
                rect.left_center() + Spacing::Lg.vec(Spacing::None),
                glyph,
                theme::ACCENT,
            );
            ui.painter().text(
                rect.left_center() + egui::vec2(32.0, 0.0),
                egui::Align2::LEFT_CENTER,
                title,
                egui::FontId::proportional(14.0),
                theme::TEXT,
            );
            if response.clicked() {
                open = !open;
                ui.data_mut(|data| data.insert_temp(id, open));
            }
            chevron(
                ui,
                response.rect.right_center() - Spacing::Md.vec(Spacing::None),
                open,
            );
            if open {
                ui.separator();
                ui.add_space(Spacing::Sm.px());
                body(ui);
            }
        });
    ui.add_space(Spacing::Md.px());
}

pub fn detail(ui: &mut egui::Ui, label: &str, value: impl Into<String>) {
    let value = value.into();
    ui.horizontal(|ui| {
        ui.add_sized(
            [84.0, 20.0],
            egui::Label::new(egui::RichText::new(label).color(theme::MUTED))
                .halign(egui::Align::Min),
        );
        ui.add(egui::Label::new(&value).truncate())
            .on_hover_text(&value);
    });
}
