use super::spacing::Spacing;
use eframe::egui::{self, Color32, FontFamily, FontId, Stroke};
use std::sync::Arc;

// Surfaces, darkest to lightest. Warm graphite: the chrome recedes, the data stands out.
/// Grid body and text inputs: the deepest surface.
pub const BACKGROUND: Color32 = Color32::from_rgb(20, 19, 18);
/// Alternate grid rows.
pub const STRIPE: Color32 = Color32::from_rgb(24, 23, 21);
/// Bars, the Inspector, popups and windows.
pub const PANEL: Color32 = Color32::from_rgb(29, 28, 26);
/// Grid header and resting control fill.
pub const HEADER: Color32 = Color32::from_rgb(37, 35, 33);
/// A control or header under the pointer.
pub const HOVER: Color32 = Color32::from_rgb(50, 47, 43);
/// Panel, popup and control outlines.
pub const BORDER: Color32 = Color32::from_rgb(56, 53, 48);
/// Lines between grid cells.
pub const GRID_LINE: Color32 = Color32::from_rgb(35, 33, 31);

/// Primary text.
pub const TEXT: Color32 = Color32::from_rgb(232, 228, 220);
/// Secondary text, labels and resting icons.
pub const MUTED: Color32 = Color32::from_rgb(160, 153, 141);
/// Disabled controls and text.
pub const DISABLED: Color32 = Color32::from_rgb(100, 95, 87);

/// The one accent (oak): focus, selection outline, active tools, primary action.
pub const ACCENT: Color32 = Color32::from_rgb(226, 168, 92);
/// Text and icons on top of an `ACCENT` fill.
pub const ON_ACCENT: Color32 = Color32::from_rgb(33, 24, 12);
/// Selected rows, cells and switched-on tools.
pub const SELECTED: Color32 = Color32::from_rgb(64, 50, 31);
/// Errors, everywhere.
pub const ERROR: Color32 = Color32::from_rgb(240, 122, 110);
/// The idle "all good" status dot.
pub const SUCCESS: Color32 = Color32::from_rgb(132, 190, 130);
/// The window close button under the pointer.
pub const CLOSE_HOVER: Color32 = Color32::from_rgb(196, 58, 48);

// Column type families, used for type labels in headers and the schema list.
/// Integer, float and decimal columns.
pub const TYPE_NUMBER: Color32 = Color32::from_rgb(132, 180, 228);
/// Date, time, timestamp and duration columns.
pub const TYPE_TEMPORAL: Color32 = Color32::from_rgb(198, 164, 224);
/// Boolean columns.
pub const TYPE_BOOL: Color32 = Color32::from_rgb(150, 204, 150);

/// Line and scatter chart marks.
pub const CHART_POINT: Color32 = TYPE_NUMBER;
/// Bar chart and histogram marks.
pub const CHART_BAR: Color32 = ACCENT;

// Widget sizes (not spacing; see `spacing.rs`).
/// The single top bar.
pub const BAR_HEIGHT: f32 = 40.0;
/// The status bar.
pub const STATUS_HEIGHT: f32 = 26.0;
/// Buttons, inputs and menu rows.
pub const CONTROL_HEIGHT: f32 = 26.0;
/// Corner radius of controls and popups.
pub const RADIUS: u8 = 4;

/// Font family for semibold text. Always registered by `apply`.
const STRONG_FAMILY: &str = "strong";
/// A system font file larger than this is ignored.
const MAX_FONT_BYTES: u64 = 16 * 1024 * 1024;

/// Body text: grid cells, buttons, labels.
pub fn body() -> FontId {
    FontId::proportional(13.0)
}

/// Small text: type labels, row numbers, status bar.
pub fn small() -> FontId {
    FontId::proportional(11.5)
}

/// Semibold text at `size`: column names, section titles, headings.
pub fn strong(size: f32) -> FontId {
    FontId::new(size, FontFamily::Name(STRONG_FAMILY.into()))
}

/// Reads one font from the Windows font folder. `None` on other platforms or
/// when the file is missing, so the built-in egui fonts are used instead.
fn system_font(file: &str) -> Option<egui::FontData> {
    let path = std::path::PathBuf::from(std::env::var_os("WINDIR")?)
        .join("Fonts")
        .join(file);
    let length = std::fs::metadata(&path).ok()?.len();
    if length > MAX_FONT_BYTES {
        return None;
    }
    std::fs::read(path).ok().map(egui::FontData::from_owned)
}

fn fonts() -> egui::FontDefinitions {
    let mut fonts = egui::FontDefinitions::default();
    let mut strong = fonts
        .families
        .get(&FontFamily::Proportional)
        .cloned()
        .unwrap_or_default();
    if let Some(regular) = system_font("segoeui.ttf") {
        fonts.font_data.insert("ui".to_owned(), Arc::new(regular));
        fonts
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .insert(0, "ui".to_owned());
        strong.insert(0, "ui".to_owned());
    }
    if let Some(semibold) = system_font("seguisb.ttf") {
        fonts
            .font_data
            .insert("ui-strong".to_owned(), Arc::new(semibold));
        strong.insert(0, "ui-strong".to_owned());
    }
    fonts
        .families
        .insert(FontFamily::Name(STRONG_FAMILY.into()), strong);
    fonts
}

pub fn apply(ctx: &egui::Context) {
    ctx.set_fonts(fonts());
    let mut style = (*ctx.style()).clone();
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = PANEL;
    visuals.window_fill = PANEL;
    visuals.extreme_bg_color = BACKGROUND;
    visuals.faint_bg_color = STRIPE;
    visuals.code_bg_color = HEADER;
    visuals.error_fg_color = ERROR;
    visuals.hyperlink_color = ACCENT;
    visuals.window_stroke = Stroke::new(1.0_f32, BORDER);
    visuals.window_corner_radius = egui::CornerRadius::same(RADIUS + 2);
    visuals.menu_corner_radius = egui::CornerRadius::same(RADIUS + 2);
    visuals.selection.bg_fill = SELECTED;
    visuals.selection.stroke = Stroke::new(1.0_f32, ACCENT);
    visuals.text_cursor.stroke = Stroke::new(1.5_f32, ACCENT);

    let widgets = &mut visuals.widgets;
    widgets.noninteractive.bg_fill = PANEL;
    widgets.noninteractive.weak_bg_fill = PANEL;
    widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, BORDER);
    widgets.noninteractive.fg_stroke = Stroke::new(1.0_f32, TEXT);
    widgets.inactive.bg_fill = HEADER;
    widgets.inactive.weak_bg_fill = HEADER;
    widgets.inactive.bg_stroke = Stroke::new(1.0_f32, BORDER);
    widgets.inactive.fg_stroke = Stroke::new(1.0_f32, TEXT);
    widgets.hovered.bg_fill = HOVER;
    widgets.hovered.weak_bg_fill = HOVER;
    widgets.hovered.bg_stroke = Stroke::new(1.0_f32, MUTED);
    widgets.hovered.fg_stroke = Stroke::new(1.0_f32, TEXT);
    widgets.active.bg_fill = SELECTED;
    widgets.active.weak_bg_fill = SELECTED;
    widgets.active.bg_stroke = Stroke::new(1.0_f32, ACCENT);
    widgets.active.fg_stroke = Stroke::new(1.0_f32, TEXT);
    widgets.open.bg_fill = HOVER;
    widgets.open.weak_bg_fill = HOVER;
    widgets.open.bg_stroke = Stroke::new(1.0_f32, BORDER);
    widgets.open.fg_stroke = Stroke::new(1.0_f32, TEXT);
    for widget in [
        &mut widgets.noninteractive,
        &mut widgets.inactive,
        &mut widgets.hovered,
        &mut widgets.active,
        &mut widgets.open,
    ] {
        widget.corner_radius = egui::CornerRadius::same(RADIUS);
        widget.expansion = 0.0;
    }
    style.visuals = visuals;

    style.spacing.item_spacing = Spacing::Sm.vec(Spacing::Sm);
    style.spacing.button_padding = Spacing::Md.vec(Spacing::Xs);
    style.spacing.interact_size.y = CONTROL_HEIGHT;
    style.spacing.window_margin = Spacing::Lg.margin();
    style.spacing.menu_margin = Spacing::Sm.margin();
    style.spacing.scroll = egui::style::ScrollStyle::solid();
    style.spacing.scroll.bar_width = 10.0;
    for text_style in [egui::TextStyle::Body, egui::TextStyle::Button] {
        style.text_styles.insert(text_style, body());
    }
    style.text_styles.insert(egui::TextStyle::Small, small());
    style
        .text_styles
        .insert(egui::TextStyle::Heading, strong(17.0));
    style
        .text_styles
        .insert(egui::TextStyle::Monospace, FontId::monospace(12.5));
    ctx.set_style(style);
}

pub fn number(value: u64) -> String {
    let digits = value.to_string();
    let mut result = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            result.push(',');
        }
        result.push(digit);
    }
    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn formats_dataset_counts() {
        for (value, expected) in [
            (0, "0"),
            (999, "999"),
            (1000, "1,000"),
            (1_000_000_000, "1,000,000,000"),
            (u64::MAX, "18,446,744,073,709,551,615"),
        ] {
            assert_eq!(super::number(value), expected);
        }
    }
}
