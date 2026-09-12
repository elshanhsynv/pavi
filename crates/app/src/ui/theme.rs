use super::spacing::Spacing;
use eframe::egui::{self, Color32, FontId, Stroke};

pub const PANEL: Color32 = Color32::from_rgb(3, 24, 59);
pub const BACKGROUND: Color32 = Color32::from_rgb(2, 18, 46);
pub const BORDER: Color32 = Color32::from_rgb(24, 70, 130);
pub const TEXT: Color32 = Color32::from_rgb(221, 233, 255);
pub const MUTED: Color32 = Color32::from_rgb(147, 189, 247);
pub const DISABLED: Color32 = Color32::from_rgb(82, 112, 159);
pub const ACCENT: Color32 = Color32::from_rgb(103, 164, 255);
pub const SELECTED: Color32 = Color32::from_rgb(24, 66, 210);
pub const HEADER: Color32 = Color32::from_rgb(5, 32, 70);
pub const GRID_LINE: Color32 = Color32::from_rgb(13, 48, 91);

pub fn apply(ctx: &egui::Context) {
    let mut style = (*ctx.style()).clone();
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = PANEL;
    visuals.window_fill = PANEL;
    visuals.extreme_bg_color = BACKGROUND;
    visuals.faint_bg_color = HEADER;
    visuals.override_text_color = Some(TEXT);
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, BORDER);
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0_f32, MUTED);
    visuals.widgets.inactive.bg_fill = HEADER;
    visuals.widgets.inactive.weak_bg_fill = HEADER;
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0_f32, BORDER);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(13, 50, 108);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(13, 50, 108);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0_f32, MUTED);
    visuals.widgets.active.bg_fill = SELECTED;
    visuals.selection.bg_fill = SELECTED;
    visuals.selection.stroke = Stroke::new(1.0_f32, ACCENT);
    visuals.window_stroke = Stroke::new(1.0_f32, BORDER);
    for widget in [
        &mut visuals.widgets.noninteractive,
        &mut visuals.widgets.inactive,
        &mut visuals.widgets.hovered,
        &mut visuals.widgets.active,
        &mut visuals.widgets.open,
    ] {
        widget.corner_radius = egui::CornerRadius::same(5);
    }
    style.visuals = visuals;
    style.spacing.item_spacing = Spacing::Md.vec(Spacing::Xs);
    style.spacing.button_padding = Spacing::Md.vec(Spacing::Sm);
    style.spacing.interact_size.y = 28.0;
    style.spacing.window_margin = Spacing::Md.margin();
    style.spacing.menu_margin = Spacing::Md.margin();
    style.spacing.scroll = egui::style::ScrollStyle::solid();
    style.spacing.scroll.bar_width = 10.0;
    for text_style in [egui::TextStyle::Body, egui::TextStyle::Button] {
        style
            .text_styles
            .insert(text_style, FontId::proportional(13.0));
    }
    style
        .text_styles
        .insert(egui::TextStyle::Small, FontId::proportional(12.0));
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
