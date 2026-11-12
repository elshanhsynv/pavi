use super::spacing::Spacing;
use eframe::egui::{self, Color32, FontFamily, FontId, Stroke};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Every colour the UI uses. There are two: [`DARK`] and [`LIGHT`] (ADR 0010).
/// UI code reads them through the functions below, never from a palette
/// directly, so the whole app switches at once.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Palette {
    // Surfaces, deepest to most raised.
    /// Grid body and text inputs.
    pub background: Color32,
    /// Alternate grid rows.
    pub stripe: Color32,
    /// Bars, the Inspector, popups and windows.
    pub panel: Color32,
    /// Grid header and resting control fill.
    pub header: Color32,
    /// A control or header under the pointer.
    pub hover: Color32,
    /// Panel, popup and control outlines.
    pub border: Color32,
    /// Lines between grid cells.
    pub grid_line: Color32,
    /// Primary text.
    pub text: Color32,
    /// Secondary text, labels and resting icons.
    pub muted: Color32,
    /// Disabled controls and text.
    pub disabled: Color32,
    /// The one accent (oak): focus, selection outline, active tools, primary action.
    pub accent: Color32,
    /// Text and icons on top of an `accent` fill.
    pub on_accent: Color32,
    /// Selected rows, cells and switched-on tools.
    pub selected: Color32,
    /// Errors, everywhere.
    pub error: Color32,
    /// The idle "all good" status dot.
    pub success: Color32,
    /// The window close button under the pointer.
    pub close_hover: Color32,
    /// Integer, float and decimal type labels.
    pub type_number: Color32,
    /// Date, time, timestamp and duration type labels.
    pub type_temporal: Color32,
    /// Boolean type labels.
    pub type_bool: Color32,
    /// Two more chart series colours, after the type colours and the accent.
    pub chart_extra: [Color32; 2],
    /// Chart grid lines behind the marks.
    pub chart_grid: Color32,
}

/// Warm graphite: the chrome recedes, the data stands out.
pub const DARK: Palette = Palette {
    background: Color32::from_rgb(20, 19, 18),
    stripe: Color32::from_rgb(24, 23, 21),
    panel: Color32::from_rgb(29, 28, 26),
    header: Color32::from_rgb(37, 35, 33),
    hover: Color32::from_rgb(50, 47, 43),
    border: Color32::from_rgb(56, 53, 48),
    grid_line: Color32::from_rgb(35, 33, 31),
    text: Color32::from_rgb(232, 228, 220),
    muted: Color32::from_rgb(160, 153, 141),
    disabled: Color32::from_rgb(100, 95, 87),
    accent: Color32::from_rgb(226, 168, 92),
    on_accent: Color32::from_rgb(33, 24, 12),
    selected: Color32::from_rgb(64, 50, 31),
    error: Color32::from_rgb(240, 122, 110),
    success: Color32::from_rgb(132, 190, 130),
    close_hover: Color32::from_rgb(196, 58, 48),
    type_number: Color32::from_rgb(132, 180, 228),
    type_temporal: Color32::from_rgb(198, 164, 224),
    type_bool: Color32::from_rgb(150, 204, 150),
    chart_extra: [
        Color32::from_rgb(232, 128, 160),
        Color32::from_rgb(120, 200, 200),
    ],
    chart_grid: Color32::from_rgb(44, 42, 39),
};

/// Warm paper with the same oak accent, darkened so text in it stays legible.
pub const LIGHT: Palette = Palette {
    background: Color32::from_rgb(253, 252, 249),
    stripe: Color32::from_rgb(247, 245, 241),
    panel: Color32::from_rgb(241, 238, 233),
    header: Color32::from_rgb(232, 228, 222),
    hover: Color32::from_rgb(220, 215, 207),
    border: Color32::from_rgb(208, 202, 193),
    grid_line: Color32::from_rgb(232, 229, 223),
    text: Color32::from_rgb(33, 31, 28),
    muted: Color32::from_rgb(104, 98, 89),
    disabled: Color32::from_rgb(160, 154, 145),
    accent: Color32::from_rgb(140, 82, 14),
    on_accent: Color32::from_rgb(255, 250, 242),
    selected: Color32::from_rgb(244, 224, 194),
    error: Color32::from_rgb(176, 40, 30),
    success: Color32::from_rgb(52, 132, 62),
    close_hover: Color32::from_rgb(196, 58, 48),
    type_number: Color32::from_rgb(30, 100, 170),
    type_temporal: Color32::from_rgb(124, 72, 166),
    type_bool: Color32::from_rgb(36, 106, 46),
    chart_extra: [
        Color32::from_rgb(190, 60, 110),
        Color32::from_rgb(20, 128, 128),
    ],
    chart_grid: Color32::from_rgb(226, 222, 215),
};

/// Whether [`LIGHT`] is in use; set only by [`set_light`].
static LIGHT_IN_USE: AtomicBool = AtomicBool::new(false);

/// The palette in use.
pub fn palette() -> &'static Palette {
    if LIGHT_IN_USE.load(Ordering::Relaxed) {
        &LIGHT
    } else {
        &DARK
    }
}

pub fn background() -> Color32 {
    palette().background
}
pub fn stripe() -> Color32 {
    palette().stripe
}
pub fn panel() -> Color32 {
    palette().panel
}
pub fn header() -> Color32 {
    palette().header
}
pub fn hover() -> Color32 {
    palette().hover
}
pub fn border() -> Color32 {
    palette().border
}
pub fn grid_line() -> Color32 {
    palette().grid_line
}
pub fn text() -> Color32 {
    palette().text
}
pub fn muted() -> Color32 {
    palette().muted
}
pub fn disabled() -> Color32 {
    palette().disabled
}
pub fn accent() -> Color32 {
    palette().accent
}
pub fn on_accent() -> Color32 {
    palette().on_accent
}
pub fn selected() -> Color32 {
    palette().selected
}
pub fn error() -> Color32 {
    palette().error
}
pub fn success() -> Color32 {
    palette().success
}
pub fn close_hover() -> Color32 {
    palette().close_hover
}
pub fn type_number() -> Color32 {
    palette().type_number
}
pub fn type_temporal() -> Color32 {
    palette().type_temporal
}
pub fn type_bool() -> Color32 {
    palette().type_bool
}

// SQL editor highlighting.
/// SQL keywords (SELECT, WHERE, …).
pub fn sql_keyword() -> Color32 {
    accent()
}
/// Aggregate functions (COUNT, SUM, …).
pub fn sql_function() -> Color32 {
    type_temporal()
}
/// Quoted text.
pub fn sql_string() -> Color32 {
    type_bool()
}
/// Numbers.
pub fn sql_number() -> Color32 {
    type_number()
}
/// Comments, and names that are not columns of the file (often a typo).
pub fn sql_muted() -> Color32 {
    muted()
}

/// Chart series, in order: the first Y column gets the first colour.
pub fn chart_series() -> [Color32; 6] {
    let palette = palette();
    [
        palette.type_number,
        palette.accent,
        palette.type_bool,
        palette.type_temporal,
        palette.chart_extra[0],
        palette.chart_extra[1],
    ]
}
/// Chart grid lines behind the marks.
pub fn chart_grid() -> Color32 {
    palette().chart_grid
}
/// Bar chart and histogram marks.
pub fn chart_bar() -> Color32 {
    accent()
}

// Widget sizes (not spacing; see `spacing.rs`).
/// The single top bar.
pub const BAR_HEIGHT: f32 = 40.0;
/// Height of the tab strip under the top bar.
pub const TAB_BAR_HEIGHT: f32 = 32.0;
/// The status bar.
pub const STATUS_HEIGHT: f32 = 26.0;
/// Buttons, inputs and menu rows.
pub const CONTROL_HEIGHT: f32 = 26.0;
/// Width of the strips along the window edges that resize the window.
pub const RESIZE_BORDER: f32 = 5.0;
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

/// System text fonts to try on `os` (as `std::env::consts::OS` names it), in
/// order: a regular file and, when the family has one, a semibold or bold
/// file. `env` reads an environment variable.
pub(crate) fn font_files(
    os: &str,
    env: &dyn Fn(&str) -> Option<std::ffi::OsString>,
) -> Vec<(std::path::PathBuf, Option<std::path::PathBuf>)> {
    use std::path::PathBuf;
    let pair = |folder: &str, regular: &str, strong: Option<&str>| {
        let folder = PathBuf::from(folder);
        (
            folder.join(regular),
            strong.map(|strong| folder.join(strong)),
        )
    };
    match os {
        "windows" => env("WINDIR")
            .map(|windows| {
                let fonts = PathBuf::from(windows).join("Fonts");
                vec![(fonts.join("segoeui.ttf"), Some(fonts.join("seguisb.ttf")))]
            })
            .unwrap_or_default(),
        "macos" => vec![
            pair(
                "/System/Library/Fonts/Supplemental",
                "Arial.ttf",
                Some("Arial Bold.ttf"),
            ),
            pair("/Library/Fonts", "Arial.ttf", Some("Arial Bold.ttf")),
        ],
        // Linux and the BSDs: Noto Sans as Debian, Ubuntu, Arch and Fedora
        // install it, then DejaVu Sans, which most desktops have.
        _ => vec![
            pair(
                "/usr/share/fonts/truetype/noto",
                "NotoSans-Regular.ttf",
                Some("NotoSans-SemiBold.ttf"),
            ),
            pair(
                "/usr/share/fonts/noto",
                "NotoSans-Regular.ttf",
                Some("NotoSans-SemiBold.ttf"),
            ),
            pair(
                "/usr/share/fonts/google-noto",
                "NotoSans-Regular.ttf",
                Some("NotoSans-SemiBold.ttf"),
            ),
            // A variable font: its default instance, regular, is what is drawn.
            pair(
                "/usr/share/fonts/google-noto-vf",
                "NotoSans[wght].ttf",
                None,
            ),
            pair(
                "/usr/share/fonts/truetype/dejavu",
                "DejaVuSans.ttf",
                Some("DejaVuSans-Bold.ttf"),
            ),
            pair(
                "/usr/share/fonts/dejavu-sans-fonts",
                "DejaVuSans.ttf",
                Some("DejaVuSans-Bold.ttf"),
            ),
            pair(
                "/usr/share/fonts/TTF",
                "DejaVuSans.ttf",
                Some("DejaVuSans-Bold.ttf"),
            ),
        ],
    }
}

/// Reads one font file. `None` when it is missing, unreadable or larger than
/// [`MAX_FONT_BYTES`].
fn read_font(path: &std::path::Path) -> Option<egui::FontData> {
    let length = std::fs::metadata(path).ok()?.len();
    if length > MAX_FONT_BYTES {
        return None;
    }
    std::fs::read(path).ok().map(egui::FontData::from_owned)
}

/// The first system font family found for this platform: regular, and
/// semibold when there is one. `None` keeps egui's built-in fonts.
fn system_fonts() -> Option<(egui::FontData, Option<egui::FontData>)> {
    let env = |name: &str| std::env::var_os(name);
    font_files(std::env::consts::OS, &env)
        .into_iter()
        .find_map(|(regular, strong)| {
            let regular = read_font(&regular)?;
            Some((regular, strong.as_deref().and_then(read_font)))
        })
}

fn fonts() -> egui::FontDefinitions {
    let mut fonts = egui::FontDefinitions::default();
    let mut strong = fonts
        .families
        .get(&FontFamily::Proportional)
        .cloned()
        .unwrap_or_default();
    if let Some((regular, semibold)) = system_fonts() {
        fonts.font_data.insert("ui".to_owned(), Arc::new(regular));
        fonts
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .insert(0, "ui".to_owned());
        strong.insert(0, "ui".to_owned());
        if let Some(semibold) = semibold {
            fonts
                .font_data
                .insert("ui-strong".to_owned(), Arc::new(semibold));
            strong.insert(0, "ui-strong".to_owned());
        }
    }
    fonts
        .families
        .insert(FontFamily::Name(STRONG_FAMILY.into()), strong);
    fonts
}

/// Loads the fonts and applies the dark style. Called once, before the
/// first frame; [`set_light`] switches the palette afterwards.
pub fn apply(ctx: &egui::Context) {
    ctx.set_fonts(fonts());
    set_light(ctx, false);
}

/// Switches every colour to the light (`true`) or dark palette. The style is
/// written to both of egui's theme slots, so egui following the system theme
/// can't swap in a style PAVI didn't make.
pub fn set_light(ctx: &egui::Context, light: bool) {
    LIGHT_IN_USE.store(light, Ordering::Relaxed);
    let style = Arc::new(style(ctx, light));
    ctx.set_style_of(egui::Theme::Dark, Arc::clone(&style));
    ctx.set_style_of(egui::Theme::Light, style);
}

fn style(ctx: &egui::Context, light: bool) -> egui::Style {
    let palette = if light { &LIGHT } else { &DARK };
    let mut style = (*ctx.style()).clone();
    let mut visuals = if light {
        egui::Visuals::light()
    } else {
        egui::Visuals::dark()
    };
    visuals.panel_fill = palette.panel;
    visuals.window_fill = palette.panel;
    visuals.extreme_bg_color = palette.background;
    visuals.faint_bg_color = palette.stripe;
    visuals.code_bg_color = palette.header;
    visuals.error_fg_color = palette.error;
    visuals.hyperlink_color = palette.accent;
    visuals.window_stroke = Stroke::new(1.0_f32, palette.border);
    visuals.window_corner_radius = egui::CornerRadius::same(RADIUS + 2);
    visuals.menu_corner_radius = egui::CornerRadius::same(RADIUS + 2);
    visuals.selection.bg_fill = palette.selected;
    visuals.selection.stroke = Stroke::new(1.0_f32, palette.accent);
    visuals.text_cursor.stroke = Stroke::new(1.5_f32, palette.accent);

    let widgets = &mut visuals.widgets;
    widgets.noninteractive.bg_fill = palette.panel;
    widgets.noninteractive.weak_bg_fill = palette.panel;
    widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, palette.border);
    widgets.noninteractive.fg_stroke = Stroke::new(1.0_f32, palette.text);
    widgets.inactive.bg_fill = palette.header;
    widgets.inactive.weak_bg_fill = palette.header;
    widgets.inactive.bg_stroke = Stroke::new(1.0_f32, palette.border);
    widgets.inactive.fg_stroke = Stroke::new(1.0_f32, palette.text);
    widgets.hovered.bg_fill = palette.hover;
    widgets.hovered.weak_bg_fill = palette.hover;
    widgets.hovered.bg_stroke = Stroke::new(1.0_f32, palette.muted);
    widgets.hovered.fg_stroke = Stroke::new(1.0_f32, palette.text);
    widgets.active.bg_fill = palette.selected;
    widgets.active.weak_bg_fill = palette.selected;
    widgets.active.bg_stroke = Stroke::new(1.0_f32, palette.accent);
    widgets.active.fg_stroke = Stroke::new(1.0_f32, palette.text);
    widgets.open.bg_fill = palette.hover;
    widgets.open.weak_bg_fill = palette.hover;
    widgets.open.bg_stroke = Stroke::new(1.0_f32, palette.border);
    widgets.open.fg_stroke = Stroke::new(1.0_f32, palette.text);
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
    style
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
    use super::*;

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

    /// WCAG relative luminance contrast between two colours.
    fn contrast(a: Color32, b: Color32) -> f32 {
        let luminance = |color: Color32| {
            let channel = |value: u8| {
                let value = f32::from(value) / 255.0;
                if value <= 0.039_28 {
                    value / 12.92
                } else {
                    ((value + 0.055) / 1.055).powf(2.4)
                }
            };
            0.2126 * channel(color.r()) + 0.7152 * channel(color.g()) + 0.0722 * channel(color.b())
        };
        let (light, dark) = {
            let (a, b) = (luminance(a), luminance(b));
            if a > b { (a, b) } else { (b, a) }
        };
        (light + 0.05) / (dark + 0.05)
    }

    #[test]
    fn both_palettes_keep_text_legible() {
        for palette in [&DARK, &LIGHT] {
            for surface in [
                palette.background,
                palette.stripe,
                palette.panel,
                palette.header,
            ] {
                // Body text: WCAG AA for normal text.
                assert!(contrast(palette.text, surface) >= 7.0);
                // Labels, type names, SQL keywords and errors are small text too.
                for color in [
                    palette.muted,
                    palette.accent,
                    palette.error,
                    palette.type_number,
                    palette.type_temporal,
                    palette.type_bool,
                ] {
                    assert!(
                        contrast(color, surface) >= 4.5,
                        "{color:?} on {surface:?}: {:.2}",
                        contrast(color, surface)
                    );
                }
            }
            assert!(contrast(palette.on_accent, palette.accent) >= 4.5);
            assert!(contrast(palette.text, palette.selected) >= 4.5);
        }
    }

    #[test]
    fn lists_system_fonts_for_each_platform() {
        let windows = |name: &str| (name == "WINDIR").then(|| "C:\\Windows".into());
        let files = font_files("windows", &windows);
        assert_eq!(files.len(), 1);
        assert!(
            files[0].0.ends_with("Fonts/segoeui.ttf") || files[0].0.ends_with("Fonts\\segoeui.ttf")
        );
        assert!(font_files("windows", &|_| None).is_empty());
        let none = |_: &str| None;
        assert!(font_files("macos", &none)[0].0.ends_with("Arial.ttf"));
        let linux = font_files("linux", &none);
        assert!(
            linux
                .iter()
                .all(|(regular, _)| regular.is_absolute() || regular.starts_with("/"))
        );
        assert!(
            linux
                .iter()
                .any(|(regular, _)| regular.ends_with("DejaVuSans.ttf"))
        );
        assert_eq!(font_files("freebsd", &none), linux);
    }

    #[test]
    fn the_fonts_found_here_lay_out_text() {
        // Whatever this machine has (or the built-in fonts) must draw both families.
        let ctx = egui::Context::default();
        apply(&ctx);
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.label("Regular text");
                ui.label(egui::RichText::new("Semibold").font(strong(13.0)));
            });
        });
    }
}
