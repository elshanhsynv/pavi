//! The keyboard shortcuts window (F1).

use eframe::egui::{self, RichText};

use super::{spacing::Spacing, theme};

/// Every keyboard shortcut, by group: the one list the window shows. Keep it in
/// step with `handle_global_shortcuts`, `handle_grid_keyboard` and the SQL editor.
pub(crate) const SHORTCUTS: &[(&str, &[(&str, &str)])] = &[
    (
        "Files and tabs",
        &[
            ("Ctrl+O", "Open a file"),
            ("Ctrl+Shift+O", "Open a folder as one table"),
            ("Ctrl+R", "Reload from disk"),
            ("Ctrl+T", "New tab"),
            ("Ctrl+W", "Close tab"),
            ("Ctrl+Tab / Ctrl+Shift+Tab", "Next / previous tab"),
            ("Ctrl+Shift+PgUp / PgDn", "Move the tab left / right"),
        ],
    ),
    (
        "View",
        &[
            ("Ctrl+1 / Ctrl+2", "Grid / SQL"),
            ("Ctrl+I", "Show or hide the Inspector"),
            ("Ctrl+= / Ctrl+-", "Zoom in / out"),
            ("Ctrl+0", "Normal size"),
            ("F1 or Ctrl+/", "This list"),
        ],
    ),
    (
        "Grid",
        &[
            ("Arrows", "Move the selection"),
            ("Page Up / Page Down", "Move 20 rows"),
            ("Home / End", "First / last row"),
            ("Shift+click, Shift+arrows", "Select a range"),
            ("Ctrl+C", "Copy the selection"),
            ("Ctrl+G", "Go to row"),
            ("Ctrl+F", "Filter"),
            ("Ctrl+Shift+F", "Find in the file"),
            ("F3 / Shift+F3", "Find next / previous"),
        ],
    ),
    (
        "SQL editor",
        &[
            ("Ctrl+Enter", "Run the query"),
            ("Tab or Enter", "Take the completion"),
            ("Up / Down", "Choose a completion"),
            ("Esc", "Close completions"),
        ],
    ),
];

/// Draws the shortcuts window while `open`.
pub(crate) fn window(ctx: &egui::Context, open: &mut bool) {
    if !*open {
        return;
    }
    if ctx.input_mut(|input| input.consume_key(egui::Modifiers::NONE, egui::Key::Escape)) {
        *open = false;
        return;
    }
    egui::Window::new("Keyboard shortcuts")
        .open(open)
        .collapsible(false)
        .resizable(false)
        .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .show(ctx, |ui| {
            // One grid for every group, so the actions line up; it scrolls in a
            // small window.
            egui::ScrollArea::vertical()
                .max_height((ctx.screen_rect().height() - 120.0).max(160.0))
                .show(ui, |ui| {
                    egui::Grid::new("shortcuts")
                        .num_columns(2)
                        .spacing([Spacing::Lg.px(), Spacing::Xs.px()])
                        .show(ui, |ui| {
                            for (index, (group, keys)) in SHORTCUTS.iter().enumerate() {
                                // A blank row sets the groups apart.
                                if index > 0 {
                                    ui.end_row();
                                }
                                ui.label(RichText::new(*group).strong());
                                ui.end_row();
                                for (keys, action) in *keys {
                                    ui.label(
                                        RichText::new(*keys).monospace().color(theme::accent()),
                                    );
                                    ui.label(*action);
                                    ui.end_row();
                                }
                            }
                        });
                });
        });
}
