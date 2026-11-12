// Release builds are GUI-only on Windows: no console window next to the app.
// Debug builds keep the console for diagnostics.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod chart;
mod compare;
mod export;
mod find;
mod inspector;
mod pages;
mod profile;
mod results;
mod session;
mod sql_editor;
mod state;

use eframe::egui;
use std::path::PathBuf;

fn main() -> eframe::Result {
    let initial_path = std::env::args_os().nth(1).map(PathBuf::from);
    let options = eframe::NativeOptions {
        centered: true,
        viewport: egui::ViewportBuilder::default()
            .with_decorations(app::native_window_frame())
            .with_inner_size([1_448.0, 900.0])
            .with_min_inner_size([900.0, 600.0]),
        ..Default::default()
    };
    eframe::run_native(
        "PAVI",
        options,
        Box::new(move |creation| {
            app::apply_theme(&creation.egui_ctx);
            Ok(Box::new(app::PaviApp::new(initial_path)))
        }),
    )
}
