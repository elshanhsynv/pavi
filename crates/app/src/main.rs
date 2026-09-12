mod app;
mod chart;
mod export;
mod inspector;
mod profile;
mod session;
mod state;

use eframe::egui;
use std::path::PathBuf;

fn main() -> eframe::Result {
    let initial_path = std::env::args_os().nth(1).map(PathBuf::from);
    let options = eframe::NativeOptions {
        centered: true,
        viewport: egui::ViewportBuilder::default()
            .with_decorations(false)
            .with_inner_size([1_448.0, 900.0])
            .with_min_inner_size([900.0, 600.0]),
        ..Default::default()
    };
    eframe::run_native(
        "PAVI",
        options,
        Box::new(move |_| Ok(Box::new(app::PaviApp::new(initial_path)))),
    )
}
