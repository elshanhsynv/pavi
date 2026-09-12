use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

impl PaviApp {
    pub(in crate::app) fn show_top_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("title_bar")
            .exact_height(52.0)
            .frame(
                egui::Frame::new()
                    .fill(theme::PANEL)
                    .inner_margin(Spacing::Xl.symmetric(Spacing::None)),
            )
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    let (logo, _) =
                        ui.allocate_exact_size(egui::vec2(32.0, 32.0), egui::Sense::hover());
                    widgets::icon(ui, logo.center(), Icon::Database, theme::ACCENT);
                    ui.vertical(|ui| {
                        ui.label(RichText::new("PAVI").size(18.0).strong());
                        ui.label(RichText::new("Parquet Viewer").small().color(theme::MUTED));
                    });
                    let (_, drag) = ui.allocate_exact_size(
                        egui::vec2((ui.available_width() - 144.0).max(0.0), 44.0),
                        egui::Sense::click_and_drag(),
                    );
                    if drag.drag_started() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::StartDrag);
                    }
                    if drag.double_clicked() {
                        toggle_maximized(ctx);
                    }
                    if ui
                        .add_sized([40.0, 32.0], egui::Button::new("−").frame(false))
                        .on_hover_text("Minimize")
                        .clicked()
                    {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                    }
                    if ui
                        .add_sized([40.0, 32.0], egui::Button::new("□").frame(false))
                        .on_hover_text("Maximize / restore")
                        .clicked()
                    {
                        toggle_maximized(ctx);
                    }
                    if ui
                        .add_sized([40.0, 32.0], egui::Button::new("×").frame(false))
                        .on_hover_text("Close")
                        .clicked()
                    {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
        let available = self.toolbar_availability();
        egui::TopBottomPanel::top("toolbar").exact_height(56.0)
            .frame(egui::Frame::new().fill(theme::PANEL).inner_margin(Spacing::Lg.symmetric(Spacing::Md)).stroke(egui::Stroke::new(1.0_f32,theme::BORDER)))
            .show(ctx, |ui| {
                egui::ScrollArea::horizontal().id_salt("toolbar_scroll").show(ui, |ui| {
                    ui.horizontal(|ui| {
                        widgets::menu(ui,"Open File…",Icon::Folder,true,|ui| {
                            if ui.button("Browse…  Ctrl+O").clicked() { self.pick_file(); ui.close_menu(); }
                            ui.separator();
                            let mut recent = None;
                            for path in &self.session.recent_files {
                                if ui.button(path.display().to_string()).clicked() { recent = Some(path.clone()); }
                            }
                            if let Some(path) = recent { self.begin_open(path); ui.close_menu(); }
                            ui.separator();
                            if ui.button("Query history / column layout…").clicked() { self.show_tools = true; ui.close_menu(); }
                        });
                        if ui.add_enabled_ui(available.enabled(ToolbarCommand::Refresh),|ui| widgets::button(ui,"Refresh",Icon::Refresh,false,egui::vec2(94.0,36.0))).inner.on_hover_text("Refresh (Ctrl+R)").clicked() {self.refresh_document();}
                        widgets::menu(ui,"Export",Icon::Export,false,|ui| self.export_menu(ui));
                        let search = ui.add_sized([(ctx.screen_rect().width()-1170.0).clamp(160.0,300.0),36.0],
                            egui::TextEdit::singleline(&mut self.filter_input).id(egui::Id::new("filter_search")).margin(egui::Margin {left:32,..Spacing::Md.margin()}).hint_text("Search / filter (Ctrl+F)…"));
                        widgets::icon(ui,search.rect.left_center()-Spacing::Xl.vec(Spacing::None),Icon::Search,theme::ACCENT);
                        search.clone().on_hover_text("Enter a filter expression, e.g. id >= 100 or label contains dummy. Press Enter to apply.");
                        if search.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) { self.apply_filter(); }
                        widgets::menu(ui,"Filter",Icon::Filter,self.filtered.as_ref().is_some_and(|f|f.kind==QueryKind::Filter),|ui| {
                            if ui.add_enabled(available.enabled(ToolbarCommand::Filter),egui::Button::new("Apply filter")).clicked() {self.apply_filter(); ui.close_menu();}
                            if ui.button("Clear filter").clicked() {self.clear_filter(); self.filter_input.clear(); ui.close_menu();}
                            ui.label("column == value, !=, contains, >, >=, <, <=");
                        });
                        widgets::menu(ui,"Sort",Icon::Sort,self.filtered.as_ref().is_some_and(|f|f.sort.is_some()),|ui| {
                            ui.label("Choose a column; select again to reverse order.");
                            let names = self.dataset.as_ref().map(|d|d.column_names.clone()).unwrap_or_default();
                            egui::ScrollArea::vertical().max_height(260.0).show(ui,|ui| {
                                for (column,name) in names.iter().enumerate().take(128) {
                                    if ui.button(self.sort_heading(column,name)).clicked() {self.toggle_sort(column);ui.close_menu();}
                                }
                            });
                            if self.filtered.as_ref().is_some_and(|f|f.sort.is_some()) && ui.button("Return to source order").clicked() {self.clear_filter();ui.close_menu();}
                        });
                        if widgets::button(ui,"Column Types",Icon::Grid,self.show_column_types,egui::vec2(122.0,36.0)).clicked() {self.show_column_types = !self.show_column_types;}
                        if widgets::button(ui,"Freeze Header",Icon::Freeze,self.freeze_header,egui::vec2(124.0,36.0)).clicked() {self.freeze_header = !self.freeze_header;}
                        if ui.add_enabled_ui(available.enabled(ToolbarCommand::Inspector),|ui| widgets::button(ui,"Inspector",Icon::File,self.show_inspector,egui::vec2(102.0,36.0))).inner.on_hover_text("Inspector (Ctrl+I)").clicked() {self.show_inspector = !self.show_inspector;self.persist_session();}
                        if ui.add_enabled_ui(available.enabled(ToolbarCommand::Charts),|ui| widgets::button(ui,"Charts",Icon::Chart,self.chart.visible,egui::vec2(82.0,36.0))).inner.clicked() {self.chart.visible = !self.chart.visible; self.persist_session();}
                        if ui.add_enabled_ui(available.enabled(ToolbarCommand::Sql),|ui| widgets::button(ui,if self.workspace==Workspace::Sql {"Grid"} else {"SQL"},Icon::Sql,self.workspace==Workspace::Sql,egui::vec2(68.0,36.0))).inner.on_hover_text("SQL / Grid (Ctrl+2 / Ctrl+1)").clicked() {self.workspace = if self.workspace==Workspace::Sql {Workspace::Grid} else {Workspace::Sql};}
                    });
                });
            });
        self.show_dataset_bar(ctx);
        if let Some(error) = &self.filter_error {
            egui::TopBottomPanel::top("filter_error").show(ctx, |ui| {
                ui.colored_label(egui::Color32::LIGHT_RED, error);
            });
        }
    }

    fn pick_file(&mut self) {
        if let Some(path) = rfd::FileDialog::new()
            .add_filter("Parquet", &["parquet"])
            .pick_file()
        {
            self.begin_open(path);
        }
    }

    fn export_menu(&mut self, ui: &mut egui::Ui) {
        for (format, label, selected) in [
            (ExportFormat::Csv, "Current result as CSV…", false),
            (ExportFormat::Parquet, "Current result as Parquet…", false),
            (ExportFormat::Csv, "Selected row as CSV…", true),
            (ExportFormat::Parquet, "Selected row as Parquet…", true),
        ] {
            let enabled = if selected {
                self.selected_batch_row().is_some()
            } else {
                self.toolbar_availability().enabled(ToolbarCommand::Export)
            };
            if ui.add_enabled(enabled, egui::Button::new(label)).clicked() {
                self.start_export(format, selected);
                ui.close_menu();
            }
        }
        ui.separator();
        for (label, row) in [("Copy cell", false), ("Copy row", true)] {
            if ui
                .add_enabled(
                    self.selected_batch_row().is_some(),
                    egui::Button::new(label),
                )
                .clicked()
            {
                self.copy_selection(ui.ctx(), row);
                ui.close_menu();
            }
        }
        if self.export.is_some() && ui.button("Cancel export").clicked() {
            self.cancel_export();
        }
    }
}

fn toggle_maximized(ctx: &egui::Context) {
    ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(
        !ctx.input(|i| i.viewport().maximized.unwrap_or(false)),
    ));
}
