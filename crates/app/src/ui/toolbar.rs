use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

/// Columns listed in the Sort and Columns menus; narrow the search to reach the rest.
const MENU_COLUMNS: usize = 128;
/// Below this window width the Sort, Columns and Export buttons drop their labels,
/// so the filter box keeps a usable width.
const COMPACT_BAR_WIDTH: f32 = 1_100.0;
/// Characters of the file name shown in the top bar.
const FILE_LABEL_CHARS: usize = 36;
/// Characters of a recent file's folder shown in the file menu.
const FOLDER_HINT_CHARS: usize = 26;

impl PaviApp {
    /// The one bar above the data: file, workspace, filter, tools, window controls.
    pub(in crate::app) fn show_top_bar(&mut self, ctx: &egui::Context) {
        let available = self.toolbar_availability();
        let compact = ctx.screen_rect().width() < COMPACT_BAR_WIDTH;
        let label = |text: &'static str| if compact { "" } else { text };
        egui::TopBottomPanel::top("top_bar")
            .exact_height(theme::BAR_HEIGHT)
            .frame(
                egui::Frame::new()
                    .fill(theme::PANEL)
                    .inner_margin(egui::Margin {
                        left: Spacing::Md as i8,
                        ..egui::Margin::ZERO
                    }),
            )
            .show(ctx, |ui| {
                // The bar itself moves the window; controls drawn after it sit on top.
                let drag = ui.interact(
                    ui.max_rect(),
                    ui.id().with("window_drag"),
                    egui::Sense::click_and_drag(),
                );
                if drag.drag_started() {
                    ctx.send_viewport_cmd(egui::ViewportCommand::StartDrag);
                }
                if drag.double_clicked() {
                    toggle_maximized(ctx);
                }
                ui.horizontal_centered(|ui| {
                    ui.spacing_mut().item_spacing.x = Spacing::Xs.px();
                    let (mark, _) =
                        ui.allocate_exact_size(egui::vec2(24.0, 24.0), egui::Sense::hover());
                    widgets::logo(ui, mark.center(), 1.0);
                    self.file_menu(ui);
                    widgets::divider(ui);
                    if let Some(workspace) = widgets::segmented(
                        ui,
                        self.workspace,
                        &[
                            (Workspace::Grid, "Grid", true),
                            (
                                Workspace::Sql,
                                "SQL",
                                available.enabled(ToolbarCommand::Sql),
                            ),
                        ],
                    ) {
                        self.workspace = workspace;
                    }
                    widgets::divider(ui);
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        self.window_controls(ui);
                        ui.add_space(Spacing::Sm.px());
                        self.panel_toggles(ui, available);
                        widgets::divider(ui);
                        ui.add_enabled_ui(available.enabled(ToolbarCommand::Export), |ui| {
                            widgets::menu(
                                ui,
                                "export",
                                Some(Icon::Export),
                                label("Export"),
                                false,
                                250.0,
                                |ui| {
                                    self.export_menu(ui);
                                },
                            )
                            .on_hover_text("Export or copy");
                        });
                        ui.add_enabled_ui(self.dataset.is_some(), |ui| {
                            widgets::menu(
                                ui,
                                "columns",
                                Some(Icon::Columns),
                                label("Columns"),
                                false,
                                340.0,
                                |ui| {
                                    self.columns_menu(ui);
                                },
                            )
                            .on_hover_text("Show, hide, reorder and resize columns");
                            self.sort_menu(ui, compact);
                        });
                        ui.add_space(Spacing::Sm.px());
                        self.filter_box(ui, available);
                    });
                });
            });
        if let Some(error) = &self.filter_error {
            egui::TopBottomPanel::top("filter_error")
                .frame(
                    egui::Frame::new()
                        .fill(theme::PANEL)
                        .inner_margin(Spacing::Lg.symmetric(Spacing::Sm)),
                )
                .show(ctx, |ui| {
                    widgets::error(ui, error);
                });
        }
    }

    fn file_menu(&mut self, ui: &mut egui::Ui) {
        let name = self.dataset.as_ref().map(|dataset| {
            dataset
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        });
        let label = name.as_deref().map_or_else(
            || "Open file".to_owned(),
            |name| {
                if name.chars().count() > FILE_LABEL_CHARS {
                    let short = name.chars().take(FILE_LABEL_CHARS).collect::<String>();
                    format!("{short}…")
                } else {
                    name.to_owned()
                }
            },
        );
        let mut open = None;
        let mut browse = false;
        let mut refresh = false;
        let response = widgets::menu(ui, "file", Some(Icon::Folder), &label, false, 420.0, |ui| {
            let path_box = ui.add(
                egui::TextEdit::singleline(&mut self.path_input)
                    .desired_width(f32::INFINITY)
                    .hint_text("Paste a file path and press Enter"),
            );
            if path_box.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)) {
                let path = self.path_input.trim();
                if !path.is_empty() {
                    open = Some(PathBuf::from(path));
                }
            }
            ui.add_space(Spacing::Xs.px());
            browse = widgets::menu_item(ui, "Browse…", "Ctrl+O").clicked();
            if let Some(dataset) = &self.dataset {
                refresh = widgets::menu_item(ui, "Reload from disk", "Ctrl+R").clicked();
                if widgets::menu_item(ui, "Copy file path", "").clicked() {
                    ui.ctx().copy_text(dataset.path.display().to_string());
                    widgets::close_menu(ui);
                }
            }
            if !self.session.recent_files.is_empty() {
                ui.separator();
                widgets::menu_caption(ui, "Recent");
                for path in &self.session.recent_files {
                    let file = path.file_name().unwrap_or_default().to_string_lossy();
                    let folder = path
                        .parent()
                        .map(|parent| tail(&parent.display().to_string(), FOLDER_HINT_CHARS))
                        .unwrap_or_default();
                    if widgets::menu_item(ui, &file, &folder)
                        .on_hover_text(path.display().to_string())
                        .clicked()
                    {
                        open = Some(path.clone());
                    }
                }
            }
            if open.is_some() || browse || refresh {
                widgets::close_menu(ui);
            }
        });
        if let Some(dataset) = &self.dataset {
            response.on_hover_text(dataset.path.display().to_string());
        }
        if browse {
            self.pick_file();
        }
        if refresh {
            self.refresh_document();
        }
        if let Some(path) = open {
            self.begin_open(path);
        }
    }

    fn filter_box(&mut self, ui: &mut egui::Ui, available: ToolbarAvailability) {
        let filtering = self
            .filtered
            .as_ref()
            .is_some_and(|filtered| filtered.kind == QueryKind::Filter);
        let clearable = filtering || !self.filter_input.is_empty();
        // The text edit reports its inner rect; the margins hold the icon and clear button.
        const LEFT: i8 = 28;
        const RIGHT: i8 = 26;
        let width = ui.available_width().clamp(120.0, 620.0) - f32::from(LEFT + RIGHT);
        let response = ui.add_enabled(
            available.enabled(ToolbarCommand::Filter),
            egui::TextEdit::singleline(&mut self.filter_input)
                .id(egui::Id::new("filter_search"))
                .desired_width(width)
                .min_size(egui::vec2(width, theme::CONTROL_HEIGHT))
                .vertical_align(Align::Center)
                .margin(egui::Margin {
                    left: LEFT,
                    right: RIGHT,
                    top: 0,
                    bottom: 0,
                })
                .hint_text("Filter rows, e.g. amount > 100"),
        );
        let rect = egui::Rect::from_min_max(
            response.rect.min - egui::vec2(f32::from(LEFT), 0.0),
            response.rect.max + egui::vec2(f32::from(RIGHT), 0.0),
        );
        widgets::icon(
            ui,
            egui::pos2(rect.left() + 15.0, rect.center().y),
            Icon::Search,
            if filtering {
                theme::ACCENT
            } else {
                theme::MUTED
            },
        );
        if filtering {
            ui.painter().rect_stroke(
                rect,
                theme::RADIUS,
                egui::Stroke::new(1.0_f32, theme::ACCENT),
                egui::StrokeKind::Inside,
            );
        }
        if response.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)) {
            if self.filter_input.trim().is_empty() {
                self.clear_filter();
            } else {
                self.apply_filter();
            }
        }
        if clearable {
            let clear = egui::Rect::from_center_size(
                egui::pos2(rect.right() - 14.0, rect.center().y),
                egui::vec2(20.0, 20.0),
            );
            let clear_response = ui
                .interact(clear, ui.id().with("clear_filter"), egui::Sense::click())
                .on_hover_text("Clear filter");
            if clear_response.hovered() {
                ui.painter().rect_filled(clear, theme::RADIUS, theme::HOVER);
            }
            ui.painter().text(
                clear.center(),
                egui::Align2::CENTER_CENTER,
                "×",
                theme::body(),
                if clear_response.hovered() {
                    theme::TEXT
                } else {
                    theme::MUTED
                },
            );
            if clear_response.clicked() {
                self.clear_filter();
                self.filter_input.clear();
            }
        }
        response.on_hover_text(
            "column == value, !=, >, >=, <, <=, contains, LIKE a%\n\
             column IN (a, b), BETWEEN a AND b, IS NULL; NOT IN / NOT LIKE / IS NOT NULL\n\
             Join tests with AND and OR; group them with brackets\n\
             Enter applies the filter (Ctrl+F)",
        );
    }

    fn sort_menu(&mut self, ui: &mut egui::Ui, compact: bool) {
        let names = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
            .unwrap_or_default();
        let sorted = self
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.sort)
            .and_then(|sort| {
                names
                    .get(sort.column)
                    .map(|name| self.sort_heading(sort.column, name))
            });
        let hint = sorted
            .as_ref()
            .map_or_else(|| "Sort".to_owned(), |sorted| format!("Sorted by {sorted}"));
        let label = if compact {
            ""
        } else {
            sorted.as_deref().unwrap_or("Sort")
        };
        let mut toggle = None;
        let mut clear = false;
        widgets::menu(
            ui,
            "sort",
            Some(Icon::Sort),
            label,
            sorted.is_some(),
            260.0,
            |ui| {
                if sorted.is_some() {
                    clear = widgets::menu_item(ui, "Back to file order", "").clicked();
                    ui.separator();
                }
                egui::ScrollArea::vertical()
                    .max_height(320.0)
                    .show(ui, |ui| {
                        for (column, name) in names.iter().enumerate().take(MENU_COLUMNS) {
                            if widgets::menu_item(ui, &self.sort_heading(column, name), "")
                                .clicked()
                            {
                                toggle = Some(column);
                            }
                        }
                    });
                if names.len() > MENU_COLUMNS {
                    widgets::menu_caption(
                        ui,
                        &format!(
                            "First {MENU_COLUMNS} columns. Right-click a grid header to sort any column."
                        ),
                    );
                }
                if clear || toggle.is_some() {
                    widgets::close_menu(ui);
                }
            },
        )
        .on_hover_text(hint);
        if clear {
            self.clear_filter();
        }
        if let Some(column) = toggle {
            self.toggle_sort(column);
        }
    }

    fn columns_menu(&mut self, ui: &mut egui::Ui) {
        let entries = self.dataset.as_ref().map_or_else(Vec::new, |dataset| {
            let search = self.column_search.to_lowercase();
            dataset
                .column_names
                .iter()
                .enumerate()
                .filter(|(_, name)| search.is_empty() || name.to_lowercase().contains(&search))
                .take(MENU_COLUMNS)
                .map(|(source, name)| (source, name.clone()))
                .collect::<Vec<_>>()
        });
        let mut toggle = None;
        let mut move_column = None;
        let mut resize = None;
        let mut fit = None;
        let mut reset = false;
        let mut unpin = false;
        ui.horizontal(|ui| {
            ui.checkbox(&mut self.show_column_types, "Show types");
            ui.add_space(Spacing::Md.px());
            ui.checkbox(&mut self.freeze_header, "Pin header");
            let pinned = self.column_layout.pinned();
            if pinned > 0 {
                ui.add_space(Spacing::Md.px());
                unpin = ui
                    .small_button(format!(
                        "Unpin {pinned} column{}",
                        if pinned == 1 { "" } else { "s" }
                    ))
                    .on_hover_text("Pin columns from a header's right-click menu")
                    .clicked();
            }
        });
        if unpin {
            self.header_action = Some(HeaderAction::Pin(0));
        }
        ui.separator();
        ui.horizontal(|ui| {
            if ui
                .button("Reset")
                .on_hover_text("Show every column in file order")
                .clicked()
            {
                reset = true;
            }
            ui.add(
                egui::TextEdit::singleline(&mut self.column_search)
                    .desired_width(f32::INFINITY)
                    .hint_text("Find a column"),
            );
        });
        if entries.is_empty() {
            widgets::menu_caption(ui, "No column matches.");
        }
        egui::ScrollArea::vertical()
            .id_salt("column_layout")
            .max_height(320.0)
            .show(ui, |ui| {
                for (source, name) in &entries {
                    ui.horizontal(|ui| {
                        let mut shown = !self.column_layout.is_hidden(*source);
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            if ui
                                .small_button("Fit")
                                .on_hover_text("Fit width to loaded values")
                                .clicked()
                            {
                                fit = Some(*source);
                            }
                            let mut width = self.column_layout.width(*source, INITIAL_COLUMN_WIDTH);
                            if ui
                                .add(
                                    egui::DragValue::new(&mut width)
                                        .range(72.0..=480.0)
                                        .speed(2.0)
                                        .suffix(" px"),
                                )
                                .on_hover_text("Column width")
                                .changed()
                            {
                                resize = Some((*source, width));
                            }
                            for (glyph, direction, hint) in [
                                (Icon::ArrowDown, 1, "Move later"),
                                (Icon::ArrowUp, -1, "Move earlier"),
                            ] {
                                if widgets::tool_button(ui, Some(glyph), "", false)
                                    .on_hover_text(hint)
                                    .clicked()
                                {
                                    move_column = Some((*source, direction));
                                }
                            }
                            ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                                if ui
                                    .add(egui::Checkbox::new(&mut shown, egui::RichText::new(name)))
                                    .changed()
                                {
                                    toggle = Some((*source, shown));
                                }
                            });
                        });
                    });
                }
            });
        if entries.len() == MENU_COLUMNS {
            widgets::menu_caption(
                ui,
                &format!("First {MENU_COLUMNS} matches. Narrow the search."),
            );
        }
        if let Some((source, shown)) = toggle {
            self.column_layout.set_hidden(source, !shown);
            self.apply_column_layout_change();
        }
        if let Some((source, direction)) = move_column
            && self.column_layout.move_source(source, direction)
        {
            self.apply_column_layout_change();
        }
        if let Some((source, width)) = resize {
            self.column_layout.set_width(source, width);
            self.save_column_layout();
        }
        if let Some(source) = fit {
            self.auto_fit_column(source);
        }
        if reset {
            self.reset_column_layout();
        }
    }

    fn export_menu(&mut self, ui: &mut egui::Ui) {
        let has_row = self.selected_batch_row().is_some();
        for (format, label, selected) in [
            (ExportFormat::Csv, "Result as CSV…", false),
            (ExportFormat::Parquet, "Result as Parquet…", false),
            (ExportFormat::Csv, "Selected row as CSV…", true),
            (ExportFormat::Parquet, "Selected row as Parquet…", true),
        ] {
            if ui
                .add_enabled_ui(!selected || has_row, |ui| widgets::menu_item(ui, label, ""))
                .inner
                .clicked()
            {
                self.start_export(format, selected);
                widgets::close_menu(ui);
            }
        }
        ui.separator();
        for (label, hint, row) in [("Copy cell", "Ctrl+C", false), ("Copy row", "", true)] {
            if ui
                .add_enabled_ui(has_row, |ui| widgets::menu_item(ui, label, hint))
                .inner
                .clicked()
            {
                self.copy_selection(ui.ctx(), row);
                widgets::close_menu(ui);
            }
        }
        if self.export.is_running() {
            ui.separator();
            if widgets::menu_item(ui, "Cancel export", "").clicked() {
                self.cancel_export();
                widgets::close_menu(ui);
            }
        }
    }

    /// Refresh, Charts and Inspector, laid out right to left.
    fn panel_toggles(&mut self, ui: &mut egui::Ui, available: ToolbarAvailability) {
        if ui
            .add_enabled_ui(available.enabled(ToolbarCommand::Inspector), |ui| {
                widgets::tool_button(ui, Some(Icon::Panel), "", self.show_inspector)
            })
            .inner
            .on_hover_text("Inspector (Ctrl+I)")
            .clicked()
        {
            self.show_inspector = !self.show_inspector;
            self.persist_session();
        }
        if ui
            .add_enabled_ui(available.enabled(ToolbarCommand::Charts), |ui| {
                widgets::tool_button(ui, Some(Icon::Chart), "", self.chart.visible)
            })
            .inner
            .on_hover_text("Charts")
            .clicked()
        {
            self.chart.visible = !self.chart.visible;
            self.persist_session();
        }
        if ui
            .add_enabled_ui(available.enabled(ToolbarCommand::Refresh), |ui| {
                widgets::tool_button(ui, Some(Icon::Refresh), "", false)
            })
            .inner
            .on_hover_text("Reload from disk (Ctrl+R)")
            .clicked()
        {
            self.refresh_document();
        }
    }

    /// Close, maximize and minimize, laid out right to left.
    fn window_controls(&mut self, ui: &mut egui::Ui) {
        let ctx = ui.ctx().clone();
        ui.spacing_mut().item_spacing.x = 0.0;
        if widgets::window_button(ui, Icon::Close, "Close", true).clicked() {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        if widgets::window_button(ui, Icon::Maximize, "Maximize or restore", false).clicked() {
            toggle_maximized(&ctx);
        }
        if widgets::window_button(ui, Icon::Minimize, "Minimize", false).clicked() {
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
        }
        ui.spacing_mut().item_spacing.x = Spacing::Xs.px();
    }

    pub(in crate::app) fn pick_file(&mut self) {
        if let Some(path) = rfd::FileDialog::new()
            .add_filter("Parquet", &["parquet"])
            .pick_file()
        {
            self.begin_open(path);
        }
    }

    pub(in crate::app) fn jump_to_input(&mut self) {
        match self.jump_row_input.trim().parse::<u64>() {
            Ok(one_based) if one_based > 0 => {
                let row = one_based - 1;
                if let Some(filtered) = &self.filtered {
                    if row < filtered.first_result
                        || row >= filtered.first_result.saturating_add(filtered.rows)
                    {
                        self.status =
                            "That result row is outside the current bounded window".to_string();
                        return;
                    }
                    self.grid.select_filtered(
                        filtered.first_result,
                        row - filtered.first_result,
                        0,
                    );
                } else if row < self.grid.rows {
                    self.grid.jump_to_row(row);
                } else {
                    self.status = "Enter a row number within the current result set".to_string();
                    return;
                }
                self.jump_target = Some(row);
                if self.filtered.is_none()
                    && let Some(page) = self.grid.page_for_row(row)
                {
                    self.request_page(page);
                }
                self.status = format!("Jumped to row {one_based}");
            }
            _ => self.status = "Enter a row number within the current result set".to_string(),
        }
    }
}

/// The last `max_chars` characters of `text`, with "…" in front when it was cut.
/// The end of a folder path tells recent files apart; the start rarely does.
fn tail(text: &str, max_chars: usize) -> String {
    let count = text.chars().count();
    if count <= max_chars {
        return text.to_owned();
    }
    let kept = text.chars().skip(count - max_chars).collect::<String>();
    format!("…{kept}")
}

/// Strips along the window edges and corners that resize the window. PAVI draws
/// its own title bar, so the window has no border of its own to drag.
pub(in crate::app) fn window_resize_handles(ctx: &egui::Context) {
    use egui::{CursorIcon, ResizeDirection};
    let (maximized, fullscreen) = ctx.input(|input| {
        let viewport = input.viewport();
        (
            viewport.maximized.unwrap_or(false),
            viewport.fullscreen.unwrap_or(false),
        )
    });
    if maximized || fullscreen {
        return;
    }
    let screen = ctx.screen_rect();
    let edge = theme::RESIZE_BORDER;
    let corner = edge * 3.0;
    let (left, right, top, bottom) = (screen.left(), screen.right(), screen.top(), screen.bottom());
    let strip = |x: std::ops::RangeInclusive<f32>, y: std::ops::RangeInclusive<f32>| {
        egui::Rect::from_x_y_ranges(x, y)
    };
    let handles = [
        (
            ResizeDirection::North,
            CursorIcon::ResizeNorth,
            strip(left + corner..=right - corner, top..=top + edge),
        ),
        (
            ResizeDirection::South,
            CursorIcon::ResizeSouth,
            strip(left + corner..=right - corner, bottom - edge..=bottom),
        ),
        (
            ResizeDirection::West,
            CursorIcon::ResizeWest,
            strip(left..=left + edge, top + corner..=bottom - corner),
        ),
        (
            ResizeDirection::East,
            CursorIcon::ResizeEast,
            strip(right - edge..=right, top + corner..=bottom - corner),
        ),
        (
            ResizeDirection::NorthWest,
            CursorIcon::ResizeNorthWest,
            strip(left..=left + corner, top..=top + edge),
        ),
        (
            ResizeDirection::NorthEast,
            CursorIcon::ResizeNorthEast,
            strip(right - corner..=right, top..=top + edge),
        ),
        (
            ResizeDirection::SouthWest,
            CursorIcon::ResizeSouthWest,
            strip(left..=left + corner, bottom - edge..=bottom),
        ),
        (
            ResizeDirection::SouthEast,
            CursorIcon::ResizeSouthEast,
            strip(right - corner..=right, bottom - edge..=bottom),
        ),
    ];
    for (direction, cursor, rect) in handles {
        egui::Area::new(egui::Id::new(("window_resize", format!("{direction:?}"))))
            .order(egui::Order::Foreground)
            .fixed_pos(rect.min)
            .movable(false)
            .constrain(false)
            .show(ctx, |ui| {
                let (_, response) = ui.allocate_exact_size(rect.size(), egui::Sense::drag());
                if response.hovered() || response.dragged() {
                    ctx.set_cursor_icon(cursor);
                }
                if response.drag_started() {
                    ctx.send_viewport_cmd(egui::ViewportCommand::BeginResize(direction));
                }
            });
    }
}

fn toggle_maximized(ctx: &egui::Context) {
    ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(
        !ctx.input(|i| i.viewport().maximized.unwrap_or(false)),
    ));
}
