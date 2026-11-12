use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

use crate::sql_editor::{self, TokenKind};

/// Lines the SQL editor shows before it scrolls.
const EDITOR_ROWS: usize = 6;
/// Width of the completion list under the cursor.
const COMPLETION_WIDTH: f32 = 260.0;
/// Width of the Saved menu.
const SAVED_MENU_WIDTH: f32 = 420.0;

/// Id of the SQL editor, for its cursor and keyboard focus.
pub fn sql_editor_id() -> egui::Id {
    egui::Id::new("sql_editor")
}

impl PaviApp {
    pub(in crate::app) fn show_sql_workspace(&mut self, ui: &mut egui::Ui) {
        let mut run = false;
        ui.spacing_mut().item_spacing.y = 0.0;
        egui::Frame::new()
            .fill(theme::PANEL)
            .inner_margin(Spacing::Md.margin())
            .show(ui, |ui| {
                let editor = self.sql_editor(ui);
                run = editor.has_focus()
                    && ui.input(|input| {
                        input.modifiers.command && input.key_pressed(egui::Key::Enter)
                    });
                ui.add_space(Spacing::Xs.px());
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled_ui(self.dataset.is_some(), |ui| {
                            widgets::primary_button(ui, Some(Icon::Play), "Run")
                        })
                        .inner
                        .on_hover_text("Run the query (Ctrl+Enter in the editor)")
                        .clicked()
                    {
                        run = true;
                    }
                    if self.running_sql.is_some() {
                        if widgets::tool_button(ui, Some(Icon::Close), "Cancel", false).clicked() {
                            self.cancel_sql();
                        }
                        ui.add(egui::Spinner::new().size(12.0).color(theme::ACCENT));
                        ui.label(RichText::new("Running…").color(theme::MUTED));
                    }
                    self.history_menu(ui);
                    self.saved_menu(ui);
                    if let Some(error) = &self.sql_error {
                        widgets::error(ui, error);
                    } else if self.running_sql.is_none() {
                        ui.label(
                            RichText::new("The table is always named dataset.")
                                .font(theme::small())
                                .color(theme::MUTED),
                        );
                    }
                });
            });
        if run {
            self.run_sql();
        }
        self.show_grid(ui);
    }

    /// The editor: highlighted SQL, with completions for column names and
    /// keywords under the cursor (↑/↓ choose, Tab or Enter take, Esc closes).
    fn sql_editor(&mut self, ui: &mut egui::Ui) -> egui::Response {
        let id = sql_editor_id();
        let columns = self
            .dataset
            .as_ref()
            .map(|dataset| dataset.column_names.clone())
            .unwrap_or_default();
        // Keys for the list are taken before the editor sees them.
        let mut accept = None;
        if let Some(completion) = &mut self.sql_completion
            && ui.memory(|memory| memory.has_focus(id))
        {
            let count = completion.items.len();
            ui.input_mut(|input| {
                if input.consume_key(egui::Modifiers::NONE, egui::Key::ArrowDown) {
                    completion.selected = (completion.selected + 1) % count;
                }
                if input.consume_key(egui::Modifiers::NONE, egui::Key::ArrowUp) {
                    completion.selected = (completion.selected + count - 1) % count;
                }
                if input.consume_key(egui::Modifiers::NONE, egui::Key::Tab)
                    || input.consume_key(egui::Modifiers::NONE, egui::Key::Enter)
                {
                    accept = Some(completion.selected);
                }
            });
            if ui.input_mut(|input| input.consume_key(egui::Modifiers::NONE, egui::Key::Escape)) {
                self.sql_completion = None;
            }
        }
        if let Some(index) = accept {
            self.accept_completion(ui.ctx(), index);
        }

        let font = egui::TextStyle::Monospace.resolve(ui.style());
        let mut layouter = |ui: &egui::Ui, text: &str, wrap_width: f32| {
            let mut job = highlight(text, &columns, font.clone());
            job.wrap.max_width = wrap_width;
            ui.fonts(|fonts| fonts.layout_job(job))
        };
        let output = egui::TextEdit::multiline(&mut self.sql_input)
            .id(id)
            .code_editor()
            .desired_rows(EDITOR_ROWS)
            .desired_width(f32::INFINITY)
            .margin(Spacing::Md.margin())
            .layouter(&mut layouter)
            .hint_text("SELECT column, COUNT(*) FROM dataset WHERE … GROUP BY column LIMIT 100")
            .show(ui);
        let cursor = output.cursor_range.map(|range| range.primary.ccursor.index);
        if !output.response.has_focus() {
            self.sql_completion = None;
        } else if output.response.changed() {
            self.sql_completion = cursor.and_then(|cursor| {
                let (start, word) = sql_editor::word_before(&self.sql_input, cursor)?;
                let items = sql_editor::suggestions(&word, &columns);
                (!items.is_empty()).then_some(SqlCompletion {
                    start,
                    cursor,
                    items,
                    selected: 0,
                })
            });
        } else if self
            .sql_completion
            .as_ref()
            .is_some_and(|completion| Some(completion.cursor) != cursor)
        {
            // The cursor moved away from the word.
            self.sql_completion = None;
        }

        // The list, just under the cursor.
        if let (Some(completion), Some(range)) = (&self.sql_completion, output.cursor_range) {
            let at = output.galley_pos
                + output
                    .galley
                    .pos_from_cursor(&range.primary)
                    .left_bottom()
                    .to_vec2();
            let mut clicked = None;
            egui::Area::new(id.with("completion"))
                .order(egui::Order::Foreground)
                .fixed_pos(at + egui::vec2(0.0, Spacing::Xs.px()))
                .show(ui.ctx(), |ui| {
                    egui::Frame::popup(ui.style()).show(ui, |ui| {
                        ui.set_width(COMPLETION_WIDTH);
                        for (index, item) in completion.items.iter().enumerate() {
                            let kind = match item.kind {
                                TokenKind::Column => "column",
                                TokenKind::Function => "function",
                                TokenKind::Table => "table",
                                _ => "keyword",
                            };
                            let row = widgets::menu_item(ui, &item.insert, kind);
                            if index == completion.selected {
                                ui.painter().rect_stroke(
                                    row.rect,
                                    theme::RADIUS,
                                    egui::Stroke::new(1.0_f32, theme::ACCENT),
                                    egui::StrokeKind::Inside,
                                );
                            }
                            if row.clicked() {
                                clicked = Some(index);
                            }
                        }
                    });
                });
            if let Some(index) = clicked {
                self.accept_completion(ui.ctx(), index);
                output.response.request_focus();
            }
        }
        output.response
    }

    /// Replaces the word being typed with completion `index` and puts the cursor
    /// after it.
    pub(in crate::app) fn accept_completion(&mut self, ctx: &egui::Context, index: usize) {
        let Some(completion) = self.sql_completion.take() else {
            return;
        };
        let Some(item) = completion.items.get(index) else {
            return;
        };
        let (text, cursor) = sql_editor::complete(
            &self.sql_input,
            completion.start,
            completion.cursor,
            &item.insert,
        );
        self.sql_input = text;
        let id = sql_editor_id();
        let mut state = egui::TextEdit::load_state(ctx, id).unwrap_or_default();
        state
            .cursor
            .set_char_range(Some(egui::text::CCursorRange::one(
                egui::text::CCursor::new(cursor),
            )));
        state.store(ctx, id);
    }

    /// Queries saved under a name: save the editor's query, load or delete one.
    fn saved_menu(&mut self, ui: &mut egui::Ui) {
        let mut load = None;
        let mut remove = None;
        let mut save = false;
        widgets::menu(
            ui,
            "saved_queries",
            Some(Icon::Bookmark),
            "Saved",
            false,
            SAVED_MENU_WIDTH,
            |ui| {
                ui.horizontal(|ui| {
                    let name = ui.add(
                        egui::TextEdit::singleline(&mut self.saved_query_name)
                            .desired_width(SAVED_MENU_WIDTH - 140.0)
                            .hint_text("Name for the query above"),
                    );
                    let enter =
                        name.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter));
                    save = widgets::tool_button(ui, Some(Icon::Bookmark), "Save", false)
                        .on_hover_text(
                            "Save the editor's query under this name; the same name replaces it",
                        )
                        .clicked()
                        || enter;
                });
                if self.session.saved_queries.is_empty() {
                    widgets::menu_caption(ui, "Queries you save are kept here.");
                    return;
                }
                ui.separator();
                egui::ScrollArea::vertical()
                    .max_height(360.0)
                    .show(ui, |ui| {
                        for (index, saved) in self.session.saved_queries.iter().enumerate() {
                            ui.horizontal(|ui| {
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    if widgets::tool_button(ui, Some(Icon::Close), "", false)
                                        .on_hover_text("Delete this saved query")
                                        .clicked()
                                    {
                                        remove = Some(index);
                                    }
                                    ui.with_layout(Layout::top_down_justified(Align::Min), |ui| {
                                        let first_line =
                                            saved.sql.lines().next().unwrap_or_default();
                                        if widgets::menu_item(ui, &saved.name, first_line)
                                            .on_hover_text(&saved.sql)
                                            .clicked()
                                        {
                                            load = Some(index);
                                        }
                                    });
                                });
                            });
                        }
                    });
                if load.is_some() {
                    widgets::close_menu(ui);
                }
            },
        );
        if save {
            match self
                .session
                .save_query(&self.saved_query_name, &self.sql_input)
            {
                Ok(name) => {
                    self.status = format!("Saved the query as \"{name}\"");
                    self.saved_query_name.clear();
                    self.persist_session();
                }
                Err(reason) => self.status = reason,
            }
        }
        if let Some(saved) = load.and_then(|index| self.session.saved_queries.get(index)) {
            // Loading does not run it: the user may want to edit it first.
            self.sql_input = saved.sql.clone();
            self.saved_query_name = saved.name.clone();
            self.status = format!("Loaded \"{}\"; Ctrl+Enter runs it", saved.name);
        }
        if let Some(index) = remove {
            self.session.remove_saved_query(index);
            self.persist_session();
        }
    }

    fn history_menu(&mut self, ui: &mut egui::Ui) {
        let mut rerun = None;
        let mut remove = None;
        let mut clear = false;
        widgets::menu(
            ui,
            "history",
            Some(Icon::History),
            "History",
            false,
            460.0,
            |ui| {
                if self.session.query_history.is_empty() {
                    widgets::menu_caption(ui, "Queries you run are kept here.");
                    return;
                }
                egui::ScrollArea::vertical()
                    .max_height(360.0)
                    .show(ui, |ui| {
                        for (index, entry) in self.session.query_history.iter().enumerate() {
                            let outcome = if entry.success {
                                entry.row_count.map_or_else(
                                    || format!("{} ms", entry.duration_ms),
                                    |rows| {
                                        format!(
                                            "{} rows, {} ms",
                                            theme::number(rows),
                                            entry.duration_ms
                                        )
                                    },
                                )
                            } else {
                                "failed".to_owned()
                            };
                            ui.horizontal(|ui| {
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    if widgets::tool_button(ui, Some(Icon::Close), "", false)
                                        .on_hover_text("Remove from history")
                                        .clicked()
                                    {
                                        remove = Some(index);
                                    }
                                    ui.with_layout(Layout::top_down_justified(Align::Min), |ui| {
                                        if widgets::menu_item(ui, &entry.sql, &outcome)
                                            .on_hover_text(&entry.sql)
                                            .clicked()
                                        {
                                            rerun = Some(entry.sql.clone());
                                        }
                                    });
                                });
                            });
                        }
                    });
                ui.separator();
                clear = widgets::menu_item(ui, "Clear history", "").clicked();
                if rerun.is_some() || clear {
                    widgets::close_menu(ui);
                }
            },
        );
        if let Some(sql) = rerun {
            self.sql_input = sql;
            self.run_sql();
        }
        if let Some(index) = remove {
            self.session.remove_history(index);
            self.persist_session();
        }
        if clear {
            self.session.clear_history();
            self.persist_session();
        }
    }
}

/// `text` laid out in the editor's font, colored by token kind.
fn highlight(text: &str, columns: &[String], font: egui::FontId) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    for (range, kind) in sql_editor::tokens(text, columns) {
        let color = match kind {
            TokenKind::Keyword => theme::SQL_KEYWORD,
            TokenKind::Function => theme::SQL_FUNCTION,
            TokenKind::String => theme::SQL_STRING,
            TokenKind::Number => theme::SQL_NUMBER,
            TokenKind::Column | TokenKind::Table => theme::TEXT,
            TokenKind::Comment | TokenKind::Plain => theme::SQL_MUTED,
        };
        job.append(
            &text[range],
            0.0,
            egui::TextFormat::simple(font.clone(), color),
        );
    }
    job
}
