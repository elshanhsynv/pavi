use super::super::*;
use super::{
    inspector::{type_color, type_label},
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};
use crate::compare::{KeyMatch, SchemaDiff, key_query};
use arrow_schema::DataType;

/// Height of the schema difference list before it scrolls.
const DIFF_LIST_HEIGHT: f32 = 220.0;

impl PaviApp {
    /// Opens the Compare window when another tab has a dataset to compare with.
    pub(in crate::app) fn open_compare(&mut self) {
        let others = self
            .compare_tables()
            .into_iter()
            .filter(|(_, _, open)| !open)
            .count();
        if self.dataset.is_none() || others == 0 {
            self.status = "Open another file in a tab to compare with".to_string();
            return;
        }
        self.compare.visible = true;
    }

    /// The Compare window: the open tab against another tab.
    pub(in crate::app) fn show_compare_window(&mut self, ctx: &egui::Context) {
        if !self.compare.visible {
            return;
        }
        let tables = self.compare_tables();
        let Some((this_name, this_source)) = tables
            .iter()
            .find(|(_, _, open)| *open)
            .map(|(name, source, _)| (name.clone(), Arc::clone(source)))
        else {
            self.compare.visible = false;
            return;
        };
        let others = tables
            .iter()
            .filter(|(_, _, open)| !open)
            .map(|(name, source, _)| (name.clone(), Arc::clone(source)))
            .collect::<Vec<_>>();
        if others.is_empty() {
            self.compare.visible = false;
            return;
        }
        if !self
            .compare
            .other
            .as_ref()
            .is_some_and(|other| others.iter().any(|(name, _)| name == other))
        {
            self.compare.other = others.first().map(|(name, _)| name.clone());
        }
        let other_name = self.compare.other.clone().unwrap_or_default();
        let Some(other_source) = others
            .iter()
            .find(|(name, _)| *name == other_name)
            .map(|(_, source)| Arc::clone(source))
        else {
            return;
        };
        let schema = |source: &ParquetSource| {
            source
                .metadata()
                .columns()
                .iter()
                .map(|column| (column.name.clone(), column.data_type.clone()))
                .collect::<Vec<_>>()
        };
        let diff = SchemaDiff::new(&schema(&this_source), &schema(&other_source));
        let keys = diff.key_candidates();
        if !self
            .compare
            .key
            .as_deref()
            .is_some_and(|key| keys.contains(&key))
        {
            self.compare.key = keys.first().map(|key| (*key).to_owned());
        }

        let mut open = true;
        let mut run = None;
        egui::Window::new("Compare")
            .open(&mut open)
            .collapsible(false)
            .default_width(520.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.spacing_mut().item_spacing = Spacing::Md.vec(Spacing::Sm);
                egui::Grid::new("compare_tables")
                    .num_columns(2)
                    .spacing(Spacing::Lg.vec(Spacing::Sm))
                    .show(ui, |ui| {
                        ui.label(RichText::new("This tab").color(theme::MUTED));
                        ui.label(table_summary(&this_name, &this_source));
                        ui.end_row();
                        ui.label(RichText::new("Compare with").color(theme::MUTED));
                        ui.horizontal(|ui| {
                            egui::ComboBox::from_id_salt("compare_other")
                                .selected_text(&other_name)
                                .show_ui(ui, |ui| {
                                    for (name, _) in &others {
                                        ui.selectable_value(
                                            &mut self.compare.other,
                                            Some(name.clone()),
                                            name,
                                        );
                                    }
                                });
                            ui.label(table_summary("", &other_source));
                        });
                        ui.end_row();
                    });
                ui.separator();
                ui.label(RichText::new("Columns").font(theme::strong(13.0)));
                if diff.identical() {
                    ui.label(
                        RichText::new(format!(
                            "Both have the same {} columns with the same types.",
                            diff.same.len()
                        ))
                        .color(theme::MUTED),
                    );
                } else {
                    ui.label(
                        RichText::new(format!(
                            "{} the same, {} only here, {} only in {other_name}, {} with another type.",
                            diff.same.len(),
                            diff.only_left.len(),
                            diff.only_right.len(),
                            diff.type_differs.len()
                        ))
                        .color(theme::MUTED),
                    );
                    egui::ScrollArea::vertical()
                        .id_salt("compare_diff")
                        .max_height(DIFF_LIST_HEIGHT)
                        .auto_shrink([false, true])
                        .show(ui, |ui| diff_list(ui, &diff, &other_name));
                }
                ui.separator();
                ui.label(RichText::new("Rows").font(theme::strong(13.0)));
                if keys.is_empty() {
                    ui.label(
                        RichText::new(
                            "No column has the same name and a comparable type in both, so rows can't be matched.",
                        )
                        .color(theme::MUTED),
                    );
                    return;
                }
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Match rows by").color(theme::MUTED));
                    egui::ComboBox::from_id_salt("compare_key")
                        .selected_text(self.compare.key.as_deref().unwrap_or_default())
                        .show_ui(ui, |ui| {
                            for key in &keys {
                                ui.selectable_value(
                                    &mut self.compare.key,
                                    Some((*key).to_owned()),
                                    *key,
                                );
                            }
                        });
                });
                ui.horizontal_wrapped(|ui| {
                    for (matching, text, hint) in [
                        (
                            KeyMatch::OnlyLeft,
                            "Only here",
                            format!("Rows of this tab whose key {other_name} doesn't have"),
                        ),
                        (
                            KeyMatch::OnlyRight,
                            "Only there",
                            format!("Rows of {other_name} whose key this tab doesn't have"),
                        ),
                        (
                            KeyMatch::Both,
                            "In both",
                            format!("Rows of this tab whose key {other_name} has too"),
                        ),
                    ] {
                        if widgets::tool_button(ui, Some(Icon::Search), text, false)
                            .on_hover_text(hint)
                            .clicked()
                        {
                            run = Some(matching);
                        }
                    }
                });
                ui.label(
                    RichText::new("Runs the query in the SQL workspace, where you can change it.")
                        .font(theme::small())
                        .color(theme::MUTED),
                );
            });
        self.compare.visible = open;
        if let (Some(matching), Some(key)) = (run, self.compare.key.clone()) {
            // Out of the way, so the query and its rows can be seen.
            self.compare.visible = false;
            self.sql_input = key_query(&key, &other_name, matching);
            self.workspace = Workspace::Sql;
            self.run_sql();
        }
    }
}

fn table_summary(name: &str, source: &ParquetSource) -> String {
    let shape = format!(
        "{} rows × {} columns",
        theme::number(source.row_count()),
        source.column_count()
    );
    if name.is_empty() {
        shape
    } else {
        format!("{name}: {shape}")
    }
}

fn diff_list(ui: &mut egui::Ui, diff: &SchemaDiff, other: &str) {
    let column = |ui: &mut egui::Ui, name: &str, data_type: &DataType| {
        ui.label(name);
        ui.label(
            RichText::new(type_label(data_type))
                .font(theme::small())
                .color(type_color(data_type)),
        );
    };
    egui::Grid::new("compare_diff_rows")
        .num_columns(3)
        .striped(true)
        .spacing(Spacing::Lg.vec(Spacing::Xs))
        .show(ui, |ui| {
            for (name, data_type) in &diff.only_left {
                ui.label(RichText::new("only here").color(theme::MUTED));
                column(ui, name, data_type);
                ui.end_row();
            }
            for (name, data_type) in &diff.only_right {
                ui.label(RichText::new(format!("only in {other}")).color(theme::MUTED));
                column(ui, name, data_type);
                ui.end_row();
            }
            for (name, here, there) in &diff.type_differs {
                ui.label(RichText::new("another type").color(theme::MUTED));
                ui.label(name);
                ui.label(
                    RichText::new(format!(
                        "{} here, {} there",
                        type_label(here),
                        type_label(there)
                    ))
                    .font(theme::small())
                    .color(theme::ERROR),
                );
                ui.end_row();
            }
        });
}
