use super::super::*;
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};
use crate::import::{DelimiterChoice, ImportChoice, ImportDialog};
use parquet_reader::{FileFormat, TextEncoding};

impl PaviApp {
    /// Opens the Import options window for `path`, filled with the options
    /// chosen for it before, and how it was read if it is open.
    pub(in crate::app) fn open_import_dialog(&mut self, path: PathBuf) {
        let choice = self
            .session
            .saved_import(&path)
            .map(ImportChoice::from_saved)
            .unwrap_or_default();
        let mut dialog = ImportDialog::new(path, choice);
        if let Some(report) = self
            .dataset
            .as_ref()
            .filter(|dataset| dataset.path == dialog.path)
            .and_then(|dataset| dataset.source.import_report())
        {
            dialog.read_as = Some(report.summary());
            dialog.retyped = report.retyped.clone();
        }
        self.import_dialog = Some(dialog);
    }

    /// Picks a file, then asks how to read it.
    pub(in crate::app) fn pick_file_with_options(&mut self) {
        if let Some(path) = rfd::FileDialog::new()
            .add_filter(
                "CSV and JSON",
                &["csv", "tsv", "txt", "json", "jsonl", "ndjson"],
            )
            .add_filter("Any file", &["*"])
            .pick_file()
        {
            let path = std::path::absolute(&path).unwrap_or(path);
            self.open_import_dialog(path);
        }
    }

    /// Keeps `choice` for `path` and reads it with it: again in its tab when
    /// it is open there (keeping the filter and sort), otherwise like any
    /// file the user opens.
    pub(in crate::app) fn apply_import(&mut self, path: PathBuf, choice: ImportChoice) {
        let saved = (!choice.is_default()).then(|| choice.to_saved(&path));
        self.session.set_saved_import(&path, saved);
        self.persist_session();
        let open_tab =
            (0..self.tab_count()).find(|index| self.tab_path_at(*index) == Some(path.as_path()));
        match open_tab {
            Some(index) => {
                self.switch_tab(index);
                if self.dataset.is_some() {
                    self.reload_document();
                } else {
                    self.begin_open(path);
                }
            }
            None => self.open_path(path),
        }
    }

    /// The Import options window: how to read a CSV or JSON file.
    pub(in crate::app) fn show_import_window(&mut self, ctx: &egui::Context) {
        let Some(dialog) = self.import_dialog.as_mut() else {
            return;
        };
        let mut open = true;
        let mut apply = None;
        let mut close = false;
        let name = parquet_reader::display_path(&dialog.path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        egui::Window::new("Import options")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .default_width(420.0)
            .show(ctx, |ui| {
                ui.spacing_mut().item_spacing = Spacing::Md.vec(Spacing::Sm);
                let format = dialog.effective_format();
                let csv = format == FileFormat::Csv;
                egui::Grid::new("import_options")
                    .num_columns(2)
                    .spacing(Spacing::Lg.vec(Spacing::Sm))
                    .show(ui, |ui| {
                        ui.label(RichText::new("File").color(theme::muted()));
                        ui.label(&name)
                            .on_hover_text(dialog.path.display().to_string());
                        ui.end_row();

                        ui.label(RichText::new("Read as").color(theme::muted()));
                        let by_extension = FileFormat::from_path(&dialog.path);
                        let by_extension = match by_extension {
                            FileFormat::Csv | FileFormat::Json => {
                                format!("By extension ({})", by_extension.name())
                            }
                            _ => "By extension".to_string(),
                        };
                        egui::ComboBox::from_id_salt("import_format")
                            .selected_text(match dialog.format {
                                None => by_extension.clone(),
                                Some(format) => format.name().to_string(),
                            })
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut dialog.format, None, by_extension);
                                ui.selectable_value(
                                    &mut dialog.format,
                                    Some(FileFormat::Csv),
                                    "CSV (or other separated text)",
                                );
                                ui.selectable_value(
                                    &mut dialog.format,
                                    Some(FileFormat::Json),
                                    "JSON",
                                );
                            });
                        ui.end_row();

                        ui.label(RichText::new("Between fields").color(theme::muted()));
                        ui.add_enabled_ui(csv, |ui| {
                            ui.horizontal(|ui| {
                                egui::ComboBox::from_id_salt("import_delimiter")
                                    .selected_text(dialog.delimiter.label())
                                    .show_ui(ui, |ui| {
                                        for choice in DelimiterChoice::ALL {
                                            ui.selectable_value(
                                                &mut dialog.delimiter,
                                                choice,
                                                choice.label(),
                                            );
                                        }
                                    });
                                if dialog.delimiter == DelimiterChoice::Other {
                                    ui.add(
                                        egui::TextEdit::singleline(&mut dialog.other)
                                            .desired_width(32.0)
                                            .char_limit(1),
                                    );
                                }
                            });
                        });
                        ui.end_row();

                        ui.label("");
                        ui.add_enabled(
                            csv,
                            egui::Checkbox::new(
                                &mut dialog.header,
                                "The first line holds the column names",
                            ),
                        );
                        ui.end_row();

                        ui.label(RichText::new("Encoding").color(theme::muted()));
                        egui::ComboBox::from_id_salt("import_encoding")
                            .selected_text(dialog.encoding.map_or("Detect", TextEncoding::name))
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut dialog.encoding, None, "Detect");
                                for encoding in TextEncoding::ALL {
                                    ui.selectable_value(
                                        &mut dialog.encoding,
                                        Some(encoding),
                                        encoding.name(),
                                    );
                                }
                            });
                        ui.end_row();

                        ui.label("");
                        ui.checkbox(&mut dialog.all_text, "Read every column as text")
                            .on_hover_text(
                                "Keeps values such as 007 or 1e5 exactly as written, \
                                 instead of guessing numbers and dates",
                            );
                        ui.end_row();
                    });
                if let Some(read_as) = &dialog.read_as {
                    ui.label(
                        RichText::new(format!("Last read as {read_as}."))
                            .color(theme::muted())
                            .small(),
                    );
                }
                if !dialog.retyped.is_empty() {
                    ui.label(
                        RichText::new(format!(
                            "Retyped after reading every value: {}.",
                            dialog.retyped.join(", ")
                        ))
                        .color(theme::muted())
                        .small(),
                    );
                }
                if let Some(error) = &dialog.error {
                    widgets::error(ui, error);
                }
                ui.add_space(Spacing::Sm.px());
                ui.horizontal(|ui| {
                    if widgets::primary_button(ui, Some(Icon::Refresh), "Open").clicked() {
                        match dialog.choice() {
                            Ok(choice) => apply = Some((dialog.path.clone(), choice)),
                            Err(error) => dialog.error = Some(error),
                        }
                    }
                    if ui
                        .button("Guess everything")
                        .on_hover_text("Go back to the extension, a guessed delimiter and encoding")
                        .clicked()
                    {
                        let mut reset =
                            ImportDialog::new(dialog.path.clone(), ImportChoice::default());
                        reset.read_as = dialog.read_as.take();
                        reset.retyped = std::mem::take(&mut dialog.retyped);
                        *dialog = reset;
                    }
                    close = ui.button("Cancel").clicked();
                });
            });
        if let Some((path, choice)) = apply {
            self.import_dialog = None;
            self.apply_import(path, choice);
        } else if !open || close {
            self.import_dialog = None;
        }
    }
}
