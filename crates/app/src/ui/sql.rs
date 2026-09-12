use super::super::*;

impl PaviApp {
    pub(in crate::app) fn show_sql_workspace(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("SQL");
            ui.label("Run against the current dataset; results stay in the bounded grid window.");
        });
        let response = ui.add(
            egui::TextEdit::multiline(&mut self.sql_input)
                .code_editor()
                .desired_rows(7)
                .desired_width(f32::INFINITY)
                .hint_text("SELECT columns FROM dataset WHERE predicate LIMIT n"),
        );
        let run_shortcut = response.has_focus()
            && ui.input(|input| input.modifiers.command && input.key_pressed(egui::Key::Enter));
        ui.horizontal(|ui| {
            if ui
                .add_enabled(self.dataset.is_some(), egui::Button::new("Run SQL"))
                .on_hover_text("Run query (Ctrl+Enter while editing)")
                .clicked()
                || run_shortcut
            {
                self.run_sql();
            }
            if ui
                .add_enabled(self.running_sql.is_some(), egui::Button::new("Cancel"))
                .clicked()
            {
                self.cancel_sql();
            }
            if self.running_sql.is_some() {
                ui.spinner();
                ui.label("SQL query running");
            }
        });
        if let Some(error) = &self.sql_error {
            ui.label(RichText::new(error).color(egui::Color32::RED));
        }
        ui.separator();
        ui.strong("Results");
        self.show_grid(ui);
    }
}
