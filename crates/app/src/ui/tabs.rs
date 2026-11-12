use super::super::{tabs::TabLabel, *};
use super::{
    spacing::Spacing,
    theme,
    widgets::{self, Icon},
};

/// Widest a tab gets; longer names are cut with "…".
const TAB_MAX_WIDTH: f32 = 220.0;
const TAB_MIN_WIDTH: f32 = 96.0;
/// Side of a tab's close button.
const CLOSE_SIZE: f32 = 18.0;

/// What the user did in the tab strip this frame.
enum TabAction {
    Show(usize),
    Close(usize),
    New,
}

impl PaviApp {
    /// One tab per open file or folder, under the top bar. Shown once a second
    /// tab is open; with one tab the top bar's file button names it.
    pub(in crate::app) fn show_tab_strip(&mut self, ctx: &egui::Context) {
        if self.tab_count() < 2 {
            return;
        }
        let labels = self.tab_labels();
        let mut action = None;
        egui::TopBottomPanel::top("tab_strip")
            .exact_height(theme::TAB_BAR_HEIGHT)
            .frame(
                egui::Frame::new()
                    .fill(theme::PANEL)
                    .inner_margin(egui::Margin {
                        left: Spacing::Md as i8,
                        ..egui::Margin::ZERO
                    }),
            )
            .show_separator_line(false)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 1.0;
                    egui::ScrollArea::horizontal()
                        .id_salt("tab_strip_scroll")
                        .scroll_bar_visibility(egui::scroll_area::ScrollBarVisibility::AlwaysHidden)
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                for (index, label) in labels.iter().enumerate() {
                                    if let Some(chosen) = tab(ui, index, label) {
                                        action = Some(chosen);
                                    }
                                }
                                ui.add_space(Spacing::Xs.px());
                                if widgets::tool_button(ui, Some(Icon::Plus), "", false)
                                    .on_hover_text("New tab (Ctrl+T)")
                                    .clicked()
                                {
                                    action = Some(TabAction::New);
                                }
                            });
                        });
                });
            });
        match action {
            Some(TabAction::Show(index)) => self.switch_tab(index),
            Some(TabAction::Close(index)) => self.close_tab(index),
            Some(TabAction::New) => {
                self.new_tab();
            }
            None => {}
        }
    }
}

/// Draws one tab: click shows it, middle-click or × closes it.
fn tab(ui: &mut egui::Ui, index: usize, label: &TabLabel) -> Option<TabAction> {
    let height = theme::TAB_BAR_HEIGHT;
    let icon_space = 22.0;
    let text_color = if label.active {
        theme::TEXT
    } else {
        theme::MUTED
    };
    let galley = widgets::elided(
        ui,
        &label.title,
        theme::body(),
        text_color,
        TAB_MAX_WIDTH - icon_space - CLOSE_SIZE - 3.0 * Spacing::Sm.px(),
    );
    let width = (icon_space + galley.size().x + CLOSE_SIZE + 3.0 * Spacing::Sm.px())
        .clamp(TAB_MIN_WIDTH, TAB_MAX_WIDTH);
    let (rect, response) = ui.allocate_exact_size(egui::vec2(width, height), egui::Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::SelectableLabel,
            ui.is_enabled(),
            label.active,
            &label.title,
        )
    });
    let hovered = response.hovered();
    let painter = ui.painter();
    let fill = if label.active {
        theme::BACKGROUND
    } else if hovered {
        theme::HOVER
    } else {
        theme::PANEL
    };
    painter.rect_filled(
        rect,
        egui::CornerRadius {
            nw: theme::RADIUS,
            ne: theme::RADIUS,
            sw: 0,
            se: 0,
        },
        fill,
    );
    if label.active {
        painter.line_segment(
            [rect.left_top(), rect.right_top()],
            egui::Stroke::new(2.0_f32, theme::ACCENT),
        );
    }
    let left = rect.left() + Spacing::Sm.px();
    widgets::icon(
        ui,
        egui::pos2(left + 7.0, rect.center().y),
        if label.folder {
            Icon::Folder
        } else {
            Icon::Columns
        },
        if label.active {
            theme::ACCENT
        } else {
            theme::MUTED
        },
    );
    painter.galley(
        egui::pos2(left + icon_space, rect.center().y - galley.size().y / 2.0),
        galley,
        text_color,
    );

    // The close button shows on the open tab and on the one under the pointer.
    let close_rect = egui::Rect::from_center_size(
        egui::pos2(
            rect.right() - Spacing::Sm.px() - CLOSE_SIZE / 2.0,
            rect.center().y,
        ),
        egui::vec2(CLOSE_SIZE, CLOSE_SIZE),
    );
    let close = ui
        .interact(
            close_rect,
            ui.id().with(("close_tab", index)),
            egui::Sense::click(),
        )
        .on_hover_text("Close tab (Ctrl+W)");
    if label.active || hovered || close.hovered() {
        if close.hovered() {
            ui.painter()
                .rect_filled(close_rect, theme::RADIUS, theme::HOVER);
        }
        widgets::icon(ui, close_rect.center(), Icon::Close, theme::MUTED);
    }
    if close.clicked() || response.middle_clicked() {
        return Some(TabAction::Close(index));
    }
    let response = match &label.path {
        Some(path) => response.on_hover_text(path.display().to_string()),
        None => response,
    };
    response.clicked().then_some(TabAction::Show(index))
}
