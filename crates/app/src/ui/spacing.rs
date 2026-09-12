use eframe::egui::{Margin, Vec2};
/// Shared density scale. Widget sizes are separate from spacing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i8)]
pub enum Spacing {
    None = 0,
    Xs = 2,
    Sm = 4,
    Md = 8,
    Lg = 12,
    Xl = 16,
}
impl Spacing {
    pub const fn px(self) -> f32 {
        self as i8 as f32
    }
    pub const fn margin(self) -> Margin {
        Margin::same(self as i8)
    }
    pub const fn symmetric(self, vertical: Self) -> Margin {
        Margin::symmetric(self as i8, vertical as i8)
    }
    pub const fn vec(self, vertical: Self) -> Vec2 {
        Vec2::new(self.px(), vertical.px())
    }
}
