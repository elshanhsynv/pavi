# 0010. A light theme beside the dark one, from one palette type

- Status: accepted
- Date: 2026-11-12
- Amends: DESIGN.md principle 5 ("One dark theme, one accent … There is no light theme")

## Context
PAVI had one dark theme, with every colour a `pub const` in `ui/theme.rs`. People who work in bright rooms, or whose desktop is light, asked for a light theme. Two themes risk drifting apart: a colour added to one and forgotten in the other, per-widget overrides for "the light case", or a second set of constants that UI code picks between.

## Decision
There are two palettes, `theme::DARK` and `theme::LIGHT`, both values of one `Palette` struct. The compiler makes sure every colour has a dark and a light value. UI code never reads a palette directly. It calls a function per colour (`theme::text()`, `theme::accent()`, …), which reads the palette in use from one process-wide flag, so the whole app switches at once.

- The user picks Dark (the default), Light or System in the file menu. The choice is saved in the session (`preferences.theme`); an unknown value reads as Dark. System follows the operating system's setting, also while PAVI runs (`egui::Context::system_theme`).
- Switching rebuilds the egui style from the palette and writes it to both of egui's theme slots, so egui's own system-theme following can never show a style PAVI didn't make.
- The light palette keeps the same design: warm surfaces (paper instead of graphite) and the one oak accent, darkened for contrast. A test holds both palettes to WCAG contrast: 7:1 for body text and 4.5:1 for muted text, the accent, errors and type colours on every surface.
- Sizes, spacing and fonts are shared; only colours differ.

## Consequences
- A new colour is a new `Palette` field, so it needs both values to compile.
- Colours are functions, not constants, so they can't be used in `const` items or patterns. No code needed that.
- The flag is global, not per window. PAVI has one window, so this is fine.
- A screenshot or saved PNG chart uses the palette on screen. A saved SVG chart does the same.
