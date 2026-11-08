# PAVI Design

How PAVI looks and behaves on screen. The code is the source of truth for values:
colors in `crates/app/src/ui/theme.rs`, spacing in `crates/app/src/ui/spacing.rs`,
icons and shared controls in `crates/app/src/ui/widgets.rs`. This file explains the rules for using them.

## Principles

1. **Data first.** The grid takes the space. Chrome (toolbar, dataset bar, status bar, Inspector) stays thin and quiet.
2. **Dense but readable.** Developer-tool density: 13 px text, 28 px rows, small gaps.
3. **Always responsive.** The UI never freezes on data work. Anything not ready shows a placeholder (`…` in a cell, a spinner or status text) and fills in when it arrives.
4. **Say what happened.** Errors and limits appear where the user acted (under the filter box, in the SQL workspace, in the grid) and in the status bar, in plain words with the limit that was hit and what to do next.
5. **One dark theme.** There is no light theme. Do not add per-widget colors; extend the theme.

## Color tokens (`ui/theme.rs`)

| Token | Use |
|---|---|
| `BACKGROUND` | Deepest background: text inputs, grid body (`extreme_bg_color`) |
| `PANEL` | Panels and windows |
| `HEADER` | Grid headers, inactive widget fill, faint background |
| `BORDER` | Panel, window and widget outlines |
| `GRID_LINE` | Lines between grid cells |
| `TEXT` | Primary text |
| `MUTED` | Secondary text, labels, hovered outlines |
| `DISABLED` | Disabled controls and text |
| `ACCENT` | Focus/selection outline, active highlights |
| `SELECTED` | Selected cells, active widget fill |

**Rule:** UI code uses `theme::*` constants only. A new color is a new named constant in `theme.rs` with a one-line comment saying what it is for.

## Spacing and sizes

- Spacing uses the `Spacing` scale only: `None 0`, `Xs 2`, `Sm 4`, `Md 8`, `Lg 12`, `Xl 16` px. Use `Spacing::Md.px()`, `.margin()`, `.symmetric()`, `.vec()`; never a raw number for a gap or padding.
- Widget sizes are separate from spacing: interactive height and grid rows are 28 px; the custom row scrollbar is 12 px; egui scrollbars are 10 px; corner radius is 5 px.
- Column widths are 72–480 px (`ColumnLayout`).
- Window: 1448×900 default, 900×600 minimum, no OS decorations (the toolbar draws drag, maximize and close).

## Typography

| Size | Use |
|---|---|
| 15 px | Chart titles, large icon labels |
| 13 px | Body, buttons, grid cells, headers (`TextStyle::Body` / `Button`) |
| 12 px | Small text, column type labels (`TextStyle::Small`) |
| 11 px | Chart axis labels |

Use `TextStyle` where possible instead of a new `FontId` size.

## Components (`ui/widgets.rs`)

- `icon` / `Icon`: painted vector icons (no image assets). Add a variant to `Icon` instead of using text glyphs or emoji.
- `button`: icon + label button with a selected state.
- `section`, `card`, `detail`: Inspector and tool-window layout pieces.
- `menu`: toolbar dropdowns.

Reuse these before writing a new control. A new shared control goes in `widgets.rs`.

## UI states every view must handle

| State | Pattern |
|---|---|
| No file | Centered prompt to open a file |
| Loading | Placeholders (`…`) plus a status bar message; no blocking spinners |
| Empty result | Say so ("No rows match") rather than an empty grid |
| Error | Red text in the area that failed, plus the status bar; the rest of the UI keeps working |
| Limit reached | Name the limit and the way out (e.g. "add a LIMIT or a filter") |
| Disabled | `DISABLED` color; `ToolbarAvailability` decides toolbar enablement |

## Interaction

- Keyboard: Ctrl+O open, Ctrl+R refresh, Ctrl+C copy selection, Ctrl+Enter run SQL, arrow keys (+Shift) move/extend the cell selection.
- Header click toggles sort (ascending → descending); "return to source order" clears it.
- Every long action can be cancelled or replaced by starting a new one.

## Known design debt

Fix these when touching the file; do not copy them:

- Error colors are inconsistent: `Color32::RED` (grid, SQL, inspector, tools, charts) and `Color32::LIGHT_RED` (status bar, toolbar). Should be one `theme::ERROR` token.
- Status-bar success green and chart series colors (blue, green, orange) are hard-coded in `ui/status.rs` and `ui/charts.rs`. Should be `theme::SUCCESS` and `theme::CHART_*` tokens.
- Font sizes are created ad hoc with `FontId::proportional(n)` in charts, grid and widgets.
