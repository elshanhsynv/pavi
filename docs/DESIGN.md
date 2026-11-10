# PAVI Design

How PAVI looks and behaves on screen. The code is the source of truth for values:
colors in `crates/app/src/ui/theme.rs`, spacing in `crates/app/src/ui/spacing.rs`,
icons and shared controls in `crates/app/src/ui/widgets.rs`. This file explains the rules for using them.

## Principles

1. **Data first.** The grid takes the space and runs edge to edge. Chrome is one 40 px top bar, one 26 px status bar and the Inspector; it stays thin and quiet.
2. **Dense but readable.** Developer-tool density: 13 px text, 24 px rows, small gaps.
3. **Always responsive.** The UI never freezes on data work. Anything not ready shows a placeholder (`…` in a cell, a spinner or status text) and fills in when it arrives.
4. **Say what happened.** Errors and limits appear where the user acted (under the filter box, in the SQL workspace, in the grid) and in the status bar, in plain words with the limit that was hit and what to do next.
5. **One dark theme, one accent.** Warm graphite surfaces and a single oak accent. There is no light theme. Do not add per-widget colors; extend the theme.
6. **One filled button per view.** `primary_button` marks the main action (Browse, Run, Draw chart); everything else is a flat `tool_button` or a menu.

## Color tokens (`ui/theme.rs`)

| Token | Use |
|---|---|
| `BACKGROUND` | Deepest surface: text inputs, grid body (`extreme_bg_color`) |
| `STRIPE` | Alternate grid rows |
| `PANEL` | Bars, the Inspector, popups and windows |
| `HEADER` | Grid header, resting control fill, the row of the selected cell |
| `HOVER` | A control or header under the pointer |
| `BORDER` | Panel, popup and control outlines |
| `GRID_LINE` | Lines between grid cells |
| `TEXT` | Primary text |
| `MUTED` | Secondary text, labels, resting icons, `null` cells |
| `DISABLED` | Disabled controls and text, `…` placeholders |
| `ACCENT` | Focus and selection outline, switched-on tools, the primary button |
| `ON_ACCENT` | Text and icons on an `ACCENT` fill |
| `SELECTED` | Selected cells, switched-on tools |
| `ERROR` | Errors, everywhere |
| `SUCCESS` | The idle status dot |
| `CLOSE_HOVER` | The window close button under the pointer |
| `TYPE_NUMBER`, `TYPE_TEMPORAL`, `TYPE_BOOL` | Type labels in grid headers and the schema list, by type family (other types use `MUTED`) |
| `CHART_POINT`, `CHART_BAR` | Chart marks |

**Rule:** UI code uses `theme::*` constants only. A new color is a new named constant in `theme.rs` with a one-line comment saying what it is for.

## Spacing and sizes

- Spacing uses the `Spacing` scale only: `None 0`, `Xs 2`, `Sm 4`, `Md 8`, `Lg 12` px. Use `Spacing::Md.px()`, `.margin()`, `.symmetric()`, `.vec()`; never a raw number for a gap or padding.
- Widget sizes are separate from spacing and live in `theme.rs`: top bar 40 px (`BAR_HEIGHT`), status bar 26 px (`STATUS_HEIGHT`), buttons, inputs and menu rows 26 px (`CONTROL_HEIGHT`), corner radius 4 px (`RADIUS`). Grid rows are 24 px and the header row 28 px; the custom row scrollbar is 12 px; egui scrollbars are 10 px.
- The row-number gutter is as wide as the last row number needs (44 px minimum). Numeric columns are right-aligned so digits line up.
- The Inspector opens 280 px wide and can be dragged between 240 and 460 px.
- Column widths are 72–480 px (`ColumnLayout`).
- Window: 1448×900 default, 900×600 minimum, no OS decorations (the top bar is the drag area and draws minimize, maximize and close).

## Typography

| Font | Use |
|---|---|
| `theme::body()`, 13 px | Body, buttons, grid cells (`TextStyle::Body` / `Button`) |
| `theme::small()`, 11.5 px | Type labels, row numbers, status bar, captions, chart axis labels (`TextStyle::Small`) |
| `theme::strong(size)` | Semibold: column names and section titles at 12.5 px, chart titles and the primary button at 13 px, headings at 17 px (`TextStyle::Heading`) |
| Monospace, 12.5 px | SQL editors (`TextStyle::Monospace`) |

Fonts come from `theme.rs` only: use `TextStyle` or these three functions, never `FontId::proportional(n)` in UI code.

On Windows the text is Segoe UI and Segoe UI Semibold, read once from the system font folder before the first frame (`app::apply_theme`, called from `main`). A font file that is missing or larger than 16 MB (`MAX_FONT_BYTES`) is skipped and egui's built-in font is used, as on other platforms.

## Components (`ui/widgets.rs`)

- `icon` / `Icon`: painted line icons (no image assets). Add a variant to `Icon` instead of using text glyphs or emoji. `logo` paints the PAVI mark.
- `tool_button`: flat button with an icon, a label or both, and a switched-on state.
- `primary_button`: the one filled button of a view.
- `menu`, `menu_item`, `menu_caption`, `close_menu`: a top-bar button with a popup below it, and its rows.
- `segmented`: the Grid / SQL switch.
- `window_button`, `divider`: top-bar pieces.
- `section`, `detail`: collapsible Inspector sections and their "label  value" rows.
- `error`: error text in the one error color.
- `placeholder`: centered empty, loading and error states.

Reuse these before writing a new control. A new shared control goes in `widgets.rs`.

## Layout

- **Top bar** (`ui/toolbar.rs`), left to right: logo, file menu (path box, Browse, Reload, Copy path, recent files), Grid / SQL switch, filter box, Sort, Columns (show types, pin header, find, show/hide, reorder, width, fit, reset), Export (CSV, Parquet, copy cell/row), Reload, Charts, Inspector, window buttons. An active filter or sort is outlined or named in `ACCENT`. Below a window width of 1,100 px (`COMPACT_BAR_WIDTH`) Sort, Columns and Export show only their icons, with the name in the tooltip, so the filter box keeps a usable width.
- **Grid** (`ui/grid.rs`): striped rows, pinned header with name, sort arrow and colored type label, row-number gutter. Above a filter, SQL or sort result a thin strip names the result and its row range. The row of the selected cell is tinted.
- **SQL workspace** (`ui/sql.rs`): editor, Run, Cancel and the History menu above the result grid.
- **Inspector** (`ui/inspector.rs`): collapsible sections Cell, Column (with profile), File, Schema, Row groups, Page cache.
- **Status bar** (`ui/status.rs`): status dot or spinner, the share of the file a running filter, sort or SQL scan has read (e.g. `42%`), and message on the left (the activity word is left out when the message already starts with it); selection, row/column/row-group counts, compression and the "Go to row" box on the right.
- **Charts** is the only floating window.

## UI states every view must handle

| State | Pattern |
|---|---|
| No file | Centered prompt: drop a file, Browse, or pick a recent file |
| Loading | Placeholders (`…`) plus a status bar message; no blocking spinners. A scan of the whole file shows how far it has got |
| Empty result | Say so ("No rows match") rather than an empty grid |
| Error | Red text in the area that failed, plus the status bar; the rest of the UI keeps working |
| Limit reached | Name the limit and the way out (e.g. "add a LIMIT or a filter") |
| Disabled | `DISABLED` color; `ToolbarAvailability` decides toolbar enablement |

## Interaction

- Keyboard: Ctrl+O open, Ctrl+R reload, Ctrl+F focus the filter box, Ctrl+G focus "Go to row", Ctrl+I toggle the Inspector, Ctrl+1 / Ctrl+2 Grid / SQL, Ctrl+C copy selection, Ctrl+Enter run SQL, arrow keys, Page Up/Down, Home and End (+Shift) move/extend the cell selection.
- Dropping a file on the window opens it.
- A header click selects the column: it is underlined in `ACCENT` and the Inspector's Column section describes it. It never starts a scan.
- A header right-click opens a menu: Sort ascending / descending, Profile column, Fit width, Hide column, Copy name.
- A cell right-click selects the cell and opens a menu: Filter to this value, Exclude this value, Copy cell, Copy row. When the cell cannot be filtered by (null, an unsupported type, an aggregate result) the two filter rows are disabled and a caption says why.
- Sorting is always an explicit choice (the Sort menu or the header menu), because it scans the whole file. A sort without a SQL `LIMIT` returns the first 100,000 rows in sort order and says so in the result strip; "Back to file order" in the Sort menu clears it.
- Every long action can be cancelled or replaced by starting a new one.

## Known design debt

Fix these when touching the file; do not copy them:

- The Charts window still uses stock egui combo boxes, buttons and text fields instead of the shared controls.
- Some pixel sizes for painted pieces (icon boxes, the 20 px Inspector rows, the status bar's fixed right-hand width) are literals in the UI files instead of named constants.
