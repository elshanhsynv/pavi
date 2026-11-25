use std::{fs::File, thread, time::Duration};

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tempfile::TempDir;

use crate::chart::DEFAULT_POINT_LIMIT;
use crate::results::MAX_FILTERED_BATCHES;
use parquet_reader::SortSpec;
use pavi_query::QueryExecution;

use super::*;

#[test]
fn desktop_layout_keeps_grid_selection_jump_and_workspace_actions_connected() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    let ctx = egui::Context::default();
    ui::theme::apply(&ctx);
    // Jumps must land at once: a click during a scroll animation hits another row.
    ctx.style_mut(|style| style.scroll_animation = egui::style::ScrollAnimation::none());

    let render = |app: &mut PaviApp, width: f32, events: Vec<egui::Event>| {
        ctx.run(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(width, 900.0),
                )),
                events,
                ..Default::default()
            },
            |ctx| {
                app.show_top_bar(ctx);
                app.show_inspector_panel(ctx);
                egui::CentralPanel::default().show(ctx, |ui| {
                    assert!(
                        ui.available_width() >= width - 310.0,
                        "Inspector must not grow into the grid"
                    );
                    app.show_grid(ui);
                });
            },
        )
    };
    let click_text = |output: &egui::FullOutput, label: &str| {
        let position = output
            .shapes
            .iter()
            .find_map(|shape| {
                if let egui::Shape::Text(text) = &shape.shape {
                    (text.galley.job.text == label
                        && (!["alpha", "id"].contains(&label)
                            || text.pos.x < ctx.screen_rect().width() - 280.0))
                        .then(|| text.pos + text.galley.size() / 2.0)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("Missing visible control: {label}"));
        vec![
            egui::Event::PointerMoved(position),
            egui::Event::PointerButton {
                pos: position,
                button: egui::PointerButton::Primary,
                pressed: true,
                modifiers: egui::Modifiers::NONE,
            },
            egui::Event::PointerButton {
                pos: position,
                button: egui::PointerButton::Primary,
                pressed: false,
                modifiers: egui::Modifiers::NONE,
            },
        ]
    };

    for (width, frozen, types) in [
        (900.0, true, true),
        (1448.0, false, true),
        (1448.0, false, false),
        (1448.0, true, false),
    ] {
        app.freeze_header = frozen;
        app.show_column_types = types;
        app.jump_target = Some(0);
        for _ in 0..3 {
            render(&mut app, width, vec![]);
        }
        let output = render(&mut app, width, vec![]);
        render(&mut app, width, click_text(&output, "alpha"));
        assert_eq!(app.grid.selection, Some((0, 1)));
        // A header click selects the column in the current row; it never sorts.
        let output = render(&mut app, width, vec![]);
        render(&mut app, width, click_text(&output, "id"));
        assert_eq!(app.grid.selection, Some((0, 0)));
        assert!(app.filtered.is_none());
        app.jump_row_input = "25".to_string();
        app.jump_to_input();
        render(&mut app, width, vec![]);
        assert_eq!(app.grid.selection, Some((24, 0)));
    }
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "SQL"));
    assert_eq!(app.workspace, Workspace::Sql);
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "Grid"));
    assert_eq!(app.workspace, Workspace::Grid);
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "Columns"));
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "Show types"));
    assert!(app.show_column_types);
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "Pin header"));
    assert!(!app.freeze_header);
    // Close the Columns menu again.
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "Grid"));
    assert!(app.pages.loaded_count() <= crate::pages::MAX_LOADED_PAGES);
    app.sql_input = "SELECT id, name FROM dataset LIMIT 5".into();
    app.run_sql();
    poll_until_idle(&mut app);
    app.jump_target = Some(0);
    for _ in 0..3 {
        render(&mut app, 1448.0, vec![]);
    }
    let output = render(&mut app, 1448.0, vec![]);
    render(&mut app, 1448.0, click_text(&output, "alpha"));
    assert_eq!(app.grid.selection, Some((0, 1)));
}

fn source(rows: usize) -> (TempDir, Arc<ParquetSource>, PathBuf) {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("filters.parquet");
    write_rows(&path, rows);
    let source = Arc::new(ParquetSource::open(&path).unwrap());
    (directory, source, path)
}

/// Writes ids `0..rows` and alternating names to `path`.
fn write_rows(path: &std::path::Path, rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(1_000))
                .build(),
        ),
    )
    .unwrap();
    writer
        .write(
            &arrow_array::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from_iter_values(0..rows as i32)),
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|row| if row % 2 == 0 { "alpha" } else { "beta" }),
                    )),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
}

fn make_app(source: Arc<ParquetSource>, path: PathBuf) -> PaviApp {
    let schema = source.schema();
    let column_names = schema
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect();
    let mut app =
        PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));
    app.dataset = Some(Dataset {
        source: Arc::clone(&source),
        path,
        column_names,
    });
    app.column_layout = ColumnLayout::new(source.column_count(), INITIAL_COLUMN_WIDTH);
    app.grid
        .ready(source.row_count(), app.column_layout.visible().len());
    app
}

fn poll_until_idle(app: &mut PaviApp) {
    for _ in 0..1_000 {
        app.poll_filtered();
        if app
            .filtered
            .as_ref()
            .is_none_or(|filtered| !filtered.needs_more())
        {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("filter did not become idle");
}

fn poll_until_opened(app: &mut PaviApp) {
    for _ in 0..1_000 {
        app.poll_open();
        if app.opening.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("open did not become idle");
}

fn poll_until_page_loaded(app: &mut PaviApp, page: u64) {
    for _ in 0..1_000 {
        let _ = app.pages.poll(&mut app.grid);
        if app.pages.get(page).is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("page did not load");
}

fn poll_chart_until_done(app: &mut PaviApp) {
    for _ in 0..2_000 {
        app.poll_chart();
        if !app.chart.loading() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("chart did not become idle");
}

fn poll_profile_until_done(app: &mut PaviApp) {
    for _ in 0..2_000 {
        app.poll_profile();
        if !app.profile.loading() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("profile did not become idle");
}

fn send_command_key(app: &mut PaviApp, key: egui::Key) {
    let context = egui::Context::default();
    context.begin_pass(egui::RawInput {
        modifiers: egui::Modifiers {
            command: true,
            ..Default::default()
        },
        events: vec![egui::Event::Key {
            key,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers: egui::Modifiers {
                command: true,
                ..Default::default()
            },
        }],
        ..Default::default()
    });
    app.handle_global_shortcuts(&context);
    let _ = context.end_pass();
}

fn filtered_ids(app: &PaviApp) -> Vec<String> {
    (0..app.grid.rows)
        .map(|row| app.filtered_cell_text(row, 0).unwrap())
        .collect()
}

#[test]
fn applies_every_supported_operator_through_the_query_path() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);

    for filter in [
        "id == 2",
        "id != 2",
        "id > 2",
        "id >= 2",
        "id < 2",
        "id <= 2",
        "name contains alpha",
    ] {
        app.filter_input = filter.to_string();
        app.apply_filter();
        poll_until_idle(&mut app);
        assert!(app.filter_error.is_none(), "{filter}");
        assert!(app.grid.rows > 0, "{filter}");
    }
}

#[test]
fn workspace_shortcuts_route_between_grid_and_sql() {
    let (_directory, source, path) = source(2);
    let mut app = make_app(source, path);

    send_command_key(&mut app, egui::Key::Num2);
    assert_eq!(app.workspace, Workspace::Sql);
    send_command_key(&mut app, egui::Key::Num1);
    assert_eq!(app.workspace, Workspace::Grid);
    send_command_key(&mut app, egui::Key::F);
    assert_eq!(app.workspace, Workspace::Grid);
}

#[test]
fn reports_invalid_and_incompatible_filters_without_replacing_the_grid() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    let rows = app.grid.rows;

    for filter in ["id", "missing == 1", "id contains two"] {
        app.filter_input = filter.to_string();
        app.apply_filter();
        assert!(app.filtered.is_none());
        assert!(app.filter_error.is_some());
        assert_eq!(app.grid.rows, rows);
    }
}

#[test]
fn applies_and_clears_a_filter() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.filter_input = "id == 2".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["2"]);

    app.clear_filter();
    assert!(app.filtered.is_none());
    assert!(app.filter_input.is_empty());
    assert_eq!(app.grid.rows, 8);
}

#[test]
fn distinguishes_zero_matches_and_execution_errors() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source.clone(), path.clone());
    app.filter_input = "id > 100".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert_eq!(app.grid.rows, 0);
    assert!(app.status.contains("No rows match"));

    let mut failing = make_app(source, path.clone());
    std::fs::remove_file(path).unwrap();
    failing.filter_input = "id == 1".to_string();
    failing.apply_filter();
    poll_until_idle(&mut failing);
    assert!(
        failing
            .filtered
            .as_ref()
            .and_then(|filtered| filtered.error.as_ref())
            .is_some()
    );
}

#[test]
fn streams_multiple_pages_with_a_bounded_result_window() {
    let rows = (parquet_reader::PAGE_ROWS * 9 + 1) as usize;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);
    app.filter_input = "id >= 0".to_string();
    app.apply_filter();
    for _ in 0..9 {
        poll_until_idle(&mut app);
        app.request_more_filtered();
    }
    poll_until_idle(&mut app);

    let filtered = app.filtered.as_ref().unwrap();
    assert!(filtered.first_result > 0);
    assert!(filtered.batches.len() <= MAX_FILTERED_BATCHES);
    assert!(filtered.rows <= parquet_reader::PAGE_ROWS * MAX_FILTERED_BATCHES as u64);
    assert_eq!(app.grid.selection, None);
}

fn send_grid_key(app: &mut PaviApp, key: egui::Key, shift: bool) {
    let modifiers = egui::Modifiers {
        shift,
        ..Default::default()
    };
    let context = egui::Context::default();
    context.begin_pass(egui::RawInput {
        modifiers,
        events: vec![egui::Event::Key {
            key,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers,
        }],
        ..Default::default()
    });
    let first_result = app
        .filtered
        .as_ref()
        .map_or(0, |filtered| filtered.first_result);
    app.handle_grid_keyboard(&context, first_result);
    let _ = context.end_pass();
}

#[test]
fn keyboard_and_shift_selection_work_after_the_result_window_slides() {
    let rows = (parquet_reader::PAGE_ROWS * 20) as usize;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);
    app.filter_input = "id >= 0".to_string();
    app.apply_filter();
    for _ in 0..19 {
        poll_until_idle(&mut app);
        app.request_more_filtered();
    }
    poll_until_idle(&mut app);

    let first = app.filtered.as_ref().unwrap().first_result;
    // Past the window size, where absolute rows used to be rejected.
    assert!(first >= parquet_reader::PAGE_ROWS * MAX_FILTERED_BATCHES as u64);
    assert_eq!(app.grid.first_row, first);

    assert!(app.grid.select_filtered(first, 10, 0));
    send_grid_key(&mut app, egui::Key::ArrowDown, false);
    assert_eq!(app.grid.selection, Some((first + 11, 0)));
    send_grid_key(&mut app, egui::Key::ArrowRight, true);
    assert_eq!(app.grid.selection, Some((first + 11, 1)));
    assert!(app.grid.is_selected(first + 11, 0));
    // Up from the first row held reads the rows above it again (ADR 0012).
    assert!(app.grid.select_filtered(first, 0, 1));
    send_grid_key(&mut app, egui::Key::ArrowUp, false);
    poll_until_jumped(&mut app);
    assert_eq!(app.grid.selection, Some((first - 1, 1)));
    assert!(app.filtered.as_ref().unwrap().first_result < first - 1);

    // Home is the result's first row, End its last once the matches are counted.
    send_grid_key(&mut app, egui::Key::Home, false);
    poll_until_jumped(&mut app);
    assert_eq!(app.grid.selection, Some((0, 1)));
    for _ in 0..2_000 {
        app.poll_filtered();
        if app.filtered.as_ref().unwrap().total().is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(app.filtered.as_ref().unwrap().total(), Some(rows as u64));
    send_grid_key(&mut app, egui::Key::End, false);
    poll_until_jumped(&mut app);
    assert_eq!(app.grid.selection, Some((rows as u64 - 1, 1)));
    send_grid_key(&mut app, egui::Key::ArrowDown, false);
    assert_eq!(
        app.grid.selection,
        Some((rows as u64 - 1, 1)),
        "stays on the last row"
    );
}

/// Polls until a jump in a result has shown its row.
fn poll_until_jumped(app: &mut PaviApp) {
    for _ in 0..5_000 {
        app.poll_filtered();
        if app
            .filtered
            .as_ref()
            .is_none_or(|filtered| filtered.pending_jump.is_none())
        {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("the jump did not finish");
}

#[test]
fn records_the_opened_path_even_if_the_path_box_changes() {
    let (directory, _source, path) = source(4);
    let store = SessionStore::new(directory.path().join("session.json"));
    let mut app = PaviApp::new_with_store(None, store);
    let other = directory.path().join("typed-meanwhile.parquet");

    app.begin_open(path.clone());
    app.path_input = other.display().to_string();
    poll_until_opened(&mut app);

    assert_eq!(app.dataset.as_ref().unwrap().path, path);
    assert_eq!(app.session.recent_files.first(), Some(&path));
    assert!(!app.session.recent_files.contains(&other));

    // A failed open removes only the path that failed, not the one being typed.
    let missing = directory.path().join("missing.parquet");
    app.begin_open(missing.clone());
    app.path_input = path.display().to_string();
    poll_until_opened(&mut app);
    assert!(matches!(app.grid.loading, LoadState::Error(_)));
    assert!(app.session.recent_files.contains(&path));
}

#[test]
fn cancels_pending_page_reads_that_scrolled_out_of_view() {
    let rows = (parquet_reader::PAGE_ROWS * 20) as usize;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);
    for page in 0..10 {
        app.request_page(page);
    }

    // A later frame in which the grid drew only page 9.
    app.pages.begin_frame();
    app.pages.note_visible(9);
    app.pages.cancel_offscreen(&mut app.grid);
    assert_eq!(
        app.pages.pending_pages(),
        [8, 9],
        "keeps one page of margin"
    );
    assert_eq!(
        app.grid.requested.iter().copied().collect::<Vec<_>>(),
        [8, 9]
    );

    // A cancelled page can be requested again, and a page requested this frame
    // (like a jump target) survives even though it was not drawn.
    app.request_page(0);
    app.pages.note_visible(9);
    app.pages.cancel_offscreen(&mut app.grid);
    assert!(app.pages.is_pending(0));
}

fn grid_frame(time: f64, events: Vec<egui::Event>) -> egui::RawInput {
    egui::RawInput {
        screen_rect: Some(egui::Rect::from_min_size(
            egui::Pos2::ZERO,
            egui::vec2(1448.0, 900.0),
        )),
        time: Some(time),
        events,
        ..Default::default()
    }
}

#[test]
fn reaches_every_row_of_a_billion_row_grid() {
    use crate::state::GRID_WINDOW_ROWS;

    const ROWS: u64 = 1_000_000_000;
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path);
    app.grid.ready(ROWS, app.column_layout.visible().len());
    let ctx = egui::Context::default();
    ui::theme::apply(&ctx);
    ctx.style_mut(|style| style.scroll_animation = egui::style::ScrollAnimation::none());
    let mut time = 0.0;
    let mut render = |app: &mut PaviApp, events: Vec<egui::Event>| {
        time += 0.1;
        let _ = ctx.run(grid_frame(time, events), |ctx| {
            egui::CentralPanel::default().show(ctx, |ui| app.show_grid(ui));
        });
    };
    for _ in 0..3 {
        render(&mut app, vec![]);
    }
    assert_eq!(app.drawn_rows.map(|(first, _)| first), Some(0));
    assert_eq!(app.grid.window_base, 0);

    // The very last row (unreachable before: egui's f32 layout ran out of precision).
    app.jump_row_input = ROWS.to_string();
    app.jump_to_input();
    for _ in 0..4 {
        render(&mut app, vec![]);
    }
    assert_eq!(app.drawn_rows.map(|(_, last)| last), Some(ROWS - 1));
    assert_eq!(app.grid.window_base, ROWS - GRID_WINDOW_ROWS);

    // The middle row, centred.
    app.jump_row_input = "500000001".to_string();
    app.jump_to_input();
    for _ in 0..4 {
        render(&mut app, vec![]);
    }
    let (first, last) = app.drawn_rows.unwrap();
    assert!((first..=last).contains(&500_000_000));
    let base_before = app.grid.window_base;

    // Wheel-scroll 150,000 rows down: past the window's inner edge, so the window
    // moves, and the view lands on the same rows it would without windowing.
    let pointer = egui::pos2(600.0, 450.0);
    let wheel = vec![
        egui::Event::PointerMoved(pointer),
        egui::Event::MouseWheel {
            unit: egui::MouseWheelUnit::Point,
            delta: egui::vec2(0.0, -150_000.0 * ROW_HEIGHT),
            modifiers: egui::Modifiers::NONE,
        },
    ];
    render(&mut app, wheel);
    for _ in 0..60 {
        render(&mut app, vec![egui::Event::PointerMoved(pointer)]);
    }
    let (scrolled_first, _) = app.drawn_rows.unwrap();
    assert!(
        scrolled_first.abs_diff(first + 150_000) <= 1,
        "expected about {}, got {scrolled_first}",
        first + 150_000
    );
    assert_ne!(app.grid.window_base, base_before, "the window moved");
    assert!(
        app.grid
            .recentered_base(scrolled_first, scrolled_first + 30)
            .is_none()
    );
}

#[test]
fn row_scrollbar_maps_clicks_to_rows_across_the_whole_file() {
    let ctx = egui::Context::default();
    let bar = egui::Rect::from_min_max(egui::pos2(500.0, 100.0), egui::pos2(512.0, 700.0));
    let click = |y: f32| {
        let position = egui::pos2(506.0, y);
        vec![
            egui::Event::PointerMoved(position),
            egui::Event::PointerButton {
                pos: position,
                button: egui::PointerButton::Primary,
                pressed: true,
                modifiers: egui::Modifiers::NONE,
            },
            egui::Event::PointerButton {
                pos: position,
                button: egui::PointerButton::Primary,
                pressed: false,
                modifiers: egui::Modifiers::NONE,
            },
        ]
    };
    let target = |events: Vec<egui::Event>| {
        let mut result = None;
        let _ = ctx.run(grid_frame(0.0, events), |ctx| {
            egui::CentralPanel::default().show(ctx, |ui| {
                result = ui::grid::row_scrollbar(ui, bar, 1_000_000_000, 0, 30);
            });
        });
        result
    };
    assert_eq!(target(vec![]), None);
    let bottom = target(click(699.0)).unwrap();
    assert_eq!(bottom, 1_000_000_000 - 31);
    let middle = target(click(400.0)).unwrap();
    assert!(middle.abs_diff(500_000_000) < 5_000_000);
    assert_eq!(target(click(101.0)), Some(0));
}

#[test]
fn replaces_and_cancels_obsolete_filters() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.filter_input = "id >= 0".to_string();
    app.apply_filter();
    let cancellation = app
        .filtered
        .as_ref()
        .and_then(|filtered| filtered.execution.as_ref())
        .and_then(QueryExecution::cancellation_token)
        .unwrap();

    app.filter_input = "id == 1".to_string();
    app.apply_filter();
    assert!(cancellation.is_cancelled());
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["1"]);
    assert!(app.grid.generation.0 >= 2);
}

#[test]
fn drops_stale_filtered_batches() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.filter_input = "id == 1".to_string();
    app.apply_filter();
    app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
    poll_until_idle(&mut app);

    assert_eq!(app.grid.rows, 0);
}

#[test]
fn runs_sql_through_the_existing_bounded_result_grid() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.sql_input = "SELECT name FROM dataset WHERE id >= 2 LIMIT 2".to_string();

    app.run_sql();
    poll_until_idle(&mut app);

    assert!(app.sql_error.is_none());
    assert_eq!(app.grid.columns, 1);
    assert_eq!(app.grid.rows, 2);
    assert_eq!(filtered_ids(&app), vec!["alpha", "beta"]);
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sql);

    app.sql_input = "SELECT id FROM dataset ORDER BY id DESC LIMIT 2".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["7", "6"]);
    assert_eq!(
        app.sort_keys(),
        [SortSpec::new(0, SortDirection::Descending, NullOrder::Last)]
    );

    app.sql_input = "SELECT COUNT(*), SUM(id) FROM dataset".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none());
    assert_eq!(app.grid.columns, 2);
    assert_eq!(app.grid.rows, 1);
    assert_eq!(filtered_ids(&app), vec!["8"]);
    assert_eq!(app.filtered_cell_text(0, 1).as_deref(), Some("28"));
    assert!(app.filtered.as_ref().is_some_and(|grid| grid.aggregate));
    let fields = app
        .filtered
        .as_ref()
        .unwrap()
        .batches
        .front()
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(fields, vec!["COUNT(*)", "SUM(id)"]);
}

#[test]
fn reports_sql_parse_and_type_errors_without_replacing_results() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    let rows = app.grid.rows;

    for sql in [
        "SELECT FROM dataset",
        "SELECT missing FROM dataset",
        "SELECT * FROM dataset WHERE id LIKE '%2%'",
    ] {
        app.sql_input = sql.to_string();
        app.run_sql();
        assert!(app.filtered.is_none(), "{sql}");
        assert!(app.sql_error.is_some(), "{sql}");
        assert_eq!(app.grid.rows, rows, "{sql}");
    }
    assert_eq!(app.session.query_history.len(), 3);
    assert!(app.session.query_history.iter().all(|entry| !entry.success));
}

#[test]
fn replaces_and_cancels_obsolete_sql_queries() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    let first_sql = "SELECT id FROM dataset";
    app.sql_input = first_sql.to_string();
    app.run_sql();
    let cancellation = app
        .filtered
        .as_ref()
        .and_then(|filtered| filtered.execution.as_ref())
        .and_then(QueryExecution::cancellation_token)
        .unwrap();

    app.sql_input = "SELECT id FROM dataset WHERE id = 1".to_string();
    app.run_sql();
    assert!(cancellation.is_cancelled());
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["1"]);
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sql);
    assert_eq!(app.session.query_history.len(), 2);
    assert_eq!(app.session.query_history[1].sql, first_sql);
    assert!(!app.session.query_history[1].success);
    assert_eq!(
        app.session.query_history[0].sql,
        "SELECT id FROM dataset WHERE id = 1"
    );
    assert!(app.session.query_history[0].success);
}

#[test]
fn restores_session_without_restoring_transient_work() {
    let (_directory, _source, path) = source(3);
    let store = SessionStore::new(path.with_extension("session.json"));
    let mut session = SessionState::default();
    session.record_recent_file(path.clone());
    session.sql_input = "SELECT id FROM dataset LIMIT 1".to_string();
    session.record_query(&session.sql_input.clone(), 5, true, Some(1));
    session.preferences.inspector_visible = false;
    session.preferences.chart_visible = true;
    store.save(&session).unwrap();

    let app = PaviApp::new_with_store(None, store);
    assert_eq!(app.path_input, path.display().to_string());
    assert_eq!(app.sql_input, "SELECT id FROM dataset LIMIT 1");
    assert!(!app.show_inspector);
    assert!(app.chart.visible);
    assert_eq!(app.session.query_history, session.query_history);
    assert!(app.opening.is_some());
    assert!(!app.pages.is_loading());
    assert_eq!(app.pages.loaded_count(), 0);
    assert!(app.filtered.is_none());
    assert!(app.running_sql.is_none());
    assert!(app.grid.selection.is_none());
}

#[test]
fn rejects_stale_sql_results_and_keeps_sql_batches_bounded() {
    let rows = (parquet_reader::PAGE_ROWS * 9 + 1) as usize;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);
    app.sql_input = "SELECT id FROM dataset".to_string();
    app.run_sql();
    app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
    poll_until_idle(&mut app);
    assert_eq!(app.grid.rows, 0);

    app.sql_input = "SELECT id FROM dataset".to_string();
    app.run_sql();
    for _ in 0..9 {
        poll_until_idle(&mut app);
        app.request_more_filtered();
    }
    poll_until_idle(&mut app);
    let sql = app.filtered.as_ref().unwrap();
    assert_eq!(sql.kind, QueryKind::Sql);
    assert!(sql.batches.len() <= MAX_FILTERED_BATCHES);
    assert!(sql.rows <= parquet_reader::PAGE_ROWS * MAX_FILTERED_BATCHES as u64);
}

#[test]
fn cancels_sql_and_returns_to_the_source_grid() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.sql_input = "SELECT id FROM dataset".to_string();
    app.run_sql();

    app.cancel_sql();

    assert!(app.filtered.is_none());
    assert_eq!(app.grid.rows, 8);
    assert_eq!(app.grid.columns, 2);
    assert!(app.status.contains("cancelled"));
}

#[test]
fn sorts_grid_headers_and_toggles_direction_without_a_second_renderer() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);

    app.toggle_sort(0);
    poll_until_idle(&mut app);
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sort);
    assert_eq!(app.sort_heading(0, "id"), "id ↑");
    assert_eq!(
        filtered_ids(&app),
        (0..8).map(|id| id.to_string()).collect::<Vec<_>>()
    );

    app.toggle_sort(0);
    poll_until_idle(&mut app);
    assert_eq!(app.sort_heading(0, "id"), "id ↓");
    assert_eq!(
        filtered_ids(&app),
        (0..8).rev().map(|id| id.to_string()).collect::<Vec<_>>()
    );
}

#[test]
fn cell_menu_filters_to_and_excludes_the_selected_value() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    let ctx = egui::Context::default();
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);

    // A text cell of the file: the value is quoted in the filter box.
    assert!(app.grid.select(2, 1));
    app.cell_action = Some(CellAction::FilterTo);
    app.apply_cell_action(&ctx);
    assert!(app.cell_action.is_none());
    assert_eq!(app.filter_input, "name == alpha");
    poll_until_idle(&mut app);
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Filter);
    assert_eq!(filtered_ids(&app), vec!["0", "2", "4", "6"]);

    // A number cell of that result: the new filter replaces the old one.
    assert!(app.grid.select_filtered(0, 1, 0));
    app.cell_action = Some(CellAction::Exclude);
    app.apply_cell_action(&ctx);
    assert_eq!(app.filter_input, "id != 2");
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["0", "1", "3", "4", "5", "6", "7"]);

    // An aggregate has no file column to filter on: it says why and keeps the result.
    app.sql_input = "SELECT COUNT(*) FROM dataset".into();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.grid.select_filtered(0, 0, 0));
    app.cell_action = Some(CellAction::FilterTo);
    app.apply_cell_action(&ctx);
    assert!(
        app.status.contains("aggregated or joined"),
        "{}",
        app.status
    );
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sql);

    // Nothing selected: nothing to filter by.
    app.grid.selection = None;
    assert!(app.selected_cell_filter(false).is_err());
}

#[test]
fn reports_scan_progress_while_a_sort_or_filter_is_scanning() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    assert_eq!(app.scan_fraction(), None, "nothing is running");

    app.toggle_sort(0);
    let fraction = app.scan_fraction().expect("a sort scan is in flight");
    assert!((0.0..=1.0).contains(&fraction));
    poll_until_idle(&mut app);
    assert_eq!(app.scan_fraction(), None, "the scan has finished");

    app.filter_input = "id >= 2".to_string();
    app.apply_filter();
    assert!(app.scan_fraction().is_some(), "a filter scan is in flight");
    poll_until_idle(&mut app);
    assert_eq!(app.scan_fraction(), None);
}

#[test]
fn chart_reset_only_reports_cleared_results_when_there_were_some() {
    let mut chart = ChartState::default();
    let prompt = chart.status.clone();
    chart.reset();
    assert_eq!(
        chart.status, prompt,
        "nothing was drawn, so nothing was cleared"
    );

    chart.error = Some("bad column".to_string());
    chart.reset();
    assert!(chart.status.contains("cleared"));
    assert!(chart.error.is_none());
}

#[test]
fn cell_menu_adds_to_the_active_filter_and_filters_null_cells() {
    // id 0..6; name is null for id 2 and 5, otherwise alpha / beta by parity.
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("nullable.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let names = (0..6).map(|id| match id {
        2 | 5 => None,
        id if id % 2 == 0 => Some("alpha"),
        _ => Some("beta"),
    });
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(
            &arrow_array::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(StringArray::from_iter(names)),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
    let mut app =
        PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));
    let ctx = egui::Context::default();
    app.begin_open(path);
    poll_until_opened(&mut app);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    let act = |app: &mut PaviApp, action: CellAction| {
        app.cell_action = Some(action);
        app.apply_cell_action(&ctx);
        poll_until_idle(app);
        filtered_ids(app)
    };

    // With no filter applied, "add" is a plain filter.
    assert!(app.active_filter().is_none());
    assert!(app.grid.select(1, 1));
    assert_eq!(act(&mut app, CellAction::AndExclude), ["0", "4"]);
    assert_eq!(app.filter_input, "name != beta");

    // Adding narrows what is applied instead of replacing it.
    assert!(app.grid.select_filtered(0, 1, 0));
    assert_eq!(act(&mut app, CellAction::AndExclude), ["0"]);
    assert_eq!(app.filter_input, "name != beta AND id != 4");
    assert!(app.grid.select_filtered(0, 0, 1));
    assert_eq!(act(&mut app, CellAction::AndFilterTo), ["0"]);
    assert_eq!(
        app.filter_input,
        "name != beta AND id != 4 AND name == alpha"
    );

    // A sort keeps the filter, so adding still works on the sorted result.
    app.sort_column(0, SortDirection::Descending);
    poll_until_idle(&mut app);
    assert!(app.active_filter().is_some());

    // A null cell filters to, or away from, nulls.
    app.clear_filter();
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    assert!(app.grid.select(2, 1));
    assert_eq!(act(&mut app, CellAction::FilterTo), ["2", "5"]);
    assert_eq!(app.filter_input, "name IS NULL");
    app.clear_filter();
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    assert!(app.grid.select(2, 1));
    assert_eq!(act(&mut app, CellAction::Exclude), ["0", "1", "3", "4"]);
    assert_eq!(app.filter_input, "name IS NOT NULL");

    // The filter box takes the whole grammar.
    app.filter_input = "id IN (0, 1, 2, 3) AND (name IS NULL OR name LIKE b%)".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert!(app.filter_error.is_none(), "{:?}", app.filter_error);
    assert_eq!(filtered_ids(&app), ["1", "2", "3"]);
    app.filter_input = "id == 1 AND".to_string();
    app.apply_filter();
    assert!(
        app.filter_error
            .as_deref()
            .is_some_and(|error| error.contains("both sides"))
    );
}

/// Text the app last put on the clipboard.
fn clipboard(ctx: &egui::Context) -> Option<String> {
    ctx.output(|output| {
        output
            .commands
            .iter()
            .rev()
            .find_map(|command| match command {
                egui::OutputCommand::CopyText(text) => Some(text.clone()),
                _ => None,
            })
    })
}

#[test]
fn copies_full_values_with_spreadsheet_quoting() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("notes.parquet");
    let long = "x".repeat(1_000);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(
            &arrow_array::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from_iter_values(0..3)),
                    Arc::new(StringArray::from(vec![long.as_str(), "a\tb", "say \"hi\""])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
    let mut app =
        PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));
    app.begin_open(path);
    poll_until_opened(&mut app);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    let ctx = egui::Context::default();

    // A long value is copied whole, not cut like the grid shows it.
    assert!(app.grid.select(0, 1));
    app.copy_selection(&ctx, false);
    assert_eq!(clipboard(&ctx).as_deref(), Some(long.as_str()));
    assert_eq!(app.status, "Cell copied to clipboard");

    // A range is tab-separated; fields with tabs or quotes are quoted.
    assert!(app.grid.extend_selection(2, 0));
    app.copy_selection(&ctx, false);
    assert_eq!(
        clipboard(&ctx).unwrap(),
        format!("0\t{long}\n1\t\"a\tb\"\n2\t\"say \"\"hi\"\"\"")
    );
    assert_eq!(app.status, "6 cells copied to clipboard");

    assert!(app.grid.select(1, 0));
    app.copy_selection(&ctx, true);
    assert_eq!(clipboard(&ctx).as_deref(), Some("1\t\"a\tb\""));
}

#[test]
fn finds_text_in_loaded_rows_and_scrolls_to_it() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.find.query = "BETA".to_string();
    // With no rows loaded, the whole file is searched (see the next test).
    app.find_next(false);
    assert_eq!(
        app.find.message.as_deref(),
        Some("Searching the whole file…")
    );
    app.find.cancel_search();

    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    // Pretend the grid shows only 100 px right of the row numbers.
    app.grid_view_x = (0.0, 100.0);
    app.find_next(false);
    assert_eq!(app.grid.selection, Some((1, 1)));
    assert_eq!(app.jump_target, Some(1));
    assert_eq!(app.find.message.as_deref(), Some("Row 2, name"));
    // The match is in the second column, past the visible 100 px: scroll to it.
    assert_eq!(app.grid_scroll_x, Some(INITIAL_COLUMN_WIDTH));
    app.find_next(false);
    assert_eq!(app.grid.selection, Some((3, 1)));
    app.find_next(true);
    assert_eq!(app.grid.selection, Some((1, 1)));
    app.find_next(true);
    assert_eq!(app.grid.selection, Some((7, 1)), "wraps around backwards");

    app.find.query = "zzz".to_string();
    app.find_next(false);
    assert_eq!(app.grid.selection, Some((7, 1)));
    // Every row is loaded, so there is nothing more to search.
    assert_eq!(app.find.message.as_deref(), Some("Not in the file"));
    assert!(app.find.searching.is_none());

    // In a filtered result, find looks at the result rows.
    app.filter_input = "id >= 4".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    app.find.query = "alpha".to_string();
    app.find_next(false);
    assert_eq!(app.grid.selection, Some((0, 1)));
    assert_eq!(app.filtered_cell_text(0, 1).as_deref(), Some("alpha"));
    assert_eq!(app.selected_row_view().unwrap().batch.num_rows(), 4);

    // Ctrl+Shift+F opens the find bar instead of focusing the filter.
    app.find = Default::default();
    let context = egui::Context::default();
    let modifiers = egui::Modifiers {
        command: true,
        shift: true,
        ..Default::default()
    };
    context.begin_pass(egui::RawInput {
        modifiers,
        events: vec![egui::Event::Key {
            key: egui::Key::F,
            physical_key: None,
            pressed: true,
            repeat: false,
            modifiers,
        }],
        ..Default::default()
    });
    app.handle_global_shortcuts(&context);
    let _ = context.end_pass();
    assert!(app.find.open && app.find.focus);
}

#[test]
fn row_view_lists_every_shown_column_of_the_selected_row() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    assert!(app.selected_row_view().is_none());
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    assert!(app.grid.select(2, 1));
    let values = |app: &PaviApp| {
        let view = app.selected_row_view().unwrap();
        view.fields
            .iter()
            .map(|(name, column)| {
                format!(
                    "{name}={}",
                    parquet_reader::value::format_cell(
                        view.batch.column(*column).as_ref(),
                        view.offset
                    )
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(values(&app), ["id=2", "name=alpha"]);

    // A SQL result lists the columns the grid shows for it, in the grid's order.
    app.sql_input = "SELECT name, id FROM dataset WHERE id > 5".into();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.grid.select(1, 0));
    assert_eq!(values(&app), ["id=7", "name=beta"]);
}

#[test]
fn pins_columns_and_keeps_them_in_view_when_scrolled_sideways() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path.clone());
    let ctx = egui::Context::default();
    ui::theme::apply(&ctx);
    ctx.style_mut(|style| style.scroll_animation = egui::style::ScrollAnimation::none());
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);

    app.header_action = Some(HeaderAction::Pin(1));
    app.apply_header_action(&ctx);
    assert_eq!(app.pinned_columns(), 1);
    assert_eq!(app.status, "1 column pinned");

    // 300 px is narrower than the row numbers, a 110 px and a 300 px column.
    app.column_layout.set_width(1, 300.0);
    let render = |app: &mut PaviApp| {
        ctx.run(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(300.0, 400.0),
                )),
                ..Default::default()
            },
            |ctx| {
                egui::CentralPanel::default()
                    .frame(egui::Frame::new())
                    .show(ctx, |ui| app.show_grid(ui));
            },
        )
    };
    let texts = |output: &egui::FullOutput, text: &str| {
        output
            .shapes
            .iter()
            .filter_map(|shape| match &shape.shape {
                egui::Shape::Text(shape) if shape.galley.job.text == text => Some(shape.pos.x),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    for _ in 0..2 {
        render(&mut app);
    }
    app.grid_scroll_x = Some(60.0);
    render(&mut app);
    let output = render(&mut app);
    assert!(
        app.grid_view_x.0 > 50.0,
        "the grid scrolled: {:?}",
        app.grid_view_x
    );
    // Row 1's number and its pinned id (0) are drawn again at the left edge.
    let numbers = texts(&output, "1");
    assert!(
        numbers.iter().any(|x| (0.0..44.0).contains(x)),
        "{numbers:?}"
    );
    let ids = texts(&output, "0");
    assert!(ids.iter().any(|x| (44.0..160.0).contains(x)), "{ids:?}");

    // Clicking the pinned cell selects that cell, not the one under it.
    let ids_x = ids.into_iter().fold(f32::MIN, f32::max);
    let click = egui::pos2(ids_x + 4.0, 28.0 + 12.0);
    for pressed in [true, false] {
        let _ = ctx.run(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(300.0, 400.0),
                )),
                events: vec![
                    egui::Event::PointerMoved(click),
                    egui::Event::PointerButton {
                        pos: click,
                        button: egui::PointerButton::Primary,
                        pressed,
                        modifiers: egui::Modifiers::NONE,
                    },
                ],
                ..Default::default()
            },
            |ctx| {
                egui::CentralPanel::default()
                    .frame(egui::Frame::new())
                    .show(ctx, |ui| app.show_grid(ui));
            },
        );
    }
    assert_eq!(app.grid.selection, Some((0, 0)));

    // The pin is saved with the column layout and comes back with the file.
    app.begin_open(path);
    poll_until_opened(&mut app);
    assert_eq!(app.pinned_columns(), 1);
    app.header_action = Some(HeaderAction::Pin(0));
    app.apply_header_action(&ctx);
    assert_eq!(app.pinned_columns(), 0);
    assert_eq!(app.status, "Columns unpinned");
}

#[test]
fn sql_editor_completes_column_names_with_the_keyboard() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.workspace = Workspace::Sql;
    app.sql_input.clear();
    let ctx = egui::Context::default();
    ui::theme::apply(&ctx);
    let key = |key: egui::Key| egui::Event::Key {
        key,
        physical_key: None,
        pressed: true,
        repeat: false,
        modifiers: egui::Modifiers::NONE,
    };
    let frame = |app: &mut PaviApp, events: Vec<egui::Event>| {
        let _ = ctx.run(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(900.0, 600.0),
                )),
                events,
                ..Default::default()
            },
            |ctx| {
                egui::CentralPanel::default().show(ctx, |ui| app.show_sql_workspace(ui));
            },
        );
    };
    frame(&mut app, vec![]);
    ctx.memory_mut(|memory| memory.request_focus(ui::sql::sql_editor_id()));
    frame(&mut app, vec![]);

    frame(&mut app, vec![egui::Event::Text("SELECT na".into())]);
    let completion = app.sql_completion.clone().expect("completions for `na`");
    assert_eq!(completion.items[0].insert, "name");
    // Tab takes the chosen completion instead of inserting a tab.
    frame(&mut app, vec![key(egui::Key::Tab)]);
    assert_eq!(app.sql_input, "SELECT name");
    assert!(app.sql_completion.is_none());
    // Typing goes on after the completed word.
    frame(&mut app, vec![egui::Event::Text(" FROM da".into())]);
    assert_eq!(
        app.sql_completion.as_ref().unwrap().items[0].insert,
        "dataset"
    );
    // Esc closes the list and leaves the text alone.
    frame(&mut app, vec![key(egui::Key::Escape)]);
    assert!(app.sql_completion.is_none());
    assert_eq!(app.sql_input, "SELECT name FROM da");
    // No list inside a string.
    frame(
        &mut app,
        vec![egui::Event::Text("taset WHERE name = 'na".into())],
    );
    assert!(app.sql_completion.is_none());
}

#[test]
fn header_actions_sort_select_and_hide_columns() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    let ctx = egui::Context::default();

    app.header_action = Some(HeaderAction::Select(1));
    app.apply_header_action(&ctx);
    assert_eq!(app.grid.selection, Some((0, 1)));
    assert_eq!(app.selected_source_column(), Some(1));
    assert!(app.filtered.is_none());

    app.header_action = Some(HeaderAction::Sort(0, SortDirection::Descending));
    app.apply_header_action(&ctx);
    assert!(app.header_action.is_none());
    poll_until_idle(&mut app);
    assert_eq!(app.sort_heading(0, "id"), "id ↓");
    assert_eq!(
        filtered_ids(&app),
        (0..8).rev().map(|id| id.to_string()).collect::<Vec<_>>()
    );
    // Selecting a column of a sorted result keeps the result.
    app.header_action = Some(HeaderAction::Select(1));
    app.apply_header_action(&ctx);
    assert_eq!(app.grid.selection.map(|(_, column)| column), Some(1));
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Sort);

    app.header_action = Some(HeaderAction::Hide(1));
    app.apply_header_action(&ctx);
    assert!(app.column_layout.is_hidden(1));
    assert_eq!(app.grid.columns, 1);
}

#[test]
fn sorts_the_whole_file_and_by_several_columns() {
    // Far more rows than the result window holds (spilling past the sort
    // budget is tested in `pavi_query` and `parquet_reader` with small budgets).
    let rows = 150_000;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);

    app.sort_column(0, SortDirection::Descending);
    assert_eq!(app.status, "Sorting by id ↓ (nulls last)");
    poll_until_idle(&mut app);
    let filtered = app.filtered.as_ref().unwrap();
    assert!(filtered.error.is_none(), "{:?}", filtered.error);
    assert_eq!(filtered_ids(&app)[0], (rows - 1).to_string());
    // Rows past the 100,000-row cap sorts once had are reachable: read on to the end.
    for _ in 0..40 {
        app.request_more_filtered();
        poll_until_idle(&mut app);
    }
    let filtered = app.filtered.as_ref().unwrap();
    assert!(filtered.finished);
    assert_eq!(filtered.received_rows(), rows as u64);
    assert_eq!(filtered_ids(&app).last().map(String::as_str), Some("0"));

    // "Then by" keeps the first key and adds a second one.
    let (_small, small, small_path) = self::source(8);
    let mut app = make_app(small, small_path);
    app.sort_column(1, SortDirection::Ascending);
    poll_until_idle(&mut app);
    app.header_action = Some(HeaderAction::ThenSort(0, SortDirection::Descending));
    app.apply_header_action(&egui::Context::default());
    assert_eq!(app.status, "Sorting by name ↑, id ↓ (nulls last)");
    poll_until_idle(&mut app);
    assert_eq!(app.sort_keys().len(), 2);
    assert_eq!(filtered_ids(&app), ["6", "4", "2", "0", "7", "5", "3", "1"]);
    // Clicking a header again sorts by that column alone.
    app.toggle_sort(0);
    poll_until_idle(&mut app);
    assert_eq!(app.sort_keys().len(), 1);
    assert_eq!(filtered_ids(&app), ["0", "1", "2", "3", "4", "5", "6", "7"]);

    // SQL takes several ORDER BY and GROUP BY columns.
    app.sql_input = "SELECT id, name FROM dataset ORDER BY name DESC, id LIMIT 3".into();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert_eq!(filtered_ids(&app), ["1", "3", "5"]);
    app.sql_input = "SELECT name, id, COUNT(*) FROM dataset GROUP BY name, id".into();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert_eq!(app.grid.columns, 3);
    assert_eq!(app.grid.rows, 8);
}

#[test]
fn sorts_an_active_filter_and_rejects_stale_sorted_results() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.filter_input = "id >= 2".to_string();
    app.apply_filter();
    let cancellation = app
        .filtered
        .as_ref()
        .and_then(|filtered| filtered.execution.as_ref())
        .and_then(QueryExecution::cancellation_token)
        .unwrap();

    app.toggle_sort(0);
    assert!(cancellation.is_cancelled());
    poll_until_idle(&mut app);
    assert_eq!(app.filtered.as_ref().unwrap().kind, QueryKind::Filter);
    assert_eq!(filtered_ids(&app), vec!["2", "3", "4", "5", "6", "7"]);

    app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
    app.toggle_sort(0);
    app.grid.generation.0 = app.grid.generation.0.saturating_add(1);
    poll_until_idle(&mut app);
    assert_eq!(app.grid.rows, 0);
}

#[test]
fn two_spellings_of_a_path_are_one_recent_file() {
    let (directory, _source, path) = source(3);
    let mut app =
        PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));
    // The same file, spelled another way.
    let detour = directory.path().join(".").join(path.file_name().unwrap());

    app.begin_open(path.clone());
    poll_until_opened(&mut app);
    app.begin_open(detour);
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 3);
    assert_eq!(app.session.recent_files, vec![path]);
}

#[test]
fn opens_a_file_dropped_on_the_window() {
    let (_directory, _source, path) = source(3);
    let mut app =
        PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));
    let context = egui::Context::default();
    context.begin_pass(egui::RawInput {
        dropped_files: vec![egui::DroppedFile {
            path: Some(path.clone()),
            ..Default::default()
        }],
        ..Default::default()
    });
    app.open_dropped_files(&context);
    let _ = context.end_pass();

    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 3);
    assert_eq!(
        app.dataset.as_ref().map(|dataset| dataset.path.clone()),
        Some(path)
    );
}

#[test]
fn opens_empty_and_replaces_active_document_work() {
    let (_first_directory, _first_source, first_path) = source(3);
    let (_second_directory, _second_source, second_path) = source(0);
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(first_path.with_extension("session.json")),
    );

    app.begin_open(first_path.clone());
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 3);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    app.workspace = Workspace::Sql;

    app.begin_open(second_path);
    assert_eq!(app.pages.loaded_count(), 0);
    assert!(!app.pages.is_loading());
    assert_eq!(app.workspace, Workspace::Grid);
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 0);
    assert!(matches!(app.grid.loading, LoadState::Ready));

    app.begin_open(first_path);
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 3);
}

#[test]
fn reports_malformed_open_and_releases_its_task() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("malformed.parquet");
    std::fs::write(&path, b"not parquet").unwrap();
    let mut app =
        PaviApp::new_with_store(None, SessionStore::new(path.with_extension("session.json")));

    app.begin_open(path);
    poll_until_opened(&mut app);

    assert!(app.dataset.is_none());
    assert!(app.opening.is_none());
    assert!(matches!(app.grid.loading, LoadState::Error(_)));
    assert!(app.status.contains("open file"));
}

#[test]
fn inspector_selection_uses_cached_values_and_document_replacement_clears_it() {
    let (_first_directory, first_source, path) = source(3);
    let mut app = make_app(first_source, path);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    assert!(app.grid.select(1, 0));
    let detail = app.selected_cell_details().unwrap();
    assert_eq!(detail.name, "id");
    assert_eq!(detail.value, "1");

    let (_second_directory, _second_source, second_path) = source(0);
    app.begin_open(second_path);
    assert!(app.grid.selection.is_none());
    assert!(app.selected_cell_details().is_none());
}

#[test]
fn inspector_handles_empty_datasets_without_a_selection() {
    let (_directory, source, path) = source(0);
    let app = make_app(source, path);

    assert_eq!(app.dataset.as_ref().unwrap().source.metadata().row_count, 0);
    assert!(app.selected_cell_details().is_none());
    assert!(app.selected_source_column().is_none());
}

#[test]
fn clamps_untrusted_row_counts_for_the_egui_row_api() {
    assert_eq!(ui_row_count(u64::MAX), usize::MAX);
}

#[test]
fn charts_execute_sql_through_the_existing_runtime_and_reduce_results() {
    let (_directory, source, path) = source(32);
    let mut app = make_app(source, path);
    app.chart.config = ChartConfig {
        kind: ChartKind::Line,
        x_column: "id".to_string(),
        y_columns: vec!["id".to_string()],
        point_limit: 4,
        bins: 4,
        title: "IDs".to_string(),
    };
    app.chart.sql_input = "SELECT id FROM dataset".to_string();

    app.run_chart();
    poll_chart_until_done(&mut app);

    let model = app.chart.model.as_ref().unwrap();
    assert_eq!(model.kind, ChartKind::Line);
    assert_eq!(model.input_rows, 32);
    assert!(model.reduced);
    assert_eq!(model.output_len(), 4);
    assert!(app.chart.error.is_none());
    assert!(app.chart.status.contains("query result"));
}

#[test]
fn charts_support_bar_scatter_and_histogram_query_results() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    for (kind, sql, x, y) in [
        (ChartKind::Bar, "SELECT name, id FROM dataset", "name", "id"),
        (ChartKind::Scatter, "SELECT id FROM dataset", "id", "id"),
        (ChartKind::Histogram, "SELECT id FROM dataset", "id", ""),
    ] {
        app.chart.config = ChartConfig {
            kind,
            x_column: x.to_string(),
            y_columns: vec![y.to_string()],
            point_limit: 8,
            bins: 4,
            title: String::new(),
        };
        app.chart.sql_input = sql.to_string();
        app.run_chart();
        poll_chart_until_done(&mut app);
        assert!(app.chart.error.is_none(), "{kind:?}");
        assert!(
            app.chart
                .model
                .as_ref()
                .is_some_and(|model| !model.is_empty())
        );
    }
}

#[test]
fn chart_current_result_and_errors_are_bounded_and_clear() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.sql_input = "SELECT id FROM dataset LIMIT 3".to_string();
    app.run_sql();
    poll_until_idle(&mut app);

    app.chart.source = ChartSource::CurrentResult;
    app.chart.config = ChartConfig {
        kind: ChartKind::Scatter,
        x_column: "id".to_string(),
        y_columns: vec!["id".to_string()],
        point_limit: 2,
        bins: 2,
        title: String::new(),
    };
    app.run_chart();
    assert_eq!(app.chart.model.as_ref().unwrap().input_rows, 3);
    assert!(app.chart.model.as_ref().unwrap().reduced);

    app.chart.source = ChartSource::Sql;
    app.chart.config.x_column = "missing".to_string();
    app.run_chart();
    poll_chart_until_done(&mut app);
    assert!(app.chart.model.is_none());
    assert!(
        app.chart
            .error
            .as_deref()
            .is_some_and(|error| error.contains("missing"))
    );
}

#[test]
fn chart_replacement_cancellation_and_stale_results_are_safe() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.chart.config = ChartConfig {
        kind: ChartKind::Line,
        x_column: "id".to_string(),
        y_columns: vec!["id".to_string()],
        point_limit: 8,
        bins: 2,
        title: String::new(),
    };
    app.chart.sql_input = "SELECT id FROM dataset".to_string();
    app.run_chart();
    let cancellation = app
        .chart
        .execution
        .as_ref()
        .and_then(QueryExecution::cancellation_token)
        .unwrap();

    app.chart.sql_input = "SELECT id FROM dataset WHERE id = 1".to_string();
    app.run_chart();
    assert!(cancellation.is_cancelled());
    poll_chart_until_done(&mut app);
    assert_eq!(app.chart.model.as_ref().unwrap().input_rows, 1);

    app.chart.sql_input = "SELECT id FROM dataset".to_string();
    app.run_chart();
    app.chart.generation.0 = app.chart.generation.0.saturating_add(1);
    poll_chart_until_done(&mut app);
    assert!(app.chart.model.is_none());
    assert!(app.chart.status.contains("stale"));
}

#[test]
fn chart_surfaces_runtime_read_failures() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path.clone());
    app.chart.config = ChartConfig {
        kind: ChartKind::Line,
        x_column: "id".to_string(),
        y_columns: vec!["id".to_string()],
        point_limit: 8,
        bins: 2,
        title: String::new(),
    };
    std::fs::remove_file(path).unwrap();
    app.run_chart();
    poll_chart_until_done(&mut app);

    assert!(app.chart.model.is_none());
    assert!(
        app.chart
            .error
            .as_deref()
            .is_some_and(|error| error.contains("Chart query"))
    );
}

#[test]
fn chart_sampling_stops_at_the_configured_bounded_input_window() {
    let rows = MAX_INPUT_ROWS + parquet_reader::PAGE_ROWS as usize;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);
    app.chart.config = ChartConfig {
        kind: ChartKind::Line,
        x_column: "id".to_string(),
        y_columns: vec!["id".to_string()],
        point_limit: DEFAULT_POINT_LIMIT,
        bins: 2,
        title: String::new(),
    };
    app.chart.sql_input = "SELECT id FROM dataset".to_string();
    app.run_chart();
    poll_chart_until_done(&mut app);

    let model = app.chart.model.as_ref().unwrap();
    assert_eq!(model.input_rows, MAX_INPUT_ROWS);
    assert!(model.input_capped);
    assert!(model.output_len() <= DEFAULT_POINT_LIMIT);
}

#[test]
fn profiles_selected_columns_through_the_query_runtime_path() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.run_profile(0);
    poll_profile_until_done(&mut app);

    let profile = app.profile.result.as_ref().unwrap();
    assert_eq!(profile.column, 0);
    assert_eq!(profile.row_count, 8);
    assert_eq!(profile.null_count, 0);
    assert_eq!(profile.distinct_count, 8);
    assert!(profile.distribution.is_some());
}

#[test]
fn stale_profile_results_are_ignored_after_replacement() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.run_profile(0);
    app.profile.generation = GenerationId(app.profile.generation.0.saturating_add(1));
    poll_profile_until_done(&mut app);

    assert!(app.profile.result.is_none());
    assert!(app.profile.status.contains("stale"));
}

#[test]
fn layout_changes_project_only_visible_columns_and_clear_page_state() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    assert_eq!(app.pages.get(0).unwrap().batches[0].num_columns(), 2);

    app.column_layout.set_hidden(0, true);
    app.apply_column_layout_change();
    assert_eq!(app.grid.columns, 1);
    assert_eq!(app.pages.loaded_count(), 0);
    assert!(app.grid.selection.is_none());

    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    assert_eq!(app.pages.get(0).unwrap().batches[0].num_columns(), 1);
    assert_eq!(app.cell_text(0, 0).as_deref(), Some("alpha"));
}

#[test]
fn layout_persists_by_schema_without_retaining_grid_pages() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(Arc::clone(&source), path.clone());
    app.column_layout.move_source(1, -1);
    app.column_layout.set_hidden(0, true);
    app.column_layout.set_width(1, 260.0);
    app.save_column_layout();

    let mut reopened = make_app(source, path);
    reopened.restore_column_layout();
    assert_eq!(reopened.column_layout.visible(), &[1]);
    assert_eq!(reopened.column_layout.width(1, 0.0), 260.0);
    assert_eq!(reopened.pages.loaded_count(), 0);
    assert!(!reopened.pages.is_loading());
}

#[test]
fn filtered_layout_maps_display_columns_without_reordering_batches() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.filter_input = "id >= 0".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    app.column_layout.set_hidden(0, true);
    app.apply_column_layout_change();

    let filtered = app.filtered.as_ref().unwrap();
    assert_eq!(filtered.display_columns, vec![(1, 1)]);
    assert_eq!(filtered.batches.front().unwrap().num_columns(), 2);
    assert_eq!(app.filtered_cell_text(0, 1).as_deref(), Some("alpha"));
}

#[test]
fn opens_files_in_tabs_that_keep_their_own_state() {
    let (_first_directory, _first, first_path) = source(8);
    let (_second_directory, _second, second_path) = source(5);
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(first_path.with_extension("session.json")),
    );
    app.open_path(first_path.clone());
    poll_until_opened(&mut app);
    assert_eq!(app.tab_count(), 1, "an empty tab is reused");
    app.filter_input = "id == 2".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);

    // A second file opens in a new tab, which is shown.
    app.open_path(second_path.clone());
    assert_eq!((app.tab_count(), app.tabs.active), (2, 1));
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 5);
    assert!(app.filtered.is_none());
    assert!(app.filter_input.is_empty());

    // The first tab still has its filter and its result.
    app.switch_tab(0);
    assert_eq!(app.filter_input, "id == 2");
    assert_eq!(filtered_ids(&app), vec!["2"]);
    assert_eq!(
        app.session.open_files,
        vec![first_path.clone(), second_path.clone()]
    );
    assert_eq!(app.session.active_file, 0);

    // Opening a file that is already open shows its tab.
    app.open_path(second_path.clone());
    assert_eq!((app.tab_count(), app.tabs.active), (2, 1));

    // Ctrl+Tab moves to the next tab; Ctrl+W closes the open one.
    send_command_key(&mut app, egui::Key::Tab);
    assert_eq!(app.tabs.active, 0);
    send_command_key(&mut app, egui::Key::W);
    assert_eq!(app.tab_count(), 1);
    assert_eq!(
        app.dataset.as_ref().map(|dataset| dataset.path.clone()),
        Some(second_path)
    );
    // Closing the last tab leaves one empty tab.
    app.close_tab(0);
    assert_eq!(app.tab_count(), 1);
    assert!(app.dataset.is_none());
    assert!(app.session.open_files.is_empty());

    // Ctrl+T opens empty tabs, up to the limit.
    send_command_key(&mut app, egui::Key::T);
    assert_eq!(app.tab_count(), 2);
    while app.tab_count() < tabs::MAX_TABS {
        assert!(app.new_tab());
    }
    assert!(!app.new_tab());
    assert!(app.status.contains("close one first"), "{}", app.status);
}

#[test]
fn reopens_the_tabs_open_when_the_app_closed() {
    let (_first_directory, _first, first_path) = source(3);
    let (_second_directory, _second, second_path) = source(4);
    let session = first_path.with_extension("session.json");
    {
        let mut app = PaviApp::new_with_store(None, SessionStore::new(session.clone()));
        app.open_path(first_path.clone());
        poll_until_opened(&mut app);
        app.open_path(second_path.clone());
        poll_until_opened(&mut app);
        app.switch_tab(0);
    }

    let mut app = PaviApp::new_with_store(None, SessionStore::new(session));
    assert_eq!((app.tab_count(), app.tabs.active), (2, 0));
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 3);
    // The other tab finishes opening when it is shown.
    app.switch_tab(1);
    poll_until_opened(&mut app);
    assert_eq!(app.grid.rows, 4);
    assert_eq!(
        app.tab_labels()
            .iter()
            .map(|label| label.path.clone())
            .collect::<Vec<_>>(),
        [Some(first_path), Some(second_path)]
    );
}

#[test]
fn opens_a_partitioned_folder_as_one_table() {
    let directory = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    for (folder, ids) in [("year=2023", 0..3), ("year=2024", 3..5)] {
        let path = directory.path().join(folder).join("part-0.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(
                &arrow_array::RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from_iter_values(ids))],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
    }
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(directory.path().join("session.json")),
    );
    app.open_path(directory.path().to_path_buf());
    poll_until_opened(&mut app);

    assert_eq!(app.grid.rows, 5);
    assert_eq!(
        app.dataset.as_ref().unwrap().column_names,
        ["id".to_string(), "year".to_string()]
    );
    assert!(app.status.ends_with("from 2 files"), "{}", app.status);
    assert!(app.tab_labels()[0].folder);
    assert_eq!(app.session.recent_files, [directory.path().to_path_buf()]);

    app.filter_input = "year == 2024".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["3", "4"]);
}

#[test]
fn opens_a_csv_file_and_filters_it() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("people.csv");
    std::fs::write(&path, "id,name\n1,Ann\n2,Bo\n3,Cy\n").unwrap();
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(directory.path().join("session.json")),
    );
    app.open_path(path.clone());
    poll_until_opened(&mut app);

    assert_eq!(app.grid.rows, 3);
    let dataset = app.dataset.as_ref().unwrap();
    assert_eq!(dataset.source.format(), parquet_reader::FileFormat::Csv);
    assert_eq!(dataset.column_names, ["id".to_string(), "name".to_string()]);
    assert_eq!(app.session.recent_files, [path]);

    app.filter_input = "id >= 2".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["2", "3"]);
}

#[test]
fn keeps_a_url_as_typed_and_reports_when_it_cannot_be_read() {
    use std::io::{BufRead, BufReader, Write};

    // A server that refuses every request with 403.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) > 0 && line != "\r\n" {
                line.clear();
            }
            let mut stream = stream;
            let _ = stream.write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    let directory = TempDir::new().unwrap();
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(directory.path().join("session.json")),
    );
    let url = PathBuf::from(format!("http://{address}/dir/data.parquet?token=secret"));
    app.open_path(url.clone());
    assert_eq!(app.opening_path.as_ref(), Some(&url), "not made absolute");
    // Shown without the query string, which may carry a token.
    assert_eq!(app.tab_labels()[0].title, "data.parquet");
    assert!(!app.status.contains("secret"), "{}", app.status);
    assert!(!app.path_input.contains("secret"), "{}", app.path_input);
    poll_until_opened(&mut app);
    assert!(app.dataset.is_none());
    let LoadState::Error(error) = &app.grid.loading else {
        panic!("expected an open error, got {:?}", app.grid.loading);
    };
    assert!(error.contains("denied"), "{error}");
    assert!(error.contains("/dir/data.parquet"), "{error}");
    assert!(!error.contains("secret"), "{error}");
    assert!(app.session.recent_files.is_empty());
}

#[test]
fn names_open_tabs_as_sql_tables_and_joins_them() {
    use std::path::Path;

    use crate::app::tabs::table_name;

    assert_eq!(
        table_name(Path::new("C:/data/flights.parquet"), &[]),
        "flights"
    );
    assert_eq!(
        table_name(Path::new("s3://b/2023-01 trips.snappy.parquet?sig=1"), &[]),
        "t_2023_01_trips"
    );
    assert_eq!(
        table_name(Path::new("/x/flights.csv"), &["flights".to_string()]),
        "flights_2"
    );
    assert_eq!(
        table_name(Path::new("/x/dataset.parquet"), &[]),
        "dataset_2"
    );

    let directory = TempDir::new().unwrap();
    let write = |name: &str, batch: arrow_array::RecordBatch| {
        let path = directory.path().join(name);
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    };
    let orders = write(
        "orders.parquet",
        arrow_array::RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
            ("customer", Arc::new(Int32Array::from(vec![10, 20, 10]))),
        ])
        .unwrap(),
    );
    let customers = write(
        "customers.parquet",
        arrow_array::RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![10, 20])) as arrow_array::ArrayRef,
            ),
            ("name", Arc::new(StringArray::from(vec!["Ann", "Bo"]))),
        ])
        .unwrap(),
    );
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(directory.path().join("session.json")),
    );
    app.open_path(customers);
    poll_until_opened(&mut app);
    app.open_path(orders);
    poll_until_opened(&mut app);

    let (_, names) = app.sql_tables().unwrap();
    assert_eq!(names, ["customers", "orders"]);
    assert!(app.sql_columns().contains(&"name".to_string()));

    app.sql_input = "SELECT dataset.id, c.name FROM dataset \
                     JOIN customers c ON dataset.customer = c.id ORDER BY dataset.id"
        .to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert_eq!(filtered_ids(&app), vec!["1", "2", "3"]);
    let names = (0..app.grid.rows)
        .map(|row| app.filtered_cell_text(row, 1).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["Ann", "Bo", "Ann"]);

    app.sql_input = "SELECT id FROM dataset WHERE customer IN \
                     (SELECT id FROM customers WHERE name = 'Bo')"
        .to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), vec!["2"]);
}

#[test]
fn profiles_every_column_and_compares_two_tabs() {
    use crate::compare::{KeyMatch, SchemaDiff, key_query};

    let directory = TempDir::new().unwrap();
    let write = |name: &str, batch: arrow_array::RecordBatch| {
        let path = directory.path().join(name);
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    };
    let before = write(
        "before.parquet",
        arrow_array::RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as arrow_array::ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("a"),
                ])),
            ),
        ])
        .unwrap(),
    );
    let after = write(
        "after.parquet",
        arrow_array::RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![3, 4, 5])) as arrow_array::ArrayRef,
            ),
            ("score", Arc::new(Int32Array::from(vec![7, 8, 9]))),
        ])
        .unwrap(),
    );
    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(directory.path().join("session.json")),
    );
    app.open_compare();
    assert!(!app.compare.visible, "nothing to compare with yet");
    app.open_path(after);
    poll_until_opened(&mut app);
    app.open_path(before);
    poll_until_opened(&mut app);

    // Every column in one scan.
    app.run_file_profile();
    assert!(app.profile.file.visible);
    for _ in 0..2_000 {
        app.poll_file_profile();
        if app.profile.file.task.is_none() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        app.profile.file.error.is_none(),
        "{:?}",
        app.profile.file.error
    );
    let profiles = app.profile.file.result.as_ref().unwrap();
    assert_eq!(profiles.len(), 2);
    assert_eq!(profiles[0].min.as_deref(), Some("1"));
    assert_eq!(profiles[0].max.as_deref(), Some("4"));
    assert_eq!(profiles[1].null_count, 1);
    assert_eq!(profiles[1].distinct_count, 2);
    assert!(!profiles[1].distinct_capped);

    // The other tab by its table name, and the rows only this tab has.
    app.open_compare();
    assert!(app.compare.visible);
    let tables = app.compare_tables();
    let names = tables
        .iter()
        .map(|(name, _, _)| name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["after", "before"]);
    let schema = |source: &ParquetSource| {
        source
            .metadata()
            .columns()
            .iter()
            .map(|column| (column.name.clone(), column.data_type.clone()))
            .collect::<Vec<_>>()
    };
    let diff = SchemaDiff::new(&schema(&tables[1].1), &schema(&tables[0].1));
    assert_eq!(diff.key_candidates(), vec!["id"]);
    assert_eq!(diff.only_left.len(), 1);
    assert_eq!(diff.only_right.len(), 1);

    // The windows draw without a real window.
    let context = egui::Context::default();
    ui::theme::apply(&context);
    context.begin_pass(egui::RawInput::default());
    app.show_compare_window(&context);
    app.show_file_profile_window(&context);
    let _ = context.end_pass();
    assert_eq!(app.compare.other.as_deref(), Some("after"));
    assert_eq!(app.compare.key.as_deref(), Some("id"));

    for (matching, expected) in [
        (KeyMatch::OnlyLeft, vec!["1", "2"]),
        (KeyMatch::OnlyRight, vec!["5"]),
        (KeyMatch::Both, vec!["3", "4"]),
    ] {
        app.sql_input = format!("{} ORDER BY id", key_query("id", "after", matching));
        app.run_sql();
        poll_until_idle(&mut app);
        assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
        assert_eq!(filtered_ids(&app), expected, "{matching:?}");
    }
}

#[test]
fn applies_and_saves_the_theme_choice() {
    use crate::session::ThemeChoice;

    let directory = TempDir::new().unwrap();
    let store = SessionStore::new(directory.path().join("session.json"));
    let mut app = PaviApp::new_with_store(None, store.clone());
    let context = egui::Context::default();
    ui::theme::apply(&context);
    // Only this test switches the palette; the dark one is restored before
    // any assertion can fail, so tests running at the same time see dark.
    app.set_theme_choice(ThemeChoice::Light);
    app.apply_theme_choice(&context);
    let light = (
        app.light_applied,
        ui::theme::panel(),
        context.style().visuals.panel_fill,
    );
    app.set_theme_choice(ThemeChoice::System);
    app.apply_theme_choice(&context);
    let system = (app.light_applied, ui::theme::panel());
    app.set_theme_choice(ThemeChoice::Dark);
    app.apply_theme_choice(&context);

    assert_eq!(
        light,
        (Some(true), ui::theme::LIGHT.panel, ui::theme::LIGHT.panel)
    );
    // A context with no system theme reported draws dark.
    assert_eq!(system, (Some(false), ui::theme::DARK.panel));
    assert_eq!(ui::theme::panel(), ui::theme::DARK.panel);
    assert_eq!(
        context.style_of(egui::Theme::Light).visuals.panel_fill,
        ui::theme::DARK.panel
    );
    assert_eq!(store.load().preferences.theme, ThemeChoice::Dark);
    app.set_theme_choice(ThemeChoice::Light);
    assert_eq!(store.load().preferences.theme, ThemeChoice::Light);
}

#[test]
fn only_macos_keeps_the_system_window_frame() {
    use crate::app::native_window_frame_on;
    assert!(native_window_frame_on("macos"));
    for os in ["windows", "linux", "freebsd"] {
        assert!(!native_window_frame_on(os), "{os}");
    }
}

#[test]
fn jumps_anywhere_in_filter_sql_and_sort_results() {
    // 100 row groups of 1,000 rows; every odd id is "beta".
    let rows = 100_000_u64;
    let (_directory, source, path) = source(rows as usize);
    let mut app = make_app(source, path);
    let id_at = |app: &PaviApp, row: u64| {
        let first = app.filtered.as_ref().unwrap().first_result;
        app.filtered_cell_text(row - first, 0).unwrap()
    };

    app.filter_input = "name == 'beta'".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    for _ in 0..2_000 {
        app.poll_filtered();
        if app.filtered.as_ref().unwrap().total().is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(app.filtered.as_ref().unwrap().total(), Some(rows / 2));

    // Far past the rows held: reading starts at the row group that holds the
    // rows just above it, not at the start of the file.
    app.jump_row_input = "40001".to_string();
    app.jump_to_input();
    assert!(
        app.status.starts_with("Going to row 40,001"),
        "{}",
        app.status
    );
    poll_until_jumped(&mut app);
    let filtered = app.filtered.as_ref().unwrap();
    assert_eq!(filtered.first_result, 40_000 - JUMP_CONTEXT_ROWS);
    assert_eq!(app.grid.selection, Some((40_000, 0)));
    assert_eq!(id_at(&app, 40_000), "80001");
    assert_eq!(app.jump_target, Some(40_000));

    // Past the end is refused with the result's size.
    app.jump_row_input = "50001".to_string();
    app.jump_to_input();
    assert_eq!(app.status, "The result has 50,000 rows");

    // SQL isn't counted: forward reads on, dropping the rows in between ...
    let history = app.session.query_history.len();
    app.sql_input = "SELECT id FROM dataset WHERE name = 'beta'".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert_eq!(app.filtered.as_ref().unwrap().total(), None);
    app.jump_to_result(45_000);
    assert!(
        app.status.starts_with("Reading on to row 45,001"),
        "{}",
        app.status
    );
    poll_until_jumped(&mut app);
    assert_eq!(id_at(&app, 45_000), "90001");
    // ... and back reads it again from the start, keeping one history entry.
    // The first run finished once; reading it again added nothing.
    app.jump_to_result(30_000);
    assert!(
        app.status.starts_with("Reading the result again"),
        "{}",
        app.status
    );
    poll_until_jumped(&mut app);
    assert_eq!(id_at(&app, 30_000), "60001");
    assert_eq!(
        app.filtered.as_ref().unwrap().first_result,
        30_000 - JUMP_CONTEXT_ROWS
    );
    for _ in 0..30 {
        poll_until_idle(&mut app);
        app.request_more_filtered();
    }
    assert_eq!(app.session.query_history.len(), history + 1);

    // A sort of the whole file knows its size at once: End is its last row.
    app.cancel_filter();
    app.apply_sort(vec![SortSpec::new(
        0,
        SortDirection::Descending,
        NullOrder::Last,
    )]);
    poll_until_idle(&mut app);
    assert_eq!(app.filtered.as_ref().unwrap().total(), Some(rows));
    send_grid_key(&mut app, egui::Key::End, false);
    poll_until_jumped(&mut app);
    assert_eq!(app.grid.selection.map(|(row, _)| row), Some(rows - 1));
    assert_eq!(id_at(&app, rows - 1), "0");
}

#[test]
fn runs_sql_with_having_order_by_aggregates_and_aliases() {
    let (_directory, source, path) = source(11);
    let mut app = make_app(source, path);
    let column_names = |app: &PaviApp| {
        app.filtered.as_ref().unwrap().batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    let cells = |app: &PaviApp| {
        (0..app.grid.rows)
            .map(|row| {
                (0..app.grid.columns)
                    .map(|column| app.filtered_cell_text(row, column).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    // 6 "alpha" and 5 "beta" rows.
    app.sql_input = "SELECT name, COUNT(*) AS n, COUNT(DISTINCT id) FROM dataset \
                     GROUP BY name HAVING n > 5 ORDER BY n DESC"
        .to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert!(app.filtered.as_ref().unwrap().aggregate);
    assert_eq!(column_names(&app), ["name", "n", "COUNT(DISTINCT id)"]);
    assert_eq!(cells(&app), [["alpha", "6", "6"]]);

    // An alias on a plain column names the result's column, so the grid
    // shows the result as it names it.
    app.sql_input = "SELECT id AS key, name FROM dataset ORDER BY key DESC LIMIT 3".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert!(app.filtered.as_ref().unwrap().aggregate);
    assert_eq!(column_names(&app), ["key", "name"]);
    assert_eq!(
        cells(&app),
        [["10", "alpha"], ["9", "beta"], ["8", "alpha"]]
    );

    app.sql_input = "SELECT DISTINCT name FROM dataset ORDER BY name DESC".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert_eq!(cells(&app), [["beta"], ["alpha"]]);

    // What isn't supported says so under the editor.
    app.sql_input = "SELECT name FROM dataset HAVING name = 'alpha'".to_string();
    app.run_sql();
    assert!(
        app.sql_error
            .as_deref()
            .is_some_and(|error| error.contains("HAVING needs GROUP BY")),
        "{:?}",
        app.sql_error
    );
}

#[test]
fn runs_sql_with_calculations() {
    let (_directory, source, path) = source(11);
    let mut app = make_app(source, path);
    let column_names = |app: &PaviApp| {
        app.filtered.as_ref().unwrap().batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    let cells = |app: &PaviApp| {
        (0..app.grid.rows)
            .map(|row| {
                (0..app.grid.columns)
                    .map(|column| app.filtered_cell_text(row, column).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    // Calculated on the way out: the grid shows the result's own names.
    app.sql_input = "SELECT id, id * 10 AS big, UPPER(name) FROM dataset LIMIT 2".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert!(app.filtered.as_ref().unwrap().aggregate);
    assert_eq!(column_names(&app), ["id", "big", "UPPER(name)"]);
    assert_eq!(cells(&app), [["0", "0", "ALPHA"], ["1", "10", "BETA"]]);

    // Calculated first (WHERE and ORDER BY on calculations).
    app.sql_input = "SELECT id, id % 3 AS rest FROM dataset WHERE id * 2 > 10 \
                     ORDER BY rest DESC, id LIMIT 3"
        .to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert_eq!(
        cells(&app),
        [["8", "2"], ["7", "1"], ["10", "1"]].map(|row| row.to_vec())
    );

    app.sql_input = "SELECT LOWER(id) FROM dataset".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    // Text functions take any value as its text.
    assert_eq!(app.sql_error, None);
    app.sql_input = "SELECT id FROM dataset WHERE name * 2 > 1".to_string();
    app.run_sql();
    assert!(
        app.sql_error
            .as_deref()
            .is_some_and(|error| error.contains("arithmetic needs numbers")),
        "{:?}",
        app.sql_error
    );
}

/// Measures the UI thread's work per frame while wheel-scrolling a wide
/// file: layout and tessellation, as the app does each frame (GPU drawing
/// is not included). A measurement, not a check: run it on purpose with
/// `cargo test --release -p pavi -- --ignored --nocapture frame_time`.
#[test]
#[ignore = "measurement; run with --release --ignored --nocapture"]
fn frame_time_while_scrolling() {
    use std::time::Instant;

    const ROWS: usize = 1_000_000;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("wide.parquet");
    let mut fields = Vec::new();
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::new();
    for column in 0..6 {
        fields.push(Field::new(
            format!("number_{column}"),
            DataType::Int32,
            false,
        ));
        columns.push(Arc::new(Int32Array::from_iter_values(
            (0..ROWS as i32).map(|row| row.wrapping_mul(column + 7)),
        )));
        fields.push(Field::new(format!("text_{column}"), DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from_iter_values(
            (0..ROWS).map(|row| format!("value {row} of column {column}")),
        )));
    }
    let batch = arrow_array::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let source = Arc::new(ParquetSource::open(&path).unwrap());
    let mut app = make_app(source, path);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);

    let ctx = egui::Context::default();
    ui::theme::apply(&ctx);
    let pointer = egui::pos2(600.0, 450.0);
    let mut times = Vec::new();
    for frame in 0..600 {
        // About 20 rows a frame: a fast, steady wheel scroll.
        let events = vec![
            egui::Event::PointerMoved(pointer),
            egui::Event::MouseWheel {
                unit: egui::MouseWheelUnit::Point,
                delta: egui::vec2(0.0, -20.0 * ROW_HEIGHT),
                modifiers: egui::Modifiers::NONE,
            },
        ];
        let started = Instant::now();
        app.pages.begin_frame();
        let _ = app.pages.poll(&mut app.grid);
        let output = ctx.run(grid_frame(f64::from(frame) / 60.0, events), |ctx| {
            app.show_top_bar(ctx);
            app.show_inspector_panel(ctx);
            egui::CentralPanel::default().show(ctx, |ui| app.show_grid(ui));
        });
        let _ = ctx.tessellate(output.shapes, output.pixels_per_point);
        times.push(started.elapsed().as_secs_f64() * 1000.0);
        // Pages arrive between frames, as they do at 60 fps.
        thread::sleep(Duration::from_millis(16));
    }
    let scrolled = app.drawn_rows.map(|(first, _)| first).unwrap_or_default();
    // The first frames lay out fonts and the panels once.
    let mut steady = times.split_off(30);
    steady.sort_by(f64::total_cmp);
    let at = |share: f64| steady[((steady.len() - 1) as f64 * share) as usize];
    println!(
        "frame_time_ms p50 {:.2} p95 {:.2} max {:.2} over {} frames, scrolled to row {scrolled}",
        at(0.5),
        at(0.95),
        at(1.0),
        steady.len()
    );
    assert!(scrolled > 1_000, "the grid scrolled");
}

#[test]
fn reopened_tabs_get_their_filter_and_sort_back() {
    let (directory, _source, path) = source(80);
    let store = SessionStore::new(directory.path().join("session.json"));
    let mut saved = SessionState::default();
    saved.set_open_files(vec![path.clone()], 0);
    saved.set_tab_views(vec![TabView {
        path: path.clone(),
        filter: "id > 70".to_string(),
        sort: vec![
            SavedSortKey {
                column: "id".to_string(),
                descending: true,
            },
            // A column the file no longer has is left out.
            SavedSortKey {
                column: "gone".to_string(),
                descending: false,
            },
        ],
    }]);
    store.save(&saved).unwrap();

    let mut app = PaviApp::new_with_store(None, store.clone());
    poll_until_opened(&mut app);
    poll_until_idle(&mut app);
    let filtered = app.filtered.as_ref().unwrap();
    assert_eq!(filtered.kind, QueryKind::Filter);
    assert_eq!(filtered.filter.as_ref().unwrap().to_string(), "id > 70");
    assert_eq!(
        app.sort_keys(),
        [SortSpec::new(0, SortDirection::Descending, NullOrder::Last)]
    );
    assert_eq!(filtered_ids(&app)[..3], ["79", "78", "77"]);
    assert_eq!(app.filter_input, "id > 70");

    // What the tab shows now is what the next start restores.
    app.persist_session();
    let views = store.load().tab_views;
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].filter, "id > 70");
    assert_eq!(views[0].sort.len(), 1);

    // A SQL result is not kept as a view.
    app.sql_input = "SELECT name FROM dataset".to_string();
    app.run_sql();
    poll_until_idle(&mut app);
    app.persist_session();
    assert!(store.load().tab_views.is_empty());
}

#[test]
fn moves_tabs_and_keeps_the_open_one_shown() {
    let (_directory, source, path) = source(4);
    let mut app = make_app(source, path.clone());
    app.new_tab();
    app.new_tab();
    // Tabs: [file, empty, empty(shown)].
    assert_eq!(app.tabs.active, 2);
    let first = app.tab_labels()[0].id;
    app.move_tab(0, 2);
    assert_eq!(app.tab_labels()[2].id, first);
    assert_eq!(app.tabs.active, 1, "the shown tab moved left by one");
    assert_eq!(app.tab_labels()[2].path.as_deref(), Some(path.as_path()));
    app.move_tab(1, 0);
    assert_eq!(app.tabs.active, 0);
    // Out of range moves are clamped or ignored.
    app.move_tab(0, 99);
    assert_eq!(app.tabs.active, 2);
    app.move_tab(7, 0);
    assert_eq!(app.tab_count(), 3);
}

#[test]
fn number_formats_change_only_what_the_grid_shows() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    app.header_action = Some(HeaderAction::NumberFormat(
        0,
        parquet_reader::NumberFormat {
            decimals: Some(2),
            thousands: false,
        },
    ));
    let ctx = egui::Context::default();
    ui::theme::apply(&ctx);
    app.apply_header_action(&ctx);
    // Text columns have no number format.
    app.column_layout.set_number_format(
        1,
        parquet_reader::NumberFormat {
            decimals: Some(2),
            thousands: false,
        },
    );
    let mut output = None;
    for _ in 0..3 {
        output = Some(ctx.run(grid_frame(0.0, vec![]), |ctx| {
            egui::CentralPanel::default().show(ctx, |ui| app.show_grid(ui));
        }));
    }
    let texts = output
        .unwrap()
        .shapes
        .iter()
        .filter_map(|shape| match &shape.shape {
            egui::Shape::Text(text) => Some(text.galley.job.text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(texts.iter().any(|text| text == "5.00"), "{texts:?}");
    assert!(texts.iter().any(|text| text == "alpha"));
    // The format is saved with the layout, and copying keeps the stored value.
    let layout = app
        .session
        .grid_layout(&PaviApp::schema_key(&["id".to_string(), "name".to_string()]).unwrap())
        .unwrap();
    assert_eq!(layout.number_formats["id"].decimals, Some(2));
    app.grid.select(5, 0);
    app.copy_selection(&ctx, false);
    assert_eq!(app.status, "Cell copied to clipboard");
}

#[test]
fn counts_values_with_a_query() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path);
    app.header_action = Some(HeaderAction::CountValues(1));
    app.apply_header_action(&egui::Context::default());
    assert_eq!(app.workspace, Workspace::Sql);
    assert!(
        app.sql_input.contains("GROUP BY \"name\""),
        "{}",
        app.sql_input
    );
    poll_until_idle(&mut app);
    assert!(app.sql_error.is_none(), "{:?}", app.sql_error);
    assert_eq!(app.grid.rows, 2);
    assert_eq!(app.filtered_cell_text(0, 1).as_deref(), Some("40"));
    assert_eq!(
        count_values_sql("a\"b"),
        "SELECT \"a\"\"b\", COUNT(*) AS count\nFROM dataset\nGROUP BY \"a\"\"b\"\nORDER BY count DESC"
    );
}

#[test]
fn copies_the_selection_as_markdown_json_or_a_sql_list() {
    let (_directory, source, path) = source(8);
    let mut app = make_app(source, path);
    app.request_page(0);
    poll_until_page_loaded(&mut app, 0);
    let ctx = egui::Context::default();
    app.grid.select(0, 0);
    app.grid.extend_selection(2, 1);
    app.copy_selection_as(&ctx, false, CopyFormat::Markdown);
    assert_eq!(app.status, "3 rows copied as a Markdown table");
    app.copy_selection_as(&ctx, false, CopyFormat::Json);
    assert_eq!(app.status, "3 rows copied as JSON");
    app.copy_selection_as(&ctx, false, CopyFormat::SqlList);
    assert_eq!(app.status, "5 values copied as a SQL IN list");
}

#[test]
fn zoom_is_restored_bounded_and_saved() {
    let (_directory, source, path) = source(4);
    let mut app = make_app(source, path);
    app.session.preferences.zoom = 1.5;
    let ctx = egui::Context::default();
    // A zoom set in one frame takes effect at the start of the next.
    let frame = |app: &mut PaviApp| {
        let _ = ctx.run(egui::RawInput::default(), |ctx| app.apply_zoom(ctx));
    };
    frame(&mut app);
    frame(&mut app);
    assert_eq!(ctx.zoom_factor(), 1.5);
    app.set_zoom(&ctx, 7.0);
    frame(&mut app);
    assert_eq!(ctx.zoom_factor(), MAX_ZOOM);
    assert_eq!(app.session.preferences.zoom, MAX_ZOOM);
    // A zoom changed with egui's own keys is picked up and kept in bounds.
    ctx.set_zoom_factor(0.2);
    frame(&mut app);
    frame(&mut app);
    assert_eq!(ctx.zoom_factor(), MIN_ZOOM);
    assert_eq!(app.session_store.load().preferences.zoom, MIN_ZOOM);
}

#[test]
fn f1_opens_the_shortcuts_list() {
    let (_directory, source, path) = source(4);
    let mut app = make_app(source, path);
    let ctx = egui::Context::default();
    let press = |key| egui::Event::Key {
        key,
        physical_key: None,
        pressed: true,
        repeat: false,
        modifiers: egui::Modifiers::NONE,
    };
    let _ = ctx.run(grid_frame(0.0, vec![press(egui::Key::F1)]), |ctx| {
        app.handle_global_shortcuts(ctx);
    });
    assert!(app.show_shortcuts);
    let mut output = None;
    // A window lays itself out in its first frame and shows in the next.
    for time in [0.1, 0.15] {
        output = Some(ctx.run(grid_frame(time, vec![]), |ctx| {
            ui::shortcuts::window(ctx, &mut app.show_shortcuts);
        }));
    }
    let output = output.unwrap();
    assert!(output.shapes.iter().any(|shape| matches!(
        &shape.shape,
        egui::Shape::Text(text) if text.galley.job.text == "Move the tab left / right"
    )));
    let _ = ctx.run(grid_frame(0.2, vec![press(egui::Key::Escape)]), |ctx| {
        ui::shortcuts::window(ctx, &mut app.show_shortcuts);
    });
    assert!(!app.show_shortcuts);
}

#[test]
fn a_restart_brings_back_filter_sort_and_number_formats() {
    let (directory, _source, path) = source(80);
    let store = SessionStore::new(directory.path().join("session.json"));
    let mut app = PaviApp::new_with_store(Some(path.clone()), store.clone());
    poll_until_opened(&mut app);
    let ctx = egui::Context::default();
    app.header_action = Some(HeaderAction::NumberFormat(
        0,
        parquet_reader::NumberFormat {
            decimals: Some(2),
            thousands: true,
        },
    ));
    app.apply_header_action(&ctx);
    app.filter_input = "id > 70".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    app.sort_column(0, SortDirection::Descending);
    poll_until_idle(&mut app);
    drop(app);

    let saved = store.load();
    assert_eq!(saved.tab_views.len(), 1, "{:?}", saved.tab_views);
    let mut app = PaviApp::new_with_store(None, store);
    poll_until_opened(&mut app);
    poll_until_idle(&mut app);
    assert_eq!(
        app.column_layout
            .number_format(0)
            .map(|format| format.decimals),
        Some(Some(2))
    );
    assert_eq!(app.filter_input, "id > 70");
    assert_eq!(filtered_ids(&app)[0], "79");
}

#[test]
fn tells_once_about_a_crash_in_the_last_run() {
    let (directory, source, path) = source(4);
    let mut app = make_app(source, path);
    let logs = directory.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    app.report_previous_crash(&logs);
    assert_eq!(app.notice, None, "no report, no notice");
    std::fs::write(
        logs.join("crash-20261119-120000.txt"),
        crate::logging::crash_report_text("boom", "frames"),
    )
    .unwrap();
    app.report_previous_crash(&logs);
    assert!(
        app.notice
            .as_deref()
            .unwrap()
            .contains("closed unexpectedly")
    );
    assert!(app.session.last_crash_report > 0);
    // The next start doesn't tell again about the same report.
    app.notice = None;
    app.report_previous_crash(&logs);
    assert_eq!(app.notice, None);
    assert_eq!(
        app.session_store.load().last_crash_report,
        app.session.last_crash_report
    );
}

/// Runs change checks, each one interval after the last on `clock`, until one
/// finds a change or `checks` have found none.
fn poll_file_watch_until_changed(app: &mut PaviApp, clock: &mut Instant, checks: u32) -> bool {
    let mut sent = 0;
    for _ in 0..2_000 {
        app.poll_file_watch(*clock);
        if app.file_watch.changed.is_some() {
            return true;
        }
        if app.file_watch.is_idle() {
            if sent == checks {
                return false;
            }
            sent += 1;
            *clock += watch::CHANGE_CHECK_INTERVAL;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("change checks did not finish");
}

#[test]
fn notices_a_file_changed_on_disk_and_reloads_it_with_its_filter_and_sort() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path.clone());
    app.filter_input = "id > 70".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    app.apply_sort(vec![SortSpec::new(
        0,
        SortDirection::Descending,
        NullOrder::Last,
    )]);
    poll_until_idle(&mut app);
    let mut clock = Instant::now();
    assert!(!poll_file_watch_until_changed(&mut app, &mut clock, 3));

    write_rows(&path, 120);
    assert!(poll_file_watch_until_changed(&mut app, &mut clock, 3));
    let changed = app.file_watch.changed.clone().unwrap();
    assert!(
        changed.contains("filters.parquet changed on disk"),
        "{changed}"
    );
    // Once it has noticed, it stops checking.
    assert_eq!(app.poll_file_watch(Instant::now()), None);

    app.reload_document();
    poll_until_opened(&mut app);
    poll_until_idle(&mut app);
    assert_eq!(app.file_watch.changed, None);
    assert_eq!(app.dataset.as_ref().unwrap().source.row_count(), 120);
    assert_eq!(app.filter_input, "id > 70");
    assert_eq!(
        app.sort_keys(),
        [SortSpec::new(0, SortDirection::Descending, NullOrder::Last)]
    );
    assert_eq!(filtered_ids(&app)[..3], ["119", "118", "117"]);
    assert!(!poll_file_watch_until_changed(&mut app, &mut clock, 2));
}

#[test]
fn a_page_of_a_changed_file_is_not_read_again_every_frame() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path.clone());
    write_rows(&path, 120);
    app.request_page(0);
    for _ in 0..1_000 {
        if let Some(status) = app.pages.poll(&mut app.grid) {
            assert!(status.contains("changed on disk"), "{status}");
            // Still marked as requested, so the grid does not ask for it again.
            assert!(app.grid.requested.contains(&0));
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("the page read did not fail");
}

#[test]
fn a_change_check_of_a_file_no_longer_shown_is_dropped() {
    let (_other_directory, other, other_path) = source(20);
    let (_directory, source, path) = source(10);
    let mut app = make_app(source, path.clone());
    write_rows(&path, 30);
    // A check of the changed file is sent, and the tab shows another file
    // before it is picked up.
    app.poll_file_watch(Instant::now());
    assert!(!app.file_watch.is_idle());
    app.dataset = Some(Dataset {
        source: other,
        path: other_path,
        column_names: vec!["id".to_string(), "name".to_string()],
    });
    for _ in 0..200 {
        app.poll_file_watch(Instant::now());
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(app.file_watch.changed, None);
}

#[test]
fn reads_a_file_with_the_import_options_chosen_and_keeps_them() {
    let directory = TempDir::new().unwrap();
    let path = std::path::absolute(directory.path().join("export.txt")).unwrap();
    std::fs::write(&path, b"1;M\xfcnchen\n2;K\xf6ln\n3;Wien\n").unwrap();
    let store = SessionStore::new(directory.path().join("session.json"));
    let mut app = PaviApp::new_with_store(None, store.clone());
    // Read as Parquet by its extension, it can't be opened.
    app.open_path(path.clone());
    poll_until_opened(&mut app);
    assert!(app.dataset.is_none());
    assert_eq!(app.failed_path.as_deref(), Some(path.as_path()));

    app.open_import_dialog(path.clone());
    let dialog = app.import_dialog.as_mut().unwrap();
    dialog.format = Some(parquet_reader::FileFormat::Csv);
    dialog.delimiter = crate::import::DelimiterChoice::Semicolon;
    dialog.header = false;
    let choice = dialog.choice().unwrap();
    app.import_dialog = None;
    app.apply_import(path.clone(), choice);
    poll_until_opened(&mut app);
    let dataset = app.dataset.as_ref().unwrap();
    assert_eq!(dataset.column_names, ["column_1", "column_2"]);
    let report = dataset.source.import_report().unwrap();
    assert_eq!(
        report.encoding,
        Some(parquet_reader::TextEncoding::Windows1252)
    );
    assert_eq!(app.tab_count(), 1, "read again in the same tab");

    // Reload reads it the same way, and keeps the filter.
    app.filter_input = "column_1 >= 2".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    app.reload_document();
    poll_until_opened(&mut app);
    poll_until_idle(&mut app);
    assert_eq!(app.dataset.as_ref().unwrap().source.row_count(), 3);
    assert_eq!(filtered_ids(&app), ["2", "3"]);

    // The next start reads it the same way too.
    app.persist_session();
    assert_eq!(
        store
            .load()
            .saved_import(&path)
            .map(|saved| saved.delimiter.clone()),
        Some(";".to_string())
    );
    let mut restarted = PaviApp::new_with_store(None, store.clone());
    poll_until_opened(&mut restarted);
    assert_eq!(
        restarted.dataset.as_ref().unwrap().column_names,
        ["column_1", "column_2"]
    );

    // Guessing everything again forgets the choice.
    restarted.apply_import(path.clone(), crate::import::ImportChoice::default());
    poll_until_opened(&mut restarted);
    assert!(restarted.session.saved_import(&path).is_none());
    assert!(restarted.dataset.is_none());
}

#[test]
fn opens_an_excel_workbook_and_offers_its_sheets() {
    let directory = TempDir::new().unwrap();
    let path = std::path::absolute(directory.path().join("book.xlsx")).unwrap();
    let batch = arrow_array::RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    let mut writer = parquet_reader::export::BatchWriter::new(
        parquet_reader::export::ExportFormat::Xlsx,
        File::create(&path).unwrap(),
        batch.schema(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let mut app = PaviApp::new_with_store(
        None,
        SessionStore::new(directory.path().join("session.json")),
    );
    app.open_path(path.clone());
    poll_until_opened(&mut app);
    let dataset = app.dataset.as_ref().unwrap();
    assert_eq!(dataset.source.format(), parquet_reader::FileFormat::Excel);
    assert_eq!(dataset.column_names, ["id", "name"]);
    app.filter_input = "id >= 2".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert_eq!(filtered_ids(&app), ["2", "3"]);

    app.open_import_dialog(path);
    let dialog = app.import_dialog.as_ref().unwrap();
    assert_eq!(dialog.sheets, ["Data"]);
    assert_eq!(
        dialog.read_as.as_deref(),
        Some("Excel, sheet Data (1 of 1), header row")
    );
}

fn poll_until_found(app: &mut PaviApp) {
    for _ in 0..2_000 {
        app.poll_find();
        if app.find.searching.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("the file search did not finish");
}

#[test]
fn finds_text_in_rows_not_loaded_by_searching_the_whole_file() {
    let (_directory, source, path) = source(50_000);
    let mut app = make_app(source, path);
    app.open_find();
    app.find.query = "BETA".to_string();
    // No rows are loaded, so the file itself is searched.
    app.find_next(false);
    assert!(app.find.searching.is_some());
    poll_until_found(&mut app);
    assert_eq!(app.grid.selection, Some((1, 1)));
    assert_eq!(app.jump_target, Some(1));
    assert_eq!(app.find.message.as_deref(), Some("Row 2, name"));

    app.find_next(false);
    poll_until_found(&mut app);
    assert_eq!(app.grid.selection, Some((3, 1)));
    app.find_next(true);
    poll_until_found(&mut app);
    assert_eq!(app.grid.selection, Some((1, 1)));
    // From the first match, back wraps around to the last.
    app.find_next(true);
    poll_until_found(&mut app);
    assert_eq!(app.grid.selection, Some((49_999, 1)));

    app.find.query = "gamma".to_string();
    app.find_next(false);
    poll_until_found(&mut app);
    assert_eq!(app.find.message.as_deref(), Some("Not in the file"));

    // Closing the find bar stops a search on its way.
    app.find.query = "alpha".to_string();
    app.find_next(false);
    app.find.cancel_search();
    assert!(app.find.searching.is_none());
}

#[test]
fn shows_the_rows_where_any_column_contains_the_find_text() {
    let (_directory, source, path) = source(80);
    let mut app = make_app(source, path);
    app.filter_input = "id < 10".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    app.find.query = "beta".to_string();
    app.show_find_matches();
    poll_until_idle(&mut app);
    // On top of the filter already applied.
    assert_eq!(app.filter_input, "id < 10 AND * contains beta");
    assert_eq!(filtered_ids(&app), ["1", "3", "5", "7", "9"]);

    // Numbers are searched as the grid shows them.
    app.filter_input = "* contains 7".to_string();
    app.apply_filter();
    poll_until_idle(&mut app);
    assert_eq!(
        filtered_ids(&app),
        [
            "7", "17", "27", "37", "47", "57", "67", "70", "71", "72", "73", "74", "75", "76",
            "77", "78", "79"
        ]
    );
}
