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
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
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
    let source = Arc::new(ParquetSource::open(&path).unwrap());
    (directory, source, path)
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
    send_grid_key(&mut app, egui::Key::Home, false);
    assert_eq!(app.grid.selection, Some((first, 1)));
    send_grid_key(&mut app, egui::Key::ArrowUp, false);
    assert_eq!(
        app.grid.selection,
        Some((first, 1)),
        "stays inside the window"
    );
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
        app.filtered.as_ref().and_then(|filtered| filtered.sort),
        Some(SortSpec::new(0, SortDirection::Descending, NullOrder::Last))
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
    assert_eq!(app.filter_input, "name == \"alpha\"");
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
    assert!(app.status.contains("Aggregated"), "{}", app.status);
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
fn sort_without_a_limit_returns_the_first_rows_instead_of_a_budget_error() {
    let rows = SORT_ROW_CAP + 500;
    let (_directory, source, path) = source(rows);
    let mut app = make_app(source, path);

    app.sort_column(0, SortDirection::Descending);
    poll_until_idle(&mut app);
    let filtered = app.filtered.as_ref().unwrap();
    assert!(filtered.error.is_none(), "{:?}", filtered.error);
    assert!(filtered.sort_capped);
    assert_eq!(filtered_ids(&app)[0], (rows - 1).to_string());
    assert!(app.status.contains("first 100,000 rows"));

    // A SQL LIMIT is the user's own cap and is left alone.
    app.sql_input = "SELECT id FROM dataset ORDER BY id LIMIT 5".into();
    app.run_sql();
    poll_until_idle(&mut app);
    app.toggle_sort(0);
    poll_until_idle(&mut app);
    assert!(!app.filtered.as_ref().unwrap().sort_capped);
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
    app.open_dropped_file(&context);
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
        y_column: "id".to_string(),
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
            y_column: y.to_string(),
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
        y_column: "id".to_string(),
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
        y_column: "id".to_string(),
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
        y_column: "id".to_string(),
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
        y_column: "id".to_string(),
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
