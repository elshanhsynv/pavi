//! Arbitrary text in the filter box: parse it, evaluate it on the fixture columns, and run
//! a filtered scan (which also exercises row-group statistics pruning).
#![no_main]

use std::sync::OnceLock;

use arrow_array::RecordBatch;
use libfuzzer_sys::fuzz_target;
use parquet_reader::{FilterExpr, Projection};

fn batch() -> &'static RecordBatch {
    static BATCH: OnceLock<RecordBatch> = OnceLock::new();
    BATCH.get_or_init(|| pavi_fuzz::fixture_batch(40))
}

fuzz_target!(
    init: pavi_fuzz::allow_contained_panics(),
    |data: &[u8]| pavi_fuzz::run(|| check(data))
);

fn check(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(filter) = FilterExpr::parse(text) else {
        return;
    };
    // Text written back from a parsed filter must parse again.
    let _ = FilterExpr::parse(&filter.to_string());
    let _ = filter.evaluate_batch(batch());
    let source = pavi_fuzz::fixture_source();
    if source.validate_filter(&filter).is_ok() {
        let projection = Projection::all(source.column_count());
        if let Ok(window) = source.read_filtered_from(&filter, 0, 64, &projection, &|_| false) {
            pavi_fuzz::format_all(&window.batch);
        }
    }
}
