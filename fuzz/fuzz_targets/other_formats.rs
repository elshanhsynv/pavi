//! Arbitrary bytes as a CSV, TSV, JSON or Arrow file (the first byte picks which): open it,
//! which reads it into a temporary Parquet file (ADR 0007), then read the first page and
//! format every cell. A panic, hang or runaway allocation here is a bug a user could hit by
//! opening a file.
#![no_main]

use std::fs;

use libfuzzer_sys::fuzz_target;
use parquet_reader::{ParquetSource, Projection};

fuzz_target!(
    init: pavi_fuzz::allow_contained_panics(),
    |data: &[u8]| pavi_fuzz::run(|| check(data))
);

fn check(data: &[u8]) {
    let Some((&selector, contents)) = data.split_first() else {
        return;
    };
    let extension = pavi_fuzz::OTHER_FORMAT_EXTENSIONS
        [usize::from(selector) % pavi_fuzz::OTHER_FORMAT_EXTENSIONS.len()];
    let path = pavi_fuzz::scratch_dir().join(format!("input.{extension}"));
    fs::write(&path, contents).expect("write fuzz input");
    let Ok(source) = ParquetSource::open(&path) else {
        return;
    };
    let projection = Projection::all(source.column_count());
    if let Ok(page) = source.read_page(0, &projection) {
        for batch in &page.batches {
            pavi_fuzz::format_all(batch);
        }
    }
}
