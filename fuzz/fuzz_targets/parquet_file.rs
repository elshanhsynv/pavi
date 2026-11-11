//! Arbitrary bytes as a Parquet file: open it, read the first and last page, run a sort and
//! an aggregate, and format every cell. A panic, hang or runaway allocation here is a bug a
//! user could hit by opening a file.
#![no_main]

use std::{fs, path::PathBuf, sync::OnceLock};

use libfuzzer_sys::fuzz_target;
use parquet_reader::{
    AggregateExpr, AggregateSpec, GroupBudget, NullOrder, PAGE_ROWS, ParquetSource, Projection,
    SortBudget, SortDirection, SortSpec,
};

fn input_path() -> &'static PathBuf {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| pavi_fuzz::scratch_dir().join("input.parquet"))
}

fuzz_target!(
    init: pavi_fuzz::allow_contained_panics(),
    |data: &[u8]| pavi_fuzz::run(|| check(data))
);

fn check(data: &[u8]) {
    let path = input_path();
    fs::write(path, data).expect("write fuzz input");
    let Ok(source) = ParquetSource::open(path) else {
        return;
    };
    let _ = source.metadata();
    let columns = source.column_count();
    let projection = Projection::all(columns);
    let pages = source.row_count().div_ceil(PAGE_ROWS);
    for page in [0, pages.saturating_sub(1)] {
        if let Ok(page) = source.read_page(page, &projection) {
            for batch in &page.batches {
                pavi_fuzz::format_all(batch);
            }
        }
    }
    if columns == 0 {
        return;
    }
    let never = |_: u64| false;
    let sort = SortSpec::new(0, SortDirection::Descending, NullOrder::Last);
    let sort_budget = SortBudget::new(10_000, 16 * 1024 * 1024).expect("budget");
    let _ = source.read_sorted(None, &projection, sort, sort_budget, Some(10), &never);
    let group_budget = GroupBudget::new(1_000, 1024 * 1024).expect("budget");
    if let Ok(spec) = AggregateSpec::grouped(0, [AggregateExpr::count_all(), AggregateExpr::max(0)])
    {
        let _ = source.read_aggregated(None, &spec, group_budget, &never);
    }
}
