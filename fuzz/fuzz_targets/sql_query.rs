//! Arbitrary text in the SQL workspace: parse, plan, then run the plan's read directly
//! against a small fixture file and format the result.
#![no_main]

use libfuzzer_sys::fuzz_target;
use parquet_reader::{GroupBudget, Projection, SortBudget};
use pavi_query::{Planner, SqlAst};

fuzz_target!(
    init: pavi_fuzz::allow_contained_panics(),
    |data: &[u8]| pavi_fuzz::run(|| check(data))
);

fn check(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(ast) = SqlAst::parse(text) else {
        return;
    };
    let source = pavi_fuzz::fixture_source();
    let Ok(logical) = ast.to_logical_plan(source.clone()) else {
        return;
    };
    let Ok(plan) = Planner::plan(&logical) else {
        return;
    };
    let _ = plan.output_schema();
    let Ok(projection) =
        Projection::columns(plan.projected_columns().to_vec(), source.column_count())
    else {
        return;
    };
    let never = |_: u64| false;
    let batches = if let Some(aggregate) = plan.aggregate() {
        let budget = GroupBudget::new(1_000, 1024 * 1024).expect("budget");
        source
            .read_aggregated(plan.filter(), aggregate, budget, &never)
            .map(|batch| vec![batch])
    } else if let Some(sort) = plan.sort() {
        let budget = SortBudget::new(10_000, 16 * 1024 * 1024).expect("budget");
        source.read_sorted(
            plan.filter(),
            &projection,
            sort,
            budget,
            plan.limit(),
            &never,
        )
    } else if let Some(filter) = plan.filter() {
        source
            .read_filtered_from(filter, 0, 64, &projection, &never)
            .map(|window| vec![window.batch])
    } else {
        let rows = plan.limit().unwrap_or(64).min(64);
        source
            .read_window(0, rows, &projection)
            .map(|batch| vec![batch])
    };
    for batch in batches.iter().flatten() {
        pavi_fuzz::format_all(batch);
    }
}
