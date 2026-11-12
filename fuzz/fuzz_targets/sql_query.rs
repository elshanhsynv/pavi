//! Arbitrary text in the SQL workspace: parse, plan, then run the plan's read directly
//! against a small fixture file and format the result. Joins and subqueries
//! (ADR 0009) run through the query engine, which prepares their rows first.
#![no_main]

use libfuzzer_sys::fuzz_target;
use parquet_reader::{GroupBudget, Projection, SortBudget, SortOutput};
use pavi_query::{Planner, QueryEngine, SqlAst};
use pavi_runtime::GenerationId;

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
    if logical.is_prepared() {
        let engine = QueryEngine::from_handle(pavi_fuzz::runtime());
        let Ok(mut execution) = engine.execute(&logical, GenerationId(0)) else {
            return;
        };
        let mut rows = 0;
        while let Ok(Some(batch)) = execution.next_batch() {
            pavi_fuzz::format_all(&batch.batch);
            rows += batch.batch.num_rows();
            if rows > 256 {
                execution.cancel();
                break;
            }
        }
        return;
    }
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
    } else if !plan.sort().is_empty() {
        // A small budget, so larger fixtures also exercise the spill and merge.
        let budget = SortBudget::new(16, 16 * 1024 * 1024).expect("budget");
        source
            .read_sorted(
                plan.filter(),
                &projection,
                plan.sort(),
                budget,
                plan.limit(),
                &never,
            )
            .and_then(|output| match output {
                SortOutput::InMemory(batches) => Ok(batches),
                SortOutput::Spilled(mut merge) => {
                    let (batches, _) = merge.next_rows(64, &|| false)?;
                    Ok(batches)
                }
            })
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
