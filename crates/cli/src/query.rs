//! `--sql`: the app's SQL subset, run by `pavi_query` on the worker pool
//! (ADR 0011). The main thread waits for each result batch.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet_reader::ParquetSource;
use pavi_query::{QueryEngine, SqlAst, Tables, table_name};
use pavi_runtime::{GenerationId, Runtime, RuntimeConfig};

use crate::Window;

/// The tables a query can name: `dataset` and `dataset_path`'s own name for
/// the main file, then each `--table [NAME=]PATH`, opened here.
pub fn tables(
    dataset: Arc<ParquetSource>,
    dataset_path: &Path,
    specs: &[String],
) -> Result<Tables> {
    let mut names = Vec::new();
    let own_name = table_name(dataset_path, &names);
    names.push(own_name.clone());
    let mut tables = Tables::new(Arc::clone(&dataset)).with(own_name, dataset);

    for spec in specs {
        let (name, path) = match spec.split_once('=') {
            Some((name, path)) if is_table_name(name) => (Some(name), path),
            _ => (None, spec.as_str()),
        };
        let source = ParquetSource::open(path).with_context(|| format!("open --table {path}"))?;
        let name = match name {
            Some(name) => {
                if name.eq_ignore_ascii_case("dataset")
                    || names.iter().any(|taken| taken.eq_ignore_ascii_case(name))
                {
                    bail!(
                        "--table {name} is already a table; the tables are {}",
                        tables.names().collect::<Vec<_>>().join(", ")
                    );
                }
                name.to_owned()
            }
            None => table_name(Path::new(path), &names),
        };
        names.push(name.clone());
        tables = tables.with(name, Arc::new(source));
    }
    Ok(tables)
}

/// A name `NAME=PATH` may give: letters, digits and `_`, not starting with a
/// digit. Anything else before a `=` is part of the path.
fn is_table_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A parsed and planned query, ready to run.
pub struct Query {
    plan: pavi_query::LogicalPlan,
    pub schema: SchemaRef,
}

/// Parses and plans `sql`; every SQL error is reported here, before any output.
pub fn plan(sql: &str, tables: &Tables) -> Result<Query> {
    let plan = SqlAst::parse(sql)?.to_logical_plan_with(tables)?;
    let schema = pavi_query::Planner::validate(&plan)?;
    Ok(Query { plan, schema })
}

impl Query {
    /// Runs the query and hands the rows inside `window` to `emit`. Stops the
    /// execution once the window is full.
    pub fn run(
        &self,
        mut window: Window,
        emit: &mut dyn FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        // Declared first so it outlives the execution that uses its workers.
        let runtime = Runtime::new(RuntimeConfig::default()).context("start query workers")?;
        let engine = QueryEngine::new(&runtime);
        let mut execution = engine.execute(&self.plan, GenerationId(1))?;
        while !window.is_done() {
            let Some(batch) = execution.next_batch()? else {
                return Ok(());
            };
            if let Some(batch) = window.take(&batch.batch) {
                emit(RecordBatch::try_new(
                    self.schema.clone(),
                    batch.columns().to_vec(),
                )?)?;
            }
        }
        execution.cancel();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_identifiers_as_table_names() {
        assert!(is_table_name("airports"));
        assert!(is_table_name("_t2"));
        assert!(!is_table_name("2t"));
        assert!(!is_table_name("C:\\data\\a"));
        assert!(!is_table_name(""));
    }
}
