use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_schema::{Schema, SchemaRef};
use parquet_reader::{
    AggregateSpec, FilterExpr, ParquetSource, Projection, SortSpec, aggregate_output_schema,
    check_aggregate, check_sort,
};

use crate::{
    LogicalPlan,
    logical::{Output, Prepared},
};

pub struct PhysicalPlan {
    pub(crate) source: Arc<ParquetSource>,
    pub(crate) projection: Projection,
    pub(crate) filter: Option<FilterExpr>,
    pub(crate) limit: Option<usize>,
    /// Sort keys, most significant first; empty when there is no sort.
    pub(crate) sort: Vec<SortSpec>,
    pub(crate) aggregate: Option<AggregateSpec>,
    /// `HAVING`, a sort of the result and aliases, applied before `limit`.
    pub(crate) output: Option<Output>,
}

pub struct Planner;

impl PhysicalPlan {
    pub fn output_schema(&self) -> Result<SchemaRef> {
        let schema = self.unnamed_schema()?;
        match &self.output {
            Some(output) => {
                let schema = output.computed_schema(&schema)?.unwrap_or(schema);
                output.rename(&schema)
            }
            None => Ok(schema),
        }
    }

    /// Whether aliases rename the result's columns.
    pub fn renames_columns(&self) -> bool {
        self.output.as_ref().is_some_and(Output::renames)
    }

    /// The result's schema before aliases rename it.
    fn unnamed_schema(&self) -> Result<SchemaRef> {
        if let Some(aggregate) = &self.aggregate {
            return self.source.aggregate_schema(aggregate);
        }
        let fields = self
            .projection
            .as_slice()
            .iter()
            .map(|column| {
                self.source
                    .schema()
                    .fields()
                    .get(*column)
                    .cloned()
                    .context("projected column is out of range")
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub fn projected_columns(&self) -> &[usize] {
        self.projection.as_slice()
    }

    pub fn filter(&self) -> Option<&FilterExpr> {
        self.filter.as_ref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Sort keys, most significant first; empty when there is no sort.
    pub fn sort(&self) -> &[SortSpec] {
        &self.sort
    }

    pub fn aggregate(&self) -> Option<&AggregateSpec> {
        self.aggregate.as_ref()
    }
}

impl Planner {
    /// Checks a plan against its input's schema without reading anything, and
    /// returns the schema of its output. Works for plans whose rows still need
    /// preparing (joins, subqueries).
    pub fn validate(logical: &LogicalPlan) -> Result<SchemaRef> {
        let mut state = PlanState::default();
        collect(logical, &mut state)?;
        let schema = match (&state.source, &state.prepared) {
            (Some(source), None) => source.schema(),
            (None, Some(prepared)) => Arc::clone(&prepared.schema),
            _ => bail!("query plan must read exactly one table"),
        };
        let column_count = schema.fields().len();
        let projection = match &state.columns {
            Some(columns) => Some(
                Projection::columns(columns.clone(), column_count)
                    .context("validate query projection")?,
            ),
            None => None,
        };
        if let Some(filter) = &state.filter {
            filter
                .validate_schema(&schema)
                .context("validate query filter")?;
        }
        if let Some(sort) = &state.sort {
            check_sort(&schema, sort).context("validate query sort")?;
        }
        let result = if let Some(aggregate) = &state.aggregate {
            if projection.is_some() {
                bail!("projection cannot be combined with aggregate output");
            }
            if state.sort.is_some() {
                bail!("sort an aggregate's results with ORDER BY on its result columns");
            }
            check_aggregate(&schema, aggregate).context("validate query aggregate")?;
            aggregate_output_schema(&schema, aggregate)?
        } else {
            let projection = projection.unwrap_or_else(|| Projection::all(column_count));
            Arc::new(Schema::new(
                projection
                    .as_slice()
                    .iter()
                    .map(|column| schema.field(*column).clone())
                    .collect::<Vec<_>>(),
            ))
        };
        match &state.output {
            Some(output) => check_output(output, &result, state.aggregate.is_some()),
            None => Ok(result),
        }
    }

    pub fn plan(logical: &LogicalPlan) -> Result<PhysicalPlan> {
        let mut state = PlanState::default();
        collect(logical, &mut state)?;
        if state.prepared.is_some() {
            bail!("this query's joined or subquery rows must be prepared before it is planned");
        }
        let source = state.source.context("query plan has no scan")?;
        let has_projection = state.columns.is_some();
        let projection = match state.columns {
            Some(columns) => Projection::columns(columns, source.column_count())
                .context("validate query projection")?,
            None => Projection::all(source.column_count()),
        };
        if let Some(filter) = &state.filter {
            source
                .validate_filter(filter)
                .context("validate query filter")?;
        }
        if let Some(sort) = &state.sort {
            source.validate_sort(sort).context("validate query sort")?;
        }
        if let Some(aggregate) = &state.aggregate {
            if has_projection {
                bail!("projection cannot be combined with aggregate output");
            }
            if state.sort.is_some() {
                bail!("sort an aggregate's results with ORDER BY on its result columns");
            }
            source
                .validate_aggregate(aggregate)
                .context("validate query aggregate")?;
        }
        let plan = PhysicalPlan {
            source,
            projection,
            filter: state.filter,
            limit: state.limit,
            sort: state.sort.unwrap_or_default(),
            aggregate: state.aggregate,
            output: None,
        };
        if let Some(output) = &state.output {
            check_output(
                output,
                plan.unnamed_schema()?.as_ref(),
                plan.aggregate.is_some(),
            )?;
        }
        Ok(PhysicalPlan {
            output: state.output,
            ..plan
        })
    }
}

/// Checks `HAVING`, the result sort and aliases against the result's schema
/// before renaming, and returns the schema after it.
fn check_output(output: &Output, result: &Schema, aggregate: bool) -> Result<SchemaRef> {
    if !aggregate && (output.having.is_some() || !output.sort.is_empty()) {
        bail!("HAVING and ORDER BY on result columns need GROUP BY or an aggregate");
    }
    let computed = output
        .computed_schema(result)
        .context("validate the calculations")?;
    let renamed = output.rename(computed.as_deref().unwrap_or(result))?;
    if let Some(having) = &output.having {
        having
            .to_filter(&renamed)?
            .validate_schema(&renamed)
            .context("validate HAVING")?;
    }
    if !output.sort.is_empty() {
        check_sort(&renamed, &output.sort).context("validate ORDER BY")?;
    }
    Ok(renamed)
}

#[derive(Default)]
struct PlanState {
    source: Option<Arc<ParquetSource>>,
    prepared: Option<Prepared>,
    columns: Option<Vec<usize>>,
    filter: Option<FilterExpr>,
    limit: Option<usize>,
    sort: Option<Vec<SortSpec>>,
    aggregate: Option<AggregateSpec>,
    output: Option<Output>,
}

fn collect(plan: &LogicalPlan, state: &mut PlanState) -> Result<()> {
    match plan {
        LogicalPlan::Scan(scan) => {
            if state.prepared.is_some() || state.source.replace(Arc::clone(&scan.source)).is_some()
            {
                bail!("query plan contains more than one scan");
            }
        }
        LogicalPlan::Prepared(prepared) => {
            if state.source.is_some() || state.prepared.replace(prepared.clone()).is_some() {
                bail!("query plan contains more than one scan");
            }
        }
        LogicalPlan::Projection { input, projection } => {
            collect(input, state)?;
            if state.columns.replace(projection.columns.clone()).is_some() {
                bail!("query plan contains more than one projection");
            }
        }
        LogicalPlan::Filter { input, filter } => {
            collect(input, state)?;
            if state.filter.replace(filter.expression.clone()).is_some() {
                bail!("query plan contains more than one filter");
            }
        }
        LogicalPlan::Limit { input, limit } => {
            collect(input, state)?;
            if state.limit.replace(limit.rows).is_some() {
                bail!("query plan contains more than one limit");
            }
        }
        LogicalPlan::Sort { input, sort } => {
            collect(input, state)?;
            if state.sort.replace(sort.keys().to_vec()).is_some() {
                bail!("query plan contains more than one sort");
            }
        }
        LogicalPlan::Aggregate { input, aggregate } => {
            collect(input, state)?;
            if state.aggregate.replace(aggregate.spec().clone()).is_some() {
                bail!("query plan contains more than one aggregate");
            }
        }
        LogicalPlan::Output { input, output } => {
            collect(input, state)?;
            if state.output.replace(output.clone()).is_some() {
                bail!("query plan contains more than one output step");
            }
        }
    }
    Ok(())
}
