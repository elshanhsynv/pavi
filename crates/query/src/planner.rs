use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_schema::{Schema, SchemaRef};
use parquet_reader::{AggregateSpec, FilterExpr, ParquetSource, Projection, SortSpec};

use crate::LogicalPlan;

pub struct PhysicalPlan {
    pub(crate) source: Arc<ParquetSource>,
    pub(crate) projection: Projection,
    pub(crate) filter: Option<FilterExpr>,
    pub(crate) limit: Option<usize>,
    pub(crate) sort: Option<SortSpec>,
    pub(crate) aggregate: Option<AggregateSpec>,
}

pub struct Planner;

impl PhysicalPlan {
    pub fn output_schema(&self) -> Result<SchemaRef> {
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

    pub fn sort(&self) -> Option<SortSpec> {
        self.sort
    }

    pub fn aggregate(&self) -> Option<&AggregateSpec> {
        self.aggregate.as_ref()
    }
}

impl Planner {
    pub fn plan(logical: &LogicalPlan) -> Result<PhysicalPlan> {
        let mut state = PlanState::default();
        collect(logical, &mut state)?;
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
        if let Some(sort) = state.sort {
            source.validate_sort(sort).context("validate query sort")?;
        }
        if let Some(aggregate) = &state.aggregate {
            if has_projection {
                bail!("projection cannot be combined with aggregate output");
            }
            if state.sort.is_some() {
                bail!("ORDER BY aggregate results is not supported");
            }
            source
                .validate_aggregate(aggregate)
                .context("validate query aggregate")?;
        }
        Ok(PhysicalPlan {
            source,
            projection,
            filter: state.filter,
            limit: state.limit,
            sort: state.sort,
            aggregate: state.aggregate,
        })
    }
}

#[derive(Default)]
struct PlanState {
    source: Option<Arc<ParquetSource>>,
    columns: Option<Vec<usize>>,
    filter: Option<FilterExpr>,
    limit: Option<usize>,
    sort: Option<SortSpec>,
    aggregate: Option<AggregateSpec>,
}

fn collect(plan: &LogicalPlan, state: &mut PlanState) -> Result<()> {
    match plan {
        LogicalPlan::Scan(scan) => {
            if state.source.replace(Arc::clone(&scan.source)).is_some() {
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
            if state.sort.replace(sort.spec()).is_some() {
                bail!("query plan contains more than one sort");
            }
        }
        LogicalPlan::Aggregate { input, aggregate } => {
            collect(input, state)?;
            if state.aggregate.replace(aggregate.spec().clone()).is_some() {
                bail!("query plan contains more than one aggregate");
            }
        }
    }
    Ok(())
}
