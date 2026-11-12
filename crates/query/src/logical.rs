use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use arrow_schema::SchemaRef;
use parquet_reader::{
    AggregateExpr, AggregateFunction, AggregateSpec, FilterExpr, NullOrder, ParquetSource,
    SortDirection, SortSpec,
};

use crate::prep::{Prep, Relation};

#[derive(Clone)]
pub struct Scan {
    pub(crate) source: Arc<ParquetSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Projection {
    pub(crate) columns: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Filter {
    pub(crate) expression: FilterExpr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limit {
    pub(crate) rows: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sort {
    /// Keys, most significant first.
    pub(crate) keys: Vec<SortSpec>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aggregate {
    pub(crate) spec: AggregateSpec,
}

/// Rows prepared before the query runs: joined tables and subqueries (ADR 0009).
/// `schema` is what they will look like; column indexes above this leaf refer
/// to its columns.
#[derive(Clone)]
pub struct Prepared {
    pub(crate) prep: Arc<Prep>,
    pub(crate) schema: SchemaRef,
}

#[derive(Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Prepared(Prepared),
    Projection {
        input: Box<LogicalPlan>,
        projection: Projection,
    },
    Filter {
        input: Box<LogicalPlan>,
        filter: Filter,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Limit,
    },
    Sort {
        input: Box<LogicalPlan>,
        sort: Sort,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        aggregate: Aggregate,
    },
}

impl Scan {
    pub fn new(source: Arc<ParquetSource>) -> Self {
        Self { source }
    }
}

impl Projection {
    pub fn new(columns: impl Into<Vec<usize>>) -> Self {
        Self {
            columns: columns.into(),
        }
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }
}

impl Filter {
    pub fn new(expression: FilterExpr) -> Self {
        Self { expression }
    }

    pub fn parse(expression: &str) -> Result<Self> {
        Ok(Self::new(FilterExpr::parse(expression)?))
    }
}

impl Limit {
    pub fn new(rows: usize) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }
}

impl Sort {
    pub fn new(column: usize, direction: SortDirection, nulls: NullOrder) -> Self {
        Self::by(vec![SortSpec::new(column, direction, nulls)])
    }

    /// A sort by several keys, most significant first.
    pub fn by(keys: Vec<SortSpec>) -> Self {
        Self { keys }
    }

    pub fn keys(&self) -> &[SortSpec] {
        &self.keys
    }
}

impl Aggregate {
    pub fn new(spec: AggregateSpec) -> Self {
        Self { spec }
    }

    pub fn spec(&self) -> &AggregateSpec {
        &self.spec
    }
}

impl LogicalPlan {
    pub fn scan(source: Arc<ParquetSource>) -> Self {
        Self::Scan(Scan::new(source))
    }

    pub fn project(self, columns: impl Into<Vec<usize>>) -> Self {
        Self::Projection {
            input: Box::new(self),
            projection: Projection::new(columns),
        }
    }

    pub fn filter(self, filter: Filter) -> Self {
        Self::Filter {
            input: Box::new(self),
            filter,
        }
    }

    pub fn limit(self, rows: usize) -> Self {
        Self::Limit {
            input: Box::new(self),
            limit: Limit::new(rows),
        }
    }

    pub fn sort(self, column: usize, direction: SortDirection, nulls: NullOrder) -> Self {
        self.sort_by(vec![SortSpec::new(column, direction, nulls)])
    }

    /// Sorts by several keys, most significant first.
    pub fn sort_by(self, keys: Vec<SortSpec>) -> Self {
        Self::Sort {
            input: Box::new(self),
            sort: Sort::by(keys),
        }
    }

    pub fn aggregate(self, expressions: impl Into<Vec<AggregateExpr>>) -> Result<Self> {
        Ok(Self::Aggregate {
            input: Box::new(self),
            aggregate: Aggregate::new(AggregateSpec::new(expressions)?),
        })
    }

    pub fn aggregate_grouped(
        self,
        group_by: impl Into<Vec<usize>>,
        expressions: impl Into<Vec<AggregateExpr>>,
    ) -> Result<Self> {
        Ok(Self::Aggregate {
            input: Box::new(self),
            aggregate: Aggregate::new(AggregateSpec::grouped_by(group_by, expressions)?),
        })
    }

    /// Whether the query joins tables or uses subqueries, so its rows are
    /// prepared before it runs and its columns are not one table's (ADR 0009).
    pub fn is_prepared(&self) -> bool {
        self.prepared().is_some()
    }

    /// The prepared leaf, when the plan has one.
    pub(crate) fn prepared(&self) -> Option<&Prepared> {
        match self {
            Self::Prepared(prepared) => Some(prepared),
            Self::Scan(_) => None,
            Self::Projection { input, .. }
            | Self::Filter { input, .. }
            | Self::Limit { input, .. }
            | Self::Sort { input, .. }
            | Self::Aggregate { input, .. } => input.prepared(),
        }
    }

    fn shapes_output(&self) -> bool {
        match self {
            Self::Projection { .. } | Self::Aggregate { .. } => true,
            Self::Scan(_) | Self::Prepared(_) => false,
            Self::Filter { input, .. } | Self::Limit { input, .. } | Self::Sort { input, .. } => {
                input.shapes_output()
            }
        }
    }

    /// The plan to run once the prepared rows are ready: the prepared leaf
    /// becomes a scan of `relation`, and column indexes are mapped to its columns.
    pub(crate) fn bind(&self, relation: &Relation) -> Result<Self> {
        self.bind_inner(relation, !self.shapes_output())
    }

    fn bind_inner(&self, relation: &Relation, reshape: bool) -> Result<Self> {
        let map = |column: usize| {
            relation
                .columns
                .get(column)
                .copied()
                .ok_or_else(|| anyhow!("prepared column {column} out of range"))
        };
        Ok(match self {
            Self::Prepared(_) => {
                let mut plan = Self::scan(Arc::clone(&relation.source));
                if let Some(filter) = &relation.filter {
                    plan = plan.filter(Filter::new(filter.clone()));
                }
                if reshape {
                    plan = plan.project(relation.columns.clone());
                }
                plan
            }
            Self::Scan(_) => bail!("a prepared query also reads a table directly"),
            Self::Projection { input, projection } => Self::Projection {
                input: Box::new(input.bind_inner(relation, reshape)?),
                projection: Projection::new(
                    projection
                        .columns
                        .iter()
                        .map(|column| map(*column))
                        .collect::<Result<Vec<_>>>()?,
                ),
            },
            Self::Filter { input, filter } => Self::Filter {
                input: Box::new(input.bind_inner(relation, reshape)?),
                filter: filter.clone(),
            },
            Self::Limit { input, limit } => Self::Limit {
                input: Box::new(input.bind_inner(relation, reshape)?),
                limit: *limit,
            },
            Self::Sort { input, sort } => Self::Sort {
                input: Box::new(input.bind_inner(relation, reshape)?),
                sort: Sort::by(
                    sort.keys
                        .iter()
                        .map(|key| Ok(SortSpec::new(map(key.column)?, key.direction, key.nulls)))
                        .collect::<Result<Vec<_>>>()?,
                ),
            },
            Self::Aggregate { input, aggregate } => {
                let spec = aggregate.spec();
                let expressions = spec
                    .expressions()
                    .iter()
                    .map(|expression| {
                        Ok(match (expression.function(), expression.column()) {
                            (AggregateFunction::Count, None) => AggregateExpr::count_all(),
                            (AggregateFunction::Count, Some(column)) => {
                                AggregateExpr::count(map(column)?)
                            }
                            (AggregateFunction::Sum, Some(column)) => {
                                AggregateExpr::sum(map(column)?)
                            }
                            (AggregateFunction::Avg, Some(column)) => {
                                AggregateExpr::avg(map(column)?)
                            }
                            (AggregateFunction::Min, Some(column)) => {
                                AggregateExpr::min(map(column)?)
                            }
                            (AggregateFunction::Max, Some(column)) => {
                                AggregateExpr::max(map(column)?)
                            }
                            (_, None) => *expression,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let group_by = spec
                    .group_by()
                    .iter()
                    .map(|column| map(*column))
                    .collect::<Result<Vec<_>>>()?;
                Self::Aggregate {
                    input: Box::new(input.bind_inner(relation, reshape)?),
                    aggregate: Aggregate::new(AggregateSpec::grouped_by(group_by, expressions)?),
                }
            }
        })
    }

    /// Replaces the plan's sort (if any) with `keys`, keeping `Limit` outside it.
    pub fn replace_sort(self, keys: Vec<SortSpec>) -> Self {
        match self {
            Self::Limit { input, limit } => Self::Limit {
                input: Box::new(input.replace_sort(keys)),
                limit,
            },
            Self::Sort { input, .. } => input.replace_sort(keys),
            plan => Self::Sort {
                input: Box::new(plan),
                sort: Sort::by(keys),
            },
        }
    }
}
