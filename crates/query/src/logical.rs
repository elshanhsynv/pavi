use std::sync::Arc;

use anyhow::Result;
use parquet_reader::{
    AggregateExpr, AggregateSpec, FilterExpr, NullOrder, ParquetSource, SortDirection, SortSpec,
};

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

#[derive(Clone)]
pub enum LogicalPlan {
    Scan(Scan),
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
