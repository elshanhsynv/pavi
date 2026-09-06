use std::sync::Arc;

use anyhow::Result;
use parquet_reader::{FilterExpr, ParquetSource};

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
    pub fn parse(expression: &str) -> Result<Self> {
        Ok(Self {
            expression: FilterExpr::parse(expression)?,
        })
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
}
