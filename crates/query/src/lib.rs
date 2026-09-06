//! A bounded logical query layer over PAVI data sources.

mod execution;
mod logical;
mod planner;
mod sql;

pub use execution::{QueryBatch, QueryEngine, QueryExecution, QueryPoll};
pub use logical::{Aggregate, Filter, Limit, LogicalPlan, Projection, Scan, Sort};
pub use parquet_reader::{
    AggregateExpr, AggregateFunction, AggregateSpec, GroupBudget, NullOrder, SortBudget,
    SortDirection, SortSpec,
};
pub use planner::{PhysicalPlan, Planner};
pub use sql::{SqlAggregate, SqlAggregateExpr, SqlAst, SqlPredicate, SqlProjection, SqlSort};
