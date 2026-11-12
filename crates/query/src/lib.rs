//! A bounded logical query layer over PAVI data sources.

mod execution;
mod logical;
mod planner;
mod prep;
mod sql;

pub use execution::{QueryBatch, QueryEngine, QueryExecution, QueryPoll};
pub use logical::{Aggregate, Filter, Limit, LogicalPlan, Prepared, Projection, Scan, Sort};
pub use parquet_reader::{
    AggregateExpr, AggregateFunction, AggregateSpec, GroupBudget, NullOrder, SortBudget,
    SortDirection, SortSpec,
};
pub use planner::{PhysicalPlan, Planner};
pub use prep::{MAX_JOINS, MAX_SUBQUERY_DEPTH};
pub use sql::{
    SqlAggregate, SqlAggregateExpr, SqlAst, SqlColumn, SqlFrom, SqlJoin, SqlPredicate,
    SqlProjection, SqlRelation, SqlSort, SqlTable, Tables,
};
