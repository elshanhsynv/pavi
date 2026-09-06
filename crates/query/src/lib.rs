//! A bounded logical query layer over PAVI data sources.

mod execution;
mod logical;
mod planner;
mod sql;

pub use execution::{QueryBatch, QueryEngine, QueryExecution, QueryPoll};
pub use logical::{Filter, Limit, LogicalPlan, Projection, Scan};
pub use planner::{PhysicalPlan, Planner};
pub use sql::{SqlAst, SqlPredicate, SqlProjection};
