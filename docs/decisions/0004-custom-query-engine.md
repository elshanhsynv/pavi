# 0004. Small custom query engine

- Status: accepted; queries over several tables are amended by [0009](0009-joins-and-subqueries.md)
- Date: 2026-11-08 (recorded after the fact)

## Context
PAVI needs filtering, single-column sort, simple aggregates and a small SQL subset over one Parquet file, with strict memory caps, cancellation within a batch, and incremental results the grid can poll.

## Decision
Parse SQL with `sqlparser` only, then build our own `LogicalPlan` → flat `PhysicalPlan` → incremental `QueryExecution` on top of `parquet_reader`. Do not depend on DataFusion, DuckDB or Polars.

## Consequences
- Full control over memory budgets, cancellation points and how results stream to the UI; smaller binary and build.
- Every SQL feature is ours to build and test. Unsupported SQL must fail with a clear parse error, never silently run differently.
- Revisit with a new ADR if requirements grow to joins, multiple files or arbitrary expressions.
