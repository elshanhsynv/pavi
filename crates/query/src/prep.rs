//! Preparing the rows a query reads when it joins tables or uses subqueries
//! (ADR 0009). The preparation runs on its own thread, so subqueries can drive
//! the worker pool without waiting on themselves, and produces a [`Relation`]:
//! a source (often a temporary table) with a filter and the columns to read.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_schema::Schema;
use parquet_reader::{
    Cancelled, FilterExpr, FilterOp, JoinKind, JoinSide, ParquetSource, TempTableWriter,
    value::scalar_text,
};
use pavi_runtime::{GenerationId, RuntimeHandle};

use crate::{LogicalPlan, QueryEngine};

/// Subqueries inside subqueries, counting the outermost query as 0.
pub const MAX_SUBQUERY_DEPTH: usize = 3;
/// `JOIN`s in one query.
pub const MAX_JOINS: usize = 4;
/// Progress is kept in millionths.
const PROGRESS_SCALE: f64 = 1_000_000.0;

/// How to build the rows a query reads.
pub(crate) enum Prep {
    /// Some columns of an open table.
    Table {
        source: Arc<ParquetSource>,
        columns: Vec<usize>,
    },
    /// Some columns of a subquery's rows, written to a temporary table.
    Query {
        plan: LogicalPlan,
        columns: Vec<usize>,
    },
    /// The input's rows that pass `filter`.
    Filter {
        input: Box<Prep>,
        filter: PrepFilter,
    },
    /// A join; `left_keys` and `right_keys` index the inputs' columns, and
    /// `names` name the output columns.
    Join {
        left: Box<Prep>,
        right: Box<Prep>,
        kind: JoinKind,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        names: Vec<String>,
    },
}

/// A `WHERE` filter over a prepared input; columns index the input's columns.
pub(crate) enum PrepFilter {
    Compare {
        column: usize,
        op: FilterOp,
        value: PrepValue,
    },
    And(Vec<PrepFilter>),
    Or(Vec<PrepFilter>),
}

pub(crate) enum PrepValue {
    Literal(String),
    /// A subquery returning one row and one column.
    Subquery(LogicalPlan),
}

/// Rows ready to query: `source` read with `filter`, keeping `columns`.
pub(crate) struct Relation {
    pub(crate) source: Arc<ParquetSource>,
    pub(crate) filter: Option<FilterExpr>,
    pub(crate) columns: Vec<usize>,
}

impl PrepFilter {
    /// Checks the filter against the input's columns before running anything:
    /// literal comparisons fully, subquery comparisons by their shape.
    pub(crate) fn validate(&self, input: &Schema) -> Result<()> {
        match self {
            Self::Compare { column, op, value } => {
                let field = input
                    .fields()
                    .get(*column)
                    .ok_or_else(|| anyhow!("filter column {column} out of range"))?;
                match value {
                    PrepValue::Literal(text) => {
                        FilterExpr::compare(field.name().clone(), op.clone(), text.clone())
                            .validate_schema(input)
                    }
                    PrepValue::Subquery(plan) => {
                        let schema = crate::Planner::validate(plan)?;
                        if schema.fields().len() != 1 {
                            bail!(
                                "a subquery compared with {} must select exactly one column",
                                field.name()
                            );
                        }
                        Ok(())
                    }
                }
            }
            Self::And(parts) | Self::Or(parts) => {
                parts.iter().try_for_each(|part| part.validate(input))
            }
        }
    }

    /// The reader's filter over `relation`, running subqueries for their values.
    fn resolve(&self, relation: &Relation, context: &PrepContext) -> Result<FilterExpr> {
        match self {
            Self::Compare { column, op, value } => {
                let source_column = relation
                    .columns
                    .get(*column)
                    .ok_or_else(|| anyhow!("filter column {column} out of range"))?;
                let name = relation
                    .source
                    .schema()
                    .field(*source_column)
                    .name()
                    .clone();
                let value = match value {
                    PrepValue::Literal(text) => Some(text.clone()),
                    PrepValue::Subquery(plan) => scalar_value(plan, context)?,
                };
                Ok(match value {
                    Some(value) => FilterExpr::compare(name, op.clone(), value),
                    // A comparison with NULL is never true.
                    None => FilterExpr::And(vec![
                        FilterExpr::compare(name.clone(), FilterOp::IsNull, String::new()),
                        FilterExpr::compare(name, FilterOp::IsNotNull, String::new()),
                    ]),
                })
            }
            Self::And(parts) => Ok(FilterExpr::And(
                parts
                    .iter()
                    .map(|part| part.resolve(relation, context))
                    .collect::<Result<_>>()?,
            )),
            Self::Or(parts) => Ok(FilterExpr::Or(
                parts
                    .iter()
                    .map(|part| part.resolve(relation, context))
                    .collect::<Result<_>>()?,
            )),
        }
    }
}

/// What a preparation shares with the thread that waits for it.
struct PrepContext {
    runtime: RuntimeHandle,
    cancelled: Arc<AtomicBool>,
    progress: Arc<AtomicU64>,
    /// Joins in the whole preparation, and how many have finished.
    joins: usize,
    joins_done: std::cell::Cell<usize>,
}

impl PrepContext {
    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

fn count_joins(prep: &Prep) -> usize {
    match prep {
        Prep::Table { .. } | Prep::Query { .. } => 0,
        Prep::Filter { input, .. } => count_joins(input),
        Prep::Join { left, right, .. } => 1 + count_joins(left) + count_joins(right),
    }
}

fn run(prep: &Prep, context: &PrepContext) -> Result<Relation> {
    if context.cancelled() {
        return Err(Cancelled.into());
    }
    match prep {
        Prep::Table { source, columns } => Ok(Relation {
            source: Arc::clone(source),
            filter: None,
            columns: columns.clone(),
        }),
        Prep::Query { plan, columns } => Ok(Relation {
            source: query_table(plan, context)?,
            filter: None,
            columns: columns.clone(),
        }),
        Prep::Filter { input, filter } => {
            let mut relation = run(input, context)?;
            let expression = filter.resolve(&relation, context)?;
            relation
                .source
                .validate_filter(&expression)
                .context("validate a filter with a subquery's value")?;
            relation.filter = Some(match relation.filter.take() {
                Some(existing) => FilterExpr::And(vec![existing, expression]),
                None => expression,
            });
            Ok(relation)
        }
        Prep::Join {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            names,
        } => {
            let left = run(left, context)?;
            let right = run(right, context)?;
            let side = |relation: Relation, keys: &[usize], output: bool| -> Result<JoinSide> {
                let keys = keys
                    .iter()
                    .map(|key| {
                        relation
                            .columns
                            .get(*key)
                            .copied()
                            .ok_or_else(|| anyhow!("join key {key} out of range"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(JoinSide {
                    source: relation.source,
                    filter: relation.filter,
                    columns: if output { relation.columns } else { Vec::new() },
                    keys,
                })
            };
            let outputs_right = matches!(kind, JoinKind::Inner | JoinKind::Left);
            let left = side(left, left_keys, true)?;
            let right = side(right, right_keys, outputs_right)?;
            let done = context.joins_done.get();
            let share = |fraction: f64| {
                (done as f64 + fraction) / context.joins.max(1) as f64 * PROGRESS_SCALE
            };
            let source = parquet_reader::join::join(&left, &right, *kind, names, &|fraction| {
                context
                    .progress
                    .store(share(fraction) as u64, Ordering::Relaxed);
                context.cancelled()
            })?;
            context.joins_done.set(done + 1);
            let columns = (0..source.column_count()).collect();
            Ok(Relation {
                source: Arc::new(source),
                filter: None,
                columns,
            })
        }
    }
}

/// Runs `plan` to the end and writes its rows to a temporary table.
fn query_table(plan: &LogicalPlan, context: &PrepContext) -> Result<Arc<ParquetSource>> {
    let engine = QueryEngine::from_handle(context.runtime.clone());
    let schema = engine.output_schema(plan)?;
    let mut execution = engine.execute(plan, GenerationId(0))?;
    let mut writer = TempTableWriter::new(&schema)?;
    loop {
        if context.cancelled() {
            execution.cancel();
            return Err(Cancelled.into());
        }
        match execution.next_batch()? {
            Some(batch) => writer.write(&batch.batch)?,
            None => break,
        }
    }
    Ok(Arc::new(writer.finish("subquery")?))
}

/// The one value a scalar subquery returns, as filter text; `None` for NULL.
fn scalar_value(plan: &LogicalPlan, context: &PrepContext) -> Result<Option<String>> {
    let engine = QueryEngine::from_handle(context.runtime.clone());
    let mut execution = engine.execute(plan, GenerationId(0))?;
    let mut value = None;
    let mut rows = 0_usize;
    while let Some(batch) = execution.next_batch()? {
        if context.cancelled() {
            execution.cancel();
            return Err(Cancelled.into());
        }
        let batch = batch.batch;
        if batch.num_columns() != 1 {
            bail!("a subquery used as a value must select exactly one column");
        }
        if rows == 0 && batch.num_rows() > 0 {
            value = scalar_text(batch.column(0).as_ref(), 0)?;
        }
        rows += batch.num_rows();
        if rows > 1 {
            execution.cancel();
            bail!(
                "a subquery used as a value returned more than one row; add a LIMIT 1 or an aggregate"
            );
        }
    }
    if rows == 0 {
        // No row is NULL in SQL.
        return Ok(None);
    }
    Ok(value)
}

/// A preparation running on its own thread.
pub(crate) struct PrepTask {
    receiver: mpsc::Receiver<Result<Relation>>,
    cancelled: Arc<AtomicBool>,
    progress: Arc<AtomicU64>,
}

impl PrepTask {
    pub(crate) fn start(prep: Arc<Prep>, runtime: RuntimeHandle) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(AtomicU64::new(0));
        let context = (Arc::clone(&cancelled), Arc::clone(&progress));
        thread::Builder::new()
            .name("pavi-query-prep".to_string())
            .spawn(move || {
                let context = PrepContext {
                    runtime,
                    cancelled: context.0,
                    progress: context.1,
                    joins: count_joins(&prep),
                    joins_done: std::cell::Cell::new(0),
                };
                let _ = sender.send(run(&prep, &context));
            })
            .context("start the query preparation thread")?;
        Ok(Self {
            receiver,
            cancelled,
            progress,
        })
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Share of the joins done, from 0 to 1.
    pub(crate) fn progress(&self) -> f32 {
        (self.progress.load(Ordering::Relaxed) as f64 / PROGRESS_SCALE).min(1.0) as f32
    }

    pub(crate) fn try_recv(&self) -> Result<Option<Relation>> {
        match self.receiver.try_recv() {
            Ok(result) => result.map(Some),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("the query preparation stopped without a result")
            }
        }
    }

    pub(crate) fn recv(&self) -> Result<Relation> {
        self.receiver
            .recv()
            .map_err(|_| anyhow!("the query preparation stopped without a result"))?
    }
}
