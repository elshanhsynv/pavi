use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use arrow_schema::{Schema, SchemaRef};
use parquet_reader::{
    AggregateExpr, AggregateSpec, FilterExpr, FilterOp, NullOrder, ParquetSource, SortDirection,
    SortSpec,
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

/// What a query does to its result after any aggregate, before `LIMIT`:
/// `HAVING`, `ORDER BY` on the result's own columns (an aggregate's
/// results), and column aliases. Column numbers are the result's columns.
/// `HAVING` and the sort apply to an aggregate's one bounded batch only.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Output {
    /// The result's columns calculated from the input's (ADR 0013), when a
    /// plain SELECT list calculates; they are made first, then named.
    pub(crate) columns: Option<Vec<crate::expr::Expr<usize>>>,
    pub(crate) having: Option<OutputFilter>,
    /// Keys on result columns, most significant first.
    pub(crate) sort: Vec<SortSpec>,
    /// A new name for each result column that has one, in result order.
    pub(crate) names: Vec<Option<String>>,
}

/// A `HAVING` test on result columns, by number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutputFilter {
    Compare {
        column: usize,
        op: FilterOp,
        value: String,
    },
    And(Vec<OutputFilter>),
    Or(Vec<OutputFilter>),
}

impl OutputFilter {
    /// The filter on a result whose columns are `schema`, naming each column
    /// by its result name.
    pub(crate) fn to_filter(&self, schema: &Schema) -> Result<FilterExpr> {
        Ok(match self {
            Self::Compare { column, op, value } => FilterExpr::compare(
                schema
                    .fields()
                    .get(*column)
                    .ok_or_else(|| anyhow!("HAVING column {column} is out of range"))?
                    .name()
                    .clone(),
                op.clone(),
                value.clone(),
            ),
            Self::And(parts) => FilterExpr::And(
                parts
                    .iter()
                    .map(|part| part.to_filter(schema))
                    .collect::<Result<_>>()?,
            ),
            Self::Or(parts) => FilterExpr::Or(
                parts
                    .iter()
                    .map(|part| part.to_filter(schema))
                    .collect::<Result<_>>()?,
            ),
        })
    }
}

impl Output {
    pub fn new(
        having: Option<OutputFilter>,
        sort: Vec<SortSpec>,
        names: Vec<Option<String>>,
    ) -> Self {
        Self {
            columns: None,
            having,
            sort,
            names,
        }
    }

    /// The same step, calculating the result's columns from the input's.
    pub fn computing(self, columns: Option<Vec<crate::expr::Expr<usize>>>) -> Self {
        Self { columns, ..self }
    }

    /// The schema of the calculated columns over an input with `input`'s
    /// columns, before naming: a column keeps its name, a calculation is
    /// `column{n}` until named.
    pub(crate) fn computed_schema(&self, input: &Schema) -> Result<Option<SchemaRef>> {
        let Some(columns) = &self.columns else {
            return Ok(None);
        };
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let name = match column {
                    crate::expr::Expr::Column(position) => input
                        .fields()
                        .get(*position)
                        .map(|field| field.name().clone())
                        .ok_or_else(|| anyhow!("result column {position} is out of range"))?,
                    _ => format!("column{}", index + 1),
                };
                Ok(arrow_schema::Field::new(
                    name,
                    column.data_type(input)?,
                    true,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Arc::new(Schema::new(fields))))
    }

    /// Whether the result's columns are not the input's own: renamed or
    /// calculated.
    pub fn renames(&self) -> bool {
        self.columns.is_some() || self.names.iter().any(Option::is_some)
    }

    /// `schema` with the new names.
    pub(crate) fn rename(&self, schema: &Schema) -> Result<SchemaRef> {
        if self.names.len() > schema.fields().len() {
            bail!("more column aliases than result columns");
        }
        Ok(Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(
                    |(index, field)| match self.names.get(index).cloned().flatten() {
                        Some(name) => field.as_ref().clone().with_name(name),
                        None => field.as_ref().clone(),
                    },
                )
                .collect::<Vec<_>>(),
        )))
    }
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
    Output {
        input: Box<LogicalPlan>,
        output: Output,
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

    /// `HAVING`, a sort of the result's own columns and aliases (see [`Output`]).
    pub fn output(self, output: Output) -> Self {
        Self::Output {
            input: Box::new(self),
            output,
        }
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
            | Self::Aggregate { input, .. }
            | Self::Output { input, .. } => input.prepared(),
        }
    }

    fn shapes_output(&self) -> bool {
        match self {
            Self::Projection { .. } | Self::Aggregate { .. } => true,
            Self::Scan(_) | Self::Prepared(_) => false,
            Self::Filter { input, .. }
            | Self::Limit { input, .. }
            | Self::Sort { input, .. }
            | Self::Output { input, .. } => input.shapes_output(),
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
                        Ok(expression.with_column(expression.column().map(map).transpose()?))
                    })
                    .collect::<Result<Vec<AggregateExpr>>>()?;
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
            // Its columns are the result's, which binding doesn't change.
            Self::Output { input, output } => Self::Output {
                input: Box::new(input.bind_inner(relation, reshape)?),
                output: output.clone(),
            },
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
            Self::Output { input, output } => Self::Output {
                input: Box::new(input.replace_sort(keys)),
                output,
            },
            plan => Self::Sort {
                input: Box::new(plan),
                sort: Sort::by(keys),
            },
        }
    }
}
