use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow_schema::{Field, Schema, SchemaRef};
use parquet_reader::{
    FilterExpr, FilterOp, JoinKind, MAX_JOIN_KEYS, MAX_SORT_KEYS, NullOrder, ParquetSource,
    SortDirection, SortSpec, aggregate::MAX_GROUP_COLUMNS, join::check_join_key_types,
};
use sqlparser::{
    ast::{
        BinaryOperator, Distinct, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
        FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, OrderBy, OrderByKind, Query,
        SelectItem, SetExpr, Statement, TableFactor, UnaryOperator,
    },
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{
    AggregateExpr, AggregateFunction, Filter, LogicalPlan, Planner,
    expr::{self as calc},
    logical::{Output, OutputFilter, Prepared},
    prep::{MAX_JOINS, MAX_SUBQUERY_DEPTH, Prep, PrepFilter, PrepValue},
};

/// The tables a query can name: `dataset` (the tab running the query) and the
/// other open tabs (ADR 0009).
#[derive(Clone, Default)]
pub struct Tables {
    entries: Vec<(String, Arc<ParquetSource>)>,
}

impl Tables {
    /// Tables with only `dataset`.
    pub fn new(dataset: Arc<ParquetSource>) -> Self {
        Self {
            entries: vec![("dataset".to_owned(), dataset)],
        }
    }

    /// Adds a table; a name already present is ignored.
    pub fn with(mut self, name: impl Into<String>, source: Arc<ParquetSource>) -> Self {
        let name = name.into();
        if !self
            .names()
            .any(|existing| existing.eq_ignore_ascii_case(&name))
        {
            self.entries.push((name, source));
        }
        self
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(name, _)| name.as_str())
    }

    fn get(&self, name: &str) -> Result<Arc<ParquetSource>> {
        self.entries
            .iter()
            .find(|(table, _)| table.eq_ignore_ascii_case(name))
            .map(|(_, source)| Arc::clone(source))
            .ok_or_else(|| {
                anyhow!(
                    "unknown table {name}; the open tables are {}",
                    self.names().collect::<Vec<_>>().join(", ")
                )
            })
    }
}

/// A SQL table name for a file or folder: its name without extensions, with
/// characters other than letters, digits and `_` turned into `_`, not starting
/// with a digit, and not `dataset` or one of `taken` (ignoring case). The app
/// names its tabs with it and the CLI its `--table` files (ADR 0009, 0011).
pub fn table_name(path: &std::path::Path, taken: &[String]) -> String {
    let shown = parquet_reader::display_path(path);
    let stem = shown
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let stem = stem.split('.').next().unwrap_or_default();
    let mut name = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        name.insert_str(0, "t_");
    }
    let free = |candidate: &str| {
        !candidate.eq_ignore_ascii_case("dataset")
            && !taken
                .iter()
                .any(|taken| taken.eq_ignore_ascii_case(candidate))
    };
    if free(&name) {
        return name;
    }
    (2..)
        .map(|suffix| format!("{name}_{suffix}"))
        .find(|candidate| free(candidate))
        .unwrap_or(name)
}

/// A column, optionally qualified by a table name or alias (`f.ORIGIN`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlColumn {
    pub table: Option<String>,
    pub name: String,
}

impl SqlColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            table: None,
            name: name.into(),
        }
    }

    pub fn qualified(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            name: name.into(),
        }
    }

    fn same(&self, other: &Self) -> bool {
        self.name.eq_ignore_ascii_case(&other.name)
            && match (&self.table, &other.table) {
                (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
                _ => true,
            }
    }
}

impl std::fmt::Display for SqlColumn {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.table {
            Some(table) => write!(formatter, "{table}.{}", self.name),
            None => formatter.write_str(&self.name),
        }
    }
}

/// Hidden column names for calculations start with this.
const CALC_PREFIX: &str = "#calc";
/// Distinct calculations in one query.
pub const MAX_CALCULATIONS: usize = 32;

/// The calculations a query makes (ADR 0013). Each is named by a hidden column
/// (`#calc0` …) that the rest of the query uses like any column; the same
/// calculation written twice is one column.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlCalcs {
    items: Vec<SqlCalc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlCalc {
    name: String,
    expr: calc::Expr<SqlColumn>,
    /// As written, for messages.
    text: String,
    /// Used by WHERE, GROUP BY, ORDER BY, HAVING or an aggregate, so it must
    /// be calculated before rows are filtered, grouped or sorted.
    outside_select: bool,
}

impl SqlCalcs {
    /// The column `expression` stands for: itself when it is a column, else
    /// the hidden column of its calculation.
    fn column(&mut self, expression: &Expr, outside_select: bool) -> Result<SqlColumn> {
        let parsed = calc::parse(expression)?;
        if let calc::Expr::Column(column) = parsed {
            return Ok(column);
        }
        if let Some(existing) = self.items.iter_mut().find(|item| item.expr == parsed) {
            existing.outside_select |= outside_select;
            return Ok(SqlColumn::new(existing.name.clone()));
        }
        if self.items.len() >= MAX_CALCULATIONS {
            bail!("a query makes at most {MAX_CALCULATIONS} different calculations");
        }
        let name = format!("{CALC_PREFIX}{}", self.items.len());
        self.items.push(SqlCalc {
            name: name.clone(),
            expr: parsed,
            text: expression.to_string(),
            outside_select,
        });
        Ok(SqlColumn::new(name))
    }

    /// Which calculation `column` names, if it is one.
    fn position(&self, column: &SqlColumn) -> Option<usize> {
        if column.table.is_some() {
            return None;
        }
        self.items.iter().position(|item| item.name == column.name)
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn outside_select(&self) -> bool {
        self.items.iter().any(|item| item.outside_select)
    }

    /// Every column the calculations read.
    fn columns(&self) -> Vec<&SqlColumn> {
        let mut columns = Vec::new();
        for item in &self.items {
            item.expr.columns(&mut columns);
        }
        columns
    }
}

/// The deliberately small SQL shape accepted by PAVI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAst {
    from: SqlFrom,
    projection: SqlProjection,
    /// `AS` names of the SELECT items, in result order; `None` where there is none.
    aliases: Vec<Option<String>>,
    /// The SQL text of SELECT items that calculate, as their default names.
    labels: Vec<Option<String>>,
    calcs: SqlCalcs,
    predicate: Option<SqlPredicate>,
    having: Option<SqlHaving>,
    /// ORDER BY keys, most significant first.
    sort: Vec<SqlSort>,
    limit: Option<usize>,
}

/// `FROM` a table or subquery, then `JOIN`s.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlFrom {
    pub first: SqlRelation,
    pub joins: Vec<SqlJoin>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlRelation {
    pub table: SqlTable,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlTable {
    Named(String),
    Subquery(Box<SqlAst>),
}

/// `[INNER | LEFT] JOIN relation ON a = b [AND c = d …]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlJoin {
    pub kind: JoinKind,
    pub relation: SqlRelation,
    pub on: Vec<(SqlColumn, SqlColumn)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlProjection {
    All,
    Columns(Vec<SqlColumn>),
    Aggregates(SqlAggregate),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAggregate {
    /// GROUP BY columns, in order; empty for one overall group.
    pub group_by: Vec<SqlColumn>,
    /// Every aggregate computed: the ones SELECT lists, and the ones only
    /// used inside its calculations.
    pub expressions: Vec<SqlAggregateExpr>,
    /// The result's columns, in SELECT order.
    pub items: Vec<SqlItem>,
}

/// One result column of an aggregate query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlItem {
    /// A GROUP BY column, by its place in GROUP BY.
    Group(usize),
    /// An aggregate, by its place in [`SqlAggregate::expressions`].
    Aggregate(usize),
    /// A calculation on the group's values and aggregates' results
    /// (`ROUND(AVG(x), 2)`); an aggregate's result is the column `#agg{n}`.
    Calc(calc::Expr<SqlColumn>),
}

/// Columns that hold an aggregate's result inside a SELECT calculation.
const AGG_PREFIX: &str = "#agg";

/// Which aggregate `column` names, when it is one's result inside a SELECT
/// calculation.
fn aggregate_result(column: &SqlColumn) -> Option<usize> {
    column
        .table
        .is_none()
        .then(|| column.name.strip_prefix(AGG_PREFIX)?.parse().ok())
        .flatten()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAggregateExpr {
    pub function: AggregateFunction,
    pub column: Option<SqlColumn>,
}

impl SqlAggregateExpr {
    /// The same aggregate of the same column (names compare ignoring case).
    fn same(&self, other: &Self) -> bool {
        self.function == other.function
            && match (&self.column, &other.column) {
                (Some(left), Some(right)) => left.same(right),
                (None, None) => true,
                _ => false,
            }
    }
}

impl std::fmt::Display for SqlAggregateExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let function = match self.function {
            AggregateFunction::Count | AggregateFunction::CountDistinct => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        };
        match (&self.column, self.function) {
            (None, _) => write!(formatter, "{function}(*)"),
            (Some(column), AggregateFunction::CountDistinct) => {
                write!(formatter, "COUNT(DISTINCT {column})")
            }
            (Some(column), _) => write!(formatter, "{function}({column})"),
        }
    }
}

/// A result column named by `ORDER BY` or `HAVING`: a column (or a SELECT
/// alias), an aggregate, or a 1-based position in the SELECT list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlTarget {
    Column(SqlColumn),
    Aggregate(SqlAggregateExpr),
    Position(usize),
}

impl SqlTarget {
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Column(left), Self::Column(right)) => left.same(right),
            (Self::Aggregate(left), Self::Aggregate(right)) => left.same(right),
            (Self::Position(left), Self::Position(right)) => left == right,
            _ => false,
        }
    }
}

impl std::fmt::Display for SqlTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(column) => column.fmt(formatter),
            Self::Aggregate(aggregate) => aggregate.fmt(formatter),
            Self::Position(position) => position.fmt(formatter),
        }
    }
}

/// A `HAVING` clause: tests on result columns joined by `AND` and `OR`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlHaving {
    Compare {
        target: SqlTarget,
        op: FilterOp,
        value: String,
    },
    And(Vec<SqlHaving>),
    Or(Vec<SqlHaving>),
}

impl SqlHaving {
    fn compare(target: SqlTarget, op: FilterOp, value: impl Into<String>) -> Self {
        Self::Compare {
            target,
            op,
            value: value.into(),
        }
    }

    fn targets<'a>(&'a self, targets: &mut Vec<&'a SqlTarget>) {
        match self {
            Self::Compare { target, .. } => targets.push(target),
            Self::And(parts) | Self::Or(parts) => {
                parts.iter().for_each(|part| part.targets(targets))
            }
        }
    }

    fn to_output(&self, index_of: &dyn Fn(&SqlTarget) -> Result<usize>) -> Result<OutputFilter> {
        Ok(match self {
            Self::Compare { target, op, value } => OutputFilter::Compare {
                column: index_of(target)?,
                op: op.clone(),
                value: value.clone(),
            },
            Self::And(parts) => OutputFilter::And(
                parts
                    .iter()
                    .map(|part| part.to_output(index_of))
                    .collect::<Result<_>>()?,
            ),
            Self::Or(parts) => OutputFilter::Or(
                parts
                    .iter()
                    .map(|part| part.to_output(index_of))
                    .collect::<Result<_>>()?,
            ),
        })
    }
}

/// A `WHERE` clause: comparisons joined by `AND` and `OR`. `IN`, `BETWEEN` and
/// their `NOT` forms are stored as the comparisons they stand for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlPredicate {
    Compare {
        column: SqlColumn,
        op: FilterOp,
        value: String,
    },
    /// `column [NOT] IN (SELECT one_column …)`.
    InSubquery {
        column: SqlColumn,
        query: Box<SqlAst>,
        negated: bool,
    },
    /// `column <op> (SELECT …)`, a subquery returning one value.
    CompareSubquery {
        column: SqlColumn,
        op: FilterOp,
        query: Box<SqlAst>,
    },
    And(Vec<SqlPredicate>),
    Or(Vec<SqlPredicate>),
}

impl SqlPredicate {
    fn compare(column: SqlColumn, op: FilterOp, value: impl Into<String>) -> Self {
        Self::Compare {
            column,
            op,
            value: value.into(),
        }
    }

    fn has_subquery(&self) -> bool {
        match self {
            Self::Compare { .. } => false,
            Self::InSubquery { .. } | Self::CompareSubquery { .. } => true,
            Self::And(parts) | Self::Or(parts) => parts.iter().any(Self::has_subquery),
        }
    }

    fn has_in_subquery(&self) -> bool {
        match self {
            Self::InSubquery { .. } => true,
            Self::Compare { .. } | Self::CompareSubquery { .. } => false,
            Self::And(parts) | Self::Or(parts) => parts.iter().any(Self::has_in_subquery),
        }
    }

    fn columns<'a>(&'a self, columns: &mut Vec<&'a SqlColumn>) {
        match self {
            Self::Compare { column, .. }
            | Self::InSubquery { column, .. }
            | Self::CompareSubquery { column, .. } => columns.push(column),
            Self::And(parts) | Self::Or(parts) => {
                parts.iter().for_each(|part| part.columns(columns))
            }
        }
    }

    /// Resolves column names against `source` and builds the reader's filter.
    fn to_filter(
        &self,
        source: &ParquetSource,
        resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
    ) -> Result<FilterExpr> {
        match self {
            Self::Compare { column, op, value } => {
                let index = resolve(column)?;
                Ok(FilterExpr::compare(
                    source.schema().field(index).name().to_owned(),
                    op.clone(),
                    value.clone(),
                ))
            }
            Self::And(parts) => parts
                .iter()
                .map(|part| part.to_filter(source, resolve))
                .collect::<Result<Vec<_>>>()
                .map(FilterExpr::And),
            Self::Or(parts) => parts
                .iter()
                .map(|part| part.to_filter(source, resolve))
                .collect::<Result<Vec<_>>>()
                .map(FilterExpr::Or),
            Self::InSubquery { .. } | Self::CompareSubquery { .. } => {
                bail!("a subquery can't be used here")
            }
        }
    }

    /// The prepared filter; `resolve` maps a column to the input's column.
    fn to_prep_filter(
        &self,
        tables: &Tables,
        resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
    ) -> Result<PrepFilter> {
        Ok(match self {
            Self::Compare { column, op, value } => PrepFilter::Compare {
                column: resolve(column)?,
                op: op.clone(),
                value: PrepValue::Literal(value.clone()),
            },
            Self::CompareSubquery { column, op, query } => PrepFilter::Compare {
                column: resolve(column)?,
                op: op.clone(),
                value: PrepValue::Subquery(query.to_logical_plan_with(tables)?),
            },
            Self::And(parts) => PrepFilter::And(
                parts
                    .iter()
                    .map(|part| part.to_prep_filter(tables, resolve))
                    .collect::<Result<_>>()?,
            ),
            Self::Or(parts) => PrepFilter::Or(
                parts
                    .iter()
                    .map(|part| part.to_prep_filter(tables, resolve))
                    .collect::<Result<_>>()?,
            ),
            Self::InSubquery { .. } => {
                bail!("IN (SELECT …) can't be combined with OR; join the tests with AND")
            }
        })
    }

    /// The parts of a top-level `AND`.
    fn conjuncts(&self) -> Vec<&SqlPredicate> {
        match self {
            Self::And(parts) => parts.iter().flat_map(Self::conjuncts).collect(),
            predicate => vec![predicate],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlSort {
    pub target: SqlTarget,
    pub direction: SortDirection,
    pub nulls: NullOrder,
}

impl SqlAst {
    /// Parses one `SELECT` statement.
    pub fn parse(sql: &str) -> Result<Self> {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).context("parse SQL")?;
        if statements.len() != 1 {
            bail!("SQL input must contain exactly one statement");
        }
        let Some(Statement::Query(query)) = statements.pop() else {
            bail!("only SELECT queries are supported");
        };
        Self::from_query(&query, 0)
    }

    fn from_query(query: &Query, depth: usize) -> Result<Self> {
        if depth > MAX_SUBQUERY_DEPTH {
            bail!("subqueries nest at most {MAX_SUBQUERY_DEPTH} deep");
        }
        if query.with.is_some() {
            bail!("WITH queries are not supported");
        }
        if query.fetch.is_some() {
            bail!("FETCH is not supported; use LIMIT");
        }
        if !query.locks.is_empty() || query.for_clause.is_some() || !query.pipe_operators.is_empty()
        {
            bail!("locking and pipe clauses are not supported");
        }

        let limit = parse_limit(query.limit_clause.as_ref())?;
        let mut calcs = SqlCalcs::default();
        let sort = parse_order_by(query.order_by.as_ref(), &mut calcs)?;
        let SetExpr::Select(select) = query.body.as_ref() else {
            bail!("only a single SELECT is supported (no UNION)");
        };
        reject_select_clauses(select)?;
        let distinct = match &select.distinct {
            None | Some(Distinct::All) => false,
            Some(Distinct::Distinct) => true,
            Some(Distinct::On(_)) => bail!("DISTINCT ON is not supported; use SELECT DISTINCT"),
        };
        let (projection, aliases, labels) =
            parse_projection(&select.projection, &select.group_by, distinct, &mut calcs)?;
        let predicate = select
            .selection
            .as_ref()
            .map(|selection| parse_predicate(selection, depth, &mut calcs))
            .transpose()?;
        let having = select
            .having
            .as_ref()
            .map(|having| parse_having(having, &mut calcs))
            .transpose()?;
        Ok(Self {
            from: parse_from(&select.from, depth)?,
            projection,
            aliases,
            labels,
            calcs,
            predicate,
            having,
            sort,
            limit,
        })
    }

    /// Whether the query's calculations must be made before its rows are
    /// filtered, grouped or sorted, so it runs on prepared rows (ADR 0013).
    /// Calculations only in a plain SELECT list are made on the way out.
    fn calculates_first(&self) -> bool {
        if self.calcs.is_empty() {
            return false;
        }
        if self.calcs.outside_select() || matches!(self.projection, SqlProjection::Aggregates(_)) {
            return true;
        }
        // ORDER BY a calculated SELECT item, by its alias or position.
        let SqlProjection::Columns(columns) = &self.projection else {
            return false;
        };
        let item = |position: usize| {
            columns
                .get(position)
                .is_some_and(|column| self.calcs.position(column).is_some())
        };
        self.sort.iter().any(|key| match &key.target {
            SqlTarget::Column(column) => self.alias_position(column).is_some_and(item),
            SqlTarget::Position(position) => item(position.saturating_sub(1)),
            SqlTarget::Aggregate(_) => false,
        })
    }

    /// The name each result column is shown with: its alias, else the SQL
    /// text of a calculation; `None` keeps the column's own name.
    fn result_names(&self) -> Vec<Option<String>> {
        self.aliases
            .iter()
            .zip(self.labels.iter().chain(std::iter::repeat(&None)))
            .map(|(alias, label)| alias.clone().or_else(|| label.clone()))
            .collect()
    }

    pub fn projection(&self) -> &SqlProjection {
        &self.projection
    }

    pub fn predicate(&self) -> Option<&SqlPredicate> {
        self.predicate.as_ref()
    }

    pub fn from(&self) -> &SqlFrom {
        &self.from
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// ORDER BY keys, most significant first.
    pub fn sort(&self) -> &[SqlSort] {
        &self.sort
    }

    /// Plans the query against `source` as the only table, `dataset`.
    pub fn to_logical_plan(&self, source: Arc<ParquetSource>) -> Result<LogicalPlan> {
        self.to_logical_plan_with(&Tables::new(source))
    }

    /// Resolves names against `tables` and plans the query. A query that joins
    /// tables or uses subqueries gets a prepared plan (ADR 0009); the rest read
    /// their table directly, as before.
    pub fn to_logical_plan_with(&self, tables: &Tables) -> Result<LogicalPlan> {
        let direct = self.from.joins.is_empty()
            && matches!(self.from.first.table, SqlTable::Named(_))
            && !self
                .predicate
                .as_ref()
                .is_some_and(SqlPredicate::has_subquery)
            && !self.calculates_first();
        let plan = if direct {
            self.direct_plan(tables)?
        } else {
            self.prepared_plan(tables)?
        };
        Planner::validate(&plan).context("validate SQL query")?;
        Ok(plan)
    }

    fn direct_plan(&self, tables: &Tables) -> Result<LogicalPlan> {
        let SqlTable::Named(name) = &self.from.first.table else {
            bail!("expected a table");
        };
        let source = tables.get(name)?;
        let alias = self.from.first.alias.as_deref().unwrap_or(name);
        let resolve = |column: &SqlColumn| -> Result<usize> {
            if let Some(table) = &column.table
                && !table.eq_ignore_ascii_case(alias)
                && !table.eq_ignore_ascii_case(name)
            {
                bail!("unknown table {table} in {column}");
            }
            resolve_column(&source, &column.name)
        };
        let filter = self
            .predicate
            .as_ref()
            .map(|predicate| predicate.to_filter(&source, &resolve).map(Filter::new))
            .transpose()?;
        self.shape(
            LogicalPlan::scan(Arc::clone(&source)),
            filter,
            &resolve,
            None,
        )
    }

    /// Adds the filter, projection, aggregate, sort and limit above `plan`;
    /// `resolve` maps a column to the input's column. `all` is how many input
    /// columns `SELECT *` shows, when the input has hidden calculations after
    /// them.
    fn shape(
        &self,
        mut plan: LogicalPlan,
        filter: Option<Filter>,
        resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
        all: Option<usize>,
    ) -> Result<LogicalPlan> {
        // Calculations in a plain SELECT list of a direct query are made on
        // the way out, over the columns they read (ADR 0013).
        let mut computed = None;
        let (projection, aggregate) = match &self.projection {
            SqlProjection::All => (all.map(|count| (0..count).collect()), None),
            SqlProjection::Columns(columns)
                if !self.calcs.is_empty() && !self.calculates_first() =>
            {
                let items = columns
                    .iter()
                    .map(|column| match self.calcs.position(column) {
                        Some(index) => self
                            .calcs
                            .items
                            .get(index)
                            .map(|item| item.expr.clone())
                            .ok_or_else(|| anyhow!("calculation {index} is missing")),
                        None => Ok(calc::Expr::Column(column.clone())),
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut inputs = Vec::new();
                for item in &items {
                    let mut read = Vec::new();
                    item.columns(&mut read);
                    for column in read {
                        let index = resolve(column)?;
                        if !inputs.contains(&index) {
                            inputs.push(index);
                        }
                    }
                }
                let position = |column: &SqlColumn| -> Result<usize> {
                    let index = resolve(column)?;
                    inputs
                        .iter()
                        .position(|input| *input == index)
                        .ok_or_else(|| anyhow!("column {column} was not read"))
                };
                computed = Some(
                    items
                        .iter()
                        .map(|item| item.map_columns(&position))
                        .collect::<Result<Vec<_>>>()?,
                );
                (Some(inputs), None)
            }
            SqlProjection::Columns(columns) => (
                Some(columns.iter().map(resolve).collect::<Result<Vec<_>>>()?),
                None,
            ),
            SqlProjection::Aggregates(aggregate) => {
                let expressions = aggregate
                    .expressions
                    .iter()
                    .map(|expression| {
                        let column = expression.column.as_ref().map(resolve).transpose()?;
                        Ok(match (expression.function, column) {
                            (AggregateFunction::Count, None) => AggregateExpr::count_all(),
                            (AggregateFunction::Count, Some(column)) => {
                                AggregateExpr::count(column)
                            }
                            (AggregateFunction::Sum, Some(column)) => AggregateExpr::sum(column),
                            (AggregateFunction::Avg, Some(column)) => AggregateExpr::avg(column),
                            (AggregateFunction::Min, Some(column)) => AggregateExpr::min(column),
                            (AggregateFunction::Max, Some(column)) => AggregateExpr::max(column),
                            (AggregateFunction::CountDistinct, Some(column)) => {
                                AggregateExpr::count_distinct(column)
                            }
                            (_, None) => bail!("only COUNT may use *"),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let group_by = aggregate
                    .group_by
                    .iter()
                    .map(resolve)
                    .collect::<Result<Vec<_>>>()?;
                (None, Some((group_by, expressions)))
            }
        };
        // ORDER BY and HAVING: on an aggregate they name its result columns
        // (an output step); otherwise ORDER BY sorts the input's columns.
        let mut output_sort = Vec::new();
        let mut having = None;
        let mut sort = Vec::new();
        if let SqlProjection::Aggregates(aggregate) = &self.projection {
            // The result's columns, when SELECT doesn't list them as the
            // aggregate makes them (GROUP BY columns, then aggregates): made
            // from its one result batch (ADR 0013).
            let groups = aggregate.group_by.len();
            let natural = aggregate.items.len() == groups + aggregate.expressions.len()
                && aggregate
                    .items
                    .iter()
                    .enumerate()
                    .all(|(index, item)| match item {
                        SqlItem::Group(group) => *group == index,
                        SqlItem::Aggregate(expression) => groups + expression == index,
                        SqlItem::Calc(_) => false,
                    });
            if !natural {
                let on_result = |column: &SqlColumn| -> Result<usize> {
                    match aggregate_result(column) {
                        Some(index) => Ok(groups + index),
                        None => aggregate
                            .group_by
                            .iter()
                            .position(|group| group.same(column))
                            .ok_or_else(|| {
                                anyhow!(
                                    "{column} must be in GROUP BY or inside an aggregate to be calculated with"
                                )
                            }),
                    }
                };
                computed = Some(
                    aggregate
                        .items
                        .iter()
                        .map(|item| match item {
                            SqlItem::Group(group) => Ok(calc::Expr::Column(*group)),
                            SqlItem::Aggregate(expression) => {
                                Ok(calc::Expr::Column(groups + expression))
                            }
                            SqlItem::Calc(expression) => expression.map_columns(&on_result),
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
            }
            let index_of = |target: &SqlTarget| self.result_index(aggregate, target);
            for key in &self.sort {
                output_sort.push(SortSpec::new(
                    index_of(&key.target)?,
                    key.direction,
                    key.nulls,
                ));
            }
            having = self
                .having
                .as_ref()
                .map(|having| having.to_output(&index_of))
                .transpose()?;
        } else {
            if self.having.is_some() {
                bail!("HAVING needs GROUP BY or an aggregate; filter rows with WHERE");
            }
            for key in &self.sort {
                let column = match &key.target {
                    SqlTarget::Column(column) => match self.alias_position(column) {
                        Some(position) => self.selected_column(position, resolve)?,
                        None => resolve(column)?,
                    },
                    SqlTarget::Position(position) => {
                        self.selected_column(position.saturating_sub(1), resolve)?
                    }
                    SqlTarget::Aggregate(aggregate) => {
                        bail!("ORDER BY {aggregate} needs the aggregate in SELECT")
                    }
                };
                sort.push(SortSpec::new(column, key.direction, key.nulls));
            }
        }

        if let Some(filter) = filter {
            plan = plan.filter(filter);
        }
        if let Some(projection) = projection {
            plan = plan.project(projection);
        }
        if let Some((group_by, expressions)) = aggregate {
            plan = if group_by.is_empty() {
                plan.aggregate(expressions)?
            } else {
                plan.aggregate_grouped(group_by, expressions)?
            };
        }
        if !sort.is_empty() {
            plan = plan.sort_by(sort);
        }
        let names = self.result_names();
        if having.is_some()
            || !output_sort.is_empty()
            || computed.is_some()
            || names.iter().any(Option::is_some)
        {
            plan = plan.output(Output::new(having, output_sort, names).computing(computed));
        }
        if let Some(limit) = self.limit {
            plan = plan.limit(limit);
        }
        Ok(plan)
    }

    /// Where `column` is a SELECT alias, its position in the SELECT list.
    fn alias_position(&self, column: &SqlColumn) -> Option<usize> {
        if column.table.is_some() {
            return None;
        }
        self.aliases.iter().position(|alias| {
            alias
                .as_ref()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(&column.name))
        })
    }

    /// The input column the SELECT item at `position` (0-based) reads, for a
    /// query without aggregates.
    fn selected_column(
        &self,
        position: usize,
        resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
    ) -> Result<usize> {
        match &self.projection {
            SqlProjection::Columns(columns) => resolve(columns.get(position).ok_or_else(|| {
                anyhow!(
                    "ORDER BY {} is past the {} selected columns",
                    position + 1,
                    columns.len()
                )
            })?),
            // SELECT *: positions are the input's columns.
            _ => Ok(position),
        }
    }

    /// The result column an aggregate query's ORDER BY or HAVING names: a
    /// GROUP BY column, an aggregate in SELECT, an alias or a position.
    fn result_index(&self, aggregate: &SqlAggregate, target: &SqlTarget) -> Result<usize> {
        let columns = aggregate.items.len();
        match target {
            SqlTarget::Position(position) if (1..=columns).contains(position) => Ok(position - 1),
            SqlTarget::Position(position) => {
                bail!("position {position} is not one of the {columns} result columns")
            }
            SqlTarget::Column(column) => self
                .alias_position(column)
                .or_else(|| {
                    aggregate.items.iter().position(|item| {
                        matches!(item, SqlItem::Group(group)
                            if aggregate.group_by.get(*group).is_some_and(|group| group.same(column)))
                    })
                })
                .ok_or_else(|| {
                    anyhow!(
                        "{column} is not a result column; name a GROUP BY column, an aggregate in SELECT or an alias"
                    )
                }),
            SqlTarget::Aggregate(wanted) => aggregate
                .items
                .iter()
                .position(|item| {
                    matches!(item, SqlItem::Aggregate(index)
                        if aggregate.expressions.get(*index).is_some_and(|expression| expression.same(wanted)))
                })
                .ok_or_else(|| anyhow!("{wanted} must also be in SELECT to sort or filter by it")),
        }
    }

    /// Every column the query names outside subqueries.
    fn referenced_columns(&self) -> Vec<&SqlColumn> {
        let mut columns = Vec::new();
        match &self.projection {
            SqlProjection::All => {}
            SqlProjection::Columns(names) => columns.extend(names),
            SqlProjection::Aggregates(aggregate) => {
                columns.extend(&aggregate.group_by);
                columns.extend(
                    aggregate
                        .expressions
                        .iter()
                        .filter_map(|expression| expression.column.as_ref()),
                );
            }
        }
        let mut targets = self
            .sort
            .iter()
            .map(|sort| &sort.target)
            .collect::<Vec<_>>();
        if let Some(having) = &self.having {
            having.targets(&mut targets);
        }
        for target in targets {
            match target {
                SqlTarget::Column(column) if self.alias_position(column).is_none() => {
                    columns.push(column)
                }
                SqlTarget::Aggregate(aggregate) => columns.extend(aggregate.column.as_ref()),
                SqlTarget::Column(_) | SqlTarget::Position(_) => {}
            }
        }
        if let Some(predicate) = &self.predicate {
            predicate.columns(&mut columns);
        }
        for join in &self.from.joins {
            for (left, right) in &join.on {
                columns.push(left);
                columns.push(right);
            }
        }
        // Hidden calculation columns are made later, from the columns they read.
        columns.retain(|column| self.calcs.position(column).is_none());
        columns.extend(self.calcs.columns());
        columns
    }

    /// Plans a query with joins or subqueries: its rows are prepared into one
    /// table first (ADR 0009), then shaped like a single-table query.
    fn prepared_plan(&self, tables: &Tables) -> Result<LogicalPlan> {
        // The tables and subqueries the query reads, in FROM order.
        let mut relations = vec![Relation::plan(&self.from.first, tables, false)?];
        for join in &self.from.joins {
            relations.push(Relation::plan(
                &join.relation,
                tables,
                join.kind == JoinKind::Left,
            )?);
        }
        for (index, relation) in relations.iter().enumerate() {
            if relations[..index]
                .iter()
                .any(|earlier| earlier.alias.eq_ignore_ascii_case(&relation.alias))
            {
                bail!(
                    "two tables are called {}; give one an alias (… AS other)",
                    relation.alias
                );
            }
        }
        let find = |column: &SqlColumn| find_column(&relations, column);

        // The columns kept from each relation: all for SELECT *, else the ones
        // the query names, in table order.
        let mut kept: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); relations.len()];
        if matches!(self.projection, SqlProjection::All) {
            for (index, relation) in relations.iter().enumerate() {
                kept[index].extend(0..relation.schema.fields().len());
            }
        }
        for column in self.referenced_columns() {
            let (relation, index) = find(column)?;
            kept[relation].insert(index);
        }
        let kept = kept
            .into_iter()
            .map(|columns| columns.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        // The prepared table's columns: each relation's kept columns, in order.
        // A name that two relations share is written `alias.name`.
        let mut output = Vec::new();
        for (relation, columns) in kept.iter().enumerate() {
            for column in columns {
                output.push((relation, *column));
            }
        }
        let names = output
            .iter()
            .map(|(relation, column)| {
                let name = relations[*relation].schema.field(*column).name();
                let shared = output
                    .iter()
                    .filter(|(other, other_column)| {
                        relations[*other]
                            .schema
                            .field(*other_column)
                            .name()
                            .eq_ignore_ascii_case(name)
                    })
                    .count()
                    > 1;
                if shared {
                    format!("{}.{name}", relations[*relation].alias)
                } else {
                    name.clone()
                }
            })
            .collect::<Vec<_>>();
        let output_index = |column: &SqlColumn| -> Result<usize> {
            let found = find(column)?;
            output
                .iter()
                .position(|kept| *kept == found)
                .ok_or_else(|| anyhow!("column {column} was not kept"))
        };
        let local_index = |relation: usize, column: &SqlColumn| -> Result<usize> {
            let (found, index) = find(column)?;
            if found != relation {
                bail!("column {column} is not in {}", relations[relation].alias);
            }
            kept[relation]
                .iter()
                .position(|kept| *kept == index)
                .ok_or_else(|| anyhow!("column {column} was not kept"))
        };

        // WHERE: a test on one table is applied while reading it, except on the
        // optional side of a LEFT JOIN; the rest apply to the joined rows.
        let mut pushed: Vec<Vec<&SqlPredicate>> = vec![Vec::new(); relations.len()];
        let mut after = Vec::new();
        if let Some(predicate) = &self.predicate {
            for conjunct in predicate.conjuncts() {
                if !matches!(conjunct, SqlPredicate::InSubquery { .. })
                    && conjunct.has_in_subquery()
                {
                    bail!("IN (SELECT …) can't be combined with OR; join the tests with AND");
                }
                let mut columns = Vec::new();
                conjunct.columns(&mut columns);
                // A test on a calculation applies once it is made.
                if columns
                    .iter()
                    .any(|column| self.calcs.position(column).is_some())
                {
                    after.push(conjunct);
                    continue;
                }
                let mut sides = columns
                    .iter()
                    .map(|column| find(column).map(|(relation, _)| relation))
                    .collect::<Result<BTreeSet<_>>>()?;
                match (sides.len(), sides.pop_first()) {
                    (1, Some(relation)) if !relations[relation].optional => {
                        pushed[relation].push(conjunct)
                    }
                    _ => after.push(conjunct),
                }
            }
        }

        let local_schema = |relation: usize| {
            Schema::new(
                kept[relation]
                    .iter()
                    .map(|column| relations[relation].schema.field(*column).clone())
                    .collect::<Vec<_>>(),
            )
        };
        let mut preps = Vec::new();
        for (index, relation) in relations.iter().enumerate() {
            let mut prep = relation.prep(kept[index].clone());
            let resolve = |column: &SqlColumn| local_index(index, column);
            prep = apply_where(prep, &pushed[index], tables, &resolve, &local_schema(index))?;
            preps.push(prep);
        }

        // Joins, left to right.
        let mut preps = preps.into_iter();
        let mut prepared = preps.next().ok_or_else(|| anyhow!("FROM names no table"))?;
        let mut width = kept[0].len();
        for (offset, join) in self.from.joins.iter().enumerate() {
            let relation = offset + 1;
            let right = preps
                .next()
                .ok_or_else(|| anyhow!("a joined table is missing"))?;
            let mut left_keys = Vec::new();
            let mut right_keys = Vec::new();
            for (first, second) in &join.on {
                let (first_side, _) = find(first)?;
                let (second_side, _) = find(second)?;
                let (earlier, joined) = match (first_side == relation, second_side == relation) {
                    (false, true) if first_side < relation => (first, second),
                    (true, false) if second_side < relation => (second, first),
                    _ => bail!(
                        "ON must compare a column of {} with a column of a table before it",
                        relations[relation].alias
                    ),
                };
                let left_key = output_index(earlier)?;
                let right_key = local_index(relation, joined)?;
                let (left_relation, left_column) = find(earlier)?;
                let (_, right_column) = find(joined)?;
                check_join_key_types(
                    relations[left_relation].schema.field(left_column),
                    relations[relation].schema.field(right_column),
                )?;
                left_keys.push(left_key);
                right_keys.push(right_key);
            }
            width += kept[relation].len();
            prepared = Prep::Join {
                left: Box::new(prepared),
                right: Box::new(right),
                kind: join.kind,
                left_keys,
                right_keys,
                names: names[..width].to_vec(),
            };
        }

        let mut schema: SchemaRef = Arc::new(Schema::new(
            output
                .iter()
                .zip(&names)
                .map(|((relation, column), name)| {
                    relations[*relation]
                        .schema
                        .field(*column)
                        .clone()
                        .with_name(name.clone())
                        .with_nullable(true)
                })
                .collect::<Vec<Field>>(),
        ));
        // Calculations, over the joined rows, become columns after them (ADR 0013).
        let base = output.len();
        if !self.calcs.is_empty() {
            let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
            let mut expressions = Vec::new();
            for item in &self.calcs.items {
                let expression = item.expr.map_columns(&output_index)?;
                let data_type = expression
                    .data_type(&schema)
                    .with_context(|| format!("calculation {}", item.text))?;
                fields.push(Arc::new(Field::new(&item.name, data_type, true)));
                expressions.push((item.name.clone(), expression));
            }
            prepared = Prep::Compute {
                input: Box::new(prepared),
                expressions,
            };
            schema = Arc::new(Schema::new(fields));
        }
        let resolve = |column: &SqlColumn| -> Result<usize> {
            match self.calcs.position(column) {
                Some(index) => Ok(base + index),
                None => output_index(column),
            }
        };
        prepared = apply_where(prepared, &after, tables, &resolve, &schema)?;
        let plan = LogicalPlan::Prepared(Prepared {
            prep: Arc::new(prepared),
            schema,
        });
        let all = (!self.calcs.is_empty()).then_some(base);
        self.shape(plan, None, &resolve, all)
    }
}

/// A table or subquery the query reads, while it is being planned.
struct Relation {
    alias: String,
    schema: SchemaRef,
    source: RelationSource,
    /// On the optional side of a LEFT JOIN.
    optional: bool,
}

enum RelationSource {
    Table(Arc<ParquetSource>),
    Query(LogicalPlan),
}

impl Relation {
    fn plan(relation: &SqlRelation, tables: &Tables, optional: bool) -> Result<Self> {
        let (alias, schema, source) = match &relation.table {
            SqlTable::Named(name) => {
                let source = tables.get(name)?;
                (
                    relation.alias.clone().unwrap_or_else(|| name.clone()),
                    source.schema(),
                    RelationSource::Table(source),
                )
            }
            SqlTable::Subquery(query) => {
                let plan = query.to_logical_plan_with(tables)?;
                (
                    relation
                        .alias
                        .clone()
                        .unwrap_or_else(|| "subquery".to_owned()),
                    Planner::validate(&plan)?,
                    RelationSource::Query(plan),
                )
            }
        };
        Ok(Self {
            alias,
            schema,
            source,
            optional,
        })
    }

    fn prep(&self, columns: Vec<usize>) -> Prep {
        match &self.source {
            RelationSource::Table(source) => Prep::Table {
                source: Arc::clone(source),
                columns,
            },
            RelationSource::Query(plan) => Prep::Query {
                plan: plan.clone(),
                columns,
            },
        }
    }
}

/// The relation and column a name refers to.
fn find_column(relations: &[Relation], column: &SqlColumn) -> Result<(usize, usize)> {
    let lookup = |relation: &Relation| -> Option<usize> {
        let fields = relation.schema.fields();
        fields
            .iter()
            .position(|field| field.name() == &column.name)
            .or_else(|| {
                let mut matches = fields
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| field.name().eq_ignore_ascii_case(&column.name));
                let first = matches.next().map(|(index, _)| index);
                if matches.next().is_some() {
                    None
                } else {
                    first
                }
            })
    };
    if let Some(table) = &column.table {
        let (index, relation) = relations
            .iter()
            .enumerate()
            .find(|(_, relation)| relation.alias.eq_ignore_ascii_case(table))
            .ok_or_else(|| anyhow!("unknown table {table} in {column}"))?;
        let found = lookup(relation).ok_or_else(|| anyhow!("unknown SQL column '{column}'"))?;
        return Ok((index, found));
    }
    let matches = relations
        .iter()
        .enumerate()
        .filter_map(|(index, relation)| lookup(relation).map(|found| (index, found)))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [found] => Ok(*found),
        [] => bail!("unknown SQL column '{column}'"),
        _ => bail!(
            "column {column} is in more than one table; write {}.{column}",
            relations[matches[0].0].alias
        ),
    }
}

/// Applies `WHERE` tests to `prep`: comparisons as a filter, `IN (SELECT …)`
/// as semi joins and `NOT IN` as anti joins. `resolve` maps a column to one of
/// `prep`'s columns, whose schema is `schema`.
fn apply_where(
    mut prep: Prep,
    conjuncts: &[&SqlPredicate],
    tables: &Tables,
    resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
    schema: &Schema,
) -> Result<Prep> {
    let mut filters = Vec::new();
    for conjunct in conjuncts {
        if let SqlPredicate::InSubquery {
            column,
            query,
            negated,
        } = conjunct
        {
            let plan = query.to_logical_plan_with(tables)?;
            let inner = Planner::validate(&plan)?;
            let [inner_field] = inner.fields().as_ref() else {
                bail!("IN (SELECT …) must select exactly one column");
            };
            let key = resolve(column)?;
            check_join_key_types(schema.field(key), inner_field)?;
            let names = schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            prep = Prep::Join {
                left: Box::new(prep),
                right: Box::new(Prep::Query {
                    plan,
                    columns: vec![0],
                }),
                kind: if *negated {
                    JoinKind::Anti
                } else {
                    JoinKind::Semi
                },
                left_keys: vec![key],
                right_keys: vec![0],
                names,
            };
        } else {
            filters.push(conjunct.to_prep_filter(tables, resolve)?);
        }
    }
    if filters.is_empty() {
        return Ok(prep);
    }
    let filter = if filters.len() == 1 {
        filters.remove(0)
    } else {
        PrepFilter::And(filters)
    };
    filter.validate(schema)?;
    Ok(Prep::Filter {
        input: Box::new(prep),
        filter,
    })
}

fn parse_order_by(order_by: Option<&OrderBy>, calcs: &mut SqlCalcs) -> Result<Vec<SqlSort>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    if order_by.interpolate.is_some() {
        bail!("ORDER BY INTERPOLATE is not supported");
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        bail!("ORDER BY ALL is not supported");
    };
    if expressions.len() > MAX_SORT_KEYS {
        bail!(
            "ORDER BY names {} columns; the limit is {MAX_SORT_KEYS}",
            expressions.len()
        );
    }
    let sort = expressions
        .iter()
        .map(|expression| {
            if expression.with_fill.is_some() {
                bail!("ORDER BY WITH FILL is not supported");
            }
            Ok(SqlSort {
                target: parse_target(&expression.expr, "ORDER BY", calcs)?,
                direction: if expression.options.asc == Some(false) {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
                nulls: if expression.options.nulls_first == Some(true) {
                    NullOrder::First
                } else {
                    NullOrder::Last
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for (index, key) in sort.iter().enumerate() {
        if sort
            .get(..index)
            .is_some_and(|earlier| earlier.iter().any(|other| other.target.same(&key.target)))
        {
            bail!("ORDER BY names {} twice", key.target);
        }
    }
    Ok(sort)
}

/// A result column named in `clause`: a column or alias, an aggregate, or a
/// 1-based position in the SELECT list.
fn parse_target(expression: &Expr, clause: &str, calcs: &mut SqlCalcs) -> Result<SqlTarget> {
    match expression {
        Expr::Nested(inner) => parse_target(inner, clause, calcs),
        Expr::Function(function) if is_aggregate(function) => {
            Ok(SqlTarget::Aggregate(parse_aggregate(function, calcs)?))
        }
        Expr::Value(_) => match parse_literal(expression)?.parse::<usize>() {
            Ok(position) if position > 0 => Ok(SqlTarget::Position(position)),
            _ => bail!("{clause} takes a column, an aggregate, a calculation or a position from 1"),
        },
        // A column, or a calculation (`ORDER BY price * qty`).
        _ => Ok(SqlTarget::Column(calcs.column(expression, true)?)),
    }
}

/// Whether a function call is one of the aggregates.
fn is_aggregate(function: &Function) -> bool {
    matches!(
        function.name.to_string().to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

/// Fails when a column appears twice (names compare case-insensitively).
fn reject_repeats<'a>(columns: impl Iterator<Item = &'a SqlColumn>, clause: &str) -> Result<()> {
    let mut seen: Vec<&SqlColumn> = Vec::new();
    for column in columns {
        if seen.iter().any(|earlier| earlier.same(column)) {
            bail!("{clause} names the column {column} twice");
        }
        seen.push(column);
    }
    Ok(())
}

fn reject_select_clauses(select: &sqlparser::ast::Select) -> Result<()> {
    if select.top.is_some() {
        bail!("TOP is not supported; use LIMIT");
    }
    if select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || !select.connect_by.is_empty()
    {
        bail!("this SELECT clause is not supported");
    }
    Ok(())
}

fn parse_from(from: &[sqlparser::ast::TableWithJoins], depth: usize) -> Result<SqlFrom> {
    let [source] = from else {
        bail!("FROM names one table; combine tables with JOIN … ON");
    };
    if source.joins.len() > MAX_JOINS {
        bail!("a query joins at most {MAX_JOINS} tables");
    }
    let joins = source
        .joins
        .iter()
        .map(|join| {
            let (kind, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinKind::Inner, constraint)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinKind::Left, constraint)
                }
                JoinOperator::Right(_) | JoinOperator::RightOuter(_) => {
                    bail!("RIGHT JOIN is not supported; swap the tables and use LEFT JOIN")
                }
                JoinOperator::FullOuter(_) => bail!("FULL JOIN is not supported"),
                JoinOperator::CrossJoin(_) => {
                    bail!("CROSS JOIN is not supported; join ON matching columns")
                }
                _ => bail!("only INNER JOIN and LEFT JOIN are supported"),
            };
            let JoinConstraint::On(condition) = constraint else {
                bail!("a join needs ON with equal columns, e.g. ON a.id = b.id");
            };
            let mut on = Vec::new();
            parse_on(condition, &mut on)?;
            if on.len() > MAX_JOIN_KEYS {
                bail!("ON compares at most {MAX_JOIN_KEYS} pairs of columns");
            }
            Ok(SqlJoin {
                kind,
                relation: parse_relation(&join.relation, depth)?,
                on,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SqlFrom {
        first: parse_relation(&source.relation, depth)?,
        joins,
    })
}

fn parse_relation(factor: &TableFactor, depth: usize) -> Result<SqlRelation> {
    let alias = |alias: &Option<sqlparser::ast::TableAlias>| -> Result<Option<String>> {
        match alias {
            Some(alias) if !alias.columns.is_empty() => {
                bail!("column aliases on a table are not supported")
            }
            Some(alias) => Ok(Some(alias.name.value.clone())),
            None => Ok(None),
        }
    };
    match factor {
        TableFactor::Table {
            name,
            alias: table_alias,
            args: None,
            ..
        } => {
            let [part] = name.0.as_slice() else {
                bail!("a table name has no schema or catalog part: {name}");
            };
            let Some(identifier) = part.as_ident() else {
                bail!("{name} is not a table name");
            };
            Ok(SqlRelation {
                table: SqlTable::Named(identifier.value.clone()),
                alias: alias(table_alias)?,
            })
        }
        TableFactor::Derived {
            lateral: false,
            subquery,
            alias: table_alias,
            ..
        } => Ok(SqlRelation {
            table: SqlTable::Subquery(Box::new(SqlAst::from_query(subquery, depth + 1)?)),
            alias: alias(table_alias)?,
        }),
        _ => bail!("FROM must name an open table or a (SELECT …) subquery"),
    }
}

/// `ON a = b [AND c = d …]`.
fn parse_on(condition: &Expr, on: &mut Vec<(SqlColumn, SqlColumn)>) -> Result<()> {
    match condition {
        Expr::Nested(inner) => parse_on(inner, on),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            parse_on(left, on)?;
            parse_on(right, on)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            on.push((parse_column(left)?, parse_column(right)?));
            Ok(())
        }
        _ => bail!("ON supports column = column tests joined by AND"),
    }
}

/// The SELECT list, and the `AS` name of each item. `SELECT DISTINCT` and
/// `GROUP BY` without aggregates list each distinct combination once.
#[allow(clippy::type_complexity)]
fn parse_projection(
    items: &[SelectItem],
    group_by: &GroupByExpr,
    distinct: bool,
    calcs: &mut SqlCalcs,
) -> Result<(SqlProjection, Vec<Option<String>>, Vec<Option<String>>)> {
    if matches!(items, [SelectItem::Wildcard(_)]) {
        if !matches!(group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
        {
            bail!("SELECT * cannot be combined with GROUP BY");
        }
        if distinct {
            bail!("SELECT DISTINCT needs the columns named, not *");
        }
        return Ok((SqlProjection::All, Vec::new(), Vec::new()));
    }
    if items.is_empty() {
        bail!("SELECT needs at least one column");
    }
    let group_by = parse_group_by(group_by, calcs)?;
    let mut columns = Vec::new();
    let mut aggregates = Vec::new();
    let mut aliases: Vec<Option<String>> = Vec::new();
    let mut labels: Vec<Option<String>> = Vec::new();
    let mut result: Vec<SqlItem> = Vec::new();
    for item in items {
        let (expression, alias) = match item {
            SelectItem::UnnamedExpr(expression) => (expression, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::QualifiedWildcard(_, _) => {
                bail!("table.* is not supported; use * or name the columns")
            }
            SelectItem::Wildcard(_) => {
                bail!("SELECT * cannot be combined with explicit columns")
            }
            SelectItem::ExprWithAliases { .. } => {
                bail!("a SELECT item has one alias")
            }
        };
        let calculated = calcs.items.len();
        let mut on_result = false;
        match expression {
            Expr::Function(function) if is_aggregate(function) => {
                aggregates.push(parse_aggregate(function, calcs)?);
                result.push(SqlItem::Aggregate(aggregates.len() - 1));
            }
            // A column, a calculation (`price * qty AS total`), or a
            // calculation on aggregates' results (`ROUND(AVG(x), 2)`).
            _ => {
                let mut inner: Vec<SqlAggregateExpr> = Vec::new();
                let mut hook = |function: &Function| -> Result<SqlColumn> {
                    inner.push(parse_aggregate(function, calcs)?);
                    Ok(SqlColumn::new(format!("{AGG_PREFIX}{}", inner.len() - 1)))
                };
                let parsed = calc::parse_with(expression, &mut hook)?;
                if inner.is_empty() {
                    let column = calcs.column(expression, false)?;
                    columns.push(column);
                    // Its place among the columns for now; GROUP BY's below.
                    result.push(SqlItem::Group(columns.len() - 1));
                } else {
                    // Each aggregate it uses is computed once, with the others.
                    let global = inner
                        .into_iter()
                        .map(|aggregate| {
                            match aggregates.iter().position(|other| other.same(&aggregate)) {
                                Some(index) => index,
                                None => {
                                    aggregates.push(aggregate);
                                    aggregates.len() - 1
                                }
                            }
                        })
                        .collect::<Vec<_>>();
                    let mapped = parsed.map_columns(&|column: &SqlColumn| {
                        Ok(
                            match aggregate_result(column).and_then(|local| global.get(local)) {
                                Some(index) => SqlColumn::new(format!("{AGG_PREFIX}{index}")),
                                None => column.clone(),
                            },
                        )
                    })?;
                    result.push(SqlItem::Calc(mapped));
                    on_result = true;
                }
            }
        }
        // An item that calculates is shown by its SQL text unless it has an alias.
        let calculates = calcs.items.len() > calculated
            || columns
                .last()
                .is_some_and(|column| calcs.position(column).is_some())
                && !matches!(expression, Expr::Function(function) if is_aggregate(function));
        let aggregate_of_calc = matches!(expression, Expr::Function(function) if is_aggregate(function))
            && aggregates
                .last()
                .and_then(|aggregate: &SqlAggregateExpr| aggregate.column.as_ref())
                .is_some_and(|column| calcs.position(column).is_some());
        labels.push((on_result || calculates || aggregate_of_calc).then(|| expression.to_string()));
        if let Some(name) = &alias
            && aliases
                .iter()
                .flatten()
                .any(|earlier| earlier.eq_ignore_ascii_case(name))
        {
            bail!("two result columns are called {name}");
        }
        aliases.push(alias);
    }
    if aggregates.is_empty() {
        if !distinct && group_by.is_empty() {
            return Ok((SqlProjection::Columns(columns), aliases, labels));
        }
        // Each distinct combination once: a grouping without aggregates.
        let same = group_by.is_empty()
            || (columns.len() == group_by.len()
                && columns
                    .iter()
                    .zip(&group_by)
                    .all(|(column, group)| column.same(group)));
        if !same {
            bail!("SELECT must list the GROUP BY columns, in the same order");
        }
        reject_repeats(columns.iter(), "SELECT DISTINCT")?;
        if columns.len() > MAX_GROUP_COLUMNS {
            bail!(
                "SELECT DISTINCT names {} columns; the limit is {MAX_GROUP_COLUMNS}",
                columns.len()
            );
        }
        let items = (0..columns.len()).map(SqlItem::Group).collect();
        return Ok((
            SqlProjection::Aggregates(SqlAggregate {
                group_by: columns,
                expressions: Vec::new(),
                items,
            }),
            aliases,
            labels,
        ));
    }
    if group_by.is_empty() && !columns.is_empty() {
        bail!("aggregate SELECT cannot include ordinary columns without GROUP BY");
    }
    // A plain column of an aggregate query is one of its GROUP BY columns; SELECT
    // may list them in any order, or leave some out.
    let result = result
        .into_iter()
        .map(|item| match item {
            SqlItem::Group(index) => {
                let column = columns
                    .get(index)
                    .ok_or_else(|| anyhow!("SELECT column {index} is missing"))?;
                group_by
                    .iter()
                    .position(|group| group.same(column))
                    .map(SqlItem::Group)
                    .ok_or_else(|| anyhow!("{column} must be in GROUP BY or inside an aggregate"))
            }
            other => Ok(other),
        })
        .collect::<Result<Vec<_>>>()?;
    // Grouped rows are distinct already, so DISTINCT changes nothing here.
    Ok((
        SqlProjection::Aggregates(SqlAggregate {
            group_by,
            expressions: aggregates,
            items: result,
        }),
        aliases,
        labels,
    ))
}

fn parse_group_by(group_by: &GroupByExpr, calcs: &mut SqlCalcs) -> Result<Vec<SqlColumn>> {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => {
            if expressions.len() > MAX_GROUP_COLUMNS {
                bail!(
                    "GROUP BY names {} columns; the limit is {MAX_GROUP_COLUMNS}",
                    expressions.len()
                );
            }
            // Columns, or calculations (`GROUP BY YEAR(day)`).
            let columns = expressions
                .iter()
                .map(|expression| calcs.column(expression, true))
                .collect::<Result<Vec<_>>>()?;
            reject_repeats(columns.iter(), "GROUP BY")?;
            Ok(columns)
        }
        _ => bail!("GROUP BY supports plain column names only (no ROLLUP, CUBE or ALL)"),
    }
}

fn parse_aggregate(function: &Function, calcs: &mut SqlCalcs) -> Result<SqlAggregateExpr> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        bail!("aggregate modifiers are not supported");
    }
    let FunctionArguments::List(arguments) = &function.args else {
        bail!("aggregate needs parentheses");
    };
    if !arguments.clauses.is_empty() {
        bail!("aggregate argument clauses are not supported");
    }
    let distinct = arguments.duplicate_treatment == Some(DuplicateTreatment::Distinct);
    let function_name = function.name.to_string().to_ascii_uppercase();
    if distinct && function_name != "COUNT" {
        bail!("only COUNT supports DISTINCT");
    }
    let function = match function_name.as_str() {
        "COUNT" if distinct => AggregateFunction::CountDistinct,
        "COUNT" => AggregateFunction::Count,
        "SUM" => AggregateFunction::Sum,
        "AVG" => AggregateFunction::Avg,
        "MIN" => AggregateFunction::Min,
        "MAX" => AggregateFunction::Max,
        _ => bail!("unsupported aggregate {function_name}"),
    };
    let column = match arguments.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            if function == AggregateFunction::Count =>
        {
            None
        }
        // A column, or a calculation (`SUM(price * qty)`).
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(expression))] => {
            Some(calcs.column(expression, true)?)
        }
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] if distinct => {
            bail!("COUNT(DISTINCT …) needs a column, not *")
        }
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => bail!("only COUNT may use *"),
        _ => bail!("aggregate needs exactly one column argument"),
    };
    Ok(SqlAggregateExpr { function, column })
}

fn comparison_op(op: &BinaryOperator) -> Result<FilterOp> {
    Ok(match op {
        BinaryOperator::Eq => FilterOp::Eq,
        BinaryOperator::NotEq => FilterOp::Ne,
        BinaryOperator::Gt => FilterOp::Gt,
        BinaryOperator::GtEq => FilterOp::Ge,
        BinaryOperator::Lt => FilterOp::Lt,
        BinaryOperator::LtEq => FilterOp::Le,
        _ => bail!("WHERE supports =, !=, >, >=, <, <=, IN, BETWEEN, LIKE and IS NULL"),
    })
}

fn parse_predicate(expression: &Expr, depth: usize, calcs: &mut SqlCalcs) -> Result<SqlPredicate> {
    match expression {
        Expr::Nested(inner) => parse_predicate(inner, depth, calcs),
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            right,
        } => {
            let parts = vec![
                parse_predicate(left, depth, calcs)?,
                parse_predicate(right, depth, calcs)?,
            ];
            Ok(if *op == BinaryOperator::And {
                SqlPredicate::And(parts)
            } else {
                SqlPredicate::Or(parts)
            })
        }
        Expr::BinaryOp { left, op, right } => match right.as_ref() {
            Expr::Subquery(query) => Ok(SqlPredicate::CompareSubquery {
                column: calcs.column(left, true)?,
                op: comparison_op(op)?,
                query: Box::new(SqlAst::from_query(query, depth + 1)?),
            }),
            right => match parse_literal(right) {
                Ok(value) => Ok(SqlPredicate::compare(
                    calcs.column(left, true)?,
                    comparison_op(op)?,
                    value,
                )),
                // Two columns or calculations (`a > b`): the test itself is
                // calculated, and must be true.
                Err(_) => Ok(SqlPredicate::compare(
                    calcs.column(expression, true)?,
                    FilterOp::Eq,
                    "true",
                )),
            },
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(SqlPredicate::InSubquery {
            column: calcs.column(expr, true)?,
            query: Box::new(SqlAst::from_query(subquery, depth + 1)?),
            negated: *negated,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let column = calcs.column(expr, true)?;
            let op = if *negated { FilterOp::Ne } else { FilterOp::Eq };
            let mut tests = list
                .iter()
                .map(|value| {
                    Ok(SqlPredicate::compare(
                        column.clone(),
                        op.clone(),
                        parse_literal(value)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(match (tests.len(), negated) {
                (0, _) => bail!("IN needs at least one value"),
                (1, _) => tests.remove(0),
                // `NOT IN (a, b)` is `!= a AND != b`; `IN (a, b)` is `= a OR = b`.
                (_, true) => SqlPredicate::And(tests),
                (_, false) => SqlPredicate::Or(tests),
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let column = calcs.column(expr, true)?;
            let (low, high) = (parse_literal(low)?, parse_literal(high)?);
            Ok(if *negated {
                SqlPredicate::Or(vec![
                    SqlPredicate::compare(column.clone(), FilterOp::Lt, low),
                    SqlPredicate::compare(column, FilterOp::Gt, high),
                ])
            } else {
                SqlPredicate::And(vec![
                    SqlPredicate::compare(column.clone(), FilterOp::Ge, low),
                    SqlPredicate::compare(column, FilterOp::Le, high),
                ])
            })
        }
        Expr::IsNull(expr) => Ok(SqlPredicate::compare(
            calcs.column(expr, true)?,
            FilterOp::IsNull,
            "",
        )),
        Expr::IsNotNull(expr) => Ok(SqlPredicate::compare(
            calcs.column(expr, true)?,
            FilterOp::IsNotNull,
            "",
        )),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                bail!("LIKE ANY is not supported");
            }
            if escape_char.is_some() {
                bail!("LIKE ... ESCAPE is not supported");
            }
            let column = calcs.column(expr, true)?;
            let pattern = parse_literal(pattern)?;
            // `LIKE '%text%'` stays the plain contains test it has always been.
            let contained = pattern
                .strip_prefix('%')
                .and_then(|value| value.strip_suffix('%'))
                .filter(|value| !value.contains('%') && !value.contains('_'));
            Ok(match (contained, negated) {
                (Some(value), false) => SqlPredicate::compare(column, FilterOp::Contains, value),
                (_, false) => SqlPredicate::compare(column, FilterOp::Like, pattern),
                (_, true) => SqlPredicate::compare(column, FilterOp::NotLike, pattern),
            })
        }
        Expr::Exists { .. } => bail!("EXISTS is not supported; use IN (SELECT …)"),
        // `NOT (…)` is calculated, and must be true.
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => Ok(SqlPredicate::compare(
            calcs.column(expression, true)?,
            FilterOp::Eq,
            "true",
        )),
        _ => {
            bail!("WHERE supports comparisons, IN, BETWEEN, LIKE and IS NULL, joined by AND and OR")
        }
    }
}

/// `HAVING`: comparisons, `IN`, `BETWEEN` and `IS [NOT] NULL` on result
/// columns (GROUP BY columns, aggregates, aliases), joined by `AND` and `OR`.
fn parse_having(expression: &Expr, calcs: &mut SqlCalcs) -> Result<SqlHaving> {
    match expression {
        Expr::Nested(inner) => parse_having(inner, calcs),
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            right,
        } => {
            let parts = vec![parse_having(left, calcs)?, parse_having(right, calcs)?];
            Ok(if *op == BinaryOperator::And {
                SqlHaving::And(parts)
            } else {
                SqlHaving::Or(parts)
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(SqlHaving::compare(
            parse_target(left, "HAVING", calcs)?,
            comparison_op(op)?,
            parse_literal(right)?,
        )),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = parse_target(expr, "HAVING", calcs)?;
            let op = if *negated { FilterOp::Ne } else { FilterOp::Eq };
            let mut tests = list
                .iter()
                .map(|value| {
                    Ok(SqlHaving::compare(
                        target.clone(),
                        op.clone(),
                        parse_literal(value)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(match (tests.len(), negated) {
                (0, _) => bail!("IN needs at least one value"),
                (1, _) => tests.remove(0),
                (_, true) => SqlHaving::And(tests),
                (_, false) => SqlHaving::Or(tests),
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let target = parse_target(expr, "HAVING", calcs)?;
            let (low, high) = (parse_literal(low)?, parse_literal(high)?);
            Ok(if *negated {
                SqlHaving::Or(vec![
                    SqlHaving::compare(target.clone(), FilterOp::Lt, low),
                    SqlHaving::compare(target, FilterOp::Gt, high),
                ])
            } else {
                SqlHaving::And(vec![
                    SqlHaving::compare(target.clone(), FilterOp::Ge, low),
                    SqlHaving::compare(target, FilterOp::Le, high),
                ])
            })
        }
        Expr::IsNull(expr) => Ok(SqlHaving::compare(
            parse_target(expr, "HAVING", calcs)?,
            FilterOp::IsNull,
            "",
        )),
        Expr::IsNotNull(expr) => Ok(SqlHaving::compare(
            parse_target(expr, "HAVING", calcs)?,
            FilterOp::IsNotNull,
            "",
        )),
        _ => bail!(
            "HAVING supports comparisons, IN, BETWEEN and IS NULL on result columns, joined by AND and OR"
        ),
    }
}

pub(crate) fn parse_column(expression: &Expr) -> Result<SqlColumn> {
    match expression {
        Expr::Identifier(identifier) => Ok(SqlColumn::new(identifier.value.clone())),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [table, column] => Ok(SqlColumn::qualified(
                table.value.clone(),
                column.value.clone(),
            )),
            _ => bail!("a column is written name or table.name"),
        },
        _ => bail!("expected a column name"),
    }
}

fn parse_literal(expression: &Expr) -> Result<String> {
    match expression {
        Expr::Value(_) => Ok(unquote_sql_literal(&expression.to_string())),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Ok(format!("-{}", parse_literal(expr)?)),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => parse_literal(expr),
        _ => bail!("the right side of WHERE must be a string, number, or boolean literal"),
    }
}

fn unquote_sql_literal(value: &str) -> String {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .map(|value| value.replace("''", "'"))
        .unwrap_or_else(|| value.to_owned())
}

fn parse_limit(limit: Option<&sqlparser::ast::LimitClause>) -> Result<Option<usize>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let rendered = limit.to_string();
    let rendered = rendered.trim();
    let value = rendered
        .strip_prefix("LIMIT ")
        .or_else(|| rendered.strip_prefix("LIMIT"))
        .ok_or_else(|| {
            anyhow::anyhow!("only LIMIT <non-negative integer> is supported: {rendered}")
        })?
        .trim()
        .parse::<usize>()
        .context("LIMIT must be a non-negative integer")?;
    Ok(Some(value))
}

fn resolve_column(source: &ParquetSource, name: &str) -> Result<usize> {
    let schema = source.schema();
    if let Some(index) = schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
    {
        return Ok(index);
    }
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| field.name().eq_ignore_ascii_case(name).then_some(index));
    let Some(index) = matches.next() else {
        bail!("unknown SQL column '{name}'");
    };
    if matches.next().is_some() {
        bail!("SQL column '{name}' is ambiguous when matched case-insensitively");
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc};

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use pavi_runtime::{GenerationId, Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;
    use crate::{AggregateExpr, QueryEngine};

    const ROWS: i32 = 4_100;

    fn source() -> (TempDir, Arc<ParquetSource>) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("sql.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("enabled", DataType::Boolean, false),
        ]));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1_000))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..ROWS)),
                        Arc::new(StringArray::from_iter_values(
                            (0..ROWS).map(|row| if row % 2 == 0 { "alpha" } else { "beta" }),
                        )),
                        Arc::new(BooleanArray::from_iter((0..ROWS).map(|row| row % 2 == 0))),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        (directory, Arc::new(ParquetSource::open(path).unwrap()))
    }

    fn runtime() -> Runtime {
        Runtime::new(RuntimeConfig {
            worker_count: 2,
            queue_capacity: 4,
        })
        .unwrap()
    }

    fn execute(sql: &str, source: Arc<ParquetSource>) -> Vec<RecordBatch> {
        let plan = SqlAst::parse(sql).unwrap().to_logical_plan(source).unwrap();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&plan, GenerationId(4))
            .unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = execution.next_batch().unwrap() {
            assert_eq!(batch.generation_id, GenerationId(4));
            batches.push(batch.batch);
        }
        batches
    }

    /// Seven flights: carrier and delay (one null).
    fn flights() -> (TempDir, Arc<ParquetSource>) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("flights.parquet");
        let batch = RecordBatch::try_from_iter(vec![
            (
                "carrier",
                Arc::new(StringArray::from(vec![
                    "AA", "AA", "BB", "BB", "BB", "CC", "AA",
                ])) as arrow_array::ArrayRef,
            ),
            (
                "delay",
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(5),
                    Some(5),
                    None,
                    Some(30),
                    Some(10),
                ])),
            ),
        ])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        (directory, Arc::new(ParquetSource::open(path).unwrap()))
    }

    /// The rows of a result as text, one `Vec` per row.
    fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            parquet_reader::value::scalar_text(column.as_ref(), row)
                                .unwrap()
                                .unwrap_or_else(|| "null".to_owned())
                        })
                        .collect(),
                );
            }
        }
        rows
    }

    fn names(batches: &[RecordBatch]) -> Vec<String> {
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[test]
    fn sorts_filters_and_names_aggregate_results() {
        let (_directory, source) = flights();
        let run = |sql: &str| execute(sql, Arc::clone(&source));

        // ORDER BY an alias of an aggregate, then a GROUP BY column; ties
        // keep the order the groups were first seen in.
        let counts = run(
            "SELECT carrier, COUNT(*) AS n FROM dataset GROUP BY carrier ORDER BY n DESC, carrier",
        );
        assert_eq!(names(&counts), ["carrier", "n"]);
        assert_eq!(rows(&counts), [["AA", "3"], ["BB", "3"], ["CC", "1"]]);

        // ORDER BY the aggregate itself, and LIMIT after the sort.
        let top = run(
            "SELECT carrier, SUM(delay) FROM dataset GROUP BY carrier ORDER BY SUM(delay) DESC LIMIT 2",
        );
        assert_eq!(rows(&top), [["AA", "40"], ["CC", "30"]]);

        // HAVING on an aggregate, an alias and a GROUP BY column.
        let having =
            run("SELECT carrier, COUNT(*) FROM dataset GROUP BY carrier HAVING COUNT(*) > 1");
        assert_eq!(rows(&having), [["AA", "3"], ["BB", "3"]]);
        let both = run(
            "SELECT carrier, COUNT(*) AS n FROM dataset GROUP BY carrier \
             HAVING n >= 3 AND carrier <> 'AA'",
        );
        assert_eq!(rows(&both), [["BB", "3"]]);
        let between = run(
            "SELECT carrier, AVG(delay) AS mean FROM dataset GROUP BY carrier \
             HAVING mean BETWEEN 5 AND 15 ORDER BY 1 DESC",
        );
        assert_eq!(rows(&between), [["BB", "5"], ["AA", "13.333333333333334"]]);

        // COUNT(DISTINCT …) skips nulls; ORDER BY a position.
        let distinct = run(
            "SELECT carrier, COUNT(DISTINCT delay) FROM dataset GROUP BY carrier ORDER BY 2 DESC",
        );
        assert_eq!(names(&distinct), ["carrier", "COUNT(DISTINCT delay)"]);
        assert_eq!(rows(&distinct), [["AA", "2"], ["BB", "1"], ["CC", "1"]]);
        assert_eq!(
            rows(&run(
                "SELECT COUNT(DISTINCT carrier) AS carriers FROM dataset"
            )),
            [["3"]]
        );

        // SELECT DISTINCT, and GROUP BY without aggregates.
        assert_eq!(
            rows(&run(
                "SELECT DISTINCT carrier FROM dataset ORDER BY carrier DESC"
            )),
            [["CC"], ["BB"], ["AA"]]
        );
        assert_eq!(
            rows(&run("SELECT carrier FROM dataset GROUP BY carrier")),
            [["AA"], ["BB"], ["CC"]]
        );
        assert_eq!(
            run("SELECT DISTINCT carrier, delay FROM dataset")
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );

        // Aliases on plain columns, and ORDER BY one (nulls last).
        let aliased = run("SELECT delay AS d, carrier FROM dataset ORDER BY d DESC LIMIT 2");
        assert_eq!(names(&aliased), ["d", "carrier"]);
        assert_eq!(rows(&aliased), [["30", "CC"], ["20", "AA"]]);

        // A query over a subquery is prepared first; the output step still applies.
        let nested = run(
            "SELECT carrier, COUNT(*) AS n FROM (SELECT carrier FROM dataset WHERE delay > 5) AS t \
             GROUP BY carrier ORDER BY n DESC",
        );
        assert_eq!(rows(&nested), [["AA", "3"], ["CC", "1"]]);
    }

    #[test]
    fn calculates_in_select_where_group_by_and_order_by() {
        let (_directory, source) = flights();
        let run = |sql: &str| execute(sql, Arc::clone(&source));

        // A plain SELECT calculates on the way out; items are named by their
        // alias or their SQL text.
        let select = run("SELECT carrier, delay * 2 AS double, delay + 1 FROM dataset LIMIT 3");
        assert_eq!(names(&select), ["carrier", "double", "delay + 1"]);
        assert_eq!(
            rows(&select),
            [["AA", "20", "11"], ["AA", "40", "21"], ["BB", "10", "6"]]
        );
        let plan = SqlAst::parse("SELECT UPPER(carrier) FROM dataset")
            .unwrap()
            .to_logical_plan(Arc::clone(&source))
            .unwrap();
        assert!(
            !plan.is_prepared(),
            "a SELECT-only calculation reads the file directly"
        );
        let case = run(
            "SELECT CASE WHEN delay >= 20 THEN 'late' WHEN delay IS NULL THEN '?' ELSE 'ok' END AS status \
             FROM dataset",
        );
        assert_eq!(
            rows(&case).concat(),
            ["ok", "late", "ok", "ok", "?", "late", "ok"]
        );

        // WHERE on a calculation, on two columns, and NOT: calculated first.
        assert_eq!(
            rows(&run("SELECT delay FROM dataset WHERE delay * 2 > 25")),
            [["20"], ["30"]]
        );
        assert_eq!(
            rows(&run(
                "SELECT delay FROM dataset WHERE delay > LENGTH(carrier) * 5"
            )),
            [["20"], ["30"]]
        );
        assert_eq!(
            rows(&run("SELECT delay FROM dataset WHERE NOT (delay > 5)")),
            [["5"], ["5"]]
        );
        // SELECT * doesn't show the hidden calculation columns.
        assert_eq!(
            names(&run("SELECT * FROM dataset WHERE delay * 2 > 25")),
            ["carrier", "delay"]
        );

        // GROUP BY a calculation, named by its text in SELECT.
        let grouped =
            run("SELECT delay > 10, COUNT(*) FROM dataset GROUP BY delay > 10 ORDER BY 1");
        assert_eq!(names(&grouped), ["delay > 10", "COUNT(*)"]);
        assert_eq!(
            rows(&grouped),
            [["false", "4"], ["true", "2"], ["null", "1"]]
        );

        // An aggregate of a calculation.
        let sums = run(
            "SELECT carrier, SUM(delay * 2) AS twice FROM dataset GROUP BY carrier ORDER BY twice DESC",
        );
        assert_eq!(rows(&sums), [["AA", "80"], ["CC", "60"], ["BB", "20"]]);

        // SELECT lists GROUP BY columns in any order, or leaves them out.
        assert_eq!(
            rows(&run(
                "SELECT COUNT(*), carrier FROM dataset GROUP BY carrier"
            )),
            [["3", "AA"], ["3", "BB"], ["1", "CC"]]
        );
        assert_eq!(
            rows(&run("SELECT COUNT(*) FROM dataset GROUP BY carrier, delay")).len(),
            5
        );

        // Calculations on aggregates' results, made from the aggregate's one
        // result batch; HAVING and ORDER BY use their aliases.
        let on_results = run(
            "SELECT carrier, ROUND(AVG(delay), 1) AS mean, SUM(delay) / COUNT(*) FROM dataset \
             GROUP BY carrier HAVING mean > 10 ORDER BY mean DESC",
        );
        assert_eq!(
            names(&on_results),
            ["carrier", "mean", "SUM(delay) / COUNT(*)"]
        );
        assert_eq!(
            rows(&on_results),
            [["CC", "30", "30"], ["AA", "13.3", "13.333333333333334"]]
        );
        assert_eq!(
            rows(&run("SELECT COUNT(*) * 2 AS twice FROM dataset")),
            [["14"]]
        );
        assert_eq!(
            rows(&run(
                "SELECT carrier || ':' || COUNT(*) FROM dataset GROUP BY carrier ORDER BY 1 DESC"
            )),
            [["CC:1"], ["BB:3"], ["AA:3"]]
        );

        // ORDER BY a calculation, and by the alias of one.
        assert_eq!(
            rows(&run(
                "SELECT carrier, delay FROM dataset ORDER BY delay * -1 LIMIT 2"
            )),
            [["CC", "30"], ["AA", "20"]]
        );
        assert_eq!(
            rows(&run(
                "SELECT delay % 7 AS rest FROM dataset ORDER BY rest DESC LIMIT 2"
            )),
            [["6"], ["5"]]
        );

        for (sql, message) in [
            (
                "SELECT carrier FROM dataset WHERE SUM(delay) > 1",
                "is an aggregate and can't be used here",
            ),
            (
                "SELECT carrier, delay + SUM(delay) FROM dataset GROUP BY carrier",
                "delay must be in GROUP BY or inside an aggregate",
            ),
            (
                "SELECT delay FROM dataset WHERE carrier * 2 > 1",
                "arithmetic needs numbers",
            ),
            ("SELECT NOW() FROM dataset", "unknown function NOW"),
        ] {
            let error = SqlAst::parse(sql)
                .and_then(|parsed| parsed.to_logical_plan(Arc::clone(&source)))
                .err()
                .unwrap_or_else(|| panic!("{sql} should be refused"));
            assert!(format!("{error:#}").contains(message), "{sql}: {error:#}");
        }
    }

    #[test]
    fn explains_what_aggregate_ordering_and_having_cannot_do() {
        let (_directory, source) = flights();
        for (sql, message) in [
            (
                "SELECT carrier, COUNT(*) FROM dataset GROUP BY carrier ORDER BY MAX(delay)",
                "MAX(delay) must also be in SELECT",
            ),
            (
                "SELECT carrier, COUNT(*) FROM dataset GROUP BY carrier ORDER BY delay",
                "delay is not a result column",
            ),
            (
                "SELECT carrier FROM dataset HAVING carrier = 'AA'",
                "HAVING needs GROUP BY or an aggregate",
            ),
            (
                "SELECT carrier, COUNT(*) FROM dataset GROUP BY carrier ORDER BY 3",
                "position 3 is not one of the 2 result columns",
            ),
            (
                "SELECT carrier FROM dataset ORDER BY COUNT(*)",
                "ORDER BY COUNT(*) needs the aggregate in SELECT",
            ),
            (
                "SELECT DISTINCT * FROM dataset",
                "SELECT DISTINCT needs the columns named",
            ),
            (
                "SELECT carrier AS x, COUNT(*) AS X FROM dataset GROUP BY carrier",
                "two result columns are called X",
            ),
            (
                "SELECT SUM(DISTINCT delay) FROM dataset",
                "only COUNT supports DISTINCT",
            ),
            (
                "SELECT COUNT(DISTINCT *) FROM dataset",
                "COUNT(DISTINCT …) needs a column, not *",
            ),
            (
                "SELECT DISTINCT ON (carrier) carrier FROM dataset",
                "DISTINCT ON is not supported",
            ),
            (
                "SELECT carrier, COUNT(*) FROM dataset GROUP BY carrier HAVING COUNT(*) > 'x'",
                "HAVING",
            ),
        ] {
            let error = SqlAst::parse(sql)
                .and_then(|parsed| parsed.to_logical_plan(Arc::clone(&source)))
                .err()
                .unwrap_or_else(|| panic!("{sql} should be refused"));
            assert!(format!("{error:#}").contains(message), "{sql}: {error:#}");
        }
    }

    #[test]
    fn translates_select_star_and_projection() {
        let (_directory, source) = source();
        let all = SqlAst::parse("SELECT * FROM dataset").unwrap();
        assert_eq!(all.projection(), &SqlProjection::All);
        assert_eq!(
            SqlAst::parse("SELECT name, id FROM dataset")
                .unwrap()
                .projection(),
            &SqlProjection::Columns(vec![SqlColumn::new("name"), SqlColumn::new("id")])
        );

        let batches = execute("SELECT name FROM dataset LIMIT 2", source);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn combines_predicates_with_and_or_in_between_like_and_null_tests() {
        let (_directory, source) = source();
        let ids = |sql: &str| {
            execute(sql, Arc::clone(&source))
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>()
        };
        // `name` is alpha for even ids and beta for odd ones; `enabled` is true for even.
        for (predicate, expected) in [
            ("id < 4 AND name = 'beta'", vec![1, 3]),
            ("id = 1 OR id = 4099", vec![1, 4099]),
            ("id = 0 OR id < 6 AND enabled = false", vec![0, 1, 3, 5]),
            ("(id = 0 OR id < 6) AND enabled = false", vec![1, 3, 5]),
            ("id IN (3, 1, 9999)", vec![1, 3]),
            ("id < 5 AND id NOT IN (0, 2)", vec![1, 3, 4]),
            ("id BETWEEN 1000 AND 1002", vec![1000, 1001, 1002]),
            ("id NOT BETWEEN 2 AND 4097", vec![0, 1, 4098, 4099]),
            ("id < 4 AND name LIKE 'al%'", vec![0, 2]),
            ("id < 4 AND name LIKE '_et_'", vec![1, 3]),
            ("id < 4 AND name NOT LIKE '%ph%'", vec![1, 3]),
            ("id < 3 AND name IS NOT NULL", vec![0, 1, 2]),
            ("name IS NULL", vec![]),
        ] {
            let sql = format!("SELECT id FROM dataset WHERE {predicate}");
            assert_eq!(ids(&sql), expected, "{sql}");
        }

        // The filter also narrows aggregates and sorts.
        let count = execute(
            "SELECT COUNT(*) FROM dataset WHERE id < 100 AND enabled = true",
            Arc::clone(&source),
        );
        assert_eq!(
            parquet_reader::value::format_cell(count[0].column(0).as_ref(), 0),
            "50"
        );
        assert_eq!(
            ids("SELECT id FROM dataset WHERE id IN (7, 5, 6) ORDER BY id DESC"),
            vec![7, 6, 5]
        );

        // `LIKE '%text%'` is still the plain contains test.
        assert_eq!(
            SqlAst::parse("SELECT id FROM dataset WHERE name LIKE '%ph%'")
                .unwrap()
                .predicate(),
            Some(&SqlPredicate::compare(
                SqlColumn::new("name"),
                FilterOp::Contains,
                "ph"
            ))
        );
        // More comparisons than the reader allows is a planning error, not a hang.
        let many = (0..=parquet_reader::filter::MAX_FILTER_TERMS)
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let error = SqlAst::parse(&format!("SELECT id FROM dataset WHERE id IN ({many})"))
            .unwrap()
            .to_logical_plan(Arc::clone(&source))
            .err()
            .unwrap();
        assert!(
            format!("{error:#}").contains("the limit is 64"),
            "{error:#}"
        );
    }

    #[test]
    fn translates_every_supported_predicate() {
        let (_directory, source) = source();
        for sql in [
            "SELECT id FROM dataset WHERE id = 2",
            "SELECT id FROM dataset WHERE id == 2",
            "SELECT id FROM dataset WHERE id != 2",
            "SELECT id FROM dataset WHERE id > 2",
            "SELECT id FROM dataset WHERE id >= 2",
            "SELECT id FROM dataset WHERE id < 2",
            "SELECT id FROM dataset WHERE id <= 2",
            "SELECT name FROM dataset WHERE name LIKE '%ph%' LIMIT 1",
            "SELECT enabled FROM dataset WHERE enabled = true LIMIT 1",
        ] {
            assert!(!execute(sql, Arc::clone(&source)).is_empty(), "{sql}");
        }
    }

    #[test]
    fn combines_filter_projection_limit_and_matches_direct_plan() {
        let (_directory, source) = source();
        let sql = "SELECT name FROM dataset WHERE id >= 1000 LIMIT 3";
        let sql_batches = execute(sql, Arc::clone(&source));
        let direct = LogicalPlan::scan(source)
            .filter(Filter::parse("id >= 1000").unwrap())
            .project(vec![1])
            .limit(3);
        let runtime = runtime();
        let direct_batch = QueryEngine::new(&runtime)
            .execute(&direct, GenerationId(4))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(sql_batches[0], direct_batch);
    }

    #[test]
    fn translates_single_column_order_by_through_the_runtime_path() {
        let (_directory, order_source) = source();
        let sql = "SELECT id FROM dataset ORDER BY id DESC NULLS LAST LIMIT 3";
        let ast = SqlAst::parse(sql).unwrap();
        assert_eq!(
            ast.sort(),
            [SqlSort {
                target: SqlTarget::Column(SqlColumn::new("id")),
                direction: SortDirection::Descending,
                nulls: NullOrder::Last,
            }]
        );
        let batches = execute(sql, order_source);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values();
        assert_eq!(ids, &[ROWS - 1, ROWS - 2, ROWS - 3]);

        let (_directory, projected_source) = source();
        let projected = execute(
            "SELECT id FROM dataset ORDER BY name ASC LIMIT 3",
            projected_source,
        );
        let ids = projected[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values();
        assert_eq!(ids, &[0, 2, 4]);
    }

    #[test]
    fn translates_aggregates_and_group_by_through_the_runtime_path() {
        let (_directory, source) = source();
        let sql = "SELECT COUNT(*), COUNT(id), SUM(id), AVG(id), MIN(name), MAX(name) FROM dataset";
        let ast = SqlAst::parse(sql).unwrap();
        assert!(matches!(ast.projection(), SqlProjection::Aggregates(_)));
        let batch = execute(sql, Arc::clone(&source)).pop().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            ROWS as u64
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            ROWS as u64
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::from(ROWS - 1) * i64::from(ROWS) / 2
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            f64::from(ROWS - 1) / 2.0
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "alpha"
        );
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "beta"
        );

        let grouped_sql = "SELECT name, COUNT(*), AVG(id) FROM dataset GROUP BY name";
        let grouped = execute(grouped_sql, Arc::clone(&source));
        let direct = LogicalPlan::scan(source)
            .aggregate_grouped(
                vec![1],
                vec![AggregateExpr::count_all(), AggregateExpr::avg(0)],
            )
            .unwrap();
        let runtime = runtime();
        let direct_batch = QueryEngine::new(&runtime)
            .execute(&direct, GenerationId(30))
            .unwrap()
            .next_batch()
            .unwrap()
            .unwrap()
            .batch;
        assert_eq!(grouped, vec![direct_batch]);
    }

    #[test]
    fn aggregates_empty_input_and_rejects_unsupported_forms() {
        let (_directory, source) = source();
        let empty = execute(
            "SELECT COUNT(*), SUM(id) FROM dataset WHERE id > 999999",
            Arc::clone(&source),
        );
        assert_eq!(empty.len(), 1);
        assert_eq!(
            empty[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert!(empty[0].column(1).is_null(0));

        for sql in [
            "SELECT id, COUNT(*) FROM dataset",
            "SELECT name, COUNT(*) FROM dataset GROUP BY id",
            "SELECT SUM(DISTINCT id) FROM dataset",
            "SELECT SUM(name) FROM dataset",
        ] {
            assert!(
                SqlAst::parse(sql)
                    .and_then(|parsed| parsed.to_logical_plan(Arc::clone(&source)))
                    .is_err(),
                "{sql}"
            );
        }
        assert!(
            SqlAst::parse("SELECT COUNT(*) FROM dataset ORDER BY id")
                .unwrap()
                .to_logical_plan(source)
                .is_err()
        );
    }

    #[test]
    fn rejects_invalid_unsupported_and_invalid_source_queries() {
        for sql in [
            "SELECT FROM dataset",
            "SELECT id FROM dataset a RIGHT JOIN dataset b ON a.id = b.id",
            "SELECT id FROM dataset a CROSS JOIN dataset b",
            "SELECT id FROM dataset a JOIN dataset b USING (id)",
            "SELECT id FROM dataset a JOIN dataset b ON a.id > b.id",
            "SELECT id FROM dataset, other",
            "SELECT a.* FROM dataset a",
            "SELECT id FROM dataset WHERE EXISTS (SELECT id FROM dataset)",
            "SELECT id FROM dataset UNION SELECT id FROM dataset",
            "SELECT id FROM dataset WHERE name LIKE 'a!%' ESCAPE '!'",
            "SELECT id FROM dataset WHERE name ILIKE 'a%'",
            "SELECT id FROM dataset WHERE id BETWEEN name AND 3",
            "SELECT id FROM dataset ORDER BY id, id",
            "SELECT id, COUNT(*) FROM dataset GROUP BY enabled",
            "SELECT id FROM dataset LIMIT 1 OFFSET 1",
        ] {
            assert!(SqlAst::parse(sql).is_err(), "{sql}");
        }

        // Calculated tests in WHERE run on prepared rows (ADR 0013).
        let (_directory, calculated) = source();
        for sql in [
            "SELECT id FROM dataset WHERE NOT (id = 1)",
            "SELECT id FROM dataset WHERE NOT enabled",
            "SELECT id FROM dataset WHERE id + 1 = 2",
        ] {
            let plan = SqlAst::parse(sql)
                .and_then(|parsed| parsed.to_logical_plan(Arc::clone(&calculated)))
                .unwrap_or_else(|error| panic!("{sql}: {error:#}"));
            assert!(plan.is_prepared(), "{sql}");
        }
        assert_eq!(
            rows(&execute(
                "SELECT id FROM dataset WHERE id + 1 = 2",
                calculated
            )),
            [["1"]]
        );

        let (_directory, source) = source();
        let error = SqlAst::parse("SELECT id FROM other")
            .unwrap()
            .to_logical_plan(Arc::clone(&source))
            .err()
            .unwrap();
        assert!(
            format!("{error:#}").contains("unknown table other"),
            "{error:#}"
        );
        assert!(
            SqlAst::parse("SELECT missing FROM dataset")
                .unwrap()
                .to_logical_plan(Arc::clone(&source))
                .is_err()
        );
        assert!(
            SqlAst::parse("SELECT * FROM dataset WHERE id LIKE '%2%'")
                .unwrap()
                .to_logical_plan(source)
                .is_err()
        );
    }

    #[test]
    fn handles_empty_results_and_streams_bounded_batches() {
        let (_directory, source) = source();
        assert!(
            execute(
                "SELECT * FROM dataset WHERE id > 999999",
                Arc::clone(&source)
            )
            .is_empty()
        );

        let batches = execute("SELECT * FROM dataset", source);
        assert!(batches.len() > 1);
        assert!(
            batches
                .iter()
                .all(|batch| batch.num_rows() <= parquet_reader::PAGE_ROWS as usize)
        );
    }

    #[test]
    fn preserves_runtime_cancellation_and_generation() {
        let (_directory, source) = source();
        let plan = SqlAst::parse("SELECT * FROM dataset")
            .unwrap()
            .to_logical_plan(source)
            .unwrap();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&plan, GenerationId(11))
            .unwrap();
        execution.cancel();
        assert!(
            execution
                .next_batch()
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    /// `orders` and `customers`, for joins and subqueries.
    fn tables() -> (TempDir, Tables) {
        let directory = TempDir::new().unwrap();
        let write = |name: &str, batch: RecordBatch| {
            let path = directory.path().join(name);
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            Arc::new(ParquetSource::open(path).unwrap())
        };
        let orders = write(
            "orders.parquet",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as arrow_array::ArrayRef,
                ),
                (
                    "customer",
                    Arc::new(Int64Array::from(vec![
                        Some(10),
                        Some(20),
                        Some(10),
                        None,
                        Some(30),
                    ])),
                ),
                (
                    "amount",
                    Arc::new(Float64Array::from(vec![5.0, 10.0, 15.0, 20.0, 50.0])),
                ),
            ])
            .unwrap(),
        );
        let customers = write(
            "customers.parquet",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int32Array::from(vec![10, 20, 30])) as arrow_array::ArrayRef,
                ),
                ("name", Arc::new(StringArray::from(vec!["Ann", "Bo", "Cy"]))),
            ])
            .unwrap(),
        );
        let tables = Tables::new(Arc::clone(&orders))
            .with("orders", orders)
            .with("customers", customers);
        (directory, tables)
    }

    fn run(sql: &str, tables: &Tables) -> Result<(Vec<String>, Vec<String>)> {
        let plan = SqlAst::parse(sql)?.to_logical_plan_with(tables)?;
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime).execute(&plan, GenerationId(9))?;
        let mut names = Vec::new();
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch()? {
            let batch = batch.batch;
            names = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            for row in 0..batch.num_rows() {
                rows.push(
                    (0..batch.num_columns())
                        .map(|column| {
                            parquet_reader::value::scalar_text(batch.column(column).as_ref(), row)
                                .unwrap()
                                .unwrap_or_else(|| "null".into())
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
        }
        Ok((names, rows))
    }

    #[test]
    fn joins_open_tables() {
        let (_directory, tables) = tables();
        let rows = |sql: &str| run(sql, &tables).unwrap().1;
        assert_eq!(
            rows(
                "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer = c.id ORDER BY o.id"
            ),
            ["1,Ann", "2,Bo", "3,Ann", "5,Cy"]
        );
        assert_eq!(
            rows(
                "SELECT o.id, name FROM orders AS o LEFT JOIN customers c ON c.id = o.customer ORDER BY o.id"
            ),
            ["1,Ann", "2,Bo", "3,Ann", "4,null", "5,Cy"]
        );
        // Tests on one table are applied while reading it; others after the join.
        assert_eq!(
            rows(
                "SELECT o.id FROM orders o JOIN customers c ON o.customer = c.id \
                  WHERE o.amount > 6 AND (c.name = 'Ann' OR o.id = 5) ORDER BY o.id"
            ),
            ["3", "5"]
        );
        // On the optional side of a LEFT JOIN a test applies after the join.
        assert_eq!(
            rows(
                "SELECT o.id FROM orders o LEFT JOIN customers c ON o.customer = c.id \
                  WHERE c.name IS NULL"
            ),
            ["4"]
        );
        assert_eq!(
            rows(
                "SELECT c.name, COUNT(*), SUM(o.amount) FROM orders o \
                  JOIN customers c ON o.customer = c.id GROUP BY c.name"
            ),
            ["Ann,2,20", "Bo,1,10", "Cy,1,50"]
        );
        // `dataset` is the tab running the query (orders here).
        assert_eq!(
            rows(
                "SELECT dataset.id FROM dataset JOIN customers ON dataset.customer = customers.id \
                  WHERE customers.name = 'Bo'"
            ),
            ["2"]
        );
        // SELECT * keeps every column; a name in both tables gets its table's name.
        let (names, all) = run(
            "SELECT * FROM orders o JOIN customers c ON o.customer = c.id LIMIT 1",
            &tables,
        )
        .unwrap();
        assert_eq!(names, ["o.id", "customer", "amount", "c.id", "name"]);
        assert_eq!(all, ["1,10,5,10,Ann"]);
    }

    #[test]
    fn runs_subqueries() {
        let (_directory, tables) = tables();
        let rows = |sql: &str| run(sql, &tables).unwrap().1;
        assert_eq!(
            rows(
                "SELECT id FROM orders WHERE customer IN (SELECT id FROM customers WHERE name < 'C') ORDER BY id"
            ),
            ["1", "2", "3"]
        );
        assert_eq!(
            rows(
                "SELECT id FROM orders WHERE customer NOT IN (SELECT id FROM customers WHERE name = 'Ann')"
            ),
            ["2", "5"]
        );
        assert_eq!(
            rows(
                "SELECT id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders) ORDER BY id"
            ),
            ["5"]
        );
        assert_eq!(
            rows("SELECT name FROM (SELECT * FROM customers WHERE id > 10) AS c ORDER BY name"),
            ["Bo", "Cy"]
        );
        assert_eq!(
            rows(
                "SELECT c.name, big.id FROM (SELECT id, customer FROM orders WHERE amount >= 15) big \
                  JOIN customers c ON big.customer = c.id ORDER BY big.id"
            ),
            ["Ann,3", "Cy,5"]
        );
        // A subquery comparison that finds no row compares with NULL: nothing passes.
        assert!(rows("SELECT id FROM orders WHERE amount = (SELECT MAX(amount) FROM orders WHERE id > 99)").is_empty());
    }

    #[test]
    fn explains_what_joins_and_subqueries_cannot_do() {
        let (_directory, tables) = tables();
        let error = |sql: &str| format!("{:#}", run(sql, &tables).err().unwrap());
        assert!(
            error("SELECT id FROM orders o JOIN customers c ON o.customer = c.id")
                .contains("more than one table"),
        );
        assert!(
            error("SELECT o.id FROM orders o JOIN customers c ON o.customer = c.name")
                .contains("types must match"),
        );
        assert!(
            error("SELECT o.id FROM orders o JOIN orders o ON o.id = o.id")
                .contains("give one an alias"),
        );
        assert!(error("SELECT id FROM missing").contains("unknown table missing"));
        assert!(
            error("SELECT id FROM orders WHERE amount > (SELECT amount FROM orders)")
                .contains("more than one row"),
        );
        assert!(
            error("SELECT id FROM orders WHERE customer IN (SELECT id, name FROM customers)")
                .contains("exactly one column"),
        );
        assert!(
            error("SELECT id FROM orders WHERE id = 1 OR customer IN (SELECT id FROM customers)")
                .contains("can't be combined with OR"),
        );
        let deep = "SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE id IN \
                    (SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE id IN \
                    (SELECT id FROM orders))))";
        assert!(error(deep).contains("nest at most"));
    }
}
