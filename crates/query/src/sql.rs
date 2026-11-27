use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow_schema::{Field, Schema, SchemaRef};
use parquet_reader::{
    FilterExpr, FilterOp, JoinKind, MAX_JOIN_KEYS, MAX_SORT_KEYS, MAX_WINDOW_OFFSET, NullOrder,
    ParquetSource, SortDirection, SortSpec, WindowExpr, WindowFrame, WindowFunction, WindowSpec,
    aggregate::MAX_GROUP_COLUMNS, join::check_join_key_types,
};
use sqlparser::{
    ast::{
        BinaryOperator, Distinct, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
        FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, OrderBy, OrderByKind, Query,
        SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, WindowFrameBound,
        WindowFrameUnits, WindowType,
    },
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{
    AggregateExpr, AggregateFunction, Filter, LogicalPlan, Planner,
    expr::{self as calc},
    logical::{Output, OutputFilter, Prepared},
    prep::{MAX_JOINS, MAX_SUBQUERY_DEPTH, Prep, PrepFilter, PrepValue, WindowGroup},
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
        self.add(parsed, expression.to_string(), outside_select)
    }

    /// The column an already parsed calculation stands for.
    fn add(
        &mut self,
        parsed: calc::Expr<SqlColumn>,
        text: String,
        outside_select: bool,
    ) -> Result<SqlColumn> {
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
            text,
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
    /// The window functions SELECT uses (ADR 0024); the one at `n` is the
    /// column `#win{n}`.
    windows: Vec<SqlWindow>,
    predicate: Option<SqlPredicate>,
    having: Option<SqlHaving>,
    /// ORDER BY keys, most significant first.
    sort: Vec<SqlSort>,
    limit: Option<usize>,
}

/// Columns that hold a window function's result.
const WIN_PREFIX: &str = "#win";
/// Window functions in one query.
pub const MAX_WINDOWS: usize = 16;
/// Different `OVER (PARTITION BY … ORDER BY …)` in one query: each sorts the rows.
pub const MAX_WINDOW_SORTS: usize = 4;

/// One window function, `function(column) OVER (PARTITION BY … ORDER BY …
/// frame)` (ADR 0024). Its column and keys may be calculations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlWindow {
    pub function: WindowFunction,
    pub column: Option<SqlColumn>,
    /// LAG/LEAD's default, as text.
    pub default: Option<String>,
    pub frame: WindowFrame,
    pub partition_by: Vec<SqlColumn>,
    pub order_by: Vec<(SqlColumn, SortDirection, NullOrder)>,
}

impl SqlWindow {
    /// Whether `other` sorts the rows the same way, so they share one pass.
    fn same_order(&self, other: &Self) -> bool {
        self.partition_by.len() == other.partition_by.len()
            && self
                .partition_by
                .iter()
                .zip(&other.partition_by)
                .all(|(left, right)| left.same(right))
            && self.order_by.len() == other.order_by.len()
            && self.order_by.iter().zip(&other.order_by).all(
                |((left, left_direction, left_nulls), (right, right_direction, right_nulls))| {
                    left.same(right)
                        && left_direction == right_direction
                        && left_nulls == right_nulls
                },
            )
    }

    fn columns(&self) -> impl Iterator<Item = &SqlColumn> {
        self.column
            .iter()
            .chain(&self.partition_by)
            .chain(self.order_by.iter().map(|(column, _, _)| column))
    }
}

/// Which window function `column` names, when it holds one's result.
fn window_result(column: &SqlColumn) -> Option<usize> {
    column
        .table
        .is_none()
        .then(|| column.name.strip_prefix(WIN_PREFIX)?.parse().ok())
        .flatten()
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
    /// `part UNION [ALL] part …` (ADR 0023): the parts' rows one after the
    /// other; without `ALL`, each distinct row once.
    Union {
        parts: Vec<SqlAst>,
        all: bool,
    },
}

/// Parts one `UNION` may join.
pub const MAX_UNION_PARTS: usize = 16;
/// Names one `WITH` may define.
pub const MAX_WITH_NAMES: usize = 16;

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
        let function = self.function.sql_name();
        match (&self.column, self.function) {
            (None, _) => write!(formatter, "{function}(*)"),
            (Some(column), AggregateFunction::CountDistinct) => {
                write!(formatter, "COUNT(DISTINCT {column})")
            }
            (Some(column), AggregateFunction::Quantile(_)) => write!(
                formatter,
                "{function}({column}, {})",
                self.function.quantile_fraction().unwrap_or_default()
            ),
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
        // `WITH name AS (…)`: each name is a subquery, written where it is used.
        let mut named: Vec<(String, SqlAst)> = Vec::new();
        if let Some(with) = &query.with {
            if with.recursive {
                bail!("WITH RECURSIVE is not supported");
            }
            if with.cte_tables.len() > MAX_WITH_NAMES {
                bail!("WITH defines at most {MAX_WITH_NAMES} names");
            }
            for table in &with.cte_tables {
                if !table.alias.columns.is_empty() {
                    bail!("WITH name(columns) is not supported; name the columns with AS inside");
                }
                let name = table.alias.name.value.clone();
                if named
                    .iter()
                    .any(|(earlier, _)| earlier.eq_ignore_ascii_case(&name))
                {
                    bail!("WITH defines {name} twice");
                }
                let mut definition = Self::from_query(&table.query, depth + 1)?;
                // A name can use the names defined before it.
                definition.inline_named(&named);
                named.push((name, definition));
            }
        }
        let mut ast = Self::from_query_body(query, depth)?;
        ast.inline_named(&named);
        Ok(ast)
    }

    /// Puts each `WITH` definition in place of the tables named after it.
    fn inline_named(&mut self, named: &[(String, SqlAst)]) {
        if named.is_empty() {
            return;
        }
        let relations = std::iter::once(&mut self.from.first)
            .chain(self.from.joins.iter_mut().map(|join| &mut join.relation));
        for relation in relations {
            match &mut relation.table {
                SqlTable::Named(name) => {
                    if let Some((found, definition)) = named
                        .iter()
                        .find(|(defined, _)| defined.eq_ignore_ascii_case(name))
                    {
                        if relation.alias.is_none() {
                            relation.alias = Some(found.clone());
                        }
                        relation.table = SqlTable::Subquery(Box::new(definition.clone()));
                    }
                }
                SqlTable::Subquery(query) => query.inline_named(named),
                SqlTable::Union { parts, .. } => {
                    for part in parts {
                        part.inline_named(named);
                    }
                }
            }
        }
        fn in_predicate(predicate: &mut SqlPredicate, named: &[(String, SqlAst)]) {
            match predicate {
                SqlPredicate::Compare { .. } => {}
                SqlPredicate::InSubquery { query, .. }
                | SqlPredicate::CompareSubquery { query, .. } => query.inline_named(named),
                SqlPredicate::And(parts) | SqlPredicate::Or(parts) => {
                    for part in parts {
                        in_predicate(part, named);
                    }
                }
            }
        }
        if let Some(predicate) = &mut self.predicate {
            in_predicate(predicate, named);
        }
    }

    /// The query without its `WITH`: one SELECT, or a `UNION` of them.
    fn from_query_body(query: &Query, depth: usize) -> Result<Self> {
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
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            // `(SELECT …) ORDER BY …`: the brackets change nothing.
            SetExpr::Query(inner) if query.order_by.is_none() && limit.is_none() => {
                return Self::from_query(inner, depth);
            }
            SetExpr::SetOperation { .. } => {
                let mut parts = Vec::new();
                let mut kinds = Vec::new();
                union_parts(query.body.as_ref(), depth, &mut parts, &mut kinds)?;
                let all = kinds.iter().all(|all| *all);
                if !all && kinds.iter().any(|all| *all) {
                    bail!(
                        "a query mixes UNION and UNION ALL; use one of them, or put brackets \
                         around a part"
                    );
                }
                if parts.len() > MAX_UNION_PARTS {
                    bail!("UNION joins at most {MAX_UNION_PARTS} queries");
                }
                // The union is read like a table: `SELECT * FROM (… UNION …)`,
                // with the query's ORDER BY and LIMIT.
                return Ok(Self {
                    from: SqlFrom {
                        first: SqlRelation {
                            table: SqlTable::Union { parts, all },
                            alias: Some("union".to_owned()),
                        },
                        joins: Vec::new(),
                    },
                    projection: SqlProjection::All,
                    aliases: Vec::new(),
                    labels: Vec::new(),
                    calcs,
                    windows: Vec::new(),
                    predicate: None,
                    having: None,
                    sort,
                    limit,
                });
            }
            _ => bail!("only SELECT, UNION and UNION ALL are supported"),
        };
        reject_select_clauses(select)?;
        let distinct = match &select.distinct {
            None | Some(Distinct::All) => false,
            Some(Distinct::Distinct) => true,
            Some(Distinct::On(_)) => bail!("DISTINCT ON is not supported; use SELECT DISTINCT"),
        };
        let mut windows = Vec::new();
        let (projection, aliases, labels) = parse_projection(
            &select.projection,
            &select.group_by,
            distinct,
            &mut calcs,
            &mut windows,
        )?;
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
            windows,
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
        // Window functions are made on prepared rows, with every calculation.
        if !self.windows.is_empty() {
            return true;
        }
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
        if self.from.joins.is_empty()
            && let SqlTable::Union { parts, all } = &self.from.first.table
        {
            return self.union_plan(parts, *all, tables);
        }
        let direct = self.from.joins.is_empty()
            && matches!(self.from.first.table, SqlTable::Named(_))
            && !self
                .predicate
                .as_ref()
                .is_some_and(SqlPredicate::has_subquery)
            && !self.calculates_first()
            && self.windows.is_empty();
        let plan = if direct {
            self.direct_plan(tables)?
        } else {
            self.prepared_plan(tables)?
        };
        Planner::validate(&plan).context("validate SQL query")?;
        Ok(plan)
    }

    /// Plans `SELECT * FROM (… UNION …)`: the parts' rows into one prepared
    /// table, with column types that hold every part's values; without ALL,
    /// grouped by every column so each row comes once.
    fn union_plan(&self, parts: &[SqlAst], all: bool, tables: &Tables) -> Result<LogicalPlan> {
        let plan = union_relation(parts, all, tables)?;
        let schema = Planner::validate(&plan)?;
        // ORDER BY the union's columns, by name or position.
        let mut keys = Vec::new();
        for key in &self.sort {
            let column = match &key.target {
                SqlTarget::Position(position) => position
                    .checked_sub(1)
                    .filter(|index| *index < schema.fields().len()),
                SqlTarget::Column(column) if column.table.is_none() => schema
                    .fields()
                    .iter()
                    .position(|field| field.name().eq_ignore_ascii_case(&column.name)),
                _ => None,
            }
            .ok_or_else(|| {
                anyhow!("ORDER BY of a UNION names one of its columns, by name or position")
            })?;
            keys.push(SortSpec::new(column, key.direction, key.nulls));
        }
        let mut plan = plan;
        if !keys.is_empty() {
            // A UNION without ALL is grouped: its one result batch is sorted.
            plan = if all {
                plan.sort_by(keys)
            } else {
                plan.output(crate::logical::Output {
                    sort: keys,
                    ..crate::logical::Output::default()
                })
            };
        }
        if let Some(limit) = self.limit {
            plan = plan.limit(limit);
        }
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
                            (function, Some(column)) => AggregateExpr::of(function, column),
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
        // Hidden calculation and window columns are made later, from the
        // columns they read.
        let hidden = |column: &SqlColumn| {
            self.calcs.position(column).is_some() || window_result(column).is_some()
        };
        columns.retain(|column| !hidden(column));
        columns.extend(
            self.calcs
                .columns()
                .into_iter()
                .filter(|column| window_result(column).is_none()),
        );
        for window in &self.windows {
            columns.extend(window.columns().filter(|column| !hidden(column)));
        }
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
        // Window functions read at least one column, for the row count.
        if !self.windows.is_empty() && kept.iter().all(BTreeSet::is_empty) {
            kept[0].insert(0);
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
        // Calculations, over the joined rows, become columns after them (ADR
        // 0013); then WHERE tests on them, then window functions (ADR 0024),
        // then the calculations that use window functions' results.
        let base = output.len();
        let uses_windows = |item: &SqlCalc| {
            let mut read = Vec::new();
            item.expr.columns(&mut read);
            read.iter().any(|column| window_result(column).is_some())
        };
        let (late, early): (Vec<usize>, Vec<usize>) =
            (0..self.calcs.items.len()).partition(|index| uses_windows(&self.calcs.items[*index]));
        let mut calc_columns = vec![0; self.calcs.items.len()];
        for (offset, index) in early.iter().enumerate() {
            calc_columns[*index] = base + offset;
        }
        let window_base = base + early.len();
        for (offset, index) in late.iter().enumerate() {
            calc_columns[*index] = window_base + self.windows.len() + offset;
        }
        let resolve = |column: &SqlColumn| -> Result<usize> {
            if let Some(index) = self.calcs.position(column) {
                return Ok(calc_columns[index]);
            }
            match window_result(column) {
                Some(index) if index < self.windows.len() => Ok(window_base + index),
                Some(_) => bail!("window function {column} is missing"),
                None => output_index(column),
            }
        };
        let compute =
            |prepared: Prep, schema: SchemaRef, items: &[usize]| -> Result<(Prep, SchemaRef)> {
                if items.is_empty() {
                    return Ok((prepared, schema));
                }
                let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
                let mut expressions = Vec::new();
                for index in items {
                    let item = &self.calcs.items[*index];
                    let expression = item.expr.map_columns(&resolve)?;
                    let data_type = expression
                        .data_type(&schema)
                        .with_context(|| format!("calculation {}", item.text))?;
                    fields.push(Arc::new(Field::new(&item.name, data_type, true)));
                    expressions.push((item.name.clone(), expression));
                }
                Ok((
                    Prep::Compute {
                        input: Box::new(prepared),
                        expressions,
                    },
                    Arc::new(Schema::new(fields)),
                ))
            };
        (prepared, schema) = compute(prepared, schema, &early)?;
        prepared = apply_where(prepared, &after, tables, &resolve, &schema)?;
        if !self.windows.is_empty() {
            (prepared, schema) = self.window_step(prepared, &schema, &resolve)?;
        }
        (prepared, schema) = compute(prepared, schema, &late)?;
        let plan = LogicalPlan::Prepared(Prepared {
            prep: Arc::new(prepared),
            schema,
        });
        let all = (!self.calcs.is_empty() || !self.windows.is_empty()).then_some(base);
        self.shape(plan, None, &resolve, all)
    }

    /// Adds a column per window function after `schema`'s (ADR 0024). Window
    /// functions with the same `OVER (PARTITION BY … ORDER BY …)` share one
    /// sort and one pass over the rows.
    fn window_step(
        &self,
        prepared: Prep,
        schema: &SchemaRef,
        resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
    ) -> Result<(Prep, SchemaRef)> {
        let mut grouped: Vec<Vec<usize>> = Vec::new();
        for (index, window) in self.windows.iter().enumerate() {
            match grouped
                .iter_mut()
                .find(|members| self.windows[members[0]].same_order(window))
            {
                Some(members) => members.push(index),
                None => grouped.push(vec![index]),
            }
        }
        if grouped.len() > MAX_WINDOW_SORTS {
            bail!(
                "window functions sort the rows at most {MAX_WINDOW_SORTS} ways in one query; \
                 this one has {} different OVER (PARTITION BY … ORDER BY …)",
                grouped.len()
            );
        }
        let mut types = vec![None; self.windows.len()];
        let mut groups = Vec::new();
        for members in grouped {
            let first = &self.windows[members[0]];
            let partition_by = first
                .partition_by
                .iter()
                .map(resolve)
                .collect::<Result<Vec<_>>>()?;
            // Sorted by PARTITION BY, then ORDER BY; a column once.
            let mut sort = partition_by
                .iter()
                .map(|column| SortSpec::new(*column, SortDirection::Ascending, NullOrder::Last))
                .collect::<Vec<_>>();
            let mut order_by = Vec::new();
            for (column, direction, nulls) in &first.order_by {
                let column = resolve(column)?;
                if !order_by.contains(&column) {
                    order_by.push(column);
                }
                if !sort.iter().any(|key| key.column == column) {
                    sort.push(SortSpec::new(column, *direction, *nulls));
                }
            }
            let windows = members
                .iter()
                .map(|index| {
                    let window = &self.windows[*index];
                    Ok(WindowExpr {
                        function: window.function,
                        column: window.column.as_ref().map(resolve).transpose()?,
                        default: window.default.clone(),
                        frame: window.frame,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let spec = WindowSpec {
                partition_by,
                order_by,
                windows,
            };
            for (index, data_type) in members
                .iter()
                .zip(parquet_reader::check_windows(schema, &spec)?)
            {
                types[*index] = Some(data_type);
            }
            groups.push(WindowGroup {
                sort,
                spec,
                names: members
                    .iter()
                    .map(|index| format!("{WIN_PREFIX}{index}"))
                    .collect(),
                windows: members,
            });
        }
        let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
        for (index, data_type) in types.into_iter().enumerate() {
            fields.push(Arc::new(Field::new(
                format!("{WIN_PREFIX}{index}"),
                data_type.ok_or_else(|| anyhow!("window function {index} has no type"))?,
                true,
            )));
        }
        Ok((
            Prep::Window {
                input: Box::new(prepared),
                groups,
                count: self.windows.len(),
            },
            Arc::new(Schema::new(fields)),
        ))
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
            SqlTable::Union { parts, all } => {
                let plan = union_relation(parts, *all, tables)?;
                (
                    relation.alias.clone().unwrap_or_else(|| "union".to_owned()),
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

/// The parts of `a UNION b UNION c`, left to right, and whether each `UNION`
/// is `ALL`.
fn union_parts(
    body: &SetExpr,
    depth: usize,
    parts: &mut Vec<SqlAst>,
    kinds: &mut Vec<bool>,
) -> Result<()> {
    match body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if *op != sqlparser::ast::SetOperator::Union {
                bail!("{op} is not supported; UNION and UNION ALL are");
            }
            let all = match set_quantifier {
                sqlparser::ast::SetQuantifier::All => true,
                sqlparser::ast::SetQuantifier::None | sqlparser::ast::SetQuantifier::Distinct => {
                    false
                }
                other => bail!("UNION {other} is not supported; use UNION or UNION ALL"),
            };
            union_parts(left, depth, parts, kinds)?;
            kinds.push(all);
            union_parts(right, depth, parts, kinds)
        }
        SetExpr::Select(select) => {
            let query = Query {
                with: None,
                body: Box::new(SetExpr::Select(select.clone())),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            };
            parts.push(SqlAst::from_query_body(&query, depth + 1)?);
            Ok(())
        }
        // `(SELECT … ORDER BY … LIMIT 3) UNION …`: a bracketed part is its own query.
        SetExpr::Query(query) => {
            parts.push(SqlAst::from_query(query, depth + 1)?);
            Ok(())
        }
        _ => bail!("each side of UNION is a SELECT"),
    }
}

/// The rows of a UNION's parts as one prepared table (ADR 0023): the parts'
/// column counts must match; each column takes a type that holds every
/// part's values (`types::common_type`) and the first part's name.
fn union_relation(parts: &[SqlAst], all: bool, tables: &Tables) -> Result<LogicalPlan> {
    let plans = parts
        .iter()
        .map(|part| part.to_logical_plan_with(tables))
        .collect::<Result<Vec<_>>>()?;
    let schemas = plans
        .iter()
        .map(Planner::validate)
        .collect::<Result<Vec<_>>>()?;
    let first = schemas
        .first()
        .ok_or_else(|| anyhow!("UNION needs at least two queries"))?;
    let mut fields = first
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect::<Vec<_>>();
    for (index, schema) in schemas.iter().enumerate().skip(1) {
        if schema.fields().len() != fields.len() {
            bail!(
                "the queries of a UNION select the same number of columns; the first selects \
                 {} and query {} selects {}",
                fields.len(),
                index + 1,
                schema.fields().len()
            );
        }
        for (position, (field, other)) in fields.iter_mut().zip(schema.fields()).enumerate() {
            let common = parquet_reader::types::common_type(field.data_type(), other.data_type())
                .ok_or_else(|| {
                anyhow!(
                    "column {} of the UNION is {} in one query and {} in another, and no \
                         type holds both",
                    position + 1,
                    field.data_type(),
                    other.data_type()
                )
            })?;
            *field = field.clone().with_data_type(common);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let columns = (0..schema.fields().len()).collect::<Vec<_>>();
    let plan = LogicalPlan::Prepared(crate::logical::Prepared {
        prep: Arc::new(Prep::Union {
            plans,
            schema: Arc::clone(&schema),
            columns: columns.clone(),
        }),
        schema,
    });
    if all {
        return Ok(plan);
    }
    // Each distinct row once: grouped by every column, as SELECT DISTINCT is.
    if columns.len() > MAX_GROUP_COLUMNS {
        bail!(
            "UNION removes repeated rows by comparing at most {MAX_GROUP_COLUMNS} columns; \
             this one has {}; use UNION ALL to keep every row",
            columns.len()
        );
    }
    plan.aggregate_grouped(columns, Vec::new())
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

/// Whether a function call is one of the aggregates (`SUM(x) OVER (…)` is a
/// window function).
fn is_aggregate(function: &Function) -> bool {
    function.over.is_none() && parquet_reader::is_aggregate_name(&function.name.to_string())
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
    windows: &mut Vec<SqlWindow>,
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
        let windowed = windows.len();
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
                    if function.over.is_some() {
                        return parse_window(function, calcs, windows);
                    }
                    inner.push(parse_aggregate(function, calcs)?);
                    Ok(SqlColumn::new(format!("{AGG_PREFIX}{}", inner.len() - 1)))
                };
                let parsed = calc::parse_with(expression, &mut hook)?;
                if inner.is_empty() {
                    // A column, a calculation, or one with window functions'
                    // results (`amount - LAG(amount) OVER (…)`).
                    let column = calcs.add(parsed, expression.to_string(), false)?;
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
            || windows.len() > windowed
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
    if !windows.is_empty() && (!aggregates.is_empty() || !group_by.is_empty() || distinct) {
        bail!(
            "window functions can't be combined with GROUP BY, DISTINCT or aggregates in one \
             SELECT; group in a subquery or WITH, then use window functions on its rows"
        );
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
    let function_name = function.name.to_string().to_ascii_uppercase();
    if function.over.is_some() {
        bail!("{function_name}(…) OVER (…): {}", calc::WINDOW_PLACE);
    }
    let within_group = function_name == "PERCENTILE_CONT";
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || (!within_group && !function.within_group.is_empty())
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
    if distinct && function_name != "COUNT" {
        bail!("only COUNT supports DISTINCT");
    }
    // The quantiles take a fraction too: `QUANTILE_CONT(x, 0.9)`, or the
    // standard `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY x)`.
    if within_group {
        let [order] = function.within_group.as_slice() else {
            bail!("PERCENTILE_CONT(p) needs WITHIN GROUP (ORDER BY column)");
        };
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(fraction))] = arguments.args.as_slice()
        else {
            bail!("PERCENTILE_CONT takes one fraction, e.g. PERCENTILE_CONT(0.9)");
        };
        let mut fraction = quantile_fraction(fraction)?;
        if order.options.asc == Some(false) {
            fraction = 1.0 - fraction;
        }
        return Ok(SqlAggregateExpr {
            function: AggregateFunction::quantile(fraction)?,
            column: Some(calcs.column(&order.expr, true)?),
        });
    }
    if matches!(function_name.as_str(), "QUANTILE_CONT" | "QUANTILE") {
        let [
            FunctionArg::Unnamed(FunctionArgExpr::Expr(column)),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(fraction)),
        ] = arguments.args.as_slice()
        else {
            bail!(
                "{function_name} takes a column and a fraction, e.g. {function_name}(price, 0.9)"
            );
        };
        return Ok(SqlAggregateExpr {
            function: AggregateFunction::quantile(quantile_fraction(fraction)?)?,
            column: Some(calcs.column(column, true)?),
        });
    }
    let function = match parquet_reader::aggregate_by_name(&function_name) {
        Some(AggregateFunction::Count) if distinct => AggregateFunction::CountDistinct,
        Some(function) => function,
        None => bail!("unsupported aggregate {function_name}"),
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

/// Parses `function(…) OVER (…)` into `windows` and names the column that
/// will hold its result (ADR 0024).
fn parse_window(
    function: &Function,
    calcs: &mut SqlCalcs,
    windows: &mut Vec<SqlWindow>,
) -> Result<SqlColumn> {
    let name = function.name.to_string().to_ascii_uppercase();
    let spec = match &function.over {
        Some(WindowType::WindowSpec(spec)) if spec.window_name.is_none() => spec,
        Some(_) => bail!("named windows (OVER w, WINDOW w AS …) are not supported; write OVER (…)"),
        None => bail!("{name} needs OVER (…) to be a window function"),
    };
    if function.uses_odbc_syntax
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        bail!("{name} … OVER (…): FILTER, IGNORE NULLS and other modifiers are not supported");
    }
    let arguments = match &function.args {
        FunctionArguments::None => Vec::new(),
        FunctionArguments::List(list) => {
            if !list.clauses.is_empty() {
                bail!("{name} … OVER (…): argument clauses are not supported");
            }
            if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
                bail!("{name}(DISTINCT …) OVER (…) is not supported");
            }
            list.args.iter().collect()
        }
        FunctionArguments::Subquery(_) => bail!("{name} … OVER (…) takes columns, not a subquery"),
    };
    fn argument<'a>(name: &str, argument: &'a FunctionArg) -> Result<&'a Expr> {
        match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => Ok(expression),
            _ => bail!("{name} … OVER (…) takes values, not named arguments or *"),
        }
    }
    let (window_function, column, default) = match name.as_str() {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" => {
            if !arguments.is_empty() {
                bail!("{name}() takes no arguments");
            }
            let function = match name.as_str() {
                "ROW_NUMBER" => WindowFunction::RowNumber,
                "RANK" => WindowFunction::Rank,
                _ => WindowFunction::DenseRank,
            };
            (function, None, None)
        }
        "LAG" | "LEAD" => {
            let (column, offset, default) = match arguments.as_slice() {
                [column] => (column, None, None),
                [column, offset] => (column, Some(offset), None),
                [column, offset, default] => (column, Some(offset), Some(default)),
                _ => bail!(
                    "{name} takes a column, then optionally an offset and a default, e.g. \
                     {name}(amount, 1, 0)"
                ),
            };
            let offset = match offset {
                Some(offset) => parse_literal(argument(&name, offset)?)
                    .ok()
                    .and_then(|text| text.parse::<usize>().ok())
                    .filter(|offset| *offset <= MAX_WINDOW_OFFSET)
                    .ok_or_else(|| {
                        anyhow!("{name}'s offset is a whole number from 0 to {MAX_WINDOW_OFFSET}")
                    })?,
                None => 1,
            };
            let default = match default
                .map(|default| argument(&name, default))
                .transpose()?
            {
                Some(Expr::Value(literal))
                    if matches!(literal.value, sqlparser::ast::Value::Null) =>
                {
                    None
                }
                Some(default) => Some(parse_literal(default).map_err(|_| {
                    anyhow!("{name}'s default is a number, text or date literal, e.g. 0 or 'none'")
                })?),
                None => None,
            };
            let function = if name == "LAG" {
                WindowFunction::Lag(offset)
            } else {
                WindowFunction::Lead(offset)
            };
            (
                function,
                Some(calcs.column(argument(&name, column)?, true)?),
                default,
            )
        }
        _ => match parquet_reader::aggregate_by_name(&name) {
            Some(
                aggregate @ (AggregateFunction::Count
                | AggregateFunction::Sum
                | AggregateFunction::Avg
                | AggregateFunction::Min
                | AggregateFunction::Max),
            ) => {
                let column = match arguments.as_slice() {
                    [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                        if aggregate == AggregateFunction::Count =>
                    {
                        None
                    }
                    [only] => Some(calcs.column(argument(&name, only)?, true)?),
                    _ => bail!("{name} … OVER (…) takes one column"),
                };
                (WindowFunction::Aggregate(aggregate), column, None)
            }
            Some(_) => bail!(
                "{name} … OVER (…) is not supported; window aggregates are COUNT, SUM, AVG, MIN \
                 and MAX"
            ),
            None => bail!(
                "{name} is not a window function; they are ROW_NUMBER, RANK, DENSE_RANK, LAG, \
                 LEAD, COUNT, SUM, AVG, MIN and MAX"
            ),
        },
    };
    let partition_by = spec
        .partition_by
        .iter()
        .map(|expression| calcs.column(expression, true))
        .collect::<Result<Vec<_>>>()?;
    let order_by = spec
        .order_by
        .iter()
        .map(|expression| {
            if expression.with_fill.is_some() {
                bail!("ORDER BY WITH FILL is not supported");
            }
            Ok((
                calcs.column(&expression.expr, true)?,
                if expression.options.asc == Some(false) {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
                if expression.options.nulls_first == Some(true) {
                    NullOrder::First
                } else {
                    NullOrder::Last
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if partition_by.len() + order_by.len() > MAX_SORT_KEYS {
        bail!(
            "OVER (…) sorts by at most {MAX_SORT_KEYS} columns, PARTITION BY and ORDER BY together"
        );
    }
    let whole = if order_by.is_empty() {
        WindowFrame::Partition
    } else {
        WindowFrame::Peers
    };
    let frame = match &spec.window_frame {
        None => whole,
        Some(frame) => {
            let end = frame
                .end_bound
                .as_ref()
                .unwrap_or(&WindowFrameBound::CurrentRow);
            match (frame.units, &frame.start_bound, end) {
                (_, WindowFrameBound::Preceding(None), WindowFrameBound::Following(None)) => {
                    WindowFrame::Partition
                }
                (
                    WindowFrameUnits::Range,
                    WindowFrameBound::Preceding(None),
                    WindowFrameBound::CurrentRow,
                ) => whole,
                (
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(None),
                    WindowFrameBound::CurrentRow,
                ) => WindowFrame::Rows,
                (
                    WindowFrameUnits::Rows,
                    WindowFrameBound::CurrentRow,
                    WindowFrameBound::CurrentRow,
                ) => WindowFrame::Preceding(0),
                (
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(Some(rows)),
                    WindowFrameBound::CurrentRow,
                ) => WindowFrame::Preceding(
                    parse_literal(rows)
                        .ok()
                        .and_then(|text| text.parse::<usize>().ok())
                        .filter(|rows| *rows <= MAX_WINDOW_OFFSET)
                        .ok_or_else(|| {
                            anyhow!(
                                "ROWS BETWEEN n PRECEDING takes a whole number from 0 to \
                                 {MAX_WINDOW_OFFSET}"
                            )
                        })?,
                ),
                _ => bail!(
                    "this window frame is not supported; use ROWS BETWEEN UNBOUNDED PRECEDING \
                     AND CURRENT ROW, ROWS BETWEEN n PRECEDING AND CURRENT ROW, or none"
                ),
            }
        }
    };
    let window = SqlWindow {
        function: window_function,
        column,
        default,
        frame,
        partition_by,
        order_by,
    };
    let index = match windows.iter().position(|other| *other == window) {
        Some(index) => index,
        None => {
            if windows.len() >= MAX_WINDOWS {
                bail!("a query uses at most {MAX_WINDOWS} window functions");
            }
            windows.push(window);
            windows.len() - 1
        }
    };
    Ok(SqlColumn::new(format!("{WIN_PREFIX}{index}")))
}

/// A quantile's fraction: a number literal from 0 to 1.
fn quantile_fraction(expression: &Expr) -> Result<f64> {
    let text = match expression {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::Number(text, _) => text.clone(),
            _ => String::new(),
        },
        _ => String::new(),
    };
    text.parse::<f64>()
        .ok()
        .filter(|fraction| (0.0..=1.0).contains(fraction))
        .ok_or_else(|| anyhow!("a quantile's fraction is a number from 0 to 1, such as 0.5"))
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
            (
                "SELECT LEFT(carrier, 2) FROM dataset",
                "unknown function LEFT",
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
            "SELECT id FROM dataset EXCEPT SELECT id FROM dataset",
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
    fn waiting_for_batches_gives_the_rows_of_a_blocking_read() {
        // A join is prepared on its own thread first; waiting covers that too.
        let (_directory, tables) = tables();
        for sql in [
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer = c.id ORDER BY o.id",
            "SELECT id, amount FROM orders WHERE amount > 6",
        ] {
            let plan = SqlAst::parse(sql)
                .unwrap()
                .to_logical_plan_with(&tables)
                .unwrap();
            let runtime = runtime();
            let mut execution = QueryEngine::new(&runtime)
                .execute(&plan, GenerationId(3))
                .unwrap();
            let mut waited = Vec::new();
            loop {
                match execution
                    .wait_next_batch(std::time::Duration::from_millis(5))
                    .unwrap()
                {
                    crate::QueryPoll::Batch(batch) => waited.push(batch.batch),
                    crate::QueryPoll::Finished => break,
                    crate::QueryPoll::Pending => {}
                }
            }
            let rows = waited
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(|row| {
                        (0..batch.num_columns())
                            .map(|column| {
                                parquet_reader::value::scalar_text(
                                    batch.column(column).as_ref(),
                                    row,
                                )
                                .unwrap()
                                .unwrap_or_else(|| "null".into())
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(rows, run(sql, &tables).unwrap().1, "{sql}");
        }
    }

    #[test]
    fn skips_rows_of_a_join_asked_for_while_it_is_prepared() {
        let (_directory, tables) = tables();
        let sql =
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer = c.id ORDER BY o.id";
        let plan = SqlAst::parse(sql)
            .unwrap()
            .to_logical_plan_with(&tables)
            .unwrap();
        let runtime = runtime();
        let mut execution = QueryEngine::new(&runtime)
            .execute(&plan, GenerationId(4))
            .unwrap();
        // Asked before the join's rows are prepared; applied once they are.
        execution.skip_rows(2);
        let mut rows = Vec::new();
        while let Some(batch) = execution.next_batch().unwrap() {
            let batch = batch.batch;
            for row in 0..batch.num_rows() {
                rows.push(
                    parquet_reader::value::scalar_text(batch.column(1).as_ref(), row)
                        .unwrap()
                        .unwrap_or_default(),
                );
            }
        }
        assert_eq!(rows, ["Ann", "Cy"]);
        assert_eq!(execution.take_skipped_rows(), 2);
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
    fn computes_spread_and_quantiles_in_sql() {
        let (_directory, tables) = tables();
        let rows = |sql: &str| run(sql, &tables).unwrap();
        // amount: 5, 10, 15, 20, 50.
        let (names, values) = rows(
            "SELECT MEDIAN(amount), QUANTILE_CONT(amount, 0.25) AS q1, \
             PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) AS q3, \
             ROUND(STDDEV(amount), 3), VAR_POP(amount), STDDEV_POP(amount) FROM orders",
        );
        assert_eq!(
            names,
            [
                "MEDIAN(amount)",
                "q1",
                "q3",
                "ROUND(STDDEV(amount), 3)",
                "VAR_POP(amount)",
                "STDDEV_POP(amount)"
            ]
        );
        // Mean 20; the squared differences add up to 1,250.
        assert_eq!(values, ["15,10,20,17.678,250,15.811388300841896"]);
        // Descending order counts from the top.
        assert_eq!(
            rows("SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount DESC) FROM orders").1,
            ["20"]
        );
        // Grouped, and on a calculation.
        assert_eq!(
            rows(
                "SELECT customer, MEDIAN(amount * 2) FROM orders WHERE customer IS NOT NULL \
                 GROUP BY customer ORDER BY customer"
            )
            .1,
            ["10,20", "20,20", "30,100"]
        );
        for (sql, message) in [
            (
                "SELECT MEDIAN(id) FROM customers WHERE name = 'x' OR MEDIAN(name) > 1",
                "aggregate",
            ),
            ("SELECT QUANTILE_CONT(amount, 2) FROM orders", "from 0 to 1"),
            (
                "SELECT QUANTILE_CONT(amount) FROM orders",
                "takes a column and a fraction",
            ),
            ("SELECT PERCENTILE_CONT(0.5) FROM orders", "WITHIN GROUP"),
            (
                "SELECT MEDIAN(name) FROM customers",
                "MEDIAN needs a number column",
            ),
        ] {
            let error = format!("{:#}", run(sql, &tables).unwrap_err());
            assert!(error.contains(message), "{sql}: {error}");
        }
    }

    #[test]
    fn runs_window_functions() {
        let (_directory, tables) = tables();
        let rows = |sql: &str| run(sql, &tables).unwrap();
        // Numbering by a sort of its own, shown in the query's order.
        let (names, numbered) = rows(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM orders ORDER BY id",
        );
        assert_eq!(names, ["id", "rn"]);
        assert_eq!(numbered, ["1,5", "2,4", "3,3", "4,2", "5,1"]);
        // Running totals per partition; NULL is a partition of its own.
        assert_eq!(
            rows(
                "SELECT id, customer, SUM(amount) OVER (PARTITION BY customer ORDER BY id) \
                 AS running FROM orders ORDER BY id"
            )
            .1,
            ["1,10,5", "2,20,10", "3,10,20", "4,null,20", "5,30,50"]
        );
        // Calculations with window functions' results, and a whole-table total.
        assert_eq!(
            rows(
                "SELECT id, amount - LAG(amount, 1, 0) OVER (ORDER BY id) AS change, \
                 ROUND(amount * 100 / SUM(amount) OVER (), 1) AS share FROM orders ORDER BY id"
            )
            .1,
            ["1,5,5", "2,5,10", "3,5,15", "4,5,20", "5,30,50"]
        );
        // Ties share a rank; NULLs sort last.
        assert_eq!(
            rows(
                "SELECT id, RANK() OVER (ORDER BY customer) AS r, \
                 DENSE_RANK() OVER (ORDER BY customer) AS d FROM orders ORDER BY id"
            )
            .1,
            ["1,1,1", "2,3,2", "3,1,1", "4,5,4", "5,4,3"]
        );
        // A moving average over the row and the one before it.
        assert_eq!(
            rows(
                "SELECT AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
                 FROM orders ORDER BY id"
            ),
            (
                vec![
                    "AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
                        .to_owned()
                ],
                ["5", "7.5", "12.5", "17.5", "35"]
                    .map(str::to_owned)
                    .to_vec()
            )
        );
        // WHERE filters before the window functions number the rows.
        assert_eq!(
            rows(
                "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM orders WHERE amount > 10 \
                 ORDER BY id"
            )
            .1,
            ["3,1", "4,2", "5,3"]
        );
        // The top row per partition, filtered in an outer query.
        assert_eq!(
            rows(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY customer \
                 ORDER BY amount DESC) AS rn FROM orders) AS ranked WHERE rn = 1 ORDER BY id"
            )
            .1,
            ["2", "3", "4", "5"]
        );
        // Over grouped rows from WITH, and over joined tables.
        assert_eq!(
            rows(
                "WITH totals AS (SELECT customer, SUM(amount) AS total FROM orders \
                 GROUP BY customer) \
                 SELECT customer, RANK() OVER (ORDER BY total DESC) AS place FROM totals \
                 ORDER BY place, customer"
            )
            .1,
            ["30,1", "10,2", "null,2", "20,4"]
        );
        assert_eq!(
            rows(
                "SELECT o.id, c.name, COUNT(*) OVER (PARTITION BY c.name) AS n FROM orders o \
                 JOIN customers c ON o.customer = c.id ORDER BY o.id"
            )
            .1,
            ["1,Ann,2", "2,Bo,1", "3,Ann,2", "5,Cy,1"]
        );
        // No column read: one is read for the row count.
        assert_eq!(
            rows("SELECT ROW_NUMBER() OVER () AS n FROM orders ORDER BY n DESC LIMIT 2").1,
            ["5", "4"]
        );
        for (sql, message) in [
            (
                "SELECT id FROM orders WHERE ROW_NUMBER() OVER (ORDER BY id) = 1",
                "work only in the SELECT list",
            ),
            (
                "SELECT SUM(ROW_NUMBER() OVER (ORDER BY id)) OVER () FROM orders",
                "work only in the SELECT list",
            ),
            (
                "SELECT customer, COUNT(*) OVER () FROM orders GROUP BY customer",
                "can't be combined with GROUP BY",
            ),
            (
                "SELECT NTILE(4) OVER (ORDER BY id) FROM orders",
                "not a window function",
            ),
            (
                "SELECT MEDIAN(amount) OVER () FROM orders",
                "window aggregates are COUNT, SUM, AVG, MIN and MAX",
            ),
            (
                "SELECT LAG(amount, 2000) OVER (ORDER BY id) FROM orders",
                "offset is a whole number from 0 to 1000",
            ),
            (
                "SELECT LAG(amount, 1, 'x') OVER (ORDER BY id) FROM orders",
                "LAG's default x is not a Float64 value",
            ),
            (
                "SELECT SUM(amount) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND \
                 UNBOUNDED FOLLOWING) FROM orders",
                "window frame is not supported",
            ),
            (
                "SELECT SUM(name) OVER () FROM customers",
                "SUM needs a number column",
            ),
            (
                "SELECT ROW_NUMBER() OVER w FROM orders WINDOW w AS (ORDER BY id)",
                "not supported",
            ),
        ] {
            let error = format!("{:#}", run(sql, &tables).unwrap_err());
            assert!(error.contains(message), "{sql}: {error}");
        }
    }

    #[test]
    fn calculates_with_text_and_dates() {
        let (_directory, tables) = tables();
        let rows = |sql: &str| run(sql, &tables).unwrap().1;
        assert_eq!(
            rows("SELECT REPLACE(name, 'n', 'N') FROM customers ORDER BY 1"),
            ["ANN", "Bo", "Cy"]
        );
        assert_eq!(
            rows(
                "SELECT DATE_TRUNC('month', DATE '2024-02-29'), \
                 DATE '2024-01-31' + INTERVAL '1 month', \
                 DATE '2024-01-31' - INTERVAL 1 DAY, \
                 DATE '2024-01-01' + INTERVAL '36 hours', \
                 DATE_DIFF('month', DATE '2023-11-30', DATE '2024-02-01'), \
                 DATE_DIFF('day', TIMESTAMP '2024-03-01 23:00:00', TIMESTAMP '2024-03-02 01:00:00') \
                 FROM customers LIMIT 1"
            ),
            ["2024-02-01,2024-02-29,2024-01-30,2024-01-02T12:00:00,3,1"]
        );
        // NOW and CURRENT_DATE are the same for every row, and in the past of nothing here.
        assert_eq!(
            rows(
                "SELECT COUNT(*) FROM orders WHERE NOW() > TIMESTAMP '2020-01-01 00:00:00' \
                 AND CURRENT_DATE >= DATE '2020-01-01'"
            ),
            ["5"]
        );
        for (sql, message) in [
            (
                "SELECT DATE_TRUNC('fortnight', DATE '2024-01-01') FROM orders",
                "not a date unit",
            ),
            (
                "SELECT DATE_TRUNC(amount, DATE '2024-01-01') FROM orders",
                "unit in quotes",
            ),
            (
                "SELECT DATE_TRUNC('day', amount) FROM orders",
                "needs a date or timestamp",
            ),
            (
                "SELECT amount + INTERVAL '1 day' FROM orders",
                "needs a date or timestamp",
            ),
            ("SELECT INTERVAL '1 day' FROM orders", "not on its own"),
            (
                "SELECT REPLACE(name, 'a') FROM customers",
                "takes 3 arguments",
            ),
        ] {
            let error = format!("{:#}", run(sql, &tables).unwrap_err());
            assert!(error.contains(message), "{sql}: {error}");
        }
    }

    #[test]
    fn unions_queries_and_names_subqueries_with_with() {
        let (_directory, tables) = tables();
        let rows = |sql: &str| run(sql, &tables).unwrap();
        // UNION ALL keeps every row; types are widened to hold both (Int32 and Int64).
        let (names, all) = rows(
            "SELECT id FROM customers UNION ALL SELECT customer FROM orders \
             WHERE customer IS NOT NULL ORDER BY 1",
        );
        assert_eq!(names, ["id"]);
        assert_eq!(all, ["10", "10", "10", "20", "20", "30", "30"]);
        // UNION keeps each row once, and takes ORDER BY and LIMIT.
        assert_eq!(
            rows(
                "SELECT id FROM customers UNION SELECT customer FROM orders \
                 WHERE customer IS NOT NULL ORDER BY id DESC LIMIT 2"
            )
            .1,
            ["30", "20"]
        );
        // A union can be read like a table.
        assert_eq!(
            rows(
                "SELECT COUNT(*) FROM (SELECT name FROM customers UNION ALL \
                 SELECT name FROM customers) AS both_lists"
            )
            .1,
            ["6"]
        );
        // WITH: names used by later names and by the query, in FROM and JOIN.
        assert_eq!(
            rows(
                "WITH big AS (SELECT id, customer, amount FROM orders WHERE amount >= 15), \
                 named AS (SELECT big.id AS order_id, c.name FROM big \
                 JOIN customers c ON big.customer = c.id) \
                 SELECT name, order_id FROM named ORDER BY order_id"
            )
            .1,
            ["Ann,3", "Cy,5"]
        );
        assert_eq!(
            rows(
                "WITH ann AS (SELECT id FROM customers WHERE name = 'Ann') \
                 SELECT id FROM orders WHERE customer IN (SELECT id FROM ann) ORDER BY id"
            )
            .1,
            ["1", "3"]
        );
        for (sql, message) in [
            (
                "SELECT id FROM orders INTERSECT SELECT id FROM customers",
                "INTERSECT is not supported",
            ),
            (
                "SELECT id, amount FROM orders UNION SELECT id FROM customers",
                "same number of columns",
            ),
            (
                "SELECT amount FROM orders UNION SELECT name FROM customers",
                "no type holds both",
            ),
            (
                "SELECT id FROM orders UNION SELECT id FROM customers UNION ALL SELECT 1 FROM orders",
                "mixes UNION and UNION ALL",
            ),
            (
                "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
                "RECURSIVE",
            ),
            (
                "WITH t(a) AS (SELECT id FROM orders) SELECT * FROM t",
                "name(columns)",
            ),
        ] {
            let error = format!("{:#}", run(sql, &tables).unwrap_err());
            assert!(error.contains(message), "{sql}: {error}");
        }
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
