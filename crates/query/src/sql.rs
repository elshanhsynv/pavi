use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow_schema::{Field, Schema, SchemaRef};
use parquet_reader::{
    FilterExpr, FilterOp, JoinKind, MAX_JOIN_KEYS, MAX_SORT_KEYS, NullOrder, ParquetSource,
    SortDirection, SortSpec, aggregate::MAX_GROUP_COLUMNS, join::check_join_key_types,
};
use sqlparser::{
    ast::{
        BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
        GroupByExpr, JoinConstraint, JoinOperator, OrderBy, OrderByKind, Query, SelectItem,
        SetExpr, Statement, TableFactor, UnaryOperator,
    },
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{
    AggregateExpr, AggregateFunction, Filter, LogicalPlan, Planner,
    logical::Prepared,
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

/// The deliberately small SQL shape accepted by PAVI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAst {
    from: SqlFrom,
    projection: SqlProjection,
    predicate: Option<SqlPredicate>,
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
    pub expressions: Vec<SqlAggregateExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAggregateExpr {
    pub function: AggregateFunction,
    pub column: Option<SqlColumn>,
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
    pub column: SqlColumn,
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
        let sort = parse_order_by(query.order_by.as_ref())?;
        let SetExpr::Select(select) = query.body.as_ref() else {
            bail!("only a single SELECT is supported (no UNION)");
        };
        reject_select_clauses(select)?;
        Ok(Self {
            from: parse_from(&select.from, depth)?,
            projection: parse_projection(&select.projection, &select.group_by)?,
            predicate: select
                .selection
                .as_ref()
                .map(|selection| parse_predicate(selection, depth))
                .transpose()?,
            sort,
            limit,
        })
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
                .is_some_and(SqlPredicate::has_subquery);
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
        self.shape(LogicalPlan::scan(Arc::clone(&source)), filter, &resolve)
    }

    /// Adds the filter, projection, aggregate, sort and limit above `plan`;
    /// `resolve` maps a column to the input's column.
    fn shape(
        &self,
        mut plan: LogicalPlan,
        filter: Option<Filter>,
        resolve: &dyn Fn(&SqlColumn) -> Result<usize>,
    ) -> Result<LogicalPlan> {
        let (projection, aggregate) = match &self.projection {
            SqlProjection::All => (None, None),
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
        let sort = self
            .sort
            .iter()
            .map(|sort| {
                Ok(SortSpec::new(
                    resolve(&sort.column)?,
                    sort.direction,
                    sort.nulls,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

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
            if matches!(self.projection, SqlProjection::Aggregates(_)) {
                bail!("ORDER BY aggregate results is not supported");
            }
            plan = plan.sort_by(sort);
        }
        if let Some(limit) = self.limit {
            plan = plan.limit(limit);
        }
        Ok(plan)
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
        columns.extend(self.sort.iter().map(|sort| &sort.column));
        if let Some(predicate) = &self.predicate {
            predicate.columns(&mut columns);
        }
        for join in &self.from.joins {
            for (left, right) in &join.on {
                columns.push(left);
                columns.push(right);
            }
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

        let schema: SchemaRef = Arc::new(Schema::new(
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
        prepared = apply_where(prepared, &after, tables, &output_index, &schema)?;
        let plan = LogicalPlan::Prepared(Prepared {
            prep: Arc::new(prepared),
            schema,
        });
        self.shape(plan, None, &output_index)
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

fn parse_order_by(order_by: Option<&OrderBy>) -> Result<Vec<SqlSort>> {
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
                column: parse_column(&expression.expr)?,
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
    reject_repeats(sort.iter().map(|key| &key.column), "ORDER BY")?;
    Ok(sort)
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
    if select.distinct.is_some() || select.top.is_some() {
        bail!("SELECT DISTINCT and TOP are not supported");
    }
    if select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
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

fn parse_projection(items: &[SelectItem], group_by: &GroupByExpr) -> Result<SqlProjection> {
    if matches!(items, [SelectItem::Wildcard(_)]) {
        if !matches!(group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
        {
            bail!("SELECT * cannot be combined with GROUP BY");
        }
        return Ok(SqlProjection::All);
    }
    if items.is_empty() {
        bail!("SELECT needs at least one column");
    }
    let group_by = parse_group_by(group_by)?;
    let mut columns = Vec::new();
    let mut aggregates = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(
                expression @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)),
            ) => columns.push(parse_column(expression)?),
            SelectItem::UnnamedExpr(Expr::Function(function)) => {
                aggregates.push(parse_aggregate(function)?)
            }
            SelectItem::QualifiedWildcard(_, _) => {
                bail!("table.* is not supported; use * or name the columns")
            }
            SelectItem::Wildcard(_) => {
                bail!("SELECT * cannot be combined with explicit columns")
            }
            _ => bail!("SELECT items must be column names or supported aggregates"),
        }
    }
    if aggregates.is_empty() {
        if !group_by.is_empty() {
            bail!("GROUP BY requires at least one aggregate");
        }
        return Ok(SqlProjection::Columns(columns));
    }
    if group_by.is_empty() && !columns.is_empty() {
        bail!("aggregate SELECT cannot include ordinary columns without GROUP BY");
    }
    // The result has the GROUP BY columns first, in GROUP BY order, so SELECT must
    // list them the same way.
    let same = columns.len() == group_by.len()
        && columns
            .iter()
            .zip(&group_by)
            .all(|(column, group)| column.same(group));
    if !same {
        bail!("SELECT must list the GROUP BY columns first, in the same order, then aggregates");
    }
    Ok(SqlProjection::Aggregates(SqlAggregate {
        group_by,
        expressions: aggregates,
    }))
}

fn parse_group_by(group_by: &GroupByExpr) -> Result<Vec<SqlColumn>> {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => {
            if expressions.len() > MAX_GROUP_COLUMNS {
                bail!(
                    "GROUP BY names {} columns; the limit is {MAX_GROUP_COLUMNS}",
                    expressions.len()
                );
            }
            let columns = expressions
                .iter()
                .map(parse_column)
                .collect::<Result<Vec<_>>>()?;
            reject_repeats(columns.iter(), "GROUP BY")?;
            Ok(columns)
        }
        _ => bail!("GROUP BY supports plain column names only (no ROLLUP, CUBE or ALL)"),
    }
}

fn parse_aggregate(function: &Function) -> Result<SqlAggregateExpr> {
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
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        bail!("DISTINCT and aggregate argument clauses are not supported");
    }
    let function_name = function.name.to_string().to_ascii_uppercase();
    let function = match function_name.as_str() {
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
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(expression))] => {
            Some(parse_column(expression)?)
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

fn parse_predicate(expression: &Expr, depth: usize) -> Result<SqlPredicate> {
    match expression {
        Expr::Nested(inner) => parse_predicate(inner, depth),
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            right,
        } => {
            let parts = vec![
                parse_predicate(left, depth)?,
                parse_predicate(right, depth)?,
            ];
            Ok(if *op == BinaryOperator::And {
                SqlPredicate::And(parts)
            } else {
                SqlPredicate::Or(parts)
            })
        }
        Expr::BinaryOp { left, op, right } => match right.as_ref() {
            Expr::Subquery(query) => Ok(SqlPredicate::CompareSubquery {
                column: parse_column(left)?,
                op: comparison_op(op)?,
                query: Box::new(SqlAst::from_query(query, depth + 1)?),
            }),
            right => Ok(SqlPredicate::compare(
                parse_column(left)?,
                comparison_op(op)?,
                parse_literal(right)?,
            )),
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(SqlPredicate::InSubquery {
            column: parse_column(expr)?,
            query: Box::new(SqlAst::from_query(subquery, depth + 1)?),
            negated: *negated,
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let column = parse_column(expr)?;
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
            let column = parse_column(expr)?;
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
            parse_column(expr)?,
            FilterOp::IsNull,
            "",
        )),
        Expr::IsNotNull(expr) => Ok(SqlPredicate::compare(
            parse_column(expr)?,
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
            let column = parse_column(expr)?;
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
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => bail!("NOT is supported only as NOT IN, NOT BETWEEN, NOT LIKE and IS NOT NULL"),
        _ => {
            bail!("WHERE supports comparisons, IN, BETWEEN, LIKE and IS NULL, joined by AND and OR")
        }
    }
}

fn parse_column(expression: &Expr) -> Result<SqlColumn> {
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
                column: SqlColumn::new("id"),
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
            "SELECT COUNT(DISTINCT id) FROM dataset",
            "SELECT SUM(name) FROM dataset",
            "SELECT COUNT(*) FROM dataset GROUP BY id, name",
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
            "SELECT id FROM dataset WHERE NOT (id = 1)",
            "SELECT id FROM dataset WHERE NOT enabled",
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
            "SELECT id FROM dataset WHERE id + 1 = 2",
            "SELECT id FROM dataset WHERE id BETWEEN name AND 3",
            "SELECT id FROM dataset ORDER BY id, id",
            "SELECT id, COUNT(*) FROM dataset GROUP BY enabled",
            "SELECT id FROM dataset LIMIT 1 OFFSET 1",
        ] {
            assert!(SqlAst::parse(sql).is_err(), "{sql}");
        }

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
