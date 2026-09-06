use std::sync::Arc;

use anyhow::{Context, Result, bail};
use parquet_reader::{FilterExpr, FilterOp, NullOrder, ParquetSource, SortDirection};
use sqlparser::{
    ast::{
        BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
        GroupByExpr, OrderBy, OrderByKind, SelectItem, SetExpr, Statement, TableFactor,
        UnaryOperator,
    },
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{AggregateExpr, AggregateFunction, Filter, LogicalPlan, Planner};

/// The deliberately small SQL shape accepted by PAVI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAst {
    projection: SqlProjection,
    predicate: Option<SqlPredicate>,
    sort: Option<SqlSort>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlProjection {
    All,
    Columns(Vec<String>),
    Aggregates(SqlAggregate),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAggregate {
    pub group_by: Option<String>,
    pub expressions: Vec<SqlAggregateExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAggregateExpr {
    pub function: AggregateFunction,
    pub column: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlPredicate {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlSort {
    pub column: String,
    pub direction: SortDirection,
    pub nulls: NullOrder,
}

impl SqlAst {
    /// Parses one `SELECT ... FROM dataset [WHERE ...] [LIMIT ...]` statement.
    pub fn parse(sql: &str) -> Result<Self> {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).context("parse SQL")?;
        if statements.len() != 1 {
            bail!("SQL input must contain exactly one statement");
        }
        let Statement::Query(query) = statements.pop().expect("checked statement count") else {
            bail!("only SELECT queries are supported");
        };
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
        let SetExpr::Select(select) = *query.body else {
            bail!("only a single SELECT is supported");
        };
        reject_select_clauses(&select)?;
        validate_from(&select.from)?;

        Ok(Self {
            projection: parse_projection(&select.projection, &select.group_by)?,
            predicate: select.selection.as_ref().map(parse_predicate).transpose()?,
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

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn sort(&self) -> Option<&SqlSort> {
        self.sort.as_ref()
    }

    /// Resolves names against a single PAVI data source and produces the existing logical plan.
    pub fn to_logical_plan(&self, source: Arc<ParquetSource>) -> Result<LogicalPlan> {
        let (projection, aggregate) = match &self.projection {
            SqlProjection::All => (None, None),
            SqlProjection::Columns(columns) => (
                Some(
                    columns
                        .iter()
                        .map(|column| resolve_column(&source, column))
                        .collect::<Result<Vec<_>>>()?,
                ),
                None,
            ),
            SqlProjection::Aggregates(aggregate) => {
                let expressions = aggregate
                    .expressions
                    .iter()
                    .map(|expression| {
                        let column = expression
                            .column
                            .as_deref()
                            .map(|column| resolve_column(&source, column))
                            .transpose()?;
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
                    .as_deref()
                    .map(|column| resolve_column(&source, column))
                    .transpose()?;
                (None, Some((group_by, expressions)))
            }
        };
        let filter = self
            .predicate
            .as_ref()
            .map(|predicate| -> Result<Filter> {
                let index = resolve_column(&source, &predicate.column)?;
                let column = source.schema().field(index).name().to_owned();
                Ok(Filter::new(FilterExpr {
                    column,
                    op: predicate.op.clone(),
                    value: predicate.value.clone(),
                }))
            })
            .transpose()?;
        let sort = self
            .sort
            .as_ref()
            .map(|sort| -> Result<(usize, SortDirection, NullOrder)> {
                Ok((
                    resolve_column(&source, &sort.column)?,
                    sort.direction,
                    sort.nulls,
                ))
            })
            .transpose()?;

        let mut plan = LogicalPlan::scan(source);
        if let Some(filter) = filter {
            plan = plan.filter(filter);
        }
        if let Some(projection) = projection {
            plan = plan.project(projection);
        }
        if let Some((group_by, expressions)) = aggregate {
            plan = if let Some(group_by) = group_by {
                plan.aggregate_grouped(group_by, expressions)?
            } else {
                plan.aggregate(expressions)?
            };
        }
        if let Some((column, direction, nulls)) = sort {
            if matches!(self.projection, SqlProjection::Aggregates(_)) {
                bail!("ORDER BY aggregate results is not supported");
            }
            plan = plan.sort(column, direction, nulls);
        }
        if let Some(limit) = self.limit {
            plan = plan.limit(limit);
        }
        Planner::plan(&plan).context("validate SQL query")?;
        Ok(plan)
    }
}

fn parse_order_by(order_by: Option<&OrderBy>) -> Result<Option<SqlSort>> {
    let Some(order_by) = order_by else {
        return Ok(None);
    };
    if order_by.interpolate.is_some() {
        bail!("ORDER BY INTERPOLATE is not supported");
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        bail!("ORDER BY ALL is not supported");
    };
    let [expression] = expressions.as_slice() else {
        bail!("ORDER BY supports exactly one column");
    };
    if expression.with_fill.is_some() {
        bail!("ORDER BY WITH FILL is not supported");
    }
    Ok(Some(SqlSort {
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
    }))
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

fn validate_from(from: &[sqlparser::ast::TableWithJoins]) -> Result<()> {
    if from.len() != 1 {
        bail!("SELECT must read exactly one dataset");
    }
    let source = &from[0];
    if !source.joins.is_empty() {
        bail!("JOIN is not supported");
    }
    let TableFactor::Table { name, alias, .. } = &source.relation else {
        bail!("FROM must name the dataset");
    };
    if alias.is_some() || !name.to_string().eq_ignore_ascii_case("dataset") {
        bail!("FROM must be the single dataset named `dataset`");
    }
    Ok(())
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
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                columns.push(identifier.value.clone())
            }
            SelectItem::UnnamedExpr(Expr::Function(function)) => {
                aggregates.push(parse_aggregate(function)?)
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                bail!("SELECT * cannot be combined with explicit columns")
            }
            _ => bail!("SELECT items must be column names or supported aggregates"),
        }
    }
    if aggregates.is_empty() {
        if group_by.is_some() {
            bail!("GROUP BY requires at least one aggregate");
        }
        return Ok(SqlProjection::Columns(columns));
    }
    let Some(group_by) = group_by.or_else(|| columns.is_empty().then_some(String::new())) else {
        bail!("aggregate SELECT cannot include ordinary columns without GROUP BY");
    };
    let group_by = (!group_by.is_empty()).then_some(group_by);
    let group_matches = match (columns.first(), group_by.as_deref()) {
        (None, None) => true,
        (Some(column), Some(group)) => column.eq_ignore_ascii_case(group),
        _ => false,
    };
    if columns.len() != usize::from(group_by.is_some()) || !group_matches {
        bail!("SELECT may include only the GROUP BY column and aggregate expressions");
    }
    Ok(SqlProjection::Aggregates(SqlAggregate {
        group_by,
        expressions: aggregates,
    }))
}

fn parse_group_by(group_by: &GroupByExpr) -> Result<Option<String>> {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty() =>
        {
            Ok(None)
        }
        GroupByExpr::Expressions(expressions, modifiers)
            if modifiers.is_empty() && expressions.len() == 1 =>
        {
            let expression = &expressions[0];
            Ok(Some(parse_column(expression)?))
        }
        _ => bail!("GROUP BY supports exactly one column"),
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

fn parse_predicate(expression: &Expr) -> Result<SqlPredicate> {
    match expression {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            ..
        } => {
            bail!("AND is not supported because a PAVI query has one filter predicate")
        }
        Expr::BinaryOp { left, op, right } => Ok(SqlPredicate {
            column: parse_column(left)?,
            op: match op {
                BinaryOperator::Eq => FilterOp::Eq,
                BinaryOperator::NotEq => FilterOp::Ne,
                BinaryOperator::Gt => FilterOp::Gt,
                BinaryOperator::GtEq => FilterOp::Ge,
                BinaryOperator::Lt => FilterOp::Lt,
                BinaryOperator::LtEq => FilterOp::Le,
                _ => bail!("WHERE supports =, !=, >, >=, <, <=, or LIKE '%text%'"),
            },
            value: parse_literal(right)?,
        }),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            ..
        } => {
            if *negated || *any {
                bail!("WHERE supports only LIKE '%text%' for contains matching");
            }
            let pattern = parse_literal(pattern)?;
            let Some(value) = pattern
                .strip_prefix('%')
                .and_then(|value| value.strip_suffix('%'))
            else {
                bail!("LIKE must use a contains pattern such as LIKE '%text%'");
            };
            if value.contains('%') || value.contains('_') {
                bail!("LIKE supports only one leading and trailing % for contains matching");
            }
            Ok(SqlPredicate {
                column: parse_column(expr)?,
                op: FilterOp::Contains,
                value: value.to_string(),
            })
        }
        _ => bail!("WHERE must be one supported column predicate"),
    }
}

fn parse_column(expression: &Expr) -> Result<String> {
    let Expr::Identifier(identifier) = expression else {
        bail!("the left side of WHERE must be a column name");
    };
    Ok(identifier.value.clone())
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
            &SqlProjection::Columns(vec!["name".to_string(), "id".to_string()])
        );

        let batches = execute("SELECT name FROM dataset LIMIT 2", source);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches[0].num_rows(), 2);
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
            Some(&SqlSort {
                column: "id".to_string(),
                direction: SortDirection::Descending,
                nulls: NullOrder::Last,
            })
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
            .aggregate_grouped(1, vec![AggregateExpr::count_all(), AggregateExpr::avg(0)])
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
            "SELECT id FROM other",
            "SELECT id FROM dataset WHERE id = 1 AND enabled = true",
            "SELECT id FROM dataset ORDER BY id, enabled",
            "SELECT id FROM dataset LIMIT 1 OFFSET 1",
        ] {
            assert!(SqlAst::parse(sql).is_err(), "{sql}");
        }

        let (_directory, source) = source();
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
}
