use std::sync::Arc;

use anyhow::{Context, Result, bail};
use parquet_reader::{FilterExpr, FilterOp, ParquetSource};
use sqlparser::{
    ast::{
        BinaryOperator, Expr, GroupByExpr, SelectItem, SetExpr, Statement, TableFactor,
        UnaryOperator,
    },
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{Filter, LogicalPlan, Planner};

/// The deliberately small SQL shape accepted by PAVI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlAst {
    projection: SqlProjection,
    predicate: Option<SqlPredicate>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlProjection {
    All,
    Columns(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlPredicate {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
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
        if query.order_by.is_some() {
            bail!("ORDER BY is not supported");
        }
        if query.fetch.is_some() {
            bail!("FETCH is not supported; use LIMIT");
        }
        if !query.locks.is_empty() || query.for_clause.is_some() || !query.pipe_operators.is_empty()
        {
            bail!("locking and pipe clauses are not supported");
        }

        let limit = parse_limit(query.limit_clause.as_ref())?;
        let SetExpr::Select(select) = *query.body else {
            bail!("only a single SELECT is supported");
        };
        reject_select_clauses(&select)?;
        validate_from(&select.from)?;

        Ok(Self {
            projection: parse_projection(&select.projection)?,
            predicate: select.selection.as_ref().map(parse_predicate).transpose()?,
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

    /// Resolves names against a single PAVI data source and produces the existing logical plan.
    pub fn to_logical_plan(&self, source: Arc<ParquetSource>) -> Result<LogicalPlan> {
        let projection = match &self.projection {
            SqlProjection::All => None,
            SqlProjection::Columns(columns) => Some(
                columns
                    .iter()
                    .map(|column| resolve_column(&source, column))
                    .collect::<Result<Vec<_>>>()?,
            ),
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

        let mut plan = LogicalPlan::scan(source);
        if let Some(filter) = filter {
            plan = plan.filter(filter);
        }
        if let Some(projection) = projection {
            plan = plan.project(projection);
        }
        if let Some(limit) = self.limit {
            plan = plan.limit(limit);
        }
        Planner::plan(&plan).context("validate SQL query")?;
        Ok(plan)
    }
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
    if !matches!(
        &select.group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    ) {
        bail!("GROUP BY is not supported");
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

fn parse_projection(items: &[SelectItem]) -> Result<SqlProjection> {
    if matches!(items, [SelectItem::Wildcard(_)]) {
        return Ok(SqlProjection::All);
    }
    if items.is_empty() {
        bail!("SELECT needs at least one column");
    }
    items
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => Ok(identifier.value.clone()),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                bail!("SELECT * cannot be combined with explicit columns")
            }
            _ => bail!("SELECT items must be column names or *"),
        })
        .collect::<Result<Vec<_>>>()
        .map(SqlProjection::Columns)
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

    use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use pavi_runtime::{GenerationId, Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;
    use crate::QueryEngine;

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
    fn rejects_invalid_unsupported_and_invalid_source_queries() {
        for sql in [
            "SELECT FROM dataset",
            "SELECT id FROM other",
            "SELECT id FROM dataset WHERE id = 1 AND enabled = true",
            "SELECT id FROM dataset ORDER BY id",
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
