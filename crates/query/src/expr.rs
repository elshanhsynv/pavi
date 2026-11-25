//! Calculated values in SQL (ADR 0013): arithmetic, `||`, comparisons, `AND` /
//! `OR` / `NOT`, `IS [NOT] NULL`, `CAST`, `CASE` and a few functions. They are
//! parsed from sqlparser's AST, typed against the input's schema before
//! anything runs, and evaluated one batch at a time, so a calculation holds no
//! more than the batch it works on.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    new_null_array,
};
use arrow_cast::cast::{CastOptions, can_cast_types, cast_with_options};
use arrow_schema::{DataType, Schema, TimeUnit};
use sqlparser::ast::{
    self, BinaryOperator as SqlOperator, CastKind, FunctionArg, FunctionArgExpr, FunctionArguments,
    UnaryOperator, Value,
};

use crate::sql::{SqlColumn, parse_column};

/// Nodes one calculation may have: a long or deeply nested expression in
/// untrusted SQL is refused before it is walked any further.
pub const MAX_EXPR_NODES: usize = 256;
/// Arguments of `CONCAT` and `COALESCE`.
pub const MAX_FUNCTION_ARGS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    Int(i64),
    /// Kept as written; parsed when evaluated.
    Float(String),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl BinaryOp {
    fn is_arithmetic(self) -> bool {
        matches!(
            self,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Rem
        )
    }

    fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Ne | Self::Lt | Self::Le | Self::Gt | Self::Ge
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Function {
    Lower,
    Upper,
    Trim,
    Length,
    Substr,
    Concat,
    Abs,
    Round,
    Coalesce,
    Year,
    Quarter,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    DayOfWeek,
    /// `REPLACE(text, from, to)`: every `from` in `text` becomes `to`.
    Replace,
    /// `DATE_TRUNC(unit, value)`: the start of the unit the value is in.
    DateTrunc(crate::dates::DateUnit),
    /// `DATE_DIFF(unit, start, end)`: unit boundaries from start to end.
    DateDiff(crate::dates::DateUnit),
    /// `value ± INTERVAL …`.
    Shift(crate::dates::Interval),
    /// `NOW()` / `CURRENT_TIMESTAMP`: the moment the query was written, as a
    /// timestamp without a zone holding UTC's wall-clock time (microseconds
    /// since 1970), the same for every row.
    Now(i64),
    /// `CURRENT_DATE`: the day the query was written (UTC), in days since 1970.
    Today(i32),
}

impl Function {
    fn date_part(self) -> Option<arrow_arith::temporal::DatePart> {
        use arrow_arith::temporal::DatePart;
        Some(match self {
            Self::Year => DatePart::Year,
            Self::Quarter => DatePart::Quarter,
            Self::Month => DatePart::Month,
            Self::Day => DatePart::Day,
            Self::Hour => DatePart::Hour,
            Self::Minute => DatePart::Minute,
            Self::Second => DatePart::Second,
            // Monday 1 … Sunday 7, as ISO 8601 counts.
            Self::DayOfWeek => DatePart::DayOfWeekMonday1,
            _ => return None,
        })
    }
}

/// A calculation over columns of type `C`: [`SqlColumn`] as written, `usize`
/// once resolved to the input's columns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr<C> {
    Column(C),
    Literal(Literal),
    Neg(Box<Expr<C>>),
    Not(Box<Expr<C>>),
    Binary {
        op: BinaryOp,
        left: Box<Expr<C>>,
        right: Box<Expr<C>>,
    },
    IsNull {
        expr: Box<Expr<C>>,
        negated: bool,
    },
    /// `safe` (`TRY_CAST`) turns values that don't convert into NULL; `CAST`
    /// fails on them.
    Cast {
        expr: Box<Expr<C>>,
        to: DataType,
        safe: bool,
    },
    Case {
        branches: Vec<(Expr<C>, Expr<C>)>,
        otherwise: Option<Box<Expr<C>>>,
    },
    Call {
        function: Function,
        args: Vec<Expr<C>>,
    },
}

impl<C> Expr<C> {
    /// Whether it is more than a column.
    pub fn is_calculated(&self) -> bool {
        !matches!(self, Self::Column(_))
    }

    /// Every column it reads.
    pub fn columns<'a>(&'a self, columns: &mut Vec<&'a C>) {
        match self {
            Self::Column(column) => columns.push(column),
            Self::Literal(_) => {}
            Self::Neg(expr)
            | Self::Not(expr)
            | Self::IsNull { expr, .. }
            | Self::Cast { expr, .. } => expr.columns(columns),
            Self::Binary { left, right, .. } => {
                left.columns(columns);
                right.columns(columns);
            }
            Self::Case {
                branches,
                otherwise,
            } => {
                for (condition, value) in branches {
                    condition.columns(columns);
                    value.columns(columns);
                }
                if let Some(otherwise) = otherwise {
                    otherwise.columns(columns);
                }
            }
            Self::Call { args, .. } => args.iter().for_each(|arg| arg.columns(columns)),
        }
    }

    /// The same calculation with every column mapped by `map`.
    pub fn map_columns<D>(&self, map: &dyn Fn(&C) -> Result<D>) -> Result<Expr<D>> {
        let boxed =
            |expr: &Expr<C>| -> Result<Box<Expr<D>>> { Ok(Box::new(expr.map_columns(map)?)) };
        Ok(match self {
            Self::Column(column) => Expr::Column(map(column)?),
            Self::Literal(literal) => Expr::Literal(literal.clone()),
            Self::Neg(expr) => Expr::Neg(boxed(expr)?),
            Self::Not(expr) => Expr::Not(boxed(expr)?),
            Self::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: boxed(left)?,
                right: boxed(right)?,
            },
            Self::IsNull { expr, negated } => Expr::IsNull {
                expr: boxed(expr)?,
                negated: *negated,
            },
            Self::Cast { expr, to, safe } => Expr::Cast {
                expr: boxed(expr)?,
                to: to.clone(),
                safe: *safe,
            },
            Self::Case {
                branches,
                otherwise,
            } => Expr::Case {
                branches: branches
                    .iter()
                    .map(|(condition, value)| {
                        Ok((condition.map_columns(map)?, value.map_columns(map)?))
                    })
                    .collect::<Result<_>>()?,
                otherwise: otherwise.as_deref().map(boxed).transpose()?,
            },
            Self::Call { function, args } => Expr::Call {
                function: *function,
                args: args
                    .iter()
                    .map(|arg| arg.map_columns(map))
                    .collect::<Result<_>>()?,
            },
        })
    }
}

// ---------------------------------------------------------------- parsing

/// What a calculation in an aggregate query's SELECT list does with an
/// aggregate it contains: names the column that will hold its result.
pub(crate) type AggregateHook<'a> = &'a mut dyn FnMut(&ast::Function) -> Result<SqlColumn>;

/// Parses a calculation from sqlparser's AST. A plain column becomes
/// [`Expr::Column`]; aggregates are refused (see [`parse_with`]).
pub(crate) fn parse(expression: &ast::Expr) -> Result<Expr<SqlColumn>> {
    parse_node(expression, &mut 0, &mut None)
}

/// Parses a calculation that may use aggregates' results
/// (`ROUND(AVG(x), 2)`): `on_aggregate` turns each aggregate into the column
/// that will hold its result.
pub(crate) fn parse_with(
    expression: &ast::Expr,
    on_aggregate: AggregateHook<'_>,
) -> Result<Expr<SqlColumn>> {
    parse_node(expression, &mut 0, &mut Some(on_aggregate))
}

/// Whether a function name is one of the aggregates.
pub(crate) fn is_aggregate_name(name: &str) -> bool {
    parquet_reader::is_aggregate_name(name)
}

fn parse_node(
    expression: &ast::Expr,
    nodes: &mut usize,
    on_aggregate: &mut Option<AggregateHook<'_>>,
) -> Result<Expr<SqlColumn>> {
    *nodes += 1;
    if *nodes > MAX_EXPR_NODES {
        bail!("a calculation has more than {MAX_EXPR_NODES} parts");
    }
    if let ast::Expr::Function(function) = expression {
        let name = function.name.to_string().to_ascii_uppercase();
        if is_aggregate_name(&name) {
            return match on_aggregate {
                Some(hook) => Ok(Expr::Column(hook(function)?)),
                None => bail!(
                    "{name} is an aggregate and can't be used here; calculations with aggregates (ROUND(AVG(x), 2)) work in SELECT, and an alias lets ORDER BY and HAVING use them"
                ),
            };
        }
    }
    let mut child = |expression: &ast::Expr| -> Result<Box<Expr<SqlColumn>>> {
        Ok(Box::new(parse_node(expression, nodes, on_aggregate)?))
    };
    Ok(match expression {
        ast::Expr::Nested(inner) => return parse_node(inner, nodes, on_aggregate),
        ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) => {
            Expr::Column(parse_column(expression)?)
        }
        ast::Expr::Value(value) => Expr::Literal(literal(&value.value, false)?),
        ast::Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            // A negative number is a literal, not a calculation.
            ast::Expr::Value(value) if matches!(value.value, Value::Number(..)) => {
                Expr::Literal(literal(&value.value, true)?)
            }
            _ => Expr::Neg(child(expr)?),
        },
        ast::Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => return parse_node(expr, nodes, on_aggregate),
        ast::Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Expr::Not(child(expr)?),
        // `value + INTERVAL '7 days'`, `value - INTERVAL 1 MONTH`.
        ast::Expr::BinaryOp {
            left,
            op: op @ (SqlOperator::Plus | SqlOperator::Minus),
            right,
        } if matches!(right.as_ref(), ast::Expr::Interval(_))
            || (matches!(left.as_ref(), ast::Expr::Interval(_)) && *op == SqlOperator::Plus) =>
        {
            let (value, interval) = match (left.as_ref(), right.as_ref()) {
                (value, ast::Expr::Interval(interval))
                    if !matches!(value, ast::Expr::Interval(_)) =>
                {
                    (value, interval)
                }
                (ast::Expr::Interval(interval), value)
                    if !matches!(value, ast::Expr::Interval(_)) =>
                {
                    (value, interval)
                }
                _ => bail!("an interval is added to a date or timestamp, not to another interval"),
            };
            let mut interval = parse_interval(interval)?;
            if *op == SqlOperator::Minus {
                interval = interval.negated();
            }
            Expr::Call {
                function: Function::Shift(interval),
                args: vec![*child(value)?],
            }
        }
        ast::Expr::Interval(_) => {
            bail!("an interval is used as date ± INTERVAL '7 days', not on its own")
        }
        ast::Expr::BinaryOp { left, op, right } => {
            let op = match op {
                SqlOperator::Plus => BinaryOp::Add,
                SqlOperator::Minus => BinaryOp::Sub,
                SqlOperator::Multiply => BinaryOp::Mul,
                SqlOperator::Divide => BinaryOp::Div,
                SqlOperator::Modulo => BinaryOp::Rem,
                SqlOperator::StringConcat => BinaryOp::Concat,
                SqlOperator::Eq => BinaryOp::Eq,
                SqlOperator::NotEq => BinaryOp::Ne,
                SqlOperator::Lt => BinaryOp::Lt,
                SqlOperator::LtEq => BinaryOp::Le,
                SqlOperator::Gt => BinaryOp::Gt,
                SqlOperator::GtEq => BinaryOp::Ge,
                SqlOperator::And => BinaryOp::And,
                SqlOperator::Or => BinaryOp::Or,
                other => bail!("the operator {other} can't be used in a calculation"),
            };
            Expr::Binary {
                op,
                left: child(left)?,
                right: child(right)?,
            }
        }
        ast::Expr::IsNull(expr) => Expr::IsNull {
            expr: child(expr)?,
            negated: false,
        },
        ast::Expr::IsNotNull(expr) => Expr::IsNull {
            expr: child(expr)?,
            negated: true,
        },
        ast::Expr::Cast {
            kind,
            expr,
            data_type,
            ..
        } => Expr::Cast {
            expr: child(expr)?,
            to: sql_type(data_type)?,
            safe: !matches!(kind, CastKind::Cast | CastKind::DoubleColon),
        },
        // `DATE '2024-03-01'`, `TIMESTAMP '…'`: the text cast to the type.
        ast::Expr::TypedString(typed) => {
            let Value::SingleQuotedString(text) = &typed.value.value else {
                bail!("{} needs its value in quotes", typed.data_type);
            };
            Expr::Cast {
                expr: Box::new(Expr::Literal(Literal::Text(text.clone()))),
                to: sql_type(&typed.data_type)?,
                safe: false,
            }
        }
        ast::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if conditions.is_empty() {
                bail!("CASE needs at least one WHEN");
            }
            let operand = operand.as_deref().map(&mut child).transpose()?;
            let mut branches = Vec::new();
            for when in conditions {
                let condition = child(&when.condition)?;
                // `CASE x WHEN v` is `CASE WHEN x = v`.
                let condition = match &operand {
                    Some(operand) => Expr::Binary {
                        op: BinaryOp::Eq,
                        left: operand.clone(),
                        right: condition,
                    },
                    None => *condition,
                };
                branches.push((condition, *child(&when.result)?));
            }
            Expr::Case {
                branches,
                otherwise: else_result.as_deref().map(&mut child).transpose()?,
            }
        }
        ast::Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_uppercase();
            if function.over.is_some() {
                bail!(
                    "{name}(…) OVER (…): window functions are not supported yet; \
                     use GROUP BY for one row per group"
                );
            }
            if function.filter.is_some()
                || !function.within_group.is_empty()
                || !matches!(function.parameters, FunctionArguments::None)
            {
                bail!("{name}: function modifiers are not supported");
            }
            // `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`: fixed when written.
            let no_arguments = match &function.args {
                FunctionArguments::None => true,
                FunctionArguments::List(list) => list.args.is_empty(),
                FunctionArguments::Subquery(_) => false,
            };
            match name.as_str() {
                "NOW" | "CURRENT_TIMESTAMP" | "GETDATE" if no_arguments => {
                    return Ok(Expr::Call {
                        function: Function::Now(crate::dates::now_micros()),
                        args: Vec::new(),
                    });
                }
                "CURRENT_DATE" | "TODAY" if no_arguments => {
                    return Ok(Expr::Call {
                        function: Function::Today(crate::dates::today_days()),
                        args: Vec::new(),
                    });
                }
                _ => {}
            }
            let FunctionArguments::List(list) = &function.args else {
                bail!("{name} needs parentheses");
            };
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                bail!("{name}: DISTINCT and argument clauses are not supported");
            }
            let args = list
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
                        Ok(*child(expression)?)
                    }
                    _ => bail!("{name} takes plain arguments"),
                })
                .collect::<Result<Vec<_>>>()?;
            // `DATE_TRUNC('month', x)` and `DATE_DIFF('day', a, b)`: the unit
            // is text written in the query, read now.
            if matches!(name.as_str(), "DATE_TRUNC" | "DATE_DIFF" | "DATEDIFF") {
                let mut args = args.into_iter();
                let unit = match args.next() {
                    Some(Expr::Literal(Literal::Text(unit))) => {
                        crate::dates::DateUnit::parse(&unit)?
                    }
                    _ => bail!("{name}'s first argument is a unit in quotes, such as 'month'"),
                };
                let args = args.collect::<Vec<_>>();
                let (function, wanted) = if name == "DATE_TRUNC" {
                    (Function::DateTrunc(unit), 1)
                } else {
                    (Function::DateDiff(unit), 2)
                };
                if args.len() != wanted {
                    bail!("{name} takes a unit and {wanted} date or timestamp argument(s)");
                }
                return Ok(Expr::Call { function, args });
            }
            call(&name, args)?
        }
        ast::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![*child(expr)?];
            match substring_from {
                Some(from) => args.push(*child(from)?),
                None => args.push(Expr::Literal(Literal::Int(1))),
            }
            if let Some(length) = substring_for {
                args.push(*child(length)?);
            }
            call("SUBSTRING", args)?
        }
        ast::Expr::Trim {
            expr,
            trim_where: None,
            trim_what: None,
            trim_characters: None,
        } => call("TRIM", vec![*child(expr)?])?,
        ast::Expr::Trim { .. } => {
            bail!("TRIM takes one argument; LEADING, TRAILING and characters are not supported")
        }
        ast::Expr::Extract { field, expr, .. } => {
            call(&field.to_string().to_ascii_uppercase(), vec![*child(expr)?])?
        }
        other => bail!("this calculation is not supported: {other}"),
    })
}

fn literal(value: &Value, negative: bool) -> Result<Literal> {
    let sign = if negative { "-" } else { "" };
    Ok(match value {
        Value::Number(text, _) => {
            let text = format!("{sign}{text}");
            if let Ok(integer) = text.parse::<i64>() {
                Literal::Int(integer)
            } else if text.parse::<f64>().is_ok_and(f64::is_finite) {
                Literal::Float(text)
            } else {
                bail!("{text} is not a number PAVI can calculate with")
            }
        }
        Value::SingleQuotedString(text) if !negative => Literal::Text(text.clone()),
        Value::Boolean(value) if !negative => Literal::Bool(*value),
        Value::Null if !negative => Literal::Null,
        other => bail!("the value {sign}{other} can't be used in a calculation"),
    })
}

/// `INTERVAL '7 days'` or `INTERVAL 7 DAY`.
fn parse_interval(interval: &ast::Interval) -> Result<crate::dates::Interval> {
    if interval.last_field.is_some() {
        bail!("an interval range such as DAY TO HOUR is not supported; write '1 day 2 hours'");
    }
    let text = match interval.value.as_ref() {
        ast::Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::Number(text, _) => text.clone(),
            other => bail!("the interval {other} is not a number of units"),
        },
        other => bail!("the interval {other} is not a number of units"),
    };
    let unit = interval
        .leading_field
        .as_ref()
        .map(|field| field.to_string());
    crate::dates::Interval::parse(&text, unit.as_deref())
}

/// A function call by name, with its arguments checked for count.
fn call(name: &str, args: Vec<Expr<SqlColumn>>) -> Result<Expr<SqlColumn>> {
    let (function, arity) = match name {
        "LOWER" | "LCASE" => (Function::Lower, 1..=1),
        "UPPER" | "UCASE" => (Function::Upper, 1..=1),
        "TRIM" => (Function::Trim, 1..=1),
        "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => (Function::Length, 1..=1),
        "SUBSTR" | "SUBSTRING" => (Function::Substr, 2..=3),
        "CONCAT" => (Function::Concat, 1..=MAX_FUNCTION_ARGS),
        "ABS" => (Function::Abs, 1..=1),
        "ROUND" => (Function::Round, 1..=2),
        "COALESCE" | "IFNULL" => (Function::Coalesce, 1..=MAX_FUNCTION_ARGS),
        "YEAR" => (Function::Year, 1..=1),
        "QUARTER" => (Function::Quarter, 1..=1),
        "MONTH" => (Function::Month, 1..=1),
        "DAY" | "DAYOFMONTH" => (Function::Day, 1..=1),
        "HOUR" => (Function::Hour, 1..=1),
        "MINUTE" => (Function::Minute, 1..=1),
        "SECOND" => (Function::Second, 1..=1),
        "DAYOFWEEK" | "DOW" | "ISODOW" => (Function::DayOfWeek, 1..=1),
        "REPLACE" => (Function::Replace, 3..=3),
        _ => bail!(
            "unknown function {name}; calculations support LOWER, UPPER, TRIM, LENGTH, SUBSTRING, \
             CONCAT, REPLACE, ABS, ROUND, COALESCE, YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, \
             SECOND, DAYOFWEEK, DATE_TRUNC, DATE_DIFF, NOW and CURRENT_DATE"
        ),
    };
    if !arity.contains(&args.len()) {
        let expected = if arity.start() == arity.end() {
            arity.start().to_string()
        } else {
            format!("{} to {}", arity.start(), arity.end())
        };
        bail!("{name} takes {expected} arguments, not {}", args.len());
    }
    Ok(Expr::Call { function, args })
}

/// An Arrow type for a SQL type name, for `CAST`.
fn sql_type(data_type: &ast::DataType) -> Result<DataType> {
    let text = data_type.to_string().to_ascii_uppercase();
    let base = text.split('(').next().unwrap_or_default().trim();
    let arguments = text
        .split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .map(|inside| {
            inside
                .split(',')
                .map(|part| part.trim().parse::<i64>())
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();
    Ok(match base {
        "TINYINT" | "INT1" => DataType::Int8,
        "SMALLINT" | "INT2" => DataType::Int16,
        "INT" | "INTEGER" | "INT4" => DataType::Int32,
        "BIGINT" | "INT8" | "LONG" => DataType::Int64,
        "REAL" | "FLOAT4" => DataType::Float32,
        "FLOAT" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "CHARACTER VARYING" => DataType::Utf8,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "DECIMAL" | "NUMERIC" | "DEC" => {
            let (precision, scale) = match arguments.as_slice() {
                [] => (18, 3),
                [precision] => (*precision, 0),
                [precision, scale] => (*precision, *scale),
                _ => bail!("DECIMAL takes a precision and a scale"),
            };
            let precision = u8::try_from(precision)
                .ok()
                .filter(|precision| (1..=38).contains(precision))
                .context("a DECIMAL's precision is 1 to 38")?;
            let scale = i8::try_from(scale)
                .ok()
                .filter(|scale| *scale >= 0 && (*scale as u8) <= precision)
                .context("a DECIMAL's scale is 0 to its precision")?;
            DataType::Decimal128(precision, scale)
        }
        _ => bail!(
            "CAST to {text} is not supported; use INT, BIGINT, DOUBLE, DECIMAL(p, s), VARCHAR, BOOLEAN, DATE or TIMESTAMP"
        ),
    })
}

// ---------------------------------------------------------------- types

fn is_number(data_type: &DataType) -> bool {
    data_type.is_numeric() || *data_type == DataType::Null
}

fn is_integer(data_type: &DataType) -> bool {
    data_type.is_integer() || *data_type == DataType::Null
}

fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    )
}

fn is_nested(data_type: &DataType) -> bool {
    data_type.is_nested()
}

/// The type arithmetic on `left` and `right` gives: whole numbers stay whole
/// (`Int64`); anything with a fraction or a decimal is a `Float64`.
fn numeric_type(left: &DataType, right: &DataType) -> DataType {
    if is_integer(left) && is_integer(right) {
        DataType::Int64
    } else {
        DataType::Float64
    }
}

/// One type for values from several branches (`CASE`, `COALESCE`).
fn common_type(types: &[DataType]) -> Result<DataType> {
    let known = types
        .iter()
        .filter(|data_type| **data_type != DataType::Null)
        .collect::<Vec<_>>();
    let Some(first) = known.first() else {
        return Ok(DataType::Null);
    };
    if known.iter().all(|data_type| data_type.is_numeric()) {
        return Ok(if known.iter().all(|data_type| data_type.is_integer()) {
            DataType::Int64
        } else {
            DataType::Float64
        });
    }
    if known.iter().all(|data_type| data_type == first) {
        return Ok((*first).clone());
    }
    if known.iter().all(|data_type| !is_nested(data_type)) {
        return Ok(DataType::Utf8);
    }
    bail!("the values have types that can't be combined")
}

/// The type two compared values are brought to, if they can be compared.
fn comparison_type(left: &DataType, right: &DataType) -> Result<DataType> {
    if is_number(left) && is_number(right) {
        return Ok(numeric_type(left, right));
    }
    if is_nested(left) || is_nested(right) {
        bail!("{left} and {right} values can't be compared");
    }
    Ok(match (left, right) {
        (left, right) if left == right => left.clone(),
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        // Text against anything else: the text is read as that type
        // (`d > '2024-01-01'`).
        (text, other) | (other, text)
            if matches!(
                text,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            other.clone()
        }
        _ => bail!("{left} and {right} values can't be compared"),
    })
}

impl Expr<usize> {
    /// The type of the result over an input with `schema`, after checking
    /// every part. Run before anything is read.
    pub fn data_type(&self, schema: &Schema) -> Result<DataType> {
        Ok(match self {
            Self::Column(column) => schema
                .fields()
                .get(*column)
                .ok_or_else(|| anyhow!("calculation column {column} is out of range"))?
                .data_type()
                .clone(),
            Self::Literal(literal) => match literal {
                Literal::Int(_) => DataType::Int64,
                Literal::Float(_) => DataType::Float64,
                Literal::Text(_) => DataType::Utf8,
                Literal::Bool(_) => DataType::Boolean,
                Literal::Null => DataType::Null,
            },
            Self::Neg(expr) => {
                let input = expr.data_type(schema)?;
                if !is_number(&input) {
                    bail!("- needs a number, not {input}");
                }
                numeric_type(&input, &input)
            }
            Self::Not(expr) => {
                let input = expr.data_type(schema)?;
                if !matches!(input, DataType::Boolean | DataType::Null) {
                    bail!("NOT needs a true/false value, not {input}");
                }
                DataType::Boolean
            }
            Self::Binary { op, left, right } => {
                let (left, right) = (left.data_type(schema)?, right.data_type(schema)?);
                match op {
                    op if op.is_arithmetic() => {
                        if !is_number(&left) || !is_number(&right) {
                            bail!("arithmetic needs numbers, not {left} and {right}");
                        }
                        if *op == BinaryOp::Div {
                            DataType::Float64
                        } else {
                            numeric_type(&left, &right)
                        }
                    }
                    BinaryOp::Concat => {
                        if is_nested(&left) || is_nested(&right) {
                            bail!("|| joins text, not {left} and {right}");
                        }
                        DataType::Utf8
                    }
                    op if op.is_comparison() => {
                        comparison_type(&left, &right)?;
                        DataType::Boolean
                    }
                    _ => {
                        for side in [&left, &right] {
                            if !matches!(side, DataType::Boolean | DataType::Null) {
                                bail!("AND and OR need true/false values, not {side}");
                            }
                        }
                        DataType::Boolean
                    }
                }
            }
            Self::IsNull { expr, .. } => {
                expr.data_type(schema)?;
                DataType::Boolean
            }
            Self::Cast { expr, to, .. } => {
                let from = expr.data_type(schema)?;
                if !can_cast_types(&from, to) {
                    bail!("{from} values can't be CAST to {to}");
                }
                to.clone()
            }
            Self::Case {
                branches,
                otherwise,
            } => {
                let mut types = Vec::new();
                for (condition, value) in branches {
                    let condition = condition.data_type(schema)?;
                    if !matches!(condition, DataType::Boolean | DataType::Null) {
                        bail!("CASE WHEN needs a true/false test, not a {condition} value");
                    }
                    types.push(value.data_type(schema)?);
                }
                if let Some(otherwise) = otherwise {
                    types.push(otherwise.data_type(schema)?);
                }
                common_type(&types).context("CASE")?
            }
            Self::Call { function, args } => {
                let types = args
                    .iter()
                    .map(|arg| arg.data_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                let first = types.first().cloned().unwrap_or(DataType::Null);
                match function {
                    Function::Lower | Function::Upper | Function::Trim | Function::Concat => {
                        if let Some(nested) = types.iter().find(|data_type| is_nested(data_type)) {
                            bail!("{function:?} needs text, not {nested}");
                        }
                        DataType::Utf8
                    }
                    Function::Length => {
                        if is_nested(&first) {
                            bail!("LENGTH needs text, not {first}");
                        }
                        DataType::Int64
                    }
                    Function::Substr => {
                        if let Some(bad) = types
                            .get(1..)
                            .unwrap_or_default()
                            .iter()
                            .find(|data_type| !is_integer(data_type))
                        {
                            bail!("SUBSTRING's start and length are whole numbers, not {bad}");
                        }
                        DataType::Utf8
                    }
                    Function::Abs => {
                        if !is_number(&first) {
                            bail!("ABS needs a number, not {first}");
                        }
                        numeric_type(&first, &first)
                    }
                    Function::Round => {
                        if !is_number(&first) {
                            bail!("ROUND needs a number, not {first}");
                        }
                        if let Some(digits) = args.get(1)
                            && !matches!(digits, Expr::Literal(Literal::Int(_)))
                        {
                            bail!("ROUND's second argument is a whole number of digits");
                        }
                        numeric_type(&first, &first)
                    }
                    Function::Coalesce => common_type(&types).context("COALESCE")?,
                    Function::Replace => {
                        if let Some(nested) = types.iter().find(|data_type| is_nested(data_type)) {
                            bail!("REPLACE needs text, not {nested}");
                        }
                        DataType::Utf8
                    }
                    Function::DateTrunc(_) | Function::Shift(_) => {
                        if !crate::dates::is_date_or_time(&first) {
                            bail!(
                                "{} needs a date or timestamp, not {first}",
                                function_name(*function)
                            );
                        }
                        match function {
                            Function::Shift(interval) => {
                                crate::dates::shift_type(&first, *interval)
                            }
                            _ => crate::dates::trunc_type(&first),
                        }
                    }
                    Function::DateDiff(_) => {
                        if let Some(bad) = types
                            .iter()
                            .find(|data_type| !crate::dates::is_date_or_time(data_type))
                        {
                            bail!("DATE_DIFF needs dates or timestamps, not {bad}");
                        }
                        DataType::Int64
                    }
                    // Without a zone, so it compares with the timestamps most
                    // files hold; the wall-clock time is UTC's.
                    Function::Now(_) => {
                        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                    }
                    Function::Today(_) => DataType::Date32,
                    _ => {
                        let time_of_day =
                            matches!(first, DataType::Time32(_) | DataType::Time64(_));
                        let wants_time = matches!(
                            function,
                            Function::Hour | Function::Minute | Function::Second
                        );
                        if !(is_temporal(&first)
                            || (wants_time && time_of_day)
                            || first == DataType::Null)
                        {
                            bail!("{function:?} needs a date or timestamp, not {first}");
                        }
                        DataType::Int32
                    }
                }
            }
        })
    }

    /// The result for every row of `batch`, whose columns are the input's.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let rows = batch.num_rows();
        let schema = batch.schema();
        Ok(match self {
            Self::Column(column) => Arc::clone(
                batch
                    .columns()
                    .get(*column)
                    .ok_or_else(|| anyhow!("calculation column {column} is out of range"))?,
            ),
            Self::Literal(literal) => literal_array(literal, rows)?,
            Self::Neg(expr) => {
                let target = self.data_type(&schema)?;
                let value = to_type(&expr.evaluate(batch)?, &target)?;
                arrow_arith::numeric::neg(value.as_ref()).map_err(overflow)?
            }
            Self::Not(expr) => {
                let value = to_type(&expr.evaluate(batch)?, &DataType::Boolean)?;
                Arc::new(arrow_arith::boolean::not(as_boolean(&value)?)?)
            }
            Self::Binary { op, left, right } => {
                let left_type = left.data_type(&schema)?;
                let right_type = right.data_type(&schema)?;
                let (left, right) = (left.evaluate(batch)?, right.evaluate(batch)?);
                binary(
                    *op,
                    &left,
                    &left_type,
                    &right,
                    &right_type,
                    &self.data_type(&schema)?,
                )?
            }
            Self::IsNull { expr, negated } => {
                let value = expr.evaluate(batch)?;
                Arc::new(if *negated {
                    arrow_arith::boolean::is_not_null(value.as_ref())?
                } else {
                    arrow_arith::boolean::is_null(value.as_ref())?
                })
            }
            Self::Cast { expr, to, safe } => {
                let value = expr.evaluate(batch)?;
                cast_with_options(
                    value.as_ref(),
                    to,
                    &CastOptions {
                        safe: *safe,
                        ..CastOptions::default()
                    },
                )
                .map_err(|error| anyhow!("CAST to {to}: {error}; TRY_CAST gives NULL instead"))?
            }
            Self::Case {
                branches,
                otherwise,
            } => {
                let target = self.data_type(&schema)?;
                let mut result = match otherwise {
                    Some(otherwise) => to_type(&otherwise.evaluate(batch)?, &target)?,
                    None => new_null_array(&target, rows),
                };
                // The first true condition wins, so apply them last to first.
                for (condition, value) in branches.iter().rev() {
                    let condition = to_type(&condition.evaluate(batch)?, &DataType::Boolean)?;
                    // A NULL test is not true. (`prep_null_mask_filter` expects
                    // a mask that has nulls.)
                    let condition = as_boolean(&condition)?;
                    let condition = if condition.null_count() == 0 {
                        condition.clone()
                    } else {
                        arrow_select::filter::prep_null_mask_filter(condition)
                    };
                    let value = to_type(&value.evaluate(batch)?, &target)?;
                    result = arrow_select::zip::zip(&condition, &value, &result)?;
                }
                result
            }
            Self::Call { function, args } => {
                let values = args
                    .iter()
                    .map(|arg| arg.evaluate(batch))
                    .collect::<Result<Vec<_>>>()?;
                call_function(*function, args, &values, &self.data_type(&schema)?, rows)?
            }
        })
    }
}

// ---------------------------------------------------------------- evaluation

fn overflow(error: arrow_schema::ArrowError) -> anyhow::Error {
    match error {
        arrow_schema::ArrowError::ArithmeticOverflow(_) => {
            anyhow!("a calculation overflowed; CAST to DOUBLE to work with larger values")
        }
        other => other.into(),
    }
}

fn as_boolean(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("expected true/false values")
}

fn as_string(array: &ArrayRef) -> Result<StringArray> {
    let text = to_type(array, &DataType::Utf8)?;
    text.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .context("expected text")
}

fn as_int64(array: &ArrayRef) -> Result<Int64Array> {
    let numbers = to_type(array, &DataType::Int64)?;
    numbers
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .context("expected whole numbers")
}

/// `array` as `target`; values that don't fit are an error.
fn to_type(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    cast_with_options(
        array.as_ref(),
        target,
        &CastOptions {
            safe: false,
            ..CastOptions::default()
        },
    )
    .map_err(|error| {
        anyhow!(
            "can't use {} values as {target}: {error}",
            array.data_type()
        )
    })
}

fn literal_array(literal: &Literal, rows: usize) -> Result<ArrayRef> {
    Ok(match literal {
        Literal::Int(value) => Arc::new(Int64Array::from_value(*value, rows)),
        Literal::Float(text) => Arc::new(Float64Array::from_value(
            text.parse::<f64>().context("a number in a calculation")?,
            rows,
        )),
        Literal::Text(text) => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            text.as_str(),
            rows,
        ))),
        Literal::Bool(value) => Arc::new(BooleanArray::from(vec![*value; rows])),
        Literal::Null => new_null_array(&DataType::Null, rows),
    })
}

fn binary(
    op: BinaryOp,
    left: &ArrayRef,
    left_type: &DataType,
    right: &ArrayRef,
    right_type: &DataType,
    result: &DataType,
) -> Result<ArrayRef> {
    use arrow_arith::numeric;
    if op.is_arithmetic() {
        let operands = if op == BinaryOp::Div {
            DataType::Float64
        } else {
            result.clone()
        };
        let left = to_type(left, &operands)?;
        let mut right = to_type(right, &operands)?;
        if matches!(op, BinaryOp::Div | BinaryOp::Rem) {
            // Dividing by zero gives NULL, as in most SQL engines.
            let zero: ArrayRef = match operands {
                DataType::Int64 => Arc::new(Int64Array::from_value(0, right.len())),
                _ => Arc::new(Float64Array::from_value(0.0, right.len())),
            };
            let is_zero = arrow_ord::cmp::eq(&right, &zero)?;
            right = arrow_select::nullif::nullif(right.as_ref(), &is_zero)?;
        }
        return match op {
            BinaryOp::Add => numeric::add(&left, &right),
            BinaryOp::Sub => numeric::sub(&left, &right),
            BinaryOp::Mul => numeric::mul(&left, &right),
            BinaryOp::Div => numeric::div(&left, &right),
            _ => numeric::rem(&left, &right),
        }
        .map_err(overflow);
    }
    if op.is_comparison() {
        let common = comparison_type(left_type, right_type)?;
        let (left, right) = (to_type(left, &common)?, to_type(right, &common)?);
        let mask = match op {
            BinaryOp::Eq => arrow_ord::cmp::eq(&left, &right),
            BinaryOp::Ne => arrow_ord::cmp::neq(&left, &right),
            BinaryOp::Lt => arrow_ord::cmp::lt(&left, &right),
            BinaryOp::Le => arrow_ord::cmp::lt_eq(&left, &right),
            BinaryOp::Gt => arrow_ord::cmp::gt(&left, &right),
            _ => arrow_ord::cmp::gt_eq(&left, &right),
        }?;
        return Ok(Arc::new(mask));
    }
    match op {
        BinaryOp::Concat => {
            let (left, right) = (as_string(left)?, as_string(right)?);
            Ok(Arc::new(
                left.iter()
                    .zip(right.iter())
                    .map(|(left, right)| Some(format!("{}{}", left?, right?)))
                    .collect::<StringArray>(),
            ))
        }
        BinaryOp::And | BinaryOp::Or => {
            let left = to_type(left, &DataType::Boolean)?;
            let right = to_type(right, &DataType::Boolean)?;
            let (left, right) = (as_boolean(&left)?, as_boolean(&right)?);
            Ok(Arc::new(if op == BinaryOp::And {
                arrow_arith::boolean::and_kleene(left, right)?
            } else {
                arrow_arith::boolean::or_kleene(left, right)?
            }))
        }
        _ => bail!("unexpected operator {op:?}"),
    }
}

fn map_text(array: &ArrayRef, map: impl Fn(&str) -> String) -> Result<ArrayRef> {
    Ok(Arc::new(
        as_string(array)?
            .iter()
            .map(|value| value.map(&map))
            .collect::<StringArray>(),
    ))
}

fn call_function(
    function: Function,
    args: &[Expr<usize>],
    values: &[ArrayRef],
    result: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    match function {
        Function::Now(micros) => {
            return Ok(Arc::new(
                arrow_array::TimestampMicrosecondArray::from_value(micros, rows),
            ));
        }
        Function::Today(days) => {
            return Ok(Arc::new(arrow_array::Date32Array::from_value(days, rows)));
        }
        _ => {}
    }
    let first = values
        .first()
        .ok_or_else(|| anyhow!("{function:?} needs an argument"))?;
    Ok(match function {
        Function::Replace => {
            let text = as_string(first)?;
            let from = as_string(values.get(1).context("REPLACE needs what to replace")?)?;
            let to = as_string(values.get(2).context("REPLACE needs a replacement")?)?;
            Arc::new(
                (0..rows)
                    .map(|row| {
                        if text.is_null(row) || from.is_null(row) || to.is_null(row) {
                            return None;
                        }
                        let from = from.value(row);
                        // Replacing nothing changes nothing.
                        Some(if from.is_empty() {
                            text.value(row).to_owned()
                        } else {
                            text.value(row).replace(from, to.value(row))
                        })
                    })
                    .collect::<StringArray>(),
            )
        }
        Function::DateTrunc(unit) => crate::dates::date_trunc(unit, first, result)?,
        Function::DateDiff(unit) => crate::dates::date_diff(
            unit,
            first,
            values.get(1).context("DATE_DIFF needs an end")?,
        )?,
        Function::Shift(interval) => crate::dates::shift(interval, first, result)?,
        Function::Lower => map_text(first, str::to_lowercase)?,
        Function::Upper => map_text(first, str::to_uppercase)?,
        Function::Trim => map_text(first, |text| text.trim().to_owned())?,
        Function::Length => Arc::new(
            as_string(first)?
                .iter()
                .map(|value| value.map(|text| text.chars().count() as i64))
                .collect::<Int64Array>(),
        ),
        Function::Substr => {
            let text = as_string(first)?;
            let start = as_int64(values.get(1).context("SUBSTRING needs a start")?)?;
            let length = values.get(2).map(as_int64).transpose()?;
            Arc::new(
                (0..rows)
                    .map(|row| {
                        if text.is_null(row) || start.is_null(row) {
                            return None;
                        }
                        let length = match &length {
                            Some(length) if length.is_null(row) => return None,
                            Some(length) => Some(length.value(row)),
                            None => None,
                        };
                        Some(substring(text.value(row), start.value(row), length))
                    })
                    .collect::<StringArray>(),
            )
        }
        Function::Concat => {
            // NULL arguments are left out, as most engines' CONCAT does.
            let texts = values.iter().map(as_string).collect::<Result<Vec<_>>>()?;
            Arc::new(
                (0..rows)
                    .map(|row| {
                        Some(
                            texts
                                .iter()
                                .filter(|text| !text.is_null(row))
                                .map(|text| text.value(row))
                                .collect::<String>(),
                        )
                    })
                    .collect::<StringArray>(),
            )
        }
        Function::Abs => {
            let value = to_type(first, result)?;
            match result {
                DataType::Int64 => Arc::new(
                    as_int64(&value)?
                        .iter()
                        .map(|value| {
                            value
                                .map(|value| value.checked_abs().context("ABS overflowed"))
                                .transpose()
                        })
                        .collect::<Result<Int64Array>>()?,
                ),
                _ => {
                    Arc::new(float64(&value)?.unary::<_, arrow_array::types::Float64Type>(f64::abs))
                }
            }
        }
        Function::Round => {
            let digits = match args.get(1) {
                Some(Expr::Literal(Literal::Int(digits))) => *digits,
                _ => 0,
            }
            .clamp(-18, 18) as i32;
            let value = to_type(first, result)?;
            match result {
                DataType::Int64 if digits >= 0 => value,
                DataType::Int64 => {
                    let step = 10_f64.powi(-digits);
                    Arc::new(
                        as_int64(&value)?
                            .iter()
                            .map(|value| {
                                value.map(|value| ((value as f64 / step).round() * step) as i64)
                            })
                            .collect::<Int64Array>(),
                    )
                }
                _ => {
                    let scale = 10_f64.powi(digits);
                    Arc::new(
                        float64(&value)?.unary::<_, arrow_array::types::Float64Type>(|value| {
                            (value * scale).round() / scale
                        }),
                    )
                }
            }
        }
        Function::Coalesce => {
            let mut values = values.iter().rev();
            let mut combined = to_type(
                values
                    .next()
                    .ok_or_else(|| anyhow!("COALESCE needs an argument"))?,
                result,
            )?;
            for value in values {
                let value = to_type(value, result)?;
                let present = arrow_arith::boolean::is_not_null(value.as_ref())?;
                combined = arrow_select::zip::zip(&present, &value, &combined)?;
            }
            combined
        }
        _ => {
            let part = function
                .date_part()
                .ok_or_else(|| anyhow!("{function:?} is not a date part"))?;
            if *first.data_type() == DataType::Null {
                new_null_array(&DataType::Int32, rows)
            } else {
                arrow_arith::temporal::date_part(first.as_ref(), part)?
            }
        }
    })
}

/// A function's SQL name, for messages.
fn function_name(function: Function) -> &'static str {
    match function {
        Function::DateTrunc(_) => "DATE_TRUNC",
        Function::DateDiff(_) => "DATE_DIFF",
        Function::Shift(_) => "± INTERVAL",
        Function::Replace => "REPLACE",
        _ => "this function",
    }
}

fn float64(array: &ArrayRef) -> Result<Float64Array> {
    to_type(array, &DataType::Float64)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .context("expected numbers")
}

/// SQL's `SUBSTRING(text, start, length)`: characters from the 1-based
/// `start`; a start before 1 still counts towards `length`.
fn substring(text: &str, start: i64, length: Option<i64>) -> String {
    let first = start.saturating_sub(1);
    let end = length.map(|length| first.saturating_add(length.max(0)));
    let skip = usize::try_from(first.max(0)).unwrap_or(usize::MAX);
    let take = match end {
        Some(end) => usize::try_from(end.max(0))
            .unwrap_or(usize::MAX)
            .saturating_sub(skip),
        None => usize::MAX,
    };
    text.chars().skip(skip).take(take).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date32Array, Int32Array};
    use arrow_schema::Field;
    use sqlparser::{dialect::GenericDialect, parser::Parser};

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("price", DataType::Float64, true),
                Field::new("qty", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("day", DataType::Date32, true),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![Some(2.5), Some(10.0), None])),
                Arc::new(Int32Array::from(vec![Some(4), Some(0), Some(7)])),
                Arc::new(StringArray::from(vec![
                    Some(" Ünïcode "),
                    Some("abc"),
                    None,
                ])),
                // 2024-02-29, 2023-12-31, null
                Arc::new(Date32Array::from(vec![Some(19_782), Some(19_722), None])),
            ],
        )
        .unwrap()
    }

    /// Evaluates `sql` (a calculation) on [`batch`], as text per row.
    fn eval(sql: &str) -> Result<Vec<Option<String>>> {
        let expression = Parser::new(&GenericDialect {})
            .try_with_sql(sql)?
            .parse_expr()?;
        let batch = batch();
        let schema = batch.schema();
        let resolved = parse(&expression)?.map_columns(&|column: &SqlColumn| {
            schema
                .index_of(&column.name)
                .map_err(|_| anyhow!("no column {}", column.name))
        })?;
        let data_type = resolved.data_type(&schema)?;
        let result = resolved.evaluate(&batch)?;
        assert_eq!(result.data_type(), &data_type, "{sql}");
        (0..result.len())
            .map(|row| parquet_reader::value::scalar_text(result.as_ref(), row))
            .collect()
    }

    fn texts(values: &[Option<&str>]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect()
    }

    #[test]
    fn calculates_arithmetic_with_sql_rules() {
        assert_eq!(
            eval("price * qty").unwrap(),
            texts(&[Some("10"), Some("0"), None])
        );
        assert_eq!(
            eval("qty + 1").unwrap(),
            texts(&[Some("5"), Some("1"), Some("8")])
        );
        // `/` gives a fraction; dividing by zero gives NULL.
        assert_eq!(
            eval("qty / 8").unwrap(),
            texts(&[Some("0.5"), Some("0"), Some("0.875")])
        );
        assert_eq!(
            eval("10 / qty").unwrap(),
            texts(&[Some("2.5"), None, Some("1.4285714285714286")])
        );
        assert_eq!(
            eval("qty % 3").unwrap(),
            texts(&[Some("1"), Some("0"), Some("1")])
        );
        assert_eq!(
            eval("-qty").unwrap(),
            texts(&[Some("-4"), Some("0"), Some("-7")])
        );
        assert!(
            eval("9223372036854775807 + qty")
                .unwrap_err()
                .to_string()
                .contains("overflowed")
        );
        assert!(
            eval("name * 2")
                .unwrap_err()
                .to_string()
                .contains("arithmetic needs numbers")
        );
    }

    #[test]
    fn calculates_text_dates_casts_and_cases() {
        assert_eq!(
            eval("UPPER(TRIM(name))").unwrap(),
            texts(&[Some("ÜNÏCODE"), Some("ABC"), None])
        );
        assert_eq!(
            eval("LENGTH(name)").unwrap(),
            texts(&[Some("9"), Some("3"), None])
        );
        assert_eq!(
            eval("SUBSTRING(name, 2, 3)").unwrap(),
            texts(&[Some("Ünï"), Some("bc"), None])
        );
        assert_eq!(
            eval("SUBSTRING(name FROM 0 FOR 2)").unwrap(),
            texts(&[Some(" "), Some("a"), None])
        );
        assert_eq!(
            eval("name || '!'").unwrap(),
            texts(&[Some(" Ünïcode !"), Some("abc!"), None])
        );
        assert_eq!(
            eval("CONCAT(name, '-', qty)").unwrap(),
            texts(&[Some(" Ünïcode -4"), Some("abc-0"), Some("-7")])
        );
        assert_eq!(
            eval("YEAR(day)").unwrap(),
            texts(&[Some("2024"), Some("2023"), None])
        );
        assert_eq!(
            eval("EXTRACT(MONTH FROM day)").unwrap(),
            texts(&[Some("2"), Some("12"), None])
        );
        assert_eq!(
            eval("DAYOFWEEK(day)").unwrap(),
            texts(&[Some("4"), Some("7"), None])
        );
        assert_eq!(
            eval("CAST(qty AS VARCHAR)").unwrap(),
            texts(&[Some("4"), Some("0"), Some("7")])
        );
        assert_eq!(
            eval("CAST(price AS DECIMAL(10, 2))").unwrap(),
            texts(&[Some("2.50"), Some("10.00"), None])
        );
        assert!(
            eval("CAST(name AS INT)")
                .unwrap_err()
                .to_string()
                .contains("TRY_CAST")
        );
        assert_eq!(
            eval("TRY_CAST(name AS INT)").unwrap(),
            texts(&[None, None, None])
        );
        assert_eq!(
            eval("CASE WHEN price > 5 THEN 'high' WHEN price IS NULL THEN 'none' ELSE 'low' END")
                .unwrap(),
            texts(&[Some("low"), Some("high"), Some("none")])
        );
        assert_eq!(
            eval("CASE qty WHEN 0 THEN 'zero' END").unwrap(),
            texts(&[None, Some("zero"), None])
        );
        assert_eq!(
            eval("COALESCE(price, qty, 0)").unwrap(),
            texts(&[Some("2.5"), Some("10"), Some("7")])
        );
        assert_eq!(
            eval("ROUND(price / 3, 2)").unwrap(),
            texts(&[Some("0.83"), Some("3.33"), None])
        );
        assert_eq!(
            eval("ABS(qty - 5)").unwrap(),
            texts(&[Some("1"), Some("5"), Some("2")])
        );
        assert_eq!(
            eval("day > '2024-01-01' AND NOT qty = 0").unwrap(),
            texts(&[Some("true"), Some("false"), None])
        );
    }

    #[test]
    fn refuses_what_it_cannot_calculate() {
        for (sql, message) in [
            ("SUM(price)", "is an aggregate"),
            ("FOO(price)", "unknown function FOO"),
            ("LOWER(name, name)", "LOWER takes 1 arguments, not 2"),
            (
                "CAST(price AS INTERVAL)",
                "CAST to INTERVAL is not supported",
            ),
            ("YEAR(name)", "needs a date or timestamp"),
            ("price > name", "can't use"),
            ("ROUND(price, qty)", "whole number of digits"),
            ("CASE WHEN qty THEN 1 END", "needs a true/false test"),
            ("price -> 1", "can't be used in a calculation"),
        ] {
            let error = eval(sql).unwrap_err();
            assert!(format!("{error:#}").contains(message), "{sql}: {error:#}");
        }
        let long = (0..MAX_EXPR_NODES)
            .map(|_| "qty")
            .collect::<Vec<_>>()
            .join(" + ");
        assert!(
            eval(&long)
                .unwrap_err()
                .to_string()
                .contains("more than 256 parts")
        );
    }
}
