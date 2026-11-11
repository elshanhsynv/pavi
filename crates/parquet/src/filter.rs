use std::cmp::Ordering;

use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
    timezone::Tz,
    types::{Date32Type, Date64Type},
};
use arrow_cast::parse::{Parser, string_to_datetime};
use arrow_schema::{DataType, Schema, TimeUnit};
use parquet::file::statistics::Statistics;

use crate::types::{is_comparable, is_numeric, is_string};

/// Most comparisons one filter may hold, counting every value of an `IN` list.
pub const MAX_FILTER_TERMS: usize = 64;
/// Deepest nesting of parentheses, `AND` and `OR` a filter may have.
pub const MAX_FILTER_DEPTH: usize = 16;
/// Longest `LIKE` pattern, in characters.
pub const MAX_LIKE_PATTERN_CHARS: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Contains,
    Gt,
    Ge,
    Lt,
    Le,
    /// SQL `LIKE`: `%` matches any run of characters, `_` exactly one.
    Like,
    NotLike,
    /// The value is ignored.
    IsNull,
    /// The value is ignored.
    IsNotNull,
}

/// One `column OP value` test. A null cell passes only `IS NULL`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Comparison {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

/// A row filter: comparisons joined by `AND` and `OR`.
///
/// `IN`, `BETWEEN` and their `NOT` forms are written as the comparisons they stand
/// for (`a IN (1, 2)` is `a == 1 OR a == 2`), so a null cell never passes them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilterExpr {
    Compare(Comparison),
    /// Every part must pass.
    And(Vec<FilterExpr>),
    /// At least one part must pass.
    Or(Vec<FilterExpr>),
}

impl FilterExpr {
    /// Parses the filter-box grammar:
    ///
    /// ```text
    /// filter  := part (OR part)*            AND binds tighter than OR
    /// part    := test (AND test)*
    /// test    := ( filter )
    ///          | column OP value            OP: == = != <> > >= < <= contains
    ///          | column [NOT] IN (value, ...)
    ///          | column [NOT] BETWEEN value AND value
    ///          | column [NOT] LIKE pattern  % any run of characters, _ one character
    ///          | column IS [NOT] NULL
    /// ```
    ///
    /// Keywords are case-insensitive. Quote a value (`"..."` or `'...'`) when it
    /// contains a keyword, a comma or parentheses.
    pub fn parse(input: &str) -> Result<Self> {
        let filter = parse_filter(input, 0)?;
        filter.check_size()?;
        Ok(filter)
    }

    /// The single test `column OP value`.
    pub fn compare(column: impl Into<String>, op: FilterOp, value: impl Into<String>) -> Self {
        Self::Compare(Comparison {
            column: column.into(),
            op,
            value: value.into(),
        })
    }

    /// Both filters must pass. Flattens nested `AND`s.
    pub fn and(self, other: Self) -> Self {
        let mut parts = match self {
            Self::And(parts) => parts,
            filter => vec![filter],
        };
        match other {
            Self::And(more) => parts.extend(more),
            filter => parts.push(filter),
        }
        Self::And(parts)
    }

    fn comparisons(&self) -> usize {
        match self {
            Self::Compare(_) => 1,
            Self::And(parts) | Self::Or(parts) => parts.iter().map(Self::comparisons).sum(),
        }
    }

    fn depth(&self) -> usize {
        match self {
            Self::Compare(_) => 0,
            Self::And(parts) | Self::Or(parts) => {
                1 + parts.iter().map(Self::depth).max().unwrap_or_default()
            }
        }
    }

    /// Rejects filters over [`MAX_FILTER_TERMS`] or [`MAX_FILTER_DEPTH`].
    fn check_size(&self) -> Result<()> {
        let comparisons = self.comparisons();
        if comparisons > MAX_FILTER_TERMS {
            bail!(
                "filter has {comparisons} comparisons; the limit is {MAX_FILTER_TERMS} \
                 (each IN value counts as one)"
            );
        }
        if self.depth() > MAX_FILTER_DEPTH {
            bail!("filter is nested more than {MAX_FILTER_DEPTH} levels deep");
        }
        Ok(())
    }

    fn for_each_comparison<'a>(
        &'a self,
        visit: &mut dyn FnMut(&'a Comparison) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Compare(comparison) => visit(comparison),
            Self::And(parts) | Self::Or(parts) => parts
                .iter()
                .try_for_each(|part| part.for_each_comparison(visit)),
        }
    }

    /// The source columns this filter reads, in ascending order, each once.
    pub fn columns(&self, schema: &Schema) -> Result<Vec<usize>> {
        let mut columns = Vec::new();
        self.for_each_comparison(&mut |comparison| {
            let column = comparison.column_index(schema)?;
            if !columns.contains(&column) {
                columns.push(column);
            }
            Ok(())
        })?;
        columns.sort_unstable();
        Ok(columns)
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<()> {
        self.check_size()?;
        self.for_each_comparison(&mut |comparison| comparison.validate_schema(schema))
    }

    /// Evaluates the filter on `batch`, whose column for source column `c` is at
    /// `position_of(c)`. `schema` is the source schema the filter was written for.
    pub fn evaluate(
        &self,
        batch: &RecordBatch,
        schema: &Schema,
        position_of: &dyn Fn(usize) -> Result<usize>,
    ) -> Result<BooleanArray> {
        match self {
            Self::Compare(comparison) => {
                let position = position_of(comparison.column_index(schema)?)?;
                comparison.evaluate_batch(batch, position)
            }
            Self::And(parts) | Self::Or(parts) => {
                let all = matches!(self, Self::And(_));
                let mut masks = parts
                    .iter()
                    .map(|part| part.evaluate(batch, schema, position_of));
                let mut combined = masks
                    .next()
                    .ok_or_else(|| anyhow!("filter has an empty AND/OR group"))??;
                for mask in masks {
                    let mask = mask?;
                    // Comparison masks never hold nulls, so the value bits are the answer.
                    let values = if all {
                        combined.values() & mask.values()
                    } else {
                        combined.values() | mask.values()
                    };
                    combined = BooleanArray::new(values, None);
                }
                Ok(combined)
            }
        }
    }

    /// Evaluates the filter on a batch that has the filter's own schema.
    pub fn evaluate_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        self.evaluate(batch, batch.schema_ref(), &Ok)
    }

    /// Returns `false` only when statistics prove that no row of a row group can
    /// pass. `column_statistics(c)` gives the statistics and Arrow type of source
    /// column `c`, or `None` when they are not available.
    pub fn might_match<'a>(
        &self,
        schema: &Schema,
        column_statistics: &dyn Fn(usize) -> Option<(Option<&'a Statistics>, &'a DataType)>,
    ) -> bool {
        match self {
            Self::Compare(comparison) => comparison
                .column_index(schema)
                .ok()
                .and_then(column_statistics)
                .is_none_or(|(statistics, data_type)| {
                    comparison.might_match_statistics(statistics, data_type)
                }),
            Self::And(parts) => parts
                .iter()
                .all(|part| part.might_match(schema, column_statistics)),
            Self::Or(parts) => parts
                .iter()
                .any(|part| part.might_match(schema, column_statistics)),
        }
    }
}

/// Writes the filter in the filter-box grammar. Text made here parses back to the
/// same filter unless a value holds both kinds of quote.
impl std::fmt::Display for FilterExpr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compare(comparison) => comparison.fmt(formatter),
            Self::And(parts) | Self::Or(parts) => {
                let all = matches!(self, Self::And(_));
                for (index, part) in parts.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(if all { " AND " } else { " OR " })?;
                    }
                    // AND binds tighter, so only an OR inside an AND needs brackets.
                    if all && matches!(part, Self::Or(_)) {
                        write!(formatter, "({part})")?;
                    } else {
                        part.fmt(formatter)?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl std::fmt::Display for Comparison {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let symbol = match self.op {
            FilterOp::IsNull => return write!(formatter, "{} IS NULL", self.column),
            FilterOp::IsNotNull => return write!(formatter, "{} IS NOT NULL", self.column),
            FilterOp::Eq => "==",
            FilterOp::Ne => "!=",
            FilterOp::Gt => ">",
            FilterOp::Ge => ">=",
            FilterOp::Lt => "<",
            FilterOp::Le => "<=",
            FilterOp::Contains => "contains",
            FilterOp::Like => "LIKE",
            FilterOp::NotLike => "NOT LIKE",
        };
        let keyword = [
            "and", "or", "not", "in", "between", "like", "is", "null", "contains",
        ]
        .iter()
        .any(|keyword| self.value.eq_ignore_ascii_case(keyword));
        let plain = !self.value.is_empty()
            && !keyword
            && self
                .value
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '.' | ':' | '+' | '-'));
        if plain {
            write!(formatter, "{} {symbol} {}", self.column, self.value)
        } else if self.value.contains('"') {
            write!(formatter, "{} {symbol} '{}'", self.column, self.value)
        } else {
            write!(formatter, "{} {symbol} \"{}\"", self.column, self.value)
        }
    }
}

/// For each byte of `input`: `None` inside a quoted value, otherwise how many
/// parentheses are open before it. A quote with no partner is an ordinary character.
fn nesting(input: &str) -> Result<Vec<Option<usize>>> {
    let bytes = input.as_bytes();
    let mut levels = Vec::with_capacity(bytes.len());
    let mut depth = 0_usize;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if matches!(byte, b'"' | b'\'')
            && let Some(length) = bytes[index + 1..].iter().position(|next| *next == byte)
        {
            levels.resize(index + length + 2, None);
            index += length + 2;
            continue;
        }
        if byte == b')' {
            depth = depth.checked_sub(1).ok_or_else(|| {
                anyhow!("filter has a ')' without a '('; quote values that contain brackets")
            })?;
        }
        levels.push(Some(depth));
        if byte == b'(' {
            depth += 1;
        }
        index += 1;
    }
    if depth > 0 {
        bail!("filter has a '(' without a ')'; quote values that contain brackets");
    }
    Ok(levels)
}

/// Finds `words` (lower-case, separated by any whitespace) at bracket level 0 and
/// outside quotes, at or after `from`. The match must be a whole word run: preceded
/// by whitespace and followed by whitespace, `(` or the end. Returns its byte range.
fn find_keyword(
    input: &str,
    levels: &[Option<usize>],
    words: &[&str],
    from: usize,
) -> Option<(usize, usize)> {
    let bytes = input.as_bytes();
    'starts: for start in from..bytes.len() {
        if levels[start] != Some(0) || start == 0 || !bytes[start - 1].is_ascii_whitespace() {
            continue;
        }
        let mut at = start;
        for (index, word) in words.iter().enumerate() {
            if index > 0 {
                let gap = bytes[at..]
                    .iter()
                    .take_while(|byte| byte.is_ascii_whitespace())
                    .count();
                if gap == 0 {
                    continue 'starts;
                }
                at += gap;
            }
            let end = at + word.len();
            if end > bytes.len()
                || !bytes[at..end].eq_ignore_ascii_case(word.as_bytes())
                || levels[at..end].iter().any(|level| *level != Some(0))
            {
                continue 'starts;
            }
            at = end;
        }
        if at == bytes.len() || bytes[at].is_ascii_whitespace() || bytes[at] == b'(' {
            return Some((start, at));
        }
    }
    None
}

/// Splits `input` at every top-level `keyword`, returning the byte ranges between.
fn split_keyword(input: &str, levels: &[Option<usize>], keyword: &str) -> Vec<(usize, usize)> {
    let mut parts = Vec::new();
    let mut start = 0;
    while let Some((from, to)) = find_keyword(input, levels, &[keyword], start) {
        parts.push((start, from));
        start = to;
    }
    parts.push((start, input.len()));
    parts
}

/// Parses one side of an `AND` or `OR`.
fn parse_part(part: &str, keyword: &str, depth: usize) -> Result<FilterExpr> {
    if part.trim().is_empty() {
        bail!("{keyword} needs a test on both sides; quote a value that contains {keyword}");
    }
    parse_filter(part, depth + 1)
}

fn parse_filter(input: &str, depth: usize) -> Result<FilterExpr> {
    let input = input.trim();
    if input.is_empty() {
        bail!("filter expression is empty");
    }
    if depth > MAX_FILTER_DEPTH {
        bail!("filter is nested more than {MAX_FILTER_DEPTH} levels deep");
    }
    let levels = nesting(input)?;

    let alternatives = split_keyword(input, &levels, "or");
    if alternatives.len() > 1 {
        return alternatives
            .into_iter()
            .map(|(start, end)| parse_part(&input[start..end], "OR", depth))
            .collect::<Result<Vec<_>>>()
            .map(FilterExpr::Or);
    }

    // `a BETWEEN 1 AND 2` holds an AND of its own: give it back to the BETWEEN.
    let mut tests: Vec<(usize, usize)> = Vec::new();
    let mut open_between = false;
    for (start, end) in split_keyword(input, &levels, "and") {
        match tests.last_mut() {
            Some(last) if open_between => last.1 = end,
            _ => tests.push((start, end)),
        }
        open_between = !open_between
            && find_keyword(&input[..end], &levels[..end], &["between"], start).is_some();
    }
    if tests.len() > 1 {
        return tests
            .into_iter()
            .map(|(start, end)| parse_part(&input[start..end], "AND", depth))
            .collect::<Result<Vec<_>>>()
            .map(FilterExpr::And);
    }

    // One bracketed group: its closing bracket is the last character.
    if input.starts_with('(')
        && input.ends_with(')')
        && levels[1..input.len() - 1]
            .iter()
            .all(|level| *level != Some(0))
    {
        return parse_filter(&input[1..input.len() - 1], depth + 1);
    }
    parse_test(input, &levels)
}

/// The symbolic operators, in the order they are looked for. Longer ones come
/// first so `>=` is not read as `>`, and `==` is found before a later `<`.
const SYMBOLS: [(&str, FilterOp); 8] = [
    ("==", FilterOp::Eq),
    ("!=", FilterOp::Ne),
    (">=", FilterOp::Ge),
    ("<=", FilterOp::Le),
    ("<>", FilterOp::Ne),
    (">", FilterOp::Gt),
    ("<", FilterOp::Lt),
    ("=", FilterOp::Eq),
];

/// Parses one test: no top-level AND/OR and no enclosing brackets.
fn parse_test(input: &str, levels: &[Option<usize>]) -> Result<FilterExpr> {
    let unquoted = |at: usize| levels[at].is_some();
    let symbol = SYMBOLS.iter().find_map(|(symbol, op)| {
        input
            .match_indices(symbol)
            .find(|(at, _)| unquoted(*at))
            .map(|(at, _)| (at, at + symbol.len(), op.clone()))
    });
    // A keyword only counts before the first symbol: in `note == not in use` the
    // words after `==` are the value.
    let limit = symbol.as_ref().map_or(input.len(), |(at, _, _)| *at);
    let keyword =
        |words: &[&str]| find_keyword(input, levels, words, 0).filter(|(start, _)| *start < limit);
    let column = |end: usize| -> Result<String> {
        let column = input[..end].trim();
        if column.is_empty() {
            bail!("filter is missing a column name: {input}");
        }
        Ok(column.to_string())
    };

    for (words, op) in [
        (&["is", "not", "null"][..], FilterOp::IsNotNull),
        (&["is", "null"][..], FilterOp::IsNull),
    ] {
        if let Some((start, end)) = keyword(words) {
            if !input[end..].trim().is_empty() {
                bail!("nothing may follow IS NULL: {input}");
            }
            return Ok(FilterExpr::compare(column(start)?, op, ""));
        }
    }
    for (words, negated) in [(&["not", "in"][..], true), (&["in"][..], false)] {
        if let Some((start, end)) = keyword(words) {
            let list = input[end..].trim();
            let Some(list) = list
                .strip_prefix('(')
                .and_then(|list| list.strip_suffix(')'))
            else {
                bail!("IN needs a bracketed list, e.g. status IN (open, held)");
            };
            let list_levels = nesting(list)?;
            let mut values = Vec::new();
            let mut from = 0;
            for (at, _) in list.match_indices(',').chain([(list.len(), "")]) {
                if at < list.len() && list_levels[at] != Some(0) {
                    continue;
                }
                values.push(unquote(list[from..at].trim()));
                from = at + 1;
            }
            if values.iter().any(String::is_empty) {
                bail!("IN list has an empty value; write empty text as \"\"");
            }
            if values.len() > MAX_FILTER_TERMS {
                bail!(
                    "IN list has {} values; the limit is {MAX_FILTER_TERMS}",
                    values.len()
                );
            }
            let column = column(start)?;
            let op = if negated { FilterOp::Ne } else { FilterOp::Eq };
            let mut tests = values
                .into_iter()
                .map(|value| FilterExpr::compare(column.clone(), op.clone(), value))
                .collect::<Vec<_>>();
            return Ok(match (tests.len(), negated) {
                (1, _) => tests.remove(0),
                (_, true) => FilterExpr::And(tests),
                (_, false) => FilterExpr::Or(tests),
            });
        }
    }
    for (words, negated) in [(&["not", "between"][..], true), (&["between"][..], false)] {
        if let Some((start, end)) = keyword(words) {
            let Some((and_start, and_end)) = find_keyword(input, levels, &["and"], end) else {
                bail!("BETWEEN needs two values, e.g. amount BETWEEN 10 AND 20");
            };
            let column = column(start)?;
            let low = unquote(input[end..and_start].trim());
            let high = unquote(input[and_end..].trim());
            return Ok(if negated {
                FilterExpr::Or(vec![
                    FilterExpr::compare(column.clone(), FilterOp::Lt, low),
                    FilterExpr::compare(column, FilterOp::Gt, high),
                ])
            } else {
                FilterExpr::And(vec![
                    FilterExpr::compare(column.clone(), FilterOp::Ge, low),
                    FilterExpr::compare(column, FilterOp::Le, high),
                ])
            });
        }
    }
    for (words, op) in [
        (&["not", "like"][..], FilterOp::NotLike),
        (&["like"][..], FilterOp::Like),
        (&["contains"][..], FilterOp::Contains),
    ] {
        if let Some((start, end)) = keyword(words) {
            return Ok(FilterExpr::compare(
                column(start)?,
                op,
                unquote(input[end..].trim()),
            ));
        }
    }
    if let Some((start, end, op)) = symbol {
        return Ok(FilterExpr::compare(
            column(start)?,
            op,
            unquote(input[end..].trim()),
        ));
    }
    bail!("unsupported filter expression: {input}");
}

impl Comparison {
    /// Parses one test with a symbolic operator, `contains`, `LIKE` or `IS NULL`.
    pub fn parse(input: &str) -> Result<Self> {
        match FilterExpr::parse(input)? {
            FilterExpr::Compare(comparison) => Ok(comparison),
            _ => bail!("expected one comparison: {input}"),
        }
    }

    pub fn column_index(&self, schema: &Schema) -> Result<usize> {
        if let Ok(index) = self.column.parse::<usize>() {
            if index < schema.fields().len() {
                return Ok(index);
            }
            bail!("filter column index {index} out of range");
        }

        schema
            .fields()
            .iter()
            .position(|field| field.name() == &self.column)
            .ok_or_else(|| anyhow!("unknown filter column '{}'", self.column))
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<()> {
        let column = self.column_index(schema)?;
        self.validate_type(schema.field(column).data_type())
    }

    /// Checks that this operator and value can be used on a column of `data_type`.
    ///
    /// Filters accept the comparable types (`types::is_comparable`): booleans with
    /// `==`/`!=`, numbers, dates and timestamps with any comparison, and text with any
    /// comparison, `contains` or `LIKE`. `IS NULL` and `IS NOT NULL` work on every type.
    pub fn validate_type(&self, data_type: &DataType) -> Result<()> {
        if matches!(self.op, FilterOp::IsNull | FilterOp::IsNotNull) {
            return Ok(());
        }
        if !is_comparable(data_type) {
            bail!("filtering is not supported for {data_type:?} columns");
        }
        if self.op == FilterOp::Contains && !is_string(data_type) {
            bail!("contains is only supported for text columns");
        }
        if matches!(self.op, FilterOp::Like | FilterOp::NotLike) {
            if !is_string(data_type) {
                bail!("LIKE is only supported for text columns");
            }
            let length = self.value.chars().count();
            if length > MAX_LIKE_PATTERN_CHARS {
                bail!(
                    "LIKE pattern has {length} characters; the limit is {MAX_LIKE_PATTERN_CHARS}"
                );
            }
        }
        if *data_type == DataType::Boolean {
            if !matches!(self.op, FilterOp::Eq | FilterOp::Ne) {
                bail!("boolean columns only support == and !=");
            }
            self.value
                .parse::<bool>()
                .map_err(|_| anyhow!("'{}' is not a boolean", self.value))?;
        } else if !is_string(data_type) {
            self.ordered_value(data_type)?;
        }
        Ok(())
    }

    /// The comparison value for number, date and timestamp columns, in the column's
    /// own units: days for `Date32`, milliseconds for `Date64`, the column's unit
    /// for timestamps. A timestamp without an offset is read in the column's time
    /// zone (naive columns: as written).
    fn ordered_value(&self, data_type: &DataType) -> Result<Number> {
        let value = self.value.as_str();
        let parsed = match data_type {
            DataType::Date32 => Date32Type::parse(value).map(|days| Number::Int(days.into())),
            DataType::Date64 => Date64Type::parse(value).map(|millis| Number::Int(millis.into())),
            DataType::Timestamp(unit, timezone) => {
                parse_timestamp(value, *unit, timezone.as_deref())
                    .map(|ticks| Number::Int(ticks.into()))
            }
            // A Float32 column holds the nearest f32 to what was written; read the
            // filter value the same way, so `x == 9.083333` matches the cell that
            // displays as 9.083333.
            DataType::Float32 => Number::parse(value).map(|number| match number {
                Number::Float(float) => Number::Float(f64::from(float as f32)),
                int @ Number::Int(_) => int,
            }),
            data_type if is_numeric(data_type) => Number::parse(value),
            _ => None,
        };
        parsed.ok_or_else(|| match data_type {
            DataType::Date32 | DataType::Date64 => {
                anyhow!("'{value}' is not a date (expected YYYY-MM-DD)")
            }
            DataType::Timestamp(_, _) => {
                anyhow!("'{value}' is not a timestamp (expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
            }
            _ => anyhow!("'{value}' is not a number"),
        })
    }

    pub fn evaluate_batch(
        &self,
        batch: &RecordBatch,
        column_position: usize,
    ) -> Result<BooleanArray> {
        let array = batch.column(column_position).as_ref();
        let data_type = array.data_type();
        self.validate_type(data_type)?;
        if matches!(self.op, FilterOp::IsNull | FilterOp::IsNotNull) {
            let wanted = self.op == FilterOp::IsNull;
            return Ok(BooleanArray::from_iter(
                (0..array.len()).map(|row| Some(array.is_null(row) == wanted)),
            ));
        }
        let mask = match data_type {
            DataType::Boolean => self.evaluate_bool(array)?,
            DataType::Utf8 => self.evaluate_strings(array, |a: &StringArray, row| a.value(row))?,
            DataType::LargeUtf8 => {
                self.evaluate_strings(array, |a: &LargeStringArray, row| a.value(row))?
            }
            DataType::Utf8View => {
                self.evaluate_strings(array, |a: &StringViewArray, row| a.value(row))?
            }
            _ => {
                let expected = self.ordered_value(data_type)?;
                macro_rules! ordered {
                    ($array:ty, $number:path) => {
                        self.evaluate_ordered(array, expected, |a: &$array, row| {
                            $number(a.value(row).into())
                        })?
                    };
                }
                match data_type {
                    DataType::Int8 => ordered!(Int8Array, Number::Int),
                    DataType::Int16 => ordered!(Int16Array, Number::Int),
                    DataType::Int32 => ordered!(Int32Array, Number::Int),
                    DataType::Int64 => ordered!(Int64Array, Number::Int),
                    DataType::UInt8 => ordered!(UInt8Array, Number::Int),
                    DataType::UInt16 => ordered!(UInt16Array, Number::Int),
                    DataType::UInt32 => ordered!(UInt32Array, Number::Int),
                    DataType::UInt64 => ordered!(UInt64Array, Number::Int),
                    DataType::Float32 => ordered!(Float32Array, Number::Float),
                    DataType::Float64 => ordered!(Float64Array, Number::Float),
                    DataType::Date32 => ordered!(Date32Array, Number::Int),
                    DataType::Date64 => ordered!(Date64Array, Number::Int),
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        ordered!(TimestampSecondArray, Number::Int)
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        ordered!(TimestampMillisecondArray, Number::Int)
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        ordered!(TimestampMicrosecondArray, Number::Int)
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        ordered!(TimestampNanosecondArray, Number::Int)
                    }
                    other => bail!("filtering is not supported for {other:?} columns"),
                }
            }
        };

        Ok(mask)
    }

    /// Returns `false` only when row-group statistics prove that no row can match.
    ///
    /// `data_type` is the Arrow type of the filtered column. Parquet stores unsigned
    /// integers, dates and timestamps in signed integer physical types, so statistics
    /// are read in the same units as [`Self::ordered_value`].
    pub fn might_match_statistics(
        &self,
        statistics: Option<&Statistics>,
        data_type: &DataType,
    ) -> bool {
        let Some(statistics) = statistics else {
            return true;
        };
        // Patterns and null tests are not pruned: a stored null count of zero can
        // also mean "not recorded", so it proves nothing.
        if matches!(
            self.op,
            FilterOp::Like | FilterOp::NotLike | FilterOp::IsNull | FilterOp::IsNotNull
        ) {
            return true;
        }
        let expected = || self.ordered_value(data_type).ok();

        match statistics {
            Statistics::Boolean(stats) => {
                let Ok(value) = self.value.parse::<bool>() else {
                    return true;
                };
                bool_range_might_match(
                    stats.min_opt().copied(),
                    stats.max_opt().copied(),
                    value,
                    &self.op,
                )
            }
            Statistics::Int32(stats) => number_range_might_match(
                stats
                    .min_opt()
                    .map(|value| int32_statistic(*value, data_type)),
                stats
                    .max_opt()
                    .map(|value| int32_statistic(*value, data_type)),
                expected(),
                &self.op,
            ),
            Statistics::Int64(stats) => number_range_might_match(
                stats
                    .min_opt()
                    .map(|value| int64_statistic(*value, data_type)),
                stats
                    .max_opt()
                    .map(|value| int64_statistic(*value, data_type)),
                expected(),
                &self.op,
            ),
            Statistics::Float(stats) => number_range_might_match(
                stats.min_opt().map(|value| Number::Float((*value).into())),
                stats.max_opt().map(|value| Number::Float((*value).into())),
                expected(),
                &self.op,
            ),
            Statistics::Double(stats) => number_range_might_match(
                stats.min_opt().map(|value| Number::Float(*value)),
                stats.max_opt().map(|value| Number::Float(*value)),
                expected(),
                &self.op,
            ),
            Statistics::ByteArray(stats) => {
                let min = stats
                    .min_bytes_opt()
                    .and_then(|value| std::str::from_utf8(value).ok());
                let max = stats
                    .max_bytes_opt()
                    .and_then(|value| std::str::from_utf8(value).ok());
                string_range_might_match(min, max, &self.value, &self.op)
            }
            _ => true,
        }
    }

    fn evaluate_bool(&self, array: &dyn Array) -> Result<BooleanArray> {
        let expected = self
            .value
            .parse::<bool>()
            .map_err(|_| anyhow!("'{}' is not a boolean", self.value))?;
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow!("boolean array type mismatch"))?;

        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_bool(array.value(row), expected, &self.op))
        })))
    }

    fn evaluate_ordered<T>(
        &self,
        array: &dyn Array,
        expected: Number,
        value_at: impl Fn(&T, usize) -> Number,
    ) -> Result<BooleanArray>
    where
        T: Array + 'static,
    {
        let array = array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| anyhow!("{:?} array type mismatch", array.data_type()))?;

        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_number(value_at(array, row), expected, &self.op))
        })))
    }

    fn evaluate_strings<T>(
        &self,
        array: &dyn Array,
        value_at: impl Fn(&T, usize) -> &str,
    ) -> Result<BooleanArray>
    where
        T: Array + 'static,
    {
        let array = array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| anyhow!("string array type mismatch"))?;

        if matches!(self.op, FilterOp::Like | FilterOp::NotLike) {
            let pattern = LikePattern::new(&self.value);
            let wanted = self.op == FilterOp::Like;
            return Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
                Some(!array.is_null(row) && pattern.matches(value_at(array, row)) == wanted)
            })));
        }
        Ok(BooleanArray::from_iter((0..array.len()).map(|row| {
            Some(!array.is_null(row) && compare_str(value_at(array, row), &self.value, &self.op))
        })))
    }
}

fn unquote(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn compare_bool(left: bool, right: bool, op: &FilterOp) -> bool {
    match op {
        FilterOp::Eq => left == right,
        FilterOp::Ne => left != right,
        _ => false,
    }
}

/// A filter operand: integers stay exact, everything else is compared as `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Number {
    Int(i128),
    Float(f64),
}

impl Number {
    fn parse(value: &str) -> Option<Self> {
        value
            .parse::<i128>()
            .map(Self::Int)
            .ok()
            .or_else(|| value.parse::<f64>().ok().map(Self::Float))
    }

    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    /// `None` when either side is NaN.
    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Some(left.cmp(&right)),
            _ => self.as_f64().partial_cmp(&other.as_f64()),
        }
    }
}

/// Parses a timestamp literal into ticks of `unit`. Text without an offset is read
/// in `timezone`; naive columns (no time zone) store wall-clock time as if UTC.
fn parse_timestamp(value: &str, unit: TimeUnit, timezone: Option<&str>) -> Option<i64> {
    let timezone: Tz = timezone.unwrap_or("+00:00").parse().ok()?;
    let datetime = string_to_datetime(&timezone, value).ok()?;
    match unit {
        TimeUnit::Second => Some(datetime.timestamp()),
        TimeUnit::Millisecond => Some(datetime.timestamp_millis()),
        TimeUnit::Microsecond => Some(datetime.timestamp_micros()),
        TimeUnit::Nanosecond => datetime.timestamp_nanos_opt(),
    }
}

fn int32_statistic(value: i32, data_type: &DataType) -> Number {
    match data_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Number::Int((value as u32).into()),
        _ => Number::Int(value.into()),
    }
}

fn int64_statistic(value: i64, data_type: &DataType) -> Number {
    match data_type {
        DataType::UInt64 => Number::Int((value as u64).into()),
        _ => Number::Int(value.into()),
    }
}

fn compare_number(left: Number, right: Number, op: &FilterOp) -> bool {
    let ordering = left.compare(right);
    match op {
        FilterOp::Eq => ordering == Some(Ordering::Equal),
        FilterOp::Ne => ordering != Some(Ordering::Equal),
        FilterOp::Gt => ordering == Some(Ordering::Greater),
        FilterOp::Ge => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
        FilterOp::Lt => ordering == Some(Ordering::Less),
        FilterOp::Le => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        FilterOp::Contains
        | FilterOp::Like
        | FilterOp::NotLike
        | FilterOp::IsNull
        | FilterOp::IsNotNull => false,
    }
}

fn compare_str(left: &str, right: &str, op: &FilterOp) -> bool {
    match op {
        FilterOp::Eq => left == right,
        FilterOp::Ne => left != right,
        FilterOp::Contains => left.contains(right),
        FilterOp::Gt => left > right,
        FilterOp::Ge => left >= right,
        FilterOp::Lt => left < right,
        FilterOp::Le => left <= right,
        // Handled before this is reached (`evaluate_strings`, `evaluate_batch`).
        FilterOp::Like | FilterOp::NotLike | FilterOp::IsNull | FilterOp::IsNotNull => false,
    }
}

/// A `LIKE` pattern split at its `%`s. Each piece is literal characters and `_`
/// (`None`), which stands for exactly one character.
struct LikePattern {
    pieces: Vec<Vec<Option<char>>>,
}

impl LikePattern {
    fn new(pattern: &str) -> Self {
        Self {
            pieces: pattern
                .split('%')
                .map(|piece| piece.chars().map(|c| (c != '_').then_some(c)).collect())
                .collect(),
        }
    }

    /// Bytes of `text` that `piece` matches at its start, if it does.
    fn piece_at(text: &str, piece: &[Option<char>]) -> Option<usize> {
        let mut chars = text.chars();
        let mut length = 0;
        for wanted in piece {
            let found = chars.next()?;
            if wanted.is_some_and(|wanted| wanted != found) {
                return None;
            }
            length += found.len_utf8();
        }
        Some(length)
    }

    fn matches(&self, text: &str) -> bool {
        let [first, middle @ .., last] = self.pieces.as_slice() else {
            // No `%`: the one piece must cover the whole text.
            return self
                .pieces
                .first()
                .is_some_and(|piece| Self::piece_at(text, piece) == Some(text.len()));
        };
        let Some(mut start) = Self::piece_at(text, first) else {
            return false;
        };
        // The last piece is tied to the end: it covers the final `last.len()` characters.
        let end = if last.is_empty() {
            text.len()
        } else {
            let Some((end, _)) = text.char_indices().rev().nth(last.len() - 1) else {
                return false;
            };
            if end < start || Self::piece_at(&text[end..], last).is_none() {
                return false;
            }
            end
        };
        // Each middle piece takes its leftmost match, which leaves the most room after it.
        for piece in middle {
            let found = text[start..end]
                .char_indices()
                .map(|(at, _)| at)
                .chain([end - start])
                .find_map(|at| {
                    Self::piece_at(&text[start + at..end], piece).map(|length| start + at + length)
                });
            match found {
                Some(next) => start = next,
                None => return false,
            }
        }
        true
    }
}

fn bool_range_might_match(
    min: Option<bool>,
    max: Option<bool>,
    value: bool,
    op: &FilterOp,
) -> bool {
    match op {
        FilterOp::Eq => min
            .zip(max)
            .is_none_or(|(min, max)| min <= value && value <= max),
        FilterOp::Ne => min.zip(max) != Some((value, value)),
        _ => true,
    }
}

fn number_range_might_match(
    min: Option<Number>,
    max: Option<Number>,
    value: Option<Number>,
    op: &FilterOp,
) -> bool {
    if *op == FilterOp::Contains {
        return true;
    }

    let Some(value) = value else {
        return true;
    };
    let Some((min, max)) = min.zip(max) else {
        return true;
    };
    // Unordered statistics (e.g. from a writer using a different sort order) prove nothing.
    if min.compare(max) == Some(Ordering::Greater) {
        return true;
    }

    // An unknown ordering (NaN) must never prune.
    let may = |ordering: Option<Ordering>, allowed: &[Ordering]| {
        ordering.is_none_or(|ordering| allowed.contains(&ordering))
    };
    let low = min.compare(value);
    let high = max.compare(value);
    match op {
        FilterOp::Eq => {
            may(low, &[Ordering::Less, Ordering::Equal])
                && may(high, &[Ordering::Greater, Ordering::Equal])
        }
        FilterOp::Ne => !(low == Some(Ordering::Equal) && high == Some(Ordering::Equal)),
        FilterOp::Gt => may(high, &[Ordering::Greater]),
        FilterOp::Ge => may(high, &[Ordering::Greater, Ordering::Equal]),
        FilterOp::Lt => may(low, &[Ordering::Less]),
        FilterOp::Le => may(low, &[Ordering::Less, Ordering::Equal]),
        _ => true,
    }
}

fn string_range_might_match(
    min: Option<&str>,
    max: Option<&str>,
    value: &str,
    op: &FilterOp,
) -> bool {
    if *op == FilterOp::Contains {
        return true;
    }

    let Some((min, max)) = min.zip(max) else {
        return true;
    };

    match op {
        FilterOp::Eq => min <= value && value <= max,
        FilterOp::Ne => !(min == value && max == value),
        FilterOp::Gt => max > value,
        FilterOp::Ge => max >= value,
        FilterOp::Lt => min < value,
        FilterOp::Le => min <= value,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn float32_columns_match_the_value_they_display() {
        use arrow_schema::Field;
        use std::sync::Arc;

        let values = Float32Array::from(vec![9.083333_f32, 0.1, 2.0]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, false)])),
            vec![Arc::new(values)],
        )
        .unwrap();
        let matches = |filter: &str| {
            let mask = Comparison::parse(filter)
                .unwrap()
                .evaluate_batch(&batch, 0)
                .unwrap();
            (0..mask.len())
                .map(|row| mask.value(row))
                .collect::<Vec<_>>()
        };
        // 9.083333 and 0.1 have no exact f32; the column holds the nearest one.
        assert_eq!(matches("x == 9.083333"), [true, false, false]);
        assert_eq!(matches("x == 0.1"), [false, true, false]);
        assert_eq!(matches("x != 0.1"), [true, false, true]);
        assert_eq!(matches("x > 0.1"), [true, false, true]);
        assert_eq!(matches("x == 2"), [false, false, true]);
    }

    fn test(column: &str, op: FilterOp, value: &str) -> FilterExpr {
        FilterExpr::compare(column, op, value)
    }

    #[test]
    fn parses_and_or_and_brackets_with_sql_precedence() {
        use FilterOp::{Eq, Gt, Lt};
        // AND binds tighter than OR.
        assert_eq!(
            FilterExpr::parse("a == 1 OR b > 2 AND c < 3").unwrap(),
            FilterExpr::Or(vec![
                test("a", Eq, "1"),
                FilterExpr::And(vec![test("b", Gt, "2"), test("c", Lt, "3")]),
            ])
        );
        // Brackets override it; keywords are case-insensitive.
        assert_eq!(
            FilterExpr::parse("(a == 1 or b > 2) and c < 3").unwrap(),
            FilterExpr::And(vec![
                FilterExpr::Or(vec![test("a", Eq, "1"), test("b", Gt, "2")]),
                test("c", Lt, "3"),
            ])
        );
        assert_eq!(FilterExpr::parse("((a == 1))").unwrap(), test("a", Eq, "1"));
        // `=` and `<>` are accepted next to `==` and `!=`.
        assert_eq!(FilterExpr::parse("a = 1").unwrap(), test("a", Eq, "1"));
        assert_eq!(
            FilterExpr::parse("a <> 1").unwrap(),
            test("a", FilterOp::Ne, "1")
        );
    }

    #[test]
    fn parses_in_between_like_and_null_tests() {
        use FilterOp::{Eq, Ge, Gt, Le, Lt, Ne};
        assert_eq!(
            FilterExpr::parse("status IN (open, \"on hold\", 'a,b')").unwrap(),
            FilterExpr::Or(vec![
                test("status", Eq, "open"),
                test("status", Eq, "on hold"),
                test("status", Eq, "a,b"),
            ])
        );
        assert_eq!(
            FilterExpr::parse("status not in (open, held)").unwrap(),
            FilterExpr::And(vec![test("status", Ne, "open"), test("status", Ne, "held")])
        );
        assert_eq!(
            FilterExpr::parse("status IN (open)").unwrap(),
            test("status", Eq, "open")
        );
        // BETWEEN keeps its own AND, also next to another AND.
        assert_eq!(
            FilterExpr::parse("x BETWEEN 1 AND 5 AND y == 2").unwrap(),
            FilterExpr::And(vec![
                FilterExpr::And(vec![test("x", Ge, "1"), test("x", Le, "5")]),
                test("y", Eq, "2"),
            ])
        );
        assert_eq!(
            FilterExpr::parse("x NOT BETWEEN 1 AND 5").unwrap(),
            FilterExpr::Or(vec![test("x", Lt, "1"), test("x", Gt, "5")])
        );
        assert_eq!(
            FilterExpr::parse("name LIKE 'A%'").unwrap(),
            test("name", FilterOp::Like, "A%")
        );
        assert_eq!(
            FilterExpr::parse("name not like %x_").unwrap(),
            test("name", FilterOp::NotLike, "%x_")
        );
        assert_eq!(
            FilterExpr::parse("name IS NULL").unwrap(),
            test("name", FilterOp::IsNull, "")
        );
        assert_eq!(
            FilterExpr::parse("name is not null").unwrap(),
            test("name", FilterOp::IsNotNull, "")
        );
    }

    #[test]
    fn quotes_and_symbols_keep_keywords_inside_values() {
        use FilterOp::Eq;
        // Quoted text is never split.
        assert_eq!(
            FilterExpr::parse("title == \"war and peace\"").unwrap(),
            test("title", Eq, "war and peace")
        );
        assert_eq!(
            FilterExpr::parse("note == 'a == b or (c)'").unwrap(),
            test("note", Eq, "a == b or (c)")
        );
        // After a symbol, IN / LIKE / IS NULL words are part of the value.
        assert_eq!(
            FilterExpr::parse("note == not in use").unwrap(),
            test("note", Eq, "not in use")
        );
        assert_eq!(
            FilterExpr::parse("note == value is null").unwrap(),
            test("note", Eq, "value is null")
        );
        // A lone apostrophe is an ordinary character.
        assert_eq!(
            FilterExpr::parse("name == O'Brien").unwrap(),
            test("name", Eq, "O'Brien")
        );
        // An unquoted AND still splits, and says what is wrong with the second half.
        let error = FilterExpr::parse("title == war and peace").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported filter expression: peace")
        );
    }

    #[test]
    fn rejects_malformed_and_oversized_filters() {
        for (filter, message) in [
            ("", "empty"),
            ("a == 1 AND", "AND needs a test on both sides"),
            ("a == 1 or  ", "OR needs a test on both sides"),
            ("(a == 1", "'(' without a ')'"),
            ("a == 1)", "')' without a '('"),
            ("a IN 1, 2", "bracketed list"),
            ("a IN (1,,2)", "empty value"),
            ("a BETWEEN 1", "two values"),
            ("a IS NULL b", "nothing may follow IS NULL"),
            ("IS NULL", "unsupported filter expression"),
            (" == 1", "missing a column name"),
        ] {
            let error = FilterExpr::parse(filter).unwrap_err().to_string();
            assert!(error.contains(message), "{filter}: {error}");
        }

        let many = (0..=MAX_FILTER_TERMS)
            .map(|value| format!("a == {value}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let error = FilterExpr::parse(&many).unwrap_err().to_string();
        assert!(error.contains("the limit is 64"), "{error}");
        let list = (0..=MAX_FILTER_TERMS)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        assert!(FilterExpr::parse(&format!("a IN ({list})")).is_err());

        let deep = format!(
            "{}a == 1{}",
            "(".repeat(MAX_FILTER_DEPTH + 2),
            ")".repeat(MAX_FILTER_DEPTH + 2)
        );
        let error = FilterExpr::parse(&deep).unwrap_err().to_string();
        assert!(error.contains("levels deep"), "{error}");
        // A filter built in code is held to the same limits.
        let built = FilterExpr::Or(vec![test("x", FilterOp::Eq, "1"); MAX_FILTER_TERMS + 1]);
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        assert!(built.validate_schema(&schema).is_err());
    }

    #[test]
    fn like_matches_percent_and_underscore() {
        for (pattern, text, expected) in [
            ("abc", "abc", true),
            ("abc", "abcd", false),
            ("a%", "abc", true),
            ("a%", "bac", false),
            ("%c", "abc", true),
            ("%c", "abcd", false),
            ("%b%", "abc", true),
            ("%", "", true),
            ("", "", true),
            ("", "a", false),
            ("a_c", "abc", true),
            ("a_c", "ac", false),
            ("_", "é", true),
            ("_", "", false),
            ("a%b%c", "a--b--c", true),
            ("a%b%c", "a--c--b", false),
            ("%ab%ab", "ab", false),
            ("%ab%ab", "abab", true),
            ("a%a", "a", false),
            ("%é_", "caféx", true),
            ("%%x%%", "x", true),
            ("A%", "abc", false),
        ] {
            assert_eq!(
                LikePattern::new(pattern).matches(text),
                expected,
                "{text:?} LIKE {pattern:?}"
            );
        }
    }

    #[test]
    fn evaluates_trees_across_columns_with_sql_null_rules() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    None,
                    Some("gamma"),
                    Some("delta"),
                ])),
            ],
        )
        .unwrap();
        let rows = |filter: &str| {
            let mask = FilterExpr::parse(filter)
                .unwrap()
                .evaluate_batch(&batch)
                .unwrap();
            assert_eq!(mask.null_count(), 0);
            (0..mask.len())
                .filter(|row| mask.value(*row))
                .collect::<Vec<_>>()
        };
        assert_eq!(rows("id >= 2 AND name LIKE %a"), [3]);
        assert_eq!(rows("id == 1 OR name == gamma"), [0, 2]);
        assert_eq!(rows("id IN (1, 4)"), [0, 3]);
        // A null passes neither a test nor its negation.
        assert_eq!(rows("id NOT IN (1, 4)"), [1]);
        assert_eq!(rows("id BETWEEN 2 AND 4"), [1, 3]);
        assert_eq!(rows("id NOT BETWEEN 2 AND 4"), [0]);
        assert_eq!(rows("name NOT LIKE %a"), Vec::<usize>::new());
        assert_eq!(rows("name NOT LIKE a%"), [2, 3]);
        assert_eq!(rows("name IS NULL"), [1]);
        assert_eq!(rows("name IS NOT NULL AND id IS NOT NULL"), [0, 3]);
        assert_eq!(rows("(id == 1 OR id == 2) AND name IS NULL"), [1]);

        let schema = batch.schema();
        assert_eq!(
            FilterExpr::parse("name == x OR id == 1 AND name == y")
                .unwrap()
                .columns(&schema)
                .unwrap(),
            [0, 1]
        );
        assert!(
            FilterExpr::parse("id LIKE 1%")
                .unwrap()
                .validate_schema(&schema)
                .is_err()
        );
        assert!(
            FilterExpr::parse("id == 1 AND missing == 2")
                .unwrap()
                .validate_schema(&schema)
                .is_err()
        );
    }

    #[test]
    fn null_tests_work_on_types_that_cannot_be_compared() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let schema = Schema::new(vec![Field::new("items", list, true)]);
        assert!(
            FilterExpr::parse("items IS NULL")
                .unwrap()
                .validate_schema(&schema)
                .is_ok()
        );
        assert!(
            FilterExpr::parse("items == 1")
                .unwrap()
                .validate_schema(&schema)
                .is_err()
        );
    }

    #[test]
    fn displays_filters_in_text_that_parses_back() {
        for filter in [
            "a == 1",
            "name == \"war and peace\"",
            "name == 'say \"hi\"'",
            "name == \"\"",
            "name == \"null\"",
            "a == 1 AND (b > 2 OR c < 3) AND d != x",
            "a == 1 OR b > 2 AND c < 3",
            "name LIKE \"A%\" AND name NOT LIKE \"%z\"",
            "name contains \"a b\"",
            "x IS NULL OR y IS NOT NULL",
            "day >= 2023-12-10 AND ts < 2023-11-15T02:00:00+01:00",
        ] {
            let parsed = FilterExpr::parse(filter).unwrap();
            assert_eq!(parsed.to_string(), filter);
            assert_eq!(FilterExpr::parse(&parsed.to_string()).unwrap(), parsed);
        }
        // IN and BETWEEN are shown as the comparisons they stand for.
        assert_eq!(
            FilterExpr::parse("a IN (1, 2) AND b BETWEEN 3 AND 4")
                .unwrap()
                .to_string(),
            "(a == 1 OR a == 2) AND b >= 3 AND b <= 4"
        );
        assert_eq!(
            test("a", FilterOp::Eq, "1")
                .and(test("b", FilterOp::Eq, "2"))
                .and(FilterExpr::parse("c == 3 AND d == 4").unwrap())
                .to_string(),
            "a == 1 AND b == 2 AND c == 3 AND d == 4"
        );
    }

    #[test]
    fn prunes_row_groups_only_when_no_part_can_match() {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ]);
        let x = Statistics::int32(Some(10), Some(20), None, Some(0), false);
        let y = Statistics::int32(Some(100), Some(200), None, Some(0), false);
        let types = [DataType::Int32, DataType::Int32];
        let might = |filter: &str| {
            FilterExpr::parse(filter)
                .unwrap()
                .might_match(&schema, &|column| match column {
                    0 => Some((Some(&x), &types[0])),
                    1 => Some((Some(&y), &types[1])),
                    _ => None,
                })
        };
        assert!(might("x == 15 AND y == 150"));
        assert!(
            !might("x == 15 AND y == 5"),
            "one impossible part rules out an AND"
        );
        assert!(might("x == 5 OR y == 150"), "one possible part keeps an OR");
        assert!(!might("x == 5 OR y == 5"));
        assert!(!might("x IN (1, 2, 3)"));
        assert!(might("x IN (1, 12)"));
        assert!(!might("x BETWEEN 30 AND 40"));
        assert!(might("x NOT BETWEEN 12 AND 14"));
        // Null tests and patterns never prune: a zero null count may mean "unknown".
        assert!(might("x IS NULL"));
        assert!(might("x IS NOT NULL AND y == 150"));
    }

    #[test]
    fn parses_supported_filters() {
        assert_eq!(
            Comparison::parse("name contains \"abc\"").unwrap(),
            Comparison {
                column: "name".to_string(),
                op: FilterOp::Contains,
                value: "abc".to_string()
            }
        );
        assert_eq!(Comparison::parse("0 >= 42").unwrap().op, FilterOp::Ge);
        assert_eq!(Comparison::parse("0 <= 42").unwrap().op, FilterOp::Le);
        assert_eq!(Comparison::parse("0 == 42").unwrap().op, FilterOp::Eq);
        assert_eq!(Comparison::parse("0 != 42").unwrap().op, FilterOp::Ne);
        assert_eq!(Comparison::parse("0 > 42").unwrap().op, FilterOp::Gt);
        assert_eq!(Comparison::parse("0 < 42").unwrap().op, FilterOp::Lt);
    }

    #[test]
    fn rejects_bad_filters() {
        assert!(Comparison::parse("").is_err());
        assert!(Comparison::parse("abc").is_err());
    }

    #[test]
    fn rejects_scalar_filters_on_nested_columns_clearly() {
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let error = Comparison::parse("items == 1")
            .unwrap()
            .validate_schema(&schema)
            .unwrap_err();
        assert!(error.to_string().contains("filtering is not supported"));
    }

    #[test]
    fn evaluates_every_operator() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
            ]))],
        )
        .unwrap();

        for (filter, expected) in [
            ("x == 2", vec![false, true, false, false]),
            ("x != 2", vec![true, false, true, false]),
            ("x > 2", vec![false, false, true, false]),
            ("x >= 2", vec![false, true, true, false]),
            ("x < 2", vec![true, false, false, false]),
            ("x <= 2", vec![true, true, false, false]),
        ] {
            let actual = Comparison::parse(filter)
                .unwrap()
                .evaluate_batch(&batch, 0)
                .unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|value| value.unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn evaluates_contains_and_type_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        let mask = Comparison::parse("s contains \"ha\"")
            .unwrap()
            .evaluate_batch(&batch, 0)
            .unwrap();
        assert_eq!(
            mask.iter().map(|value| value.unwrap()).collect::<Vec<_>>(),
            vec![true, false, false]
        );
        assert!(
            Comparison::parse("b > true")
                .unwrap()
                .evaluate_batch(&batch, 1)
                .is_err()
        );
        assert!(
            Comparison::parse("b contains true")
                .unwrap()
                .evaluate_batch(&batch, 1)
                .is_err()
        );
    }

    #[test]
    fn compares_large_integers_exactly() {
        let base = 9_007_199_254_740_992_i64; // 2^53: the first integer f64 can't step past by 1
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![base, base + 1, base + 2]))],
        )
        .unwrap();

        for (filter, expected) in [
            ("x == 9007199254740993", vec![false, true, false]),
            ("x != 9007199254740993", vec![true, false, true]),
            ("x > 9007199254740992", vec![false, true, true]),
            ("x <= 9007199254740993", vec![true, true, false]),
        ] {
            let actual = Comparison::parse(filter)
                .unwrap()
                .evaluate_batch(&batch, 0)
                .unwrap();
            assert_eq!(
                actual.iter().map(Option::unwrap).collect::<Vec<_>>(),
                expected,
                "{filter}"
            );
        }
    }

    #[test]
    fn decimal_literals_still_work_on_integer_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let mask = Comparison::parse("x > 1.5")
            .unwrap()
            .evaluate_batch(&batch, 0)
            .unwrap();
        assert_eq!(
            mask.iter().map(Option::unwrap).collect::<Vec<_>>(),
            vec![false, true, true]
        );
    }

    #[test]
    fn reads_unsigned_statistics_as_unsigned() {
        // u32 3_000_000_000 and 3_000_000_001 as stored in Parquet's signed INT32.
        let stats = Statistics::int32(
            Some(3_000_000_000_u32 as i32),
            Some(3_000_000_001_u32 as i32),
            None,
            Some(0),
            false,
        );
        let might_match = |filter: &str| {
            Comparison::parse(filter)
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::UInt32)
        };
        assert!(might_match("x > 2999999999"));
        assert!(might_match("x == 3000000001"));
        assert!(!might_match("x < 10"));
        assert!(!might_match("x > 3000000001"));

        let stats = Statistics::int64(
            Some(((1_u64 << 63) + 5) as i64),
            Some(((1_u64 << 63) + 6) as i64),
            None,
            Some(0),
            false,
        );
        let filter = Comparison::parse("x > 9223372036854775808").unwrap();
        assert!(filter.might_match_statistics(Some(&stats), &DataType::UInt64));
    }

    #[test]
    fn prunes_statistics_without_false_negatives() {
        let stats = Statistics::int32(Some(10), Some(20), None, Some(0), false);

        assert!(
            !Comparison::parse("x == 9")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            Comparison::parse("x == 10")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            Comparison::parse("x > 19")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            !Comparison::parse("x > 20")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
        assert!(
            Comparison::parse("x != 9")
                .unwrap()
                .might_match_statistics(Some(&stats), &DataType::Int32)
        );
    }
}
