use anyhow::{Result, bail};

/// One supported aggregate operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `COUNT(DISTINCT column)`: distinct non-null values. Its values count
    /// against the group budget.
    CountDistinct,
    /// Sample standard deviation (`STDDEV`, `STDDEV_SAMP`); null for fewer
    /// than two values.
    StdDev,
    /// Population standard deviation (`STDDEV_POP`).
    StdDevPop,
    /// Sample variance (`VARIANCE`, `VAR_SAMP`); null for fewer than two values.
    Variance,
    /// Population variance (`VAR_POP`).
    VarPop,
    /// The middle value, as `QUANTILE_CONT(column, 0.5)`. Keeps every value
    /// of its group (see [`MAX_QUANTILE_VALUES`]).
    Median,
    /// The continuous quantile at this fraction, in millionths (0 to
    /// 1,000,000): `QUANTILE_CONT(column, p)` or `PERCENTILE_CONT(p) WITHIN
    /// GROUP (ORDER BY column)`, interpolating between the two nearest values.
    /// Keeps every value of its group (see [`MAX_QUANTILE_VALUES`]).
    Quantile(u32),
}

/// Values `MEDIAN` and `QUANTILE_CONT` may keep for one query, across all its
/// groups and quantiles (8 bytes each: 128 MB).
pub const MAX_QUANTILE_VALUES: usize = 16 * 1024 * 1024;

impl AggregateFunction {
    /// Whether it keeps every value of its group: `MEDIAN` and `QUANTILE_CONT`.
    pub fn keeps_values(self) -> bool {
        matches!(self, Self::Median | Self::Quantile(_))
    }

    /// The quantile's fraction (0 to 1), for `MEDIAN` and `QUANTILE_CONT`.
    pub fn quantile_fraction(self) -> Option<f64> {
        match self {
            Self::Median => Some(0.5),
            Self::Quantile(millionths) => Some(f64::from(millionths) / 1_000_000.0),
            _ => None,
        }
    }

    /// The quantile at `fraction` (0 to 1, kept to millionths).
    pub fn quantile(fraction: f64) -> Result<Self> {
        if !(0.0..=1.0).contains(&fraction) {
            bail!("a quantile is a fraction from 0 to 1, not {fraction}");
        }
        Ok(Self::Quantile((fraction * 1_000_000.0).round() as u32))
    }

    /// Whether it takes numbers and decimals only (SUM, AVG and the statistics).
    pub fn needs_numbers(self) -> bool {
        matches!(
            self,
            Self::Sum
                | Self::Avg
                | Self::StdDev
                | Self::StdDevPop
                | Self::Variance
                | Self::VarPop
                | Self::Median
                | Self::Quantile(_)
        )
    }

    /// The SQL name it is written with.
    pub fn sql_name(self) -> &'static str {
        match self {
            Self::Count | Self::CountDistinct => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::StdDev => "STDDEV",
            Self::StdDevPop => "STDDEV_POP",
            Self::Variance => "VARIANCE",
            Self::VarPop => "VAR_POP",
            Self::Median => "MEDIAN",
            Self::Quantile(_) => "QUANTILE_CONT",
        }
    }
}

/// The aggregate a SQL function name stands for, other than `COUNT(DISTINCT …)`
/// and the quantiles, which take more than a column.
pub fn aggregate_by_name(name: &str) -> Option<AggregateFunction> {
    Some(match name.to_ascii_uppercase().as_str() {
        "COUNT" => AggregateFunction::Count,
        "SUM" => AggregateFunction::Sum,
        "AVG" | "MEAN" => AggregateFunction::Avg,
        "MIN" => AggregateFunction::Min,
        "MAX" => AggregateFunction::Max,
        "STDDEV" | "STDDEV_SAMP" => AggregateFunction::StdDev,
        "STDDEV_POP" => AggregateFunction::StdDevPop,
        "VARIANCE" | "VAR_SAMP" => AggregateFunction::Variance,
        "VAR_POP" => AggregateFunction::VarPop,
        "MEDIAN" => AggregateFunction::Median,
        _ => return None,
    })
}

/// Whether a SQL function name is an aggregate (the quantile forms included).
pub fn is_aggregate_name(name: &str) -> bool {
    aggregate_by_name(name).is_some()
        || matches!(
            name.to_ascii_uppercase().as_str(),
            "QUANTILE_CONT" | "QUANTILE" | "PERCENTILE_CONT"
        )
}

/// One aggregate expression over an optional source column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateExpr {
    function: AggregateFunction,
    column: Option<usize>,
}

impl AggregateExpr {
    pub fn count_all() -> Self {
        Self {
            function: AggregateFunction::Count,
            column: None,
        }
    }

    pub fn count(column: usize) -> Self {
        Self {
            function: AggregateFunction::Count,
            column: Some(column),
        }
    }

    pub fn sum(column: usize) -> Self {
        Self {
            function: AggregateFunction::Sum,
            column: Some(column),
        }
    }

    pub fn avg(column: usize) -> Self {
        Self {
            function: AggregateFunction::Avg,
            column: Some(column),
        }
    }

    pub fn min(column: usize) -> Self {
        Self {
            function: AggregateFunction::Min,
            column: Some(column),
        }
    }

    pub fn max(column: usize) -> Self {
        Self {
            function: AggregateFunction::Max,
            column: Some(column),
        }
    }

    /// `function` over `column`.
    pub fn of(function: AggregateFunction, column: usize) -> Self {
        Self {
            function,
            column: Some(column),
        }
    }

    pub fn count_distinct(column: usize) -> Self {
        Self {
            function: AggregateFunction::CountDistinct,
            column: Some(column),
        }
    }

    /// The same aggregate over another column (for plans whose columns are
    /// renumbered).
    pub fn with_column(self, column: Option<usize>) -> Self {
        Self { column, ..self }
    }

    pub fn function(self) -> AggregateFunction {
        self.function
    }

    pub fn column(self) -> Option<usize> {
        self.column
    }
}

/// Most columns one GROUP BY may name.
pub const MAX_GROUP_COLUMNS: usize = 8;

/// Aggregates and the columns they are grouped by (none for one overall group).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateSpec {
    expressions: Vec<AggregateExpr>,
    group_by: Vec<usize>,
}

impl AggregateSpec {
    pub fn new(expressions: impl Into<Vec<AggregateExpr>>) -> Result<Self> {
        Self::grouped_by(Vec::new(), expressions)
    }

    pub fn grouped(group_by: usize, expressions: impl Into<Vec<AggregateExpr>>) -> Result<Self> {
        Self::grouped_by(vec![group_by], expressions)
    }

    /// Groups by every column in `group_by`, in that order. With no
    /// expressions it lists each distinct combination once (`SELECT DISTINCT`).
    pub fn grouped_by(
        group_by: impl Into<Vec<usize>>,
        expressions: impl Into<Vec<AggregateExpr>>,
    ) -> Result<Self> {
        let expressions = expressions.into();
        let group_by = group_by.into();
        if expressions.is_empty() && group_by.is_empty() {
            bail!("aggregate needs at least one expression");
        }
        if group_by.len() > MAX_GROUP_COLUMNS {
            bail!(
                "GROUP BY names {} columns; the limit is {MAX_GROUP_COLUMNS}",
                group_by.len()
            );
        }
        if (1..group_by.len()).any(|index| group_by[..index].contains(&group_by[index])) {
            bail!("GROUP BY names the same column twice");
        }
        Ok(Self {
            expressions,
            group_by,
        })
    }

    pub fn expressions(&self) -> &[AggregateExpr] {
        &self.expressions
    }

    /// The grouping columns, in order; empty for one overall group.
    pub fn group_by(&self) -> &[usize] {
        &self.group_by
    }
}

/// Bound on retained grouped aggregate state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupBudget {
    max_groups: usize,
    max_bytes: usize,
}

impl GroupBudget {
    pub const DEFAULT_MAX_GROUPS: usize = 10_000;
    pub const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;

    pub fn new(max_groups: usize, max_bytes: usize) -> Result<Self> {
        if max_groups == 0 {
            bail!("group budget needs at least one group");
        }
        if max_bytes == 0 {
            bail!("group budget needs a positive byte limit");
        }
        Ok(Self {
            max_groups,
            max_bytes,
        })
    }

    pub fn max_groups(self) -> usize {
        self.max_groups
    }

    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

impl Default for GroupBudget {
    fn default() -> Self {
        Self {
            max_groups: Self::DEFAULT_MAX_GROUPS,
            max_bytes: Self::DEFAULT_MAX_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_or_unbounded_specs() {
        assert!(AggregateSpec::new([]).is_err());
        // Distinct values: groups without aggregates.
        assert!(AggregateSpec::grouped_by(vec![0], []).is_ok());
        assert!(GroupBudget::new(0, 1).is_err());
        assert!(GroupBudget::new(1, 0).is_err());
    }
}
