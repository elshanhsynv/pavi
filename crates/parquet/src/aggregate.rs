use anyhow::{Result, bail};

/// One supported aggregate operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
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

    /// Groups by every column in `group_by`, in that order.
    pub fn grouped_by(
        group_by: impl Into<Vec<usize>>,
        expressions: impl Into<Vec<AggregateExpr>>,
    ) -> Result<Self> {
        let expressions = expressions.into();
        let group_by = group_by.into();
        if expressions.is_empty() {
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
        assert!(GroupBudget::new(0, 1).is_err());
        assert!(GroupBudget::new(1, 0).is_err());
    }
}
