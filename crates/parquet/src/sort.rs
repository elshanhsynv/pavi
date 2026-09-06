use anyhow::{Result, bail};

/// Direction for one PAVI sort key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Explicit and stable placement for null sort values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullOrder {
    First,
    Last,
}

/// One top-level source column and its ordering options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortSpec {
    pub column: usize,
    pub direction: SortDirection,
    pub nulls: NullOrder,
}

/// Bound on the source rows and estimated Arrow workspace retained for one sort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortBudget {
    max_rows: usize,
    max_bytes: usize,
}

impl SortSpec {
    pub fn new(column: usize, direction: SortDirection, nulls: NullOrder) -> Self {
        Self {
            column,
            direction,
            nulls,
        }
    }
}

impl SortBudget {
    pub const DEFAULT_MAX_ROWS: usize = 100_000;
    pub const DEFAULT_MAX_BYTES: usize = 128 * 1024 * 1024;

    pub fn new(max_rows: usize, max_bytes: usize) -> Result<Self> {
        if max_rows == 0 {
            bail!("sort budget needs at least one row");
        }
        if max_bytes == 0 {
            bail!("sort budget needs a positive byte limit");
        }
        Ok(Self {
            max_rows,
            max_bytes,
        })
    }

    pub fn max_rows(self) -> usize {
        self.max_rows
    }

    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

impl Default for SortBudget {
    fn default() -> Self {
        Self {
            max_rows: Self::DEFAULT_MAX_ROWS,
            max_bytes: Self::DEFAULT_MAX_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unbounded_sort_budgets() {
        assert!(SortBudget::new(0, 1).is_err());
        assert!(SortBudget::new(1, 0).is_err());
    }
}
