use anyhow::{Result, bail};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Projection {
    columns: Vec<usize>,
}

impl Projection {
    pub fn all(column_count: usize) -> Self {
        Self {
            columns: (0..column_count).collect(),
        }
    }

    pub fn columns(columns: impl Into<Vec<usize>>, column_count: usize) -> Result<Self> {
        let columns = columns.into();
        if columns.is_empty() {
            return Ok(Self::all(column_count));
        }

        for (position, column) in columns.iter().enumerate() {
            if *column >= column_count {
                bail!("column index {column} out of range");
            }

            if columns[..position].contains(column) {
                bail!("duplicate column index {column}");
            }
        }

        Ok(Self { columns })
    }

    pub fn as_slice(&self) -> &[usize] {
        &self.columns
    }

    pub fn parquet_columns(&self) -> Vec<usize> {
        let mut columns = self.columns.clone();
        columns.sort_unstable();
        columns.dedup();
        columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_all_columns() {
        assert_eq!(Projection::all(3).as_slice(), &[0, 1, 2]);
        assert_eq!(
            Projection::columns(Vec::new(), 3).unwrap().as_slice(),
            &[0, 1, 2]
        );
    }

    #[test]
    fn preserves_requested_order() {
        assert_eq!(
            Projection::columns(vec![2, 0], 3).unwrap().as_slice(),
            &[2, 0]
        );
    }

    #[test]
    fn rejects_bad_columns() {
        assert!(Projection::columns(vec![3], 3).is_err());
        assert!(Projection::columns(vec![1, 1], 3).is_err());
    }
}
