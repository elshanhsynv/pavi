use std::sync::Arc;

use anyhow::{Result, bail};
use arrow_schema::Schema;

#[derive(Clone, Debug)]
pub struct DatasetMetadata {
    pub schema: Arc<Schema>,
    pub row_count: u64,
    pub column_count: usize,
    pub row_groups: Vec<RowGroupInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowGroupInfo {
    pub index: usize,
    pub first_row: u64,
    pub row_count: u64,
}

impl DatasetMetadata {
    pub fn new(
        schema: Arc<Schema>,
        row_group_counts: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        let mut first_row = 0_u64;
        let mut row_groups = Vec::new();

        for (index, row_count) in row_group_counts.into_iter().enumerate() {
            row_groups.push(RowGroupInfo {
                index,
                first_row,
                row_count,
            });
            first_row = first_row
                .checked_add(row_count)
                .ok_or_else(|| anyhow::anyhow!("cumulative row count overflow"))?;
        }

        Ok(Self {
            column_count: schema.fields().len(),
            schema,
            row_count: first_row,
            row_groups,
        })
    }

    pub fn row_group_for_row(&self, row: u64) -> Option<&RowGroupInfo> {
        if row >= self.row_count {
            return None;
        }

        let index = self
            .row_groups
            .partition_point(|group| group.first_row + group.row_count <= row);
        self.row_groups.get(index)
    }

    pub fn overlapping_row_group_indexes(&self, first_row: u64, row_count: usize) -> Vec<usize> {
        if row_count == 0 || first_row >= self.row_count {
            return Vec::new();
        }

        let end_row = first_row
            .saturating_add(row_count as u64)
            .min(self.row_count);
        let start = self
            .row_groups
            .partition_point(|group| group.first_row + group.row_count <= first_row);

        self.row_groups[start..]
            .iter()
            .take_while(|group| group.first_row < end_row)
            .map(|group| group.index)
            .collect()
    }

    pub fn validate_window(&self, first_row: u64, row_count: usize) -> Result<usize> {
        if first_row > self.row_count {
            bail!("first row {first_row} is past row count {}", self.row_count);
        }

        Ok(if first_row == self.row_count {
            0
        } else {
            row_count.min((self.row_count - first_row) as usize)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Schema;

    use super::*;

    fn metadata(counts: &[u64]) -> DatasetMetadata {
        DatasetMetadata::new(Arc::new(Schema::empty()), counts.iter().copied()).unwrap()
    }

    #[test]
    fn builds_cumulative_row_ranges() {
        let metadata = metadata(&[3, 0, 4]);
        assert_eq!(
            metadata.row_groups,
            vec![
                RowGroupInfo {
                    index: 0,
                    first_row: 0,
                    row_count: 3
                },
                RowGroupInfo {
                    index: 1,
                    first_row: 3,
                    row_count: 0
                },
                RowGroupInfo {
                    index: 2,
                    first_row: 3,
                    row_count: 4
                }
            ]
        );
    }

    #[test]
    fn finds_row_groups_by_binary_search() {
        let metadata = metadata(&[3, 4]);
        assert_eq!(metadata.row_group_for_row(0).unwrap().index, 0);
        assert_eq!(metadata.row_group_for_row(3).unwrap().index, 1);
        assert!(metadata.row_group_for_row(7).is_none());
    }

    #[test]
    fn finds_overlapping_groups() {
        let metadata = metadata(&[3, 4, 5]);
        assert_eq!(metadata.overlapping_row_group_indexes(2, 6), vec![0, 1, 2]);
        assert_eq!(metadata.overlapping_row_group_indexes(3, 4), vec![1]);
        assert_eq!(
            metadata.overlapping_row_group_indexes(12, 1),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn rejects_cumulative_overflow() {
        let result = DatasetMetadata::new(Arc::new(Schema::empty()), [u64::MAX, 1]);
        assert!(result.is_err());
    }
}
