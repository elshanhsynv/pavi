use arrow_array::RecordBatch;

use crate::Projection;

pub const PAGE_ROWS: u64 = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowWindow {
    pub first_row: u64,
    pub row_count: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PageKey {
    pub page_index: u64,
    pub projection: Projection,
}

#[derive(Clone, Debug)]
pub struct DataPage {
    pub key: PageKey,
    pub window: RowWindow,
    pub batches: Vec<RecordBatch>,
    pub byte_size: usize,
}

impl PageKey {
    pub fn new(page_index: u64, projection: Projection) -> Self {
        Self {
            page_index,
            projection,
        }
    }
}

impl RowWindow {
    pub fn for_page(page_index: u64, total_rows: u64) -> Self {
        let first_row = page_index.saturating_mul(PAGE_ROWS);
        let remaining = total_rows.saturating_sub(first_row);
        Self {
            first_row,
            row_count: remaining.min(PAGE_ROWS) as usize,
        }
    }
}

impl DataPage {
    pub fn new(key: PageKey, window: RowWindow, batches: Vec<RecordBatch>) -> Self {
        let byte_size = batches.iter().map(RecordBatch::get_array_memory_size).sum();

        Self {
            key,
            window,
            batches,
            byte_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_first_middle_and_final_pages() {
        assert_eq!(
            RowWindow::for_page(0, 10_000),
            RowWindow {
                first_row: 0,
                row_count: 4096
            }
        );
        assert_eq!(
            RowWindow::for_page(1, 10_000),
            RowWindow {
                first_row: 4096,
                row_count: 4096
            }
        );
        assert_eq!(
            RowWindow::for_page(2, 10_000),
            RowWindow {
                first_row: 8192,
                row_count: 1808
            }
        );
    }
}
