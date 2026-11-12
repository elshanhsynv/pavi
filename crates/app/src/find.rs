//! Find text in the rows already loaded in memory.
//!
//! Filters scan the file; find only looks at the bounded window of rows the grid
//! holds (at most `pages::MAX_LOADED_PAGES` pages, or the filtered result window),
//! so it never reads from disk. It runs on the UI thread, so one search step is
//! capped at [`MAX_FIND_CELLS`] cells and the next step resumes where it stopped.

use arrow_array::{Array, RecordBatch};
use parquet_reader::value::format_cell_with_limit;

/// Most cells one find step looks at before handing back to the UI.
pub(crate) const MAX_FIND_CELLS: usize = 500_000;
/// Characters of each cell that find compares; the grid shows no more either.
const FIND_CELL_CHARS: usize = 256;

/// A run of consecutive loaded rows: `batch` row 0 is absolute row `first_row`.
pub(crate) struct Segment<'a> {
    pub(crate) first_row: u64,
    pub(crate) batch: &'a RecordBatch,
}

/// A cell: absolute row and display column.
pub(crate) type Cell = (u64, usize);

#[derive(Debug, PartialEq)]
pub(crate) enum FindOutcome {
    Found(Cell),
    /// No loaded cell contains the text.
    NotFound,
    /// The cell budget ran out at this cell; search again from here.
    Stopped(Cell),
}

/// Looks for `query` (case-insensitive) in the loaded rows, row by row and, within
/// a row, display column by display column. `columns[d]` is the batch column of
/// display column `d`.
///
/// The search starts at the cell after `from` (before it, `backwards`), or at the
/// first (last) loaded cell when `from` is `None` or not loaded, and wraps around
/// once, so `from` itself is looked at last.
pub(crate) fn find(
    segments: &[Segment<'_>],
    columns: &[usize],
    query: &str,
    from: Option<Cell>,
    backwards: bool,
    budget: usize,
) -> FindOutcome {
    let needle = query.to_lowercase();
    let rows = segments
        .iter()
        .map(|segment| segment.batch.num_rows())
        .collect::<Vec<_>>();
    let total = rows.iter().sum::<usize>().saturating_mul(columns.len());
    if needle.is_empty() || total == 0 {
        return FindOutcome::NotFound;
    }

    // Cells are numbered 0..total in reading order.
    let index_of = |(row, column): Cell| -> Option<usize> {
        let mut before = 0_usize;
        for (segment, count) in segments.iter().zip(&rows) {
            let offset = row.checked_sub(segment.first_row)?;
            if offset < *count as u64 && column < columns.len() {
                return Some((before + offset as usize) * columns.len() + column);
            }
            before += count;
        }
        None
    };
    let cell_at = |index: usize| -> Option<(Cell, &dyn Array, usize)> {
        let (mut row, column) = (index / columns.len(), index % columns.len());
        for (segment, count) in segments.iter().zip(&rows) {
            if row < *count {
                let array = segment.batch.columns().get(columns[column])?;
                return Some((
                    (segment.first_row + row as u64, column),
                    array.as_ref(),
                    row,
                ));
            }
            row -= count;
        }
        None
    };

    let start = from.and_then(index_of);
    for step in 0..total.min(budget) {
        let index = match (start, backwards) {
            (Some(start), false) => (start + 1 + step) % total,
            (Some(start), true) => (start + total - 1 - step % total) % total,
            (None, false) => step,
            (None, true) => total - 1 - step,
        };
        let Some((cell, array, row)) = cell_at(index) else {
            continue;
        };
        if format_cell_with_limit(array, row, FIND_CELL_CHARS)
            .to_lowercase()
            .contains(&needle)
        {
            return FindOutcome::Found(cell);
        }
        if step + 1 == budget && budget < total {
            return FindOutcome::Stopped(cell);
        }
    }
    FindOutcome::NotFound
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, StringArray};

    use super::*;

    fn batch(ids: std::ops::Range<i32>, names: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from_iter_values(ids)) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn finds_forward_backward_and_wraps_around() {
        // Two loaded runs with a gap between them (rows 0–2 and 10–12).
        let first = batch(0..3, &["Alpha", "beta", "gamma"]);
        let second = batch(10..13, &["delta", "BETA", "alphabeta"]);
        let segments = [
            Segment {
                first_row: 0,
                batch: &first,
            },
            Segment {
                first_row: 10,
                batch: &second,
            },
        ];
        let columns = [0, 1];
        let find = |query: &str, from: Option<Cell>, backwards: bool| {
            find(&segments, &columns, query, from, backwards, MAX_FIND_CELLS)
        };

        assert_eq!(find("beta", None, false), FindOutcome::Found((1, 1)));
        // Case does not matter, and the search moves on from the last match.
        assert_eq!(
            find("beta", Some((1, 1)), false),
            FindOutcome::Found((11, 1))
        );
        assert_eq!(
            find("beta", Some((11, 1)), false),
            FindOutcome::Found((12, 1))
        );
        // It wraps to the start.
        assert_eq!(
            find("beta", Some((12, 1)), false),
            FindOutcome::Found((1, 1))
        );
        assert_eq!(
            find("beta", Some((1, 1)), true),
            FindOutcome::Found((12, 1))
        );
        assert_eq!(find("alpha", None, true), FindOutcome::Found((12, 1)));
        // Numbers are found as the grid shows them; the display column is returned.
        assert_eq!(find("11", None, false), FindOutcome::Found((11, 0)));
        // A start cell that is not loaded starts from the beginning.
        assert_eq!(
            find("gamma", Some((5, 0)), false),
            FindOutcome::Found((2, 1))
        );
        assert_eq!(find("zeta", None, false), FindOutcome::NotFound);
        assert_eq!(find("", None, false), FindOutcome::NotFound);
        // Only shown columns are searched.
        assert_eq!(
            super::find(&segments, &[0], "beta", None, false, MAX_FIND_CELLS),
            FindOutcome::NotFound
        );
    }

    #[test]
    fn stops_at_the_cell_budget_and_resumes_from_there() {
        let rows = batch(0..4, &["a", "b", "c", "needle"]);
        let segments = [Segment {
            first_row: 0,
            batch: &rows,
        }];
        let columns = [0, 1];
        // Eight cells; the match is the last one.
        let stopped = find(&segments, &columns, "needle", None, false, 3);
        assert_eq!(stopped, FindOutcome::Stopped((1, 0)));
        let FindOutcome::Stopped(at) = stopped else {
            unreachable!()
        };
        let stopped = find(&segments, &columns, "needle", Some(at), false, 3);
        assert_eq!(stopped, FindOutcome::Stopped((2, 1)));
        let FindOutcome::Stopped(at) = stopped else {
            unreachable!()
        };
        assert_eq!(
            find(&segments, &columns, "needle", Some(at), false, 3),
            FindOutcome::Found((3, 1))
        );
    }
}
