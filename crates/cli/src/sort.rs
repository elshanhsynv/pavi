//! `--sort "a:desc,b"`: sort keys by column name or index.

use anyhow::{Result, bail};
use parquet_reader::{NullOrder, ParquetSource, SortDirection, SortSpec};

use crate::resolve_column;

/// Parses comma-separated keys, each a column optionally followed by `:asc`
/// or `:desc` (ascending by default). Nulls sort last, as in the grid. A
/// column whose name itself ends in `:asc` or `:desc` can be given by index.
pub fn parse_sort(text: &str, parquet: &ParquetSource) -> Result<Vec<SortSpec>> {
    let mut keys = Vec::new();
    for key in text.split(',') {
        let key = key.trim();
        if key.is_empty() {
            bail!("empty key in --sort");
        }
        let (column, direction) = match key.rsplit_once(':') {
            Some((column, direction)) if direction.eq_ignore_ascii_case("asc") => {
                (column, SortDirection::Ascending)
            }
            Some((column, direction)) if direction.eq_ignore_ascii_case("desc") => {
                (column, SortDirection::Descending)
            }
            _ => (key, SortDirection::Ascending),
        };
        let column = resolve_column(column.trim(), parquet)?;
        keys.push(SortSpec::new(column, direction, NullOrder::Last));
    }
    // The types and the number of keys, before anything is printed.
    parquet.validate_sort(&keys)?;
    Ok(keys)
}
