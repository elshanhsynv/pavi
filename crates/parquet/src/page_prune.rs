//! Page-level and bloom filter pruning for filtered scans (ADR 0014).
//!
//! A row group's footer statistics can only rule out the whole group. Parquet's
//! page index also keeps each data page's min and max, so a filter test can
//! rule out pages: a lookup in a sorted or clustered column then reads one
//! page of each row group instead of all of it. The same test that prunes row
//! groups ([`Comparison::might_match_statistics`]) decides for each page, so
//! the two can never disagree. Without a page index, or for a test it can't
//! judge, every row is read, exactly as before.
//!
//! Min and max can't rule out a value that falls between them, as a lookup of
//! one id in an unsorted column does. A column chunk's bloom filter can: when
//! it says an `==` value is absent, the row group is skipped.

use std::ops::Range;

use arrow_schema::Schema;
use parquet::{
    basic::Type,
    bloom_filter::Sbbf,
    data_type::{ByteArray, FixedLenByteArray},
    file::{
        metadata::RowGroupMetaData,
        page_index::{
            column_index::ColumnIndexMetaData,
            index_reader::{decode_column_index, decode_offset_index},
        },
        reader::ChunkReader,
        statistics::Statistics,
    },
};

use crate::filter::{BloomKey, Comparison, FilterExpr, FilterOp};

/// Bytes of one column chunk's page index (its column index plus its offset
/// index) that pruning reads; a chunk with a larger index is read whole.
pub const MAX_PAGE_INDEX_BYTES: usize = 4 * 1024 * 1024;

/// Bytes of one column chunk's bloom filter that pruning reads; a chunk with a
/// larger filter, or one whose length the footer doesn't give, isn't checked.
pub const MAX_BLOOM_FILTER_BYTES: usize = 16 * 1024 * 1024;

/// Sorted, disjoint row ranges within one row group.
pub(crate) type Ranges = Vec<Range<usize>>;

/// What pruning needs to know about the row group it looks at.
pub(crate) struct PageContext<'a, R: ChunkReader> {
    pub(crate) reader: &'a R,
    pub(crate) row_group: &'a RowGroupMetaData,
    pub(crate) schema: &'a Schema,
    /// The Parquet leaf column of a source column; `None` for nested and
    /// partition columns, which have no page index.
    pub(crate) leaf_of: &'a dyn Fn(usize) -> Option<usize>,
}

/// The rows of a row group that `filter` might match, judged from the page
/// indexes of the columns it tests; `None` when no page index narrows it.
pub(crate) fn row_group_ranges<R: ChunkReader>(
    filter: &FilterExpr,
    context: &PageContext<'_, R>,
) -> Option<Ranges> {
    match filter {
        FilterExpr::Compare(comparison) => comparison_ranges(comparison, context),
        // Page indexes hold least and greatest values, not text: no narrowing.
        FilterExpr::AnyContains(_) => None,
        // Rows must pass every part: what each part allows, intersected.
        FilterExpr::And(parts) => parts
            .iter()
            .filter_map(|part| row_group_ranges(part, context))
            .reduce(|left, right| intersect(&left, &right)),
        // Rows may pass any part: a part that can't be narrowed allows all.
        FilterExpr::Or(parts) => {
            let mut all = Vec::new();
            for part in parts {
                all = union(&all, &row_group_ranges(part, context)?);
            }
            Some(all)
        }
    }
}

fn comparison_ranges<R: ChunkReader>(
    comparison: &Comparison,
    context: &PageContext<'_, R>,
) -> Option<Ranges> {
    // Null tests: a page's null count may be missing, so it proves nothing.
    if matches!(comparison.op, FilterOp::IsNull | FilterOp::IsNotNull) {
        return None;
    }
    let column = comparison.column_index(context.schema).ok()?;
    let data_type = context.schema.fields().get(column)?.data_type();
    let leaf = (context.leaf_of)(column)?;
    let chunk = context.row_group.columns().get(leaf)?;
    let read = |offset: Option<i64>, length: Option<i32>| {
        let length = usize::try_from(length?).ok()?;
        if length == 0 || length > MAX_PAGE_INDEX_BYTES {
            return None;
        }
        context
            .reader
            .get_bytes(u64::try_from(offset?).ok()?, length)
            .ok()
    };
    let column_index = decode_column_index(
        &read(chunk.column_index_offset(), chunk.column_index_length())?,
        chunk.column_type(),
    )
    .ok()?;
    let offset_index = decode_offset_index(&read(
        chunk.offset_index_offset(),
        chunk.offset_index_length(),
    )?)
    .ok()?;
    let locations = offset_index.page_locations();
    if column_index.num_pages() != locations.len() as u64 {
        return None;
    }
    let rows = usize::try_from(context.row_group.num_rows()).ok()?;
    let mut ranges: Ranges = Vec::new();
    for (page, location) in locations.iter().enumerate() {
        let start = usize::try_from(location.first_row_index).ok()?;
        let end = match locations.get(page + 1) {
            Some(next) => usize::try_from(next.first_row_index).ok()?,
            None => rows,
        };
        if start >= end || end > rows {
            return None;
        }
        // A page of nulls passes no comparison; otherwise its min and max
        // decide, as a row group's do.
        let keep = !column_index.is_null_page(page)
            && page_statistics(&column_index, page).is_none_or(|statistics| {
                comparison.might_match_statistics(Some(&statistics), data_type)
            });
        if keep {
            match ranges.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => ranges.push(start..end),
            }
        }
    }
    Some(ranges)
}

/// `true` only when the column chunks' bloom filters prove no row of the row
/// group passes `filter`. `reader` opens the file when a filter must be read.
pub(crate) fn bloom_rules_out<'r, R: ChunkReader + 'r>(
    filter: &FilterExpr,
    row_group: &RowGroupMetaData,
    schema: &Schema,
    leaf_of: &dyn Fn(usize) -> Option<usize>,
    reader: &dyn Fn() -> Option<&'r R>,
) -> bool {
    let rules_out = |part| bloom_rules_out(part, row_group, schema, leaf_of, reader);
    match filter {
        FilterExpr::Compare(comparison) => {
            bloom_excludes(comparison, row_group, schema, leaf_of, reader).unwrap_or(false)
        }
        FilterExpr::And(parts) => parts.iter().any(rules_out),
        FilterExpr::Or(parts) => !parts.is_empty() && parts.iter().all(rules_out),
        // A bloom filter answers "is this exact value here", not "contains".
        FilterExpr::AnyContains(_) => false,
    }
}

/// `Some(true)` when the bloom filter says the `==` value is absent; `None`
/// when there is no filter to ask.
fn bloom_excludes<'r, R: ChunkReader + 'r>(
    comparison: &Comparison,
    row_group: &RowGroupMetaData,
    schema: &Schema,
    leaf_of: &dyn Fn(usize) -> Option<usize>,
    reader: &dyn Fn() -> Option<&'r R>,
) -> Option<bool> {
    let column = comparison.column_index(schema).ok()?;
    let key = comparison.bloom_key(schema.fields().get(column)?.data_type())?;
    let chunk = row_group.columns().get(leaf_of(column)?)?;
    chunk.bloom_filter_offset()?;
    let length = usize::try_from(chunk.bloom_filter_length()?).ok()?;
    if length == 0 || length > MAX_BLOOM_FILTER_BYTES {
        return None;
    }
    let bloom = Sbbf::read_from_column_chunk(chunk, reader()?).ok()??;
    // A damaged filter with no blocks can't be asked.
    if bloom.num_blocks() == 0 {
        return None;
    }
    let present = match (&key, chunk.column_type()) {
        (BloomKey::Int32(value), Type::INT32) => bloom.check(value),
        (BloomKey::Int64(value), Type::INT64) => bloom.check(value),
        (BloomKey::Bytes(value), Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY) => {
            bloom.check(value.as_slice())
        }
        _ => return None,
    };
    Some(!present)
}

/// One page's min and max as footer-style statistics.
fn page_statistics(index: &ColumnIndexMetaData, page: usize) -> Option<Statistics> {
    Some(match index {
        ColumnIndexMetaData::BOOLEAN(index) => Statistics::boolean(
            index.min_value(page).copied(),
            index.max_value(page).copied(),
            None,
            None,
            false,
        ),
        ColumnIndexMetaData::INT32(index) => Statistics::int32(
            index.min_value(page).copied(),
            index.max_value(page).copied(),
            None,
            None,
            false,
        ),
        ColumnIndexMetaData::INT64(index) => Statistics::int64(
            index.min_value(page).copied(),
            index.max_value(page).copied(),
            None,
            None,
            false,
        ),
        ColumnIndexMetaData::FLOAT(index) => Statistics::float(
            index.min_value(page).copied(),
            index.max_value(page).copied(),
            None,
            None,
            false,
        ),
        ColumnIndexMetaData::DOUBLE(index) => Statistics::double(
            index.min_value(page).copied(),
            index.max_value(page).copied(),
            None,
            None,
            false,
        ),
        ColumnIndexMetaData::BYTE_ARRAY(index) => Statistics::byte_array(
            index
                .min_value(page)
                .map(|bytes| ByteArray::from(bytes.to_vec())),
            index
                .max_value(page)
                .map(|bytes| ByteArray::from(bytes.to_vec())),
            None,
            None,
            false,
        ),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => Statistics::fixed_len_byte_array(
            index
                .min_value(page)
                .map(|bytes| FixedLenByteArray::from(bytes.to_vec())),
            index
                .max_value(page)
                .map(|bytes| FixedLenByteArray::from(bytes.to_vec())),
            None,
            None,
            false,
        ),
        _ => return None,
    })
}

/// Rows in both.
pub(crate) fn intersect(left: &[Range<usize>], right: &[Range<usize>]) -> Ranges {
    let mut out: Ranges = Vec::new();
    let (mut i, mut j) = (0, 0);
    while let (Some(a), Some(b)) = (left.get(i), right.get(j)) {
        let start = a.start.max(b.start);
        let end = a.end.min(b.end);
        if start < end {
            out.push(start..end);
        }
        if a.end < b.end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// Rows in either.
pub(crate) fn union(left: &[Range<usize>], right: &[Range<usize>]) -> Ranges {
    let mut all = left.iter().chain(right).cloned().collect::<Vec<_>>();
    all.sort_by_key(|range| range.start);
    let mut out: Ranges = Vec::new();
    for range in all {
        match out.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => out.push(range),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn combines_row_ranges() {
        assert_eq!(intersect(&[0..10, 20..30], &[5..25]), [5..10, 20..25]);
        assert_eq!(intersect(&[0..10], &[10..20]), Vec::<Range<usize>>::new());
        assert_eq!(union(&[0..5, 20..30], &[3..10, 30..31]), [0..10, 20..31]);
        assert_eq!(union(&[], &[1..2]), [1..2]);
    }
}
