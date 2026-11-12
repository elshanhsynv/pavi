//! Checks size claims in Parquet's Thrift structures before the `parquet` crate acts on them.
//!
//! Footers (`FileMetaData`) and page headers are Thrift compact protocol. Several parts of the
//! `parquet` crate trust counts written in them, and a ~10 KB crafted file can make it spin or
//! allocate gigabytes (found by the `parquet_file` fuzz target; see docs/FUZZING.md):
//!
//! * skipping an unknown `list<bool>` loops over its *claimed* size without consuming input;
//! * a schema element's `num_children` sizes a `Vec::with_capacity` before the children are read;
//! * a dictionary page's `num_values` sizes the dictionary before its data is decoded.
//!
//! An allocation failure aborts the process, so these must be rejected before parsing. Each check
//! here only rejects claims that valid data can never make; anything it cannot decode is left to
//! the `parquet` crate to report, so it never rejects a file the real parser would accept.

use anyhow::{Result, bail};
use parquet::file::reader::ChunkReader;

/// Nesting deeper than this is left to the `parquet` crate, which has its own depth limit.
const MAX_DEPTH: u32 = 64;

/// Reads the footer of `file` and checks every collection's claimed size and every schema
/// element's claimed number of children.
pub(crate) fn check_footer(file: &impl ChunkReader) -> Result<()> {
    let Some(footer) = read_footer(file)? else {
        return Ok(());
    };
    match Reader::new(&footer).file_metadata() {
        Err(Problem::Oversized {
            claimed,
            remaining,
            offset,
        }) => bail!(
            "malformed Parquet footer: a collection at footer byte {offset} claims {claimed} \
             elements but only {remaining} bytes remain"
        ),
        Err(Problem::TooManyChildren { claimed, elements }) => bail!(
            "malformed Parquet footer: a schema element claims {claimed} children but the \
             schema has only {elements} elements"
        ),
        // Truncated or unknown encodings are the parquet crate's to report.
        Ok(()) | Err(Problem::Undecodable) => Ok(()),
    }
}

/// The plaintext footer bytes, or `None` when there is nothing this check can read
/// (too short, not `PAR1`, encrypted footer, or a length that doesn't fit the file).
fn read_footer(file: &impl ChunkReader) -> Result<Option<Vec<u8>>> {
    let len = file.len();
    if len < 12 {
        return Ok(None);
    }
    let tail = file.get_bytes(len - 8, 8)?;
    if &tail[4..] != b"PAR1" {
        return Ok(None);
    }
    let footer_len = u64::from(u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]));
    if footer_len + 8 > len {
        return Ok(None);
    }
    Ok(Some(
        file.get_bytes(len - 8 - footer_len, usize::try_from(footer_len)?)?
            .to_vec(),
    ))
}

/// The fields of a page header that size allocations.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PageHeaderSizes {
    /// `PageType` (2 = dictionary page).
    pub page_type: Option<i64>,
    pub uncompressed_size: Option<i64>,
    pub compressed_size: Option<i64>,
    /// `DictionaryPageHeader.num_values`, for dictionary pages.
    pub dictionary_values: Option<i64>,
}

pub(crate) const DICTIONARY_PAGE: i64 = 2;

/// Decodes the size fields of a Thrift `PageHeader` at the start of `bytes`.
/// Returns `None` if `bytes` doesn't hold a decodable header.
pub(crate) fn page_header_sizes(bytes: &[u8]) -> Option<PageHeaderSizes> {
    Reader::new(bytes).page_header().ok()
}

#[derive(Debug, PartialEq, Eq)]
enum Problem {
    Oversized {
        claimed: u64,
        remaining: usize,
        offset: usize,
    },
    TooManyChildren {
        claimed: i64,
        elements: u64,
    },
    Undecodable,
}

// Compact protocol type ids.
const BOOL_TRUE: u8 = 1;
const BOOL_FALSE: u8 = 2;
const BYTE: u8 = 3;
const I16: u8 = 4;
const I32: u8 = 5;
const I64: u8 = 6;
const DOUBLE: u8 = 7;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const SET: u8 = 10;
const MAP: u8 = 11;
const STRUCT: u8 = 12;
const UUID: u8 = 13;

// Field ids used by the checks.
const FILE_METADATA_SCHEMA: i64 = 2;
const SCHEMA_ELEMENT_NUM_CHILDREN: i64 = 5;
const PAGE_HEADER_TYPE: i64 = 1;
const PAGE_HEADER_UNCOMPRESSED_SIZE: i64 = 2;
const PAGE_HEADER_COMPRESSED_SIZE: i64 = 3;
const PAGE_HEADER_DICTIONARY: i64 = 7;
const DICTIONARY_HEADER_NUM_VALUES: i64 = 1;

/// A minimal Thrift compact-protocol reader that validates sizes as it goes.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn byte(&mut self) -> Result<u8, Problem> {
        let value = *self.buf.get(self.pos).ok_or(Problem::Undecodable)?;
        self.pos += 1;
        Ok(value)
    }

    /// Decodes a varint exactly like the `parquet` crate (`read_vlq`): any length, with the
    /// shift wrapping at 64 bits. Over-long encodings must decode to the same value it sees,
    /// or a crafted header could pass this check and still reach the parser with a huge count.
    fn varint(&mut self) -> Result<u64, Problem> {
        let mut result = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.byte()?;
            result |= u64::from(byte & 0x7F).wrapping_shl(shift);
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift = shift.wrapping_add(7);
        }
    }

    fn zigzag(&mut self) -> Result<i64, Problem> {
        let raw = self.varint()?;
        Ok((raw >> 1) as i64 ^ -((raw & 1) as i64))
    }

    /// A Thrift `i32`, truncated like the `parquet` crate's `read_i32`.
    fn i32(&mut self) -> Result<i64, Problem> {
        Ok(i64::from(self.zigzag()? as i32))
    }

    fn skip(&mut self, bytes: u64) -> Result<(), Problem> {
        let bytes = usize::try_from(bytes).map_err(|_| Problem::Undecodable)?;
        if bytes > self.remaining() {
            return Err(Problem::Undecodable);
        }
        self.pos += bytes;
        Ok(())
    }

    /// Reads the next struct field header: `None` at the stop byte, else `(field id, type)`.
    fn field(&mut self, last_id: &mut i64) -> Result<Option<(i64, u8)>, Problem> {
        let header = self.byte()?;
        if header == 0 {
            return Ok(None);
        }
        let delta = i64::from(header >> 4);
        let id = if delta == 0 {
            i64::from(self.zigzag()? as i16)
        } else {
            *last_id + delta
        };
        *last_id = id;
        Ok(Some((id, header & 0x0F)))
    }

    /// Reads a list or set header and checks its claimed size: `(element type, size)`.
    fn list_header(&mut self) -> Result<(u8, u64), Problem> {
        let at = self.pos;
        let header = self.byte()?;
        let claimed = match header >> 4 {
            15 => self.varint()?,
            short => u64::from(short),
        };
        self.check_size(claimed, at)?;
        Ok((header & 0x0F, claimed))
    }

    /// Valid data needs at least one byte per element, so a larger claim is impossible.
    fn check_size(&self, claimed: u64, header_at: usize) -> Result<(), Problem> {
        let remaining = self.remaining();
        if claimed > remaining as u64 {
            return Err(Problem::Oversized {
                claimed,
                remaining,
                offset: header_at,
            });
        }
        Ok(())
    }

    /// `FileMetaData`: every collection is checked, and so is each schema element's
    /// `num_children` against the number of schema elements.
    ///
    /// Like the `parquet` crate, known fields are read as their declared type whatever the wire
    /// type says; otherwise a mislabelled field would make the two parsers read different values.
    fn file_metadata(&mut self) -> Result<(), Problem> {
        let mut last = 0;
        while let Some((id, kind)) = self.field(&mut last)? {
            if id == FILE_METADATA_SCHEMA {
                let (element, elements) = self.list_header()?;
                if element != STRUCT {
                    return Err(Problem::Undecodable); // the parquet crate rejects this too
                }
                for _ in 0..elements {
                    self.schema_element(elements)?;
                }
            } else {
                self.field_value(kind, 0)?;
            }
        }
        Ok(())
    }

    /// `SchemaElement`, read field by field as the `parquet` crate does.
    fn schema_element(&mut self, elements: u64) -> Result<(), Problem> {
        let mut last = 0;
        while let Some((id, kind)) = self.field(&mut last)? {
            match id {
                SCHEMA_ELEMENT_NUM_CHILDREN => {
                    let claimed = self.i32()?;
                    if claimed < 0 || claimed as u64 > elements {
                        return Err(Problem::TooManyChildren { claimed, elements });
                    }
                }
                // type, type_length, repetition_type, converted_type, scale, precision, field_id
                1 | 2 | 3 | 6 | 7 | 8 | 9 => {
                    self.i32()?;
                }
                4 => {
                    let len = self.varint()?; // name
                    self.skip(len)?;
                }
                10 => self.value(STRUCT, 1)?, // logicalType
                _ => self.field_value(kind, 1)?,
            }
        }
        Ok(())
    }

    /// `PageHeader`, read field by field as the `parquet` crate does.
    fn page_header(&mut self) -> Result<PageHeaderSizes, Problem> {
        let mut sizes = PageHeaderSizes::default();
        let mut last = 0;
        while let Some((id, kind)) = self.field(&mut last)? {
            match id {
                PAGE_HEADER_TYPE => sizes.page_type = Some(self.i32()?),
                PAGE_HEADER_UNCOMPRESSED_SIZE => sizes.uncompressed_size = Some(self.i32()?),
                PAGE_HEADER_COMPRESSED_SIZE => sizes.compressed_size = Some(self.i32()?),
                4 => {
                    self.i32()?; // crc
                }
                PAGE_HEADER_DICTIONARY => {
                    let mut last = 0;
                    while let Some((id, kind)) = self.field(&mut last)? {
                        match id {
                            DICTIONARY_HEADER_NUM_VALUES => {
                                sizes.dictionary_values = Some(self.i32()?)
                            }
                            2 => {
                                self.i32()?; // encoding
                            }
                            3 if kind != BOOL_TRUE && kind != BOOL_FALSE => self.skip(1)?, // is_sorted
                            _ => self.field_value(kind, 1)?,
                        }
                    }
                }
                5 | 6 | 8 => self.value(STRUCT, 1)?, // data / index / data v2 page headers
                _ => self.field_value(kind, 0)?,
            }
        }
        Ok(sizes)
    }

    /// A struct field's value: bools live in the field header.
    fn field_value(&mut self, kind: u8, depth: u32) -> Result<(), Problem> {
        match kind {
            BOOL_TRUE | BOOL_FALSE => Ok(()),
            kind => self.value(kind, depth),
        }
    }

    /// One value of type `kind` as written inside a list, set, map or struct field.
    fn value(&mut self, kind: u8, depth: u32) -> Result<(), Problem> {
        if depth > MAX_DEPTH {
            return Err(Problem::Undecodable);
        }
        match kind {
            BOOL_TRUE | BOOL_FALSE | BYTE => self.skip(1),
            I16 | I32 | I64 => self.varint().map(|_| ()),
            DOUBLE => self.skip(8),
            BINARY => {
                let len = self.varint()?;
                self.skip(len)
            }
            LIST | SET => {
                let (element, claimed) = self.list_header()?;
                for _ in 0..claimed {
                    self.value(element, depth + 1)?;
                }
                Ok(())
            }
            MAP => {
                let at = self.pos;
                let claimed = self.varint()?;
                if claimed == 0 {
                    return Ok(());
                }
                let kinds = self.byte()?;
                self.check_size(claimed, at)?;
                for _ in 0..claimed {
                    self.value(kinds >> 4, depth + 1)?;
                    self.value(kinds & 0x0F, depth + 1)?;
                }
                Ok(())
            }
            STRUCT => {
                let mut last = 0;
                while let Some((_, kind)) = self.field(&mut last)? {
                    self.field_value(kind, depth + 1)?;
                }
                Ok(())
            }
            UUID => self.skip(16),
            _ => Err(Problem::Undecodable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn walk(bytes: &[u8]) -> Result<(), Problem> {
        Reader::new(bytes).file_metadata()
    }

    /// One long-form struct field: delta 0, `kind`, zigzag(id = 100).
    fn field(kind: u8) -> Vec<u8> {
        vec![kind, 0xC8, 0x01]
    }

    #[test]
    fn accepts_well_formed_values() {
        let mut footer = field(I32);
        footer.push(0x04); // i32 = 2
        footer.extend(field(BINARY));
        footer.extend([0x03, b'a', b'b', b'c']);
        footer.extend(field(LIST));
        footer.extend([0x31, 0x01, 0x02, 0x01]); // list<bool> of 3
        footer.extend(field(MAP));
        footer.extend([0x01, 0x85, 0x01, b'k', 0x02]); // map<binary, i32> { "k": 1 }
        footer.extend(field(STRUCT));
        footer.extend([0x15, 0x02, 0x00]); // { 1: i32 = 1 }
        footer.push(0x00);
        assert_eq!(walk(&footer), Ok(()));
    }

    #[test]
    fn rejects_a_bool_list_claiming_more_elements_than_bytes() {
        let mut footer = field(LIST);
        footer.extend([0xF1, 0xFF, 0xFF, 0xFF, 0xFF, 0x07]); // list<bool>, i32::MAX elements
        footer.push(0x00);
        assert_eq!(
            walk(&footer),
            Err(Problem::Oversized {
                claimed: i32::MAX as u64,
                remaining: 1,
                offset: 3,
            })
        );
    }

    #[test]
    fn rejects_an_oversized_map() {
        let mut footer = field(MAP);
        footer.extend([0xFF, 0xFF, 0x03, 0x11]); // 65535 entries of bool → bool
        footer.push(0x00);
        assert!(matches!(walk(&footer), Err(Problem::Oversized { .. })));
    }

    /// FileMetaData { 2: schema = [ { 5: num_children = n }, { } ] }
    fn schema_with_children(num_children: i64) -> Vec<u8> {
        let mut footer = vec![0x29, 0x2C]; // field 2 (delta 2, list), list<struct> of 2
        footer.push(0x55); // field 5 (delta 5, i32)
        let mut zigzag = ((num_children << 1) ^ (num_children >> 63)) as u64;
        while zigzag >= 0x80 {
            footer.push((zigzag as u8) | 0x80);
            zigzag >>= 7;
        }
        footer.push(zigzag as u8);
        footer.extend([0x00, 0x00, 0x00]); // end element 1, empty element 2, end FileMetaData
        footer
    }

    #[test]
    fn checks_schema_child_counts_against_the_schema_size() {
        assert_eq!(walk(&schema_with_children(1)), Ok(()));
        assert_eq!(
            walk(&schema_with_children(i32::MAX.into())),
            Err(Problem::TooManyChildren {
                claimed: i32::MAX.into(),
                elements: 2,
            })
        );
        assert!(matches!(
            walk(&schema_with_children(-1)),
            Err(Problem::TooManyChildren { claimed: -1, .. })
        ));
    }

    #[test]
    fn decodes_dictionary_page_header_sizes() {
        // { 1: type = 2, 2: uncompressed = 20, 3: compressed = 22, 7: { 1: num_values = 5, 2: encoding = 0 } }
        let header = [
            0x15, 0x04, 0x15, 0x28, 0x15, 0x2C, 0x4C, 0x15, 0x0A, 0x15, 0x00, 0x00, 0x00,
        ];
        assert_eq!(
            page_header_sizes(&header),
            Some(PageHeaderSizes {
                page_type: Some(DICTIONARY_PAGE),
                uncompressed_size: Some(20),
                compressed_size: Some(22),
                dictionary_values: Some(5),
            })
        );
        assert_eq!(page_header_sizes(&header[..5]), None);
    }

    /// Fuzzing found a dictionary page whose `num_values` was a 12-byte varint; the parquet
    /// crate decodes it (wrapping shift, truncated to i32) to ~1.08 billion.
    #[test]
    fn decodes_over_long_varints_like_the_parquet_crate() {
        // 0x7F × 9, then 0x83 0x00: bits beyond 64 wrap around instead of failing.
        let bytes = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x83, 0x00,
        ];
        let mut expected = 0_u64;
        for (index, byte) in bytes.iter().enumerate() {
            expected |= u64::from(byte & 0x7F).wrapping_shl(7 * index as u32);
        }
        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.varint(), Ok(expected));
        assert_eq!(reader.remaining(), 0);

        // A dictionary page header whose num_values uses that encoding is still decoded.
        let mut header = vec![0x15, 0x04, 0x15, 0x28, 0x15, 0x2C, 0x4C, 0x15];
        header.extend([
            0xFE, 0xFF, 0xFF, 0xFF, 0x87, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ]);
        header.extend([0x00, 0x00]);
        let sizes = page_header_sizes(&header).unwrap();
        let mut raw = 0_u64;
        for (index, byte) in [
            0xFE_u8, 0xFF, 0xFF, 0xFF, 0x87, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ]
        .iter()
        .enumerate()
        {
            raw |= u64::from(byte & 0x7F).wrapping_shl(7 * index as u32);
        }
        let zigzag = (raw >> 1) as i64 ^ -((raw & 1) as i64);
        assert_eq!(sizes.dictionary_values, Some(i64::from(zigzag as i32)));
    }

    #[test]
    fn leaves_truncated_or_unknown_input_to_the_parquet_crate() {
        assert_eq!(walk(&[]), Err(Problem::Undecodable));
        assert_eq!(walk(&field(BINARY)), Err(Problem::Undecodable));
        assert_eq!(walk(&[0x0E, 0xC8, 0x01, 0x00]), Err(Problem::Undecodable)); // type 14
    }
}
