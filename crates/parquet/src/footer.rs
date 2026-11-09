//! Rejects Parquet footers whose lists, sets or maps claim more elements than the footer has bytes.
//!
//! The footer (`FileMetaData`) is Thrift compact protocol. Every element of a list, set or map
//! takes at least one byte, so a valid footer can never claim more elements than it has bytes
//! left. The `parquet` crate skips unknown fields by looping over the *claimed* element count,
//! and skipping a bool element consumes no input, so a 9-byte unknown `list<bool>` field
//! claiming `i32::MAX` elements costs seconds of CPU (apache/arrow-rs issue pending). Walking
//! the footer first with this bound makes such files fail fast.
//!
//! This check is deliberately narrow: anything it cannot decode is left for the `parquet` crate
//! to report, so it never rejects a footer the real parser would accept.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use anyhow::{Result, bail};

/// Nesting deeper than this is left to the `parquet` crate, which has its own depth limit.
const MAX_DEPTH: u32 = 64;

/// Reads the footer of `file` and checks every collection's claimed size.
pub(crate) fn check_collection_sizes(file: &File) -> Result<()> {
    let Some(footer) = read_footer(file)? else {
        return Ok(());
    };
    match Walker::new(&footer).walk() {
        Err(Problem::Oversized {
            claimed,
            remaining,
            offset,
        }) => bail!(
            "malformed Parquet footer: a collection at footer byte {offset} claims {claimed} \
             elements but only {remaining} bytes remain"
        ),
        // Truncated or unknown encodings are the parquet crate's to report.
        Ok(()) | Err(Problem::Undecodable) => Ok(()),
    }
}

/// The plaintext footer bytes, or `None` when there is nothing this check can read
/// (too short, not `PAR1`, encrypted footer, or a length that doesn't fit the file).
fn read_footer(mut file: &File) -> Result<Option<Vec<u8>>> {
    let len = file.metadata()?.len();
    if len < 12 {
        return Ok(None);
    }
    let mut tail = [0_u8; 8];
    file.seek(SeekFrom::Start(len - 8))?;
    file.read_exact(&mut tail)?;
    if &tail[4..] != b"PAR1" {
        return Ok(None);
    }
    let footer_len = u64::from(u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]));
    if footer_len + 8 > len {
        return Ok(None);
    }
    let mut footer = vec![0_u8; usize::try_from(footer_len)?];
    file.seek(SeekFrom::Start(len - 8 - footer_len))?;
    file.read_exact(&mut footer)?;
    Ok(Some(footer))
}

#[derive(Debug, PartialEq, Eq)]
enum Problem {
    Oversized {
        claimed: u64,
        remaining: usize,
        offset: usize,
    },
    Undecodable,
}

struct Walker<'a> {
    buf: &'a [u8],
    pos: usize,
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

impl<'a> Walker<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn walk(&mut self) -> Result<(), Problem> {
        self.read_struct(0)
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn byte(&mut self) -> Result<u8, Problem> {
        let value = *self.buf.get(self.pos).ok_or(Problem::Undecodable)?;
        self.pos += 1;
        Ok(value)
    }

    fn varint(&mut self) -> Result<u64, Problem> {
        let mut result = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            result |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
        }
        Err(Problem::Undecodable)
    }

    fn skip(&mut self, bytes: u64) -> Result<(), Problem> {
        let bytes = usize::try_from(bytes).map_err(|_| Problem::Undecodable)?;
        if bytes > self.remaining() {
            return Err(Problem::Undecodable);
        }
        self.pos += bytes;
        Ok(())
    }

    /// Fails when a collection claims more elements than there are bytes left.
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

    fn read_struct(&mut self, depth: u32) -> Result<(), Problem> {
        if depth > MAX_DEPTH {
            return Err(Problem::Undecodable);
        }
        loop {
            let header = self.byte()?;
            if header == 0 {
                return Ok(());
            }
            if header >> 4 == 0 {
                self.varint()?; // long-form field id
            }
            match header & 0x0F {
                BOOL_TRUE | BOOL_FALSE => {} // value lives in the field header
                kind => self.read_value(kind, depth)?,
            }
        }
    }

    /// Reads one value of type `kind` (as written inside a list, set, map or struct field).
    fn read_value(&mut self, kind: u8, depth: u32) -> Result<(), Problem> {
        match kind {
            BOOL_TRUE | BOOL_FALSE | BYTE => self.skip(1),
            I16 | I32 | I64 => self.varint().map(|_| ()),
            DOUBLE => self.skip(8),
            BINARY => {
                let len = self.varint()?;
                self.skip(len)
            }
            LIST | SET => {
                let at = self.pos;
                let header = self.byte()?;
                let element = header & 0x0F;
                let claimed = match header >> 4 {
                    15 => self.varint()?,
                    short => u64::from(short),
                };
                self.check_size(claimed, at)?;
                for _ in 0..claimed {
                    self.read_value(element, depth + 1)?;
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
                    self.read_value(kinds >> 4, depth + 1)?;
                    self.read_value(kinds & 0x0F, depth + 1)?;
                }
                Ok(())
            }
            STRUCT => self.read_struct(depth + 1),
            UUID => self.skip(16),
            _ => Err(Problem::Undecodable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn walk(bytes: &[u8]) -> Result<(), Problem> {
        Walker::new(bytes).walk()
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

    #[test]
    fn leaves_truncated_or_unknown_input_to_the_parquet_crate() {
        assert_eq!(walk(&[]), Err(Problem::Undecodable));
        assert_eq!(walk(&field(BINARY)), Err(Problem::Undecodable));
        assert_eq!(walk(&[0x0E, 0xC8, 0x01, 0x00]), Err(Problem::Undecodable)); // type 14
    }
}
