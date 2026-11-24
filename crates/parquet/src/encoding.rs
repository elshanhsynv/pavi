//! Text encodings of CSV and JSON files (ADR 0020). The arrow readers take
//! UTF-8 only, so other encodings are decoded to UTF-8 as the file is read.

use std::io::Read;

/// How a CSV or JSON file's text is encoded.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TextEncoding {
    /// UTF-8, with or without a byte order mark.
    Utf8,
    /// UTF-16, little-endian (what Windows calls "Unicode").
    Utf16Le,
    /// UTF-16, big-endian.
    Utf16Be,
    /// Windows-1252, the Western European code page; a superset of ISO-8859-1
    /// (Latin-1) for every printable character.
    Windows1252,
}

impl TextEncoding {
    pub const ALL: [Self; 4] = [Self::Utf8, Self::Utf16Le, Self::Utf16Be, Self::Windows1252];

    pub fn name(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Utf16Le => "UTF-16 LE",
            Self::Utf16Be => "UTF-16 BE",
            Self::Windows1252 => "Windows-1252",
        }
    }

    /// Reads a name as people write it: `utf-8`, `utf8`, `utf-16le`,
    /// `utf-16be`, `windows-1252`, `cp1252`, `latin1`, `iso-8859-1`, any case.
    pub fn parse(text: &str) -> Option<Self> {
        let key = text
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '_', ' '], "");
        Some(match key.as_str() {
            "utf8" => Self::Utf8,
            "utf16le" | "utf16" | "unicode" => Self::Utf16Le,
            "utf16be" => Self::Utf16Be,
            "windows1252" | "cp1252" | "latin1" | "iso88591" | "ansi" => Self::Windows1252,
            _ => return None,
        })
    }

    /// The encoding of a file that starts with `head`: a byte order mark if
    /// there is one; UTF-16 when every other byte of the start is zero (text
    /// mostly in ASCII); UTF-8 when `head` is valid UTF-8 (a character cut off
    /// at its end is allowed); Windows-1252 otherwise.
    pub fn detect(head: &[u8]) -> Self {
        if head.starts_with(&[0xEF, 0xBB, 0xBF]) {
            return Self::Utf8;
        }
        if head.starts_with(&[0xFF, 0xFE]) {
            return Self::Utf16Le;
        }
        if head.starts_with(&[0xFE, 0xFF]) {
            return Self::Utf16Be;
        }
        let sample = &head[..head.len().min(1024) & !1];
        if sample.len() >= 4 {
            let zeros = |offset: usize| {
                sample
                    .iter()
                    .skip(offset)
                    .step_by(2)
                    .filter(|b| **b == 0)
                    .count()
            };
            let half = sample.len() / 2;
            if zeros(1) * 10 >= half * 9 && zeros(0) * 10 < half {
                return Self::Utf16Le;
            }
            if zeros(0) * 10 >= half * 9 && zeros(1) * 10 < half {
                return Self::Utf16Be;
            }
        }
        match std::str::from_utf8(head) {
            Ok(_) => Self::Utf8,
            // A character cut off by the end of `head` is not an error.
            Err(error) if error.error_len().is_none() => Self::Utf8,
            Err(_) => Self::Windows1252,
        }
    }
}

/// The characters Windows-1252 puts at 0x80..=0x9F; the rest of the code page
/// is the same as Unicode's first 256 code points. Unassigned bytes become
/// U+FFFD.
const WINDOWS_1252_HIGH: [char; 32] = [
    '€', '\u{FFFD}', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '\u{FFFD}', 'Ž',
    '\u{FFFD}', '\u{FFFD}', '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '\u{FFFD}',
    'ž', 'Ÿ',
];

/// Reads `inner` in `encoding` and gives its text as UTF-8, without the byte
/// order mark. Bytes that aren't valid in the encoding become U+FFFD (UTF-8
/// input is passed through as it is; the arrow readers check it).
pub(crate) struct Decoded<R> {
    inner: R,
    encoding: TextEncoding,
    /// Raw bytes read but not decoded yet (an odd byte, half a surrogate pair).
    pending: Vec<u8>,
    /// Decoded UTF-8 not handed out yet.
    out: Vec<u8>,
    out_at: usize,
    /// Whether the start (and its byte order mark) has been looked at.
    started: bool,
    done: bool,
}

/// Raw bytes read from the inner reader at a time.
const CHUNK_BYTES: usize = 64 * 1024;

impl<R: Read> Decoded<R> {
    pub(crate) fn new(inner: R, encoding: TextEncoding) -> Self {
        Self {
            inner,
            encoding,
            pending: Vec::new(),
            out: Vec::new(),
            out_at: 0,
            started: false,
            done: false,
        }
    }

    /// Reads the next chunk and decodes what can be decoded of it.
    fn fill(&mut self) -> std::io::Result<()> {
        self.out.clear();
        self.out_at = 0;
        let mut chunk = vec![0_u8; CHUNK_BYTES];
        let read = self.inner.read(&mut chunk)?;
        if read == 0 {
            self.done = true;
        }
        self.pending.extend_from_slice(&chunk[..read]);
        if !self.started {
            // Wait for enough bytes to see a byte order mark.
            if self.pending.len() < 3 && !self.done {
                return Ok(());
            }
            self.started = true;
            let mark: &[u8] = match self.encoding {
                TextEncoding::Utf8 => &[0xEF, 0xBB, 0xBF],
                TextEncoding::Utf16Le => &[0xFF, 0xFE],
                TextEncoding::Utf16Be => &[0xFE, 0xFF],
                TextEncoding::Windows1252 => &[],
            };
            if !mark.is_empty() && self.pending.starts_with(mark) {
                self.pending.drain(..mark.len());
            }
        }
        match self.encoding {
            TextEncoding::Utf8 => self.out = std::mem::take(&mut self.pending),
            TextEncoding::Windows1252 => {
                let mut text = String::with_capacity(self.pending.len());
                for byte in self.pending.drain(..) {
                    text.push(match byte {
                        0x80..=0x9F => WINDOWS_1252_HIGH[usize::from(byte - 0x80)],
                        other => char::from(other),
                    });
                }
                self.out = text.into_bytes();
            }
            TextEncoding::Utf16Le | TextEncoding::Utf16Be => {
                let little = self.encoding == TextEncoding::Utf16Le;
                let mut units: Vec<u16> = self
                    .pending
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| {
                        if little {
                            u16::from_le_bytes(*pair)
                        } else {
                            u16::from_be_bytes(*pair)
                        }
                    })
                    .collect();
                let mut keep = self.pending.len() % 2;
                // A high surrogate at the end waits for its pair.
                if !self.done
                    && units
                        .last()
                        .is_some_and(|unit| (0xD800..0xDC00).contains(unit))
                {
                    units.pop();
                    keep += 2;
                }
                let text: String = char::decode_utf16(units)
                    .map(|unit| unit.unwrap_or(char::REPLACEMENT_CHARACTER))
                    .collect();
                self.out = text.into_bytes();
                let consumed = self.pending.len() - keep;
                self.pending.drain(..consumed);
                if self.done && !self.pending.is_empty() {
                    // An odd byte at the very end.
                    self.pending.clear();
                    self.out
                        .extend_from_slice(char::REPLACEMENT_CHARACTER.to_string().as_bytes());
                }
            }
        }
        Ok(())
    }
}

impl<R: Read> Read for Decoded<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        while self.out_at >= self.out.len() {
            if self.done {
                return Ok(0);
            }
            self.fill()?;
        }
        let available = &self.out[self.out_at..];
        let count = available.len().min(buffer.len());
        buffer[..count].copy_from_slice(&available[..count]);
        self.out_at += count;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8], encoding: TextEncoding) -> String {
        let mut text = String::new();
        Decoded::new(bytes, encoding)
            .read_to_string(&mut text)
            .unwrap();
        text
    }

    fn utf16(text: &str, little: bool, mark: bool) -> Vec<u8> {
        let mut bytes = Vec::new();
        if mark {
            bytes.extend(if little { [0xFF, 0xFE] } else { [0xFE, 0xFF] });
        }
        for unit in text.encode_utf16() {
            bytes.extend(if little {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            });
        }
        bytes
    }

    #[test]
    fn decodes_each_encoding_to_utf8_without_the_byte_order_mark() {
        let text = "id,name\n1,Zoë 😀 €\n";
        assert_eq!(decode(text.as_bytes(), TextEncoding::Utf8), text);
        let marked = [&[0xEF, 0xBB, 0xBF][..], text.as_bytes()].concat();
        assert_eq!(decode(&marked, TextEncoding::Utf8), text);
        for little in [true, false] {
            let encoding = if little {
                TextEncoding::Utf16Le
            } else {
                TextEncoding::Utf16Be
            };
            assert_eq!(decode(&utf16(text, little, true), encoding), text);
            assert_eq!(decode(&utf16(text, little, false), encoding), text);
        }
        assert_eq!(
            decode(b"caf\xe9 \x80 \x9c \x81", TextEncoding::Windows1252),
            "café € œ \u{FFFD}"
        );
    }

    #[test]
    fn decodes_utf16_split_across_chunks() {
        // A surrogate pair and an odd byte land on the chunk boundary.
        let mut text = "a".repeat(CHUNK_BYTES / 2 - 1);
        text.push('😀');
        text.push_str("tail");
        let bytes = utf16(&text, true, false);
        assert_eq!(decode(&bytes, TextEncoding::Utf16Le), text);
        let odd = [&bytes[..], &[0x41]].concat();
        assert!(decode(&odd, TextEncoding::Utf16Le).ends_with("tail\u{FFFD}"));
    }

    #[test]
    fn detects_the_encoding_from_the_start_of_the_file() {
        assert_eq!(TextEncoding::detect(b"a,b\n1,2\n"), TextEncoding::Utf8);
        assert_eq!(TextEncoding::detect("é".as_bytes()), TextEncoding::Utf8);
        // A two-byte character cut in half at the end of the sample.
        assert_eq!(
            TextEncoding::detect(&"aé".as_bytes()[..2]),
            TextEncoding::Utf8
        );
        assert_eq!(
            TextEncoding::detect(b"caf\xe9,1\n"),
            TextEncoding::Windows1252
        );
        assert_eq!(
            TextEncoding::detect(&utf16("id,name\n", true, true)),
            TextEncoding::Utf16Le
        );
        assert_eq!(
            TextEncoding::detect(&utf16("id,name\n", true, false)),
            TextEncoding::Utf16Le
        );
        assert_eq!(
            TextEncoding::detect(&utf16("id,name\n", false, false)),
            TextEncoding::Utf16Be
        );
        assert_eq!(
            TextEncoding::parse(" CP1252 "),
            Some(TextEncoding::Windows1252)
        );
        assert_eq!(TextEncoding::parse("utf-16le"), Some(TextEncoding::Utf16Le));
        assert_eq!(TextEncoding::parse("ebcdic"), None);
    }
}
