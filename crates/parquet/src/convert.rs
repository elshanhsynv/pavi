//! CSV, JSON and Arrow IPC files are read into a temporary Parquet file when
//! opened, so every feature reads them the way it reads Parquet (ADR 0007).

use std::{
    cell::Cell,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::{
    page::PAGE_ROWS,
    source::{Cancelled, GuardedReader, panic_message},
    spill::TempFolder,
};

/// Largest temporary Parquet file a conversion may write.
pub const MAX_CONVERTED_BYTES: u64 = 32 * 1024 * 1024 * 1024;
/// CSV and JSON records read to guess the column types.
pub const MAX_INFER_ROWS: usize = 10_000;
/// Rows read and written at a time.
const BATCH_ROWS: usize = 8_192;
/// Rows per row group of the converted file, so filters can skip row groups.
const ROW_GROUP_ROWS: usize = 64 * PAGE_ROWS as usize;
/// A row group is written out once this much is buffered, whatever its rows.
const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
/// Bytes of a CSV file looked at to guess its delimiter.
const SNIFF_BYTES: u64 = 64 * 1024;
const CONVERT_PREFIX: &str = "pavi-open-";
/// Largest metadata message (schema, batch header, footer) read from an Arrow file.
pub const MAX_ARROW_MESSAGE_BYTES: u64 = 64 * 1024 * 1024;
/// Largest size one compressed Arrow buffer may claim to decompress to.
pub const MAX_ARROW_BUFFER_BYTES: i64 = 1024 * 1024 * 1024;

/// The kind of file a path holds, decided by its extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileFormat {
    Parquet,
    /// Comma, tab, semicolon or pipe separated, with a header row.
    Csv,
    /// Newline-delimited JSON objects, or one array of objects.
    Json,
    /// Arrow IPC: the file format (Feather v2) or the stream format.
    Arrow,
}

impl FileFormat {
    /// File extensions PAVI opens, for file dialogs.
    pub const EXTENSIONS: &[&str] = &[
        "parquet", "csv", "tsv", "json", "jsonl", "ndjson", "arrow", "feather", "ipc", "arrows",
    ];

    /// The format for `path`'s extension; anything not listed is read as Parquet.
    pub fn from_path(path: &Path) -> Self {
        let path = crate::remote::path_part(path);
        let extension = path
            .extension()
            .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        match extension.as_str() {
            "csv" | "tsv" => Self::Csv,
            "json" | "jsonl" | "ndjson" => Self::Json,
            "arrow" | "feather" | "ipc" | "arrows" => Self::Arrow,
            _ => Self::Parquet,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Parquet => "Parquet",
            Self::Csv => "CSV",
            Self::Json => "JSON",
            Self::Arrow => "Arrow",
        }
    }
}

/// A temporary Parquet copy of a file, deleted with this value.
#[derive(Debug)]
pub(crate) struct ConvertedFile {
    pub(crate) path: PathBuf,
    _folder: TempFolder,
}

/// A reader that remembers how far into the file it is.
struct Tracked<R> {
    inner: R,
    position: Rc<Cell<u64>>,
}

impl<R: Read> Read for Tracked<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.position
            .set(self.position.get().saturating_add(read as u64));
        Ok(read)
    }
}

impl<R: Seek> Seek for Tracked<R> {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let at = self.inner.seek(position)?;
        self.position.set(at);
        Ok(at)
    }
}

type Batches = Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>>>;

/// Reads `source` as `format` and writes it to a temporary Parquet file.
/// `checkpoint` is called before each batch with the share of the file read so
/// far (0 to 1); when it returns `true` the conversion ends with [`Cancelled`].
pub(crate) fn convert(
    source: &Path,
    format: FileFormat,
    checkpoint: &dyn Fn(f64) -> bool,
) -> Result<ConvertedFile> {
    let total = std::fs::metadata(source)
        .with_context(|| format!("open {}", source.display()))?
        .len()
        .max(1);
    let position = Rc::new(Cell::new(0_u64));
    // Each pass (sniffing, type inference, reading) starts from the beginning.
    let open = || -> Result<Tracked<File>> {
        position.set(0);
        Ok(Tracked {
            inner: File::open(source).with_context(|| format!("open {}", source.display()))?,
            position: Rc::clone(&position),
        })
    };
    // The arrow readers panic on some malformed input instead of returning an
    // error (found by the `other_formats` fuzz target); as for Parquet pages, a
    // panic becomes an error so callers outside the runtime are protected too.
    let readers = catch_unwind(AssertUnwindSafe(|| match format {
        FileFormat::Csv => csv_batches(source, &open),
        FileFormat::Json => json_batches(&open),
        FileFormat::Arrow => arrow_batches(open()?),
        FileFormat::Parquet => bail!("{} is a Parquet file", source.display()),
    }));
    let (schema, batches) = match readers {
        Ok(readers) => readers?,
        Err(payload) => bail!(
            "malformed {} data: {}",
            format.name(),
            panic_message(&*payload)
        ),
    };
    let batches = GuardedReader::new(batches);
    if schema.fields().is_empty() {
        bail!("{} has no columns", source.display());
    }
    let inferred = matches!(format, FileFormat::Csv | FileFormat::Json);

    let folder = TempFolder::create(CONVERT_PREFIX)?;
    let path = folder.path.join("converted.parquet");
    let file =
        File::create(&path).with_context(|| format!("create temporary file {}", path.display()))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))
        .context("this file has a column type Parquet cannot store")?;
    for batch in batches {
        if checkpoint(position.get() as f64 / total as f64) {
            return Err(Cancelled.into());
        }
        let batch = batch.map_err(|error| {
            if inferred {
                error.context(format!(
                    "column types were guessed from the first {MAX_INFER_ROWS} records, \
                     and a later value does not fit them"
                ))
            } else {
                error
            }
        })?;
        writer.write(&batch)?;
        if writer.in_progress_size() > MAX_BUFFERED_BYTES {
            writer.flush()?;
        }
        if writer.bytes_written() as u64 > MAX_CONVERTED_BYTES {
            bail!(
                "reading this file needs more than {} GB of temporary disk space",
                MAX_CONVERTED_BYTES / (1024 * 1024 * 1024)
            );
        }
    }
    writer
        .close()
        .context("finish the temporary Parquet file")?;
    Ok(ConvertedFile {
        path,
        _folder: folder,
    })
}

fn csv_batches(
    source: &Path,
    open: &dyn Fn() -> Result<Tracked<File>>,
) -> Result<(SchemaRef, Batches)> {
    let tab_separated = source
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("tsv"));
    let delimiter = if tab_separated {
        b'\t'
    } else {
        let mut head = Vec::new();
        open()?.take(SNIFF_BYTES).read_to_end(&mut head)?;
        sniff_delimiter(&head)
    };
    let format = arrow_csv::reader::Format::default()
        .with_header(true)
        .with_delimiter(delimiter);
    let (schema, _) = format
        .infer_schema(BufReader::new(open()?), Some(MAX_INFER_ROWS))
        .context("read the CSV header and guess column types")?;
    let schema = Arc::new(schema);
    let reader = arrow_csv::ReaderBuilder::new(Arc::clone(&schema))
        .with_format(format)
        .with_batch_size(BATCH_ROWS)
        .build(BufReader::new(open()?))?;
    Ok((schema, Box::new(reader)))
}

/// The most frequent of `,` `;` tab `|` outside quotes in the first line; `,`
/// when there is none.
fn sniff_delimiter(head: &[u8]) -> u8 {
    const CANDIDATES: [u8; 4] = *b",;\t|";
    let mut counts = [0_usize; 4];
    let mut quoted = false;
    for &byte in head {
        match byte {
            b'"' => quoted = !quoted,
            b'\n' | b'\r' if !quoted => break,
            _ if !quoted => {
                if let Some(position) = CANDIDATES.iter().position(|candidate| *candidate == byte) {
                    counts[position] += 1;
                }
            }
            _ => {}
        }
    }
    counts
        .iter()
        .enumerate()
        .max_by_key(|(position, count)| (**count, std::cmp::Reverse(*position)))
        .filter(|(_, count)| **count > 0)
        .map_or(b',', |(position, _)| CANDIDATES[position])
}

fn json_batches(open: &dyn Fn() -> Result<Tracked<File>>) -> Result<(SchemaRef, Batches)> {
    let mut head = Vec::new();
    open()?.take(SNIFF_BYTES).read_to_end(&mut head)?;
    let array = head
        .iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte == b'[');
    let lines = |file: Tracked<File>| -> Box<dyn BufRead> {
        if array {
            Box::new(BufReader::new(ArrayAsLines::new(file)))
        } else {
            Box::new(BufReader::new(file))
        }
    };
    let (schema, _) = arrow_json::reader::infer_json_schema(lines(open()?), Some(MAX_INFER_ROWS))
        .context("read the JSON records and guess column types")?;
    let schema = Arc::new(schema);
    let reader = arrow_json::ReaderBuilder::new(Arc::clone(&schema))
        .with_batch_size(BATCH_ROWS)
        .build(lines(open()?))?;
    Ok((schema, Box::new(reader)))
}

/// Turns one JSON array of values into whitespace-separated values, which the
/// JSON reader takes: the outer brackets become spaces and the commas between
/// elements become line breaks. Brackets and commas inside elements and inside
/// strings are kept.
struct ArrayAsLines<R> {
    inner: R,
    depth: u32,
    in_string: bool,
    escaped: bool,
}

impl<R> ArrayAsLines<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            depth: 0,
            in_string: false,
            escaped: false,
        }
    }
}

impl<R: Read> Read for ArrayAsLines<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        for byte in &mut buffer[..read] {
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if *byte == b'\\' {
                    self.escaped = true;
                } else if *byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }
            match *byte {
                b'"' => self.in_string = true,
                b'[' | b'{' => {
                    self.depth = self.depth.saturating_add(1);
                    if self.depth == 1 {
                        *byte = b' ';
                    }
                }
                b']' | b'}' => {
                    if self.depth == 1 {
                        *byte = b' ';
                    }
                    self.depth = self.depth.saturating_sub(1);
                }
                b',' if self.depth == 1 => *byte = b'\n',
                _ => {}
            }
        }
        Ok(read)
    }
}

/// Checks an Arrow file's layout before `arrow-ipc` reads it: every message and
/// block must lie inside the file, metadata messages must be at most
/// [`MAX_ARROW_MESSAGE_BYTES`], and compressed buffers may claim at most
/// [`MAX_ARROW_BUFFER_BYTES`] once decompressed.
///
/// `arrow-ipc` sizes its buffers from these lengths before reading them; a
/// decompression buffer that can't be allocated aborts the process (found by
/// the `other_formats` fuzz target).
fn check_arrow_layout<R: Read + Seek>(file: &mut R, length: u64, file_format: bool) -> Result<()> {
    if !file_format {
        let mut position = 0_u64;
        while position < length {
            let Some((message_start, message_length)) = message_prefix(file, position)? else {
                break;
            };
            let bytes = read_message(file, message_start, message_length, length)?;
            let message = arrow_ipc::root_as_message(&bytes)
                .map_err(|error| anyhow!("malformed Arrow message: {error}"))?;
            let body_start = message_start + message_length;
            let body_length = u64::try_from(message.bodyLength())
                .map_err(|_| anyhow!("malformed Arrow data: negative message body length"))?;
            let body_end = body_start
                .checked_add(body_length)
                .filter(|end| *end <= length)
                .ok_or_else(|| {
                    anyhow!("malformed Arrow data: a message body runs past the end of the file")
                })?;
            check_buffers(file, &bytes, body_start, body_length)?;
            position = body_end;
        }
        return Ok(());
    }
    // File format: ... <footer> <footer length: i32> "ARROW1".
    if length < 18 {
        bail!("malformed Arrow data: the file is too short");
    }
    let mut footer_length = [0_u8; 4];
    file.seek(SeekFrom::Start(length - 10))?;
    file.read_exact(&mut footer_length)?;
    let footer_length = u64::try_from(i32::from_le_bytes(footer_length))
        .ok()
        .filter(|footer_length| {
            *footer_length + 18 <= length && *footer_length <= MAX_ARROW_MESSAGE_BYTES
        })
        .ok_or_else(|| anyhow!("malformed Arrow data: the footer length is impossible"))?;
    let mut footer = vec![0_u8; footer_length as usize];
    file.seek(SeekFrom::Start(length - 10 - footer_length))?;
    file.read_exact(&mut footer)?;
    let footer = arrow_ipc::root_as_footer(&footer)
        .map_err(|error| anyhow!("malformed Arrow footer: {error}"))?;
    let blocks = footer
        .dictionaries()
        .into_iter()
        .flatten()
        .chain(footer.recordBatches().into_iter().flatten());
    for block in blocks {
        let (Ok(offset), Ok(metadata), Ok(body)) = (
            u64::try_from(block.offset()),
            u64::try_from(block.metaDataLength()),
            u64::try_from(block.bodyLength()),
        ) else {
            bail!("malformed Arrow data: a block has a negative offset or length");
        };
        let fits = offset
            .checked_add(metadata)
            .and_then(|end| end.checked_add(body))
            .is_some_and(|end| end <= length);
        if !fits || metadata == 0 || metadata > MAX_ARROW_MESSAGE_BYTES {
            bail!("malformed Arrow data: a record batch block runs past the end of the file");
        }
        let Some((message_start, message_length)) = message_prefix(file, offset)? else {
            bail!("malformed Arrow data: a record batch block is empty");
        };
        if message_start + message_length > offset + metadata {
            bail!("malformed Arrow data: a message is longer than its block");
        }
        let bytes = read_message(file, message_start, message_length, length)?;
        check_buffers(file, &bytes, offset + metadata, body)?;
    }
    Ok(())
}

/// Reads the length prefix of the message at `position`: where its metadata
/// starts and how long it is. `None` at the end-of-stream marker.
fn message_prefix<R: Read + Seek>(file: &mut R, position: u64) -> Result<Option<(u64, u64)>> {
    let mut word = [0_u8; 4];
    file.seek(SeekFrom::Start(position))?;
    if file.read_exact(&mut word).is_err() {
        return Ok(None);
    }
    let (start, length) = if word == [0xff; 4] {
        if file.read_exact(&mut word).is_err() {
            return Ok(None);
        }
        (position + 8, i32::from_le_bytes(word))
    } else {
        // Before Arrow 0.15 messages had no continuation marker.
        (position + 4, i32::from_le_bytes(word))
    };
    match u64::try_from(length) {
        Ok(0) => Ok(None),
        Ok(length) => Ok(Some((start, length))),
        Err(_) => bail!("malformed Arrow data: negative message length"),
    }
}

fn read_message<R: Read + Seek>(
    file: &mut R,
    start: u64,
    message_length: u64,
    file_length: u64,
) -> Result<Vec<u8>> {
    if message_length > MAX_ARROW_MESSAGE_BYTES
        || start
            .checked_add(message_length)
            .is_none_or(|end| end > file_length)
    {
        bail!("malformed Arrow data: a message runs past the end of the file");
    }
    let mut bytes = vec![0_u8; message_length as usize];
    file.seek(SeekFrom::Start(start))?;
    file.read_exact(&mut bytes)?;
    arrow_ipc::root_as_message(&bytes)
        .map_err(|error| anyhow!("malformed Arrow message: {error}"))?;
    Ok(bytes)
}

/// Checks that a batch message's buffers lie inside its body and, when it is
/// compressed, that no buffer claims more than [`MAX_ARROW_BUFFER_BYTES`].
fn check_buffers<R: Read + Seek>(
    file: &mut R,
    message: &[u8],
    body_start: u64,
    body_length: u64,
) -> Result<()> {
    let message = arrow_ipc::root_as_message(message)
        .map_err(|error| anyhow!("malformed Arrow message: {error}"))?;
    let batch = message.header_as_record_batch().or_else(|| {
        message
            .header_as_dictionary_batch()
            .and_then(|dictionary| dictionary.data())
    });
    let Some(batch) = batch else {
        return Ok(());
    };
    let compressed = batch.compression().is_some();
    for buffer in batch.buffers().into_iter().flatten() {
        let (Ok(offset), Ok(buffer_length)) = (
            u64::try_from(buffer.offset()),
            u64::try_from(buffer.length()),
        ) else {
            bail!("malformed Arrow data: a buffer has a negative offset or length");
        };
        if offset
            .checked_add(buffer_length)
            .is_none_or(|end| end > body_length)
        {
            bail!("malformed Arrow data: a buffer runs past its message body");
        }
        if compressed && buffer_length >= 8 {
            let mut size = [0_u8; 8];
            file.seek(SeekFrom::Start(body_start + offset))?;
            file.read_exact(&mut size)?;
            if i64::from_le_bytes(size) > MAX_ARROW_BUFFER_BYTES {
                bail!(
                    "an Arrow buffer claims to be {} bytes uncompressed; PAVI reads buffers of \
                     at most {} MB",
                    i64::from_le_bytes(size),
                    MAX_ARROW_BUFFER_BYTES / (1024 * 1024)
                );
            }
        }
    }
    Ok(())
}

fn arrow_batches(mut file: Tracked<File>) -> Result<(SchemaRef, Batches)> {
    let mut magic = Vec::new();
    (&mut file).take(6).read_to_end(&mut magic)?;
    file.seek(SeekFrom::Start(0))?;
    if magic.starts_with(b"FEA1") {
        bail!("this is a Feather version 1 file; save it again as Feather version 2 (Arrow IPC)");
    }
    let length = file.inner.metadata()?.len();
    check_arrow_layout(&mut file, length, magic == b"ARROW1")?;
    file.seek(SeekFrom::Start(0))?;
    if magic == b"ARROW1" {
        let reader = arrow_ipc::reader::FileReader::try_new_buffered(file, None)
            .context("read the Arrow file")?;
        Ok((reader.schema(), Box::new(reader)))
    } else {
        let reader = arrow_ipc::reader::StreamReader::try_new_buffered(file, None)
            .context("read the Arrow stream")?;
        Ok((reader.schema(), Box::new(reader)))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{ParquetSource, Projection, value::scalar_text};

    fn named(source: &ParquetSource, name: &str) -> Vec<String> {
        texts(source, source.schema().index_of(name).unwrap())
    }

    fn texts(source: &ParquetSource, column: usize) -> Vec<String> {
        let batch = source
            .read_window(0, 100, &Projection::all(source.column_count()))
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                scalar_text(batch.column(column).as_ref(), row)
                    .unwrap()
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect()
    }

    fn write(directory: &Path, name: &str, contents: &[u8]) -> PathBuf {
        let path = directory.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn decides_the_format_by_extension() {
        assert_eq!(FileFormat::from_path(Path::new("a.CSV")), FileFormat::Csv);
        assert_eq!(FileFormat::from_path(Path::new("a.tsv")), FileFormat::Csv);
        assert_eq!(
            FileFormat::from_path(Path::new("a.ndjson")),
            FileFormat::Json
        );
        assert_eq!(
            FileFormat::from_path(Path::new("a.feather")),
            FileFormat::Arrow
        );
        assert_eq!(
            FileFormat::from_path(Path::new("a.parquet")),
            FileFormat::Parquet
        );
        assert_eq!(
            FileFormat::from_path(Path::new("noext")),
            FileFormat::Parquet
        );
    }

    #[test]
    fn guesses_the_csv_delimiter_from_the_first_line() {
        assert_eq!(sniff_delimiter(b"a,b,c\n1;2;3;4;5"), b',');
        assert_eq!(sniff_delimiter(b"a;b;\"c,d,e\"\n"), b';');
        assert_eq!(sniff_delimiter(b"a\tb\n"), b'\t');
        assert_eq!(sniff_delimiter(b"a|b|c"), b'|');
        assert_eq!(sniff_delimiter(b"single"), b',');
    }

    #[test]
    fn opens_csv_files_with_inferred_types() {
        let directory = tempfile::tempdir().unwrap();
        let path = write(
            directory.path(),
            "people.csv",
            b"id;name;score;active;born\n1;Ann;1.5;true;2001-02-03\n2;\"Bo; Jr\";;false;\n",
        );
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.format(), FileFormat::Csv);
        assert_eq!(source.row_count(), 2);
        let schema = source.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(4).data_type(), &DataType::Date32);
        assert_eq!(texts(&source, 1), ["Ann", "Bo; Jr"]);
        assert_eq!(texts(&source, 2), ["1.5", "null"]);
        assert_eq!(
            source.total_bytes(),
            std::fs::metadata(&path).unwrap().len()
        );

        // The temporary copy goes away with the source.
        let converted = source.converted_path().unwrap().to_path_buf();
        assert!(converted.is_file());
        drop(source);
        assert!(!converted.exists());

        // A tab-separated file, and a file with only a header.
        let tsv = write(directory.path(), "t.tsv", b"a\tb\n1\tx,y\n");
        assert_eq!(texts(&ParquetSource::open(&tsv).unwrap(), 1), ["x,y"]);
        let empty = write(directory.path(), "e.csv", b"a,b\n");
        assert_eq!(ParquetSource::open(&empty).unwrap().row_count(), 0);
    }

    #[test]
    fn says_when_a_value_does_not_fit_the_guessed_type() {
        let directory = tempfile::tempdir().unwrap();
        let mut contents = b"n\n".to_vec();
        for value in 0..MAX_INFER_ROWS + 5 {
            writeln!(contents, "{value}").unwrap();
        }
        contents.extend_from_slice(b"oops\n");
        let path = write(directory.path(), "late.csv", &contents);
        let error = format!("{:#}", ParquetSource::open(&path).err().unwrap());
        assert!(error.contains("guessed from the first 10000"), "{error}");
    }

    #[test]
    fn opens_json_lines_and_json_arrays() {
        let directory = tempfile::tempdir().unwrap();
        let lines = write(
            directory.path(),
            "rows.jsonl",
            b"{\"id\": 1, \"tag\": \"a,b]\", \"point\": {\"x\": 1.5}}\n{\"id\": 2, \"tags\": [1, 2]}\n",
        );
        let source = ParquetSource::open(&lines).unwrap();
        assert_eq!(source.format(), FileFormat::Json);
        assert_eq!(source.row_count(), 2);
        assert_eq!(named(&source, "id"), ["1", "2"]);
        assert_eq!(named(&source, "tag"), ["a,b]", "null"]);

        let array = write(
            directory.path(),
            "rows.json",
            b" [ {\"id\": 1, \"note\": \"say \\\"hi\\\", [ok]\"},\n {\"id\": 2, \"list\": [3, 4]} ]\n",
        );
        let source = ParquetSource::open(&array).unwrap();
        assert_eq!(source.row_count(), 2);
        assert_eq!(named(&source, "note"), ["say \"hi\", [ok]", "null"]);
    }

    #[test]
    fn opens_arrow_files_and_streams_and_refuses_feather_v1() {
        let directory = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        let file_path = directory.path().join("rows.feather");
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            File::create(&file_path).unwrap(),
            &schema,
            options,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let source = ParquetSource::open(&file_path).unwrap();
        assert_eq!(source.format(), FileFormat::Arrow);
        assert_eq!(texts(&source, 1), ["a", "null", "c"]);

        let stream_path = directory.path().join("rows.arrows");
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(File::create(&stream_path).unwrap(), &schema)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            texts(&ParquetSource::open(&stream_path).unwrap(), 0),
            ["1", "2", "3"]
        );

        // A compressed buffer that claims to decompress to 64 GB is refused
        // before arrow-ipc allocates for it.
        let zeros = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![0; 4096]))],
        )
        .unwrap();
        let mut compressed = Vec::new();
        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            &mut compressed,
            &zeros.schema(),
            arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
                .unwrap(),
        )
        .unwrap();
        writer.write(&zeros).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let mut bytes = compressed;
        let claimed = 4096 * 4_u64; // uncompressed size of the id buffer
        let at = bytes
            .windows(8)
            .position(|window| window == claimed.to_le_bytes())
            .unwrap();
        bytes[at..at + 8].copy_from_slice(&(64_u64 << 30).to_le_bytes());
        let huge = write(directory.path(), "huge.feather", &bytes);
        let error = format!("{:#}", ParquetSource::open(&huge).err().unwrap());
        assert!(error.contains("uncompressed"), "{error}");
        // A file cut short.
        let short = write(directory.path(), "short.arrow", &bytes[..bytes.len() / 2]);
        assert!(ParquetSource::open(&short).is_err());

        let old = write(directory.path(), "old.feather", b"FEA1\0\0\0\0");
        let error = format!("{:#}", ParquetSource::open(&old).err().unwrap());
        assert!(error.contains("Feather version 1"), "{error}");

        // Cancelled while reading.
        assert!(
            ParquetSource::open_cancellable(&file_path, &|_| true)
                .err()
                .unwrap()
                .downcast_ref::<Cancelled>()
                .is_some()
        );
    }
}
