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
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

pub use crate::encoding::TextEncoding;
use crate::{
    encoding::Decoded,
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

/// How to read a CSV or JSON file (ADR 0020). The default guesses everything.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ImportOptions {
    /// CSV: the byte between fields; `None` guesses it (a tab for `.tsv`,
    /// otherwise the most frequent of `,` `;` tab `|` in the first line).
    pub delimiter: Option<u8>,
    /// CSV: whether the first line names the columns. Without one, columns are
    /// named `column_1`, `column_2`, ….
    pub header: bool,
    /// The text encoding; `None` detects it ([`TextEncoding::detect`]).
    pub encoding: Option<TextEncoding>,
    /// Reads every column as text instead of guessing types (JSON: every
    /// column that isn't a list or an object).
    pub all_text: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            delimiter: None,
            header: true,
            encoding: None,
            all_text: false,
        }
    }
}

/// How a CSV or JSON file was actually read: the options with every guess
/// resolved, and what changed while it was read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportReport {
    pub format: FileFormat,
    /// CSV only.
    pub delimiter: Option<u8>,
    pub header: bool,
    pub encoding: TextEncoding,
    pub all_text: bool,
    /// Columns whose type was chosen again after reading every value, because
    /// a value past the first [`MAX_INFER_ROWS`] records didn't fit the guess.
    pub retyped: Vec<String>,
}

impl ImportReport {
    /// How the file was read, in one line: `CSV, ; between fields, header
    /// line, UTF-8`.
    pub fn summary(&self) -> String {
        let mut parts = vec![self.format.name().to_string()];
        if let Some(delimiter) = self.delimiter {
            parts.push(format!("{} between fields", delimiter_name(delimiter)));
            parts.push(
                if self.header {
                    "header line"
                } else {
                    "no header line"
                }
                .to_string(),
            );
        }
        parts.push(self.encoding.name().to_string());
        if self.all_text {
            parts.push("every column as text".to_string());
        }
        parts.join(", ")
    }

    /// The options that read the file this way again.
    pub fn options(&self) -> ImportOptions {
        ImportOptions {
            delimiter: self.delimiter,
            header: self.header,
            encoding: Some(self.encoding),
            all_text: self.all_text,
        }
    }
}

/// A delimiter as people name it: `\t` as "Tab", others as themselves.
pub fn delimiter_name(delimiter: u8) -> String {
    match delimiter {
        b'\t' => "Tab".to_string(),
        b' ' => "Space".to_string(),
        other => char::from(other).to_string(),
    }
}

/// A temporary Parquet copy of a file, deleted with this value.
#[derive(Debug)]
pub(crate) struct ConvertedFile {
    pub(crate) path: PathBuf,
    /// How a CSV or JSON file was read; `None` for Arrow.
    pub(crate) report: Option<ImportReport>,
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

/// A reader that reports progress and stops when cancelled, for passes that
/// read the whole file without handing out batches (type inference).
struct Watched<'a, R> {
    inner: R,
    position: Rc<Cell<u64>>,
    total: u64,
    checkpoint: &'a dyn Fn(f64) -> bool,
    cancelled: Rc<Cell<bool>>,
    /// Bytes read since the last checkpoint.
    since: u64,
}

impl<R: Read> Read for Watched<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.since >= 1024 * 1024 {
            self.since = 0;
            if (self.checkpoint)(self.position.get() as f64 / self.total as f64) {
                self.cancelled.set(true);
                return Err(std::io::Error::other(Cancelled));
            }
        }
        let read = self.inner.read(buffer)?;
        self.since += read as u64;
        Ok(read)
    }
}

/// A value that doesn't fit the column type it was read as; the context of
/// the reader's error.
#[derive(Debug)]
struct ValueMismatch;

impl std::fmt::Display for ValueMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "column types were guessed from the first {MAX_INFER_ROWS} records, and a later              value does not fit them"
        )
    }
}

type Batches = Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>>>;

/// Reads `source` as `format` and writes it to a temporary Parquet file.
/// `checkpoint` is called with the share of the file read so far (0 to 1);
/// when it returns `true` the conversion ends with [`Cancelled`].
pub(crate) fn convert(
    source: &Path,
    format: FileFormat,
    options: &ImportOptions,
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
    let progress = Progress {
        position: &position,
        total,
        checkpoint,
        cancelled: Rc::new(Cell::new(false)),
    };
    if format == FileFormat::Arrow {
        let (schema, batches) = guarded_readers(format, || arrow_batches(open()?))?;
        let path = write_parquet(source, format, schema, batches, &progress)?;
        return Ok(ConvertedFile {
            path: path.0,
            report: None,
            _folder: path.1,
        });
    }
    if format == FileFormat::Parquet {
        bail!("{} is a Parquet file", source.display());
    }

    let mut head = Vec::new();
    open()?.take(SNIFF_BYTES).read_to_end(&mut head)?;
    let encoding = options
        .encoding
        .unwrap_or_else(|| TextEncoding::detect(&head));
    let mut decoded_head = String::new();
    // Only the start is decoded: a character cut off at its end is replaced.
    let _ = Decoded::new(&head[..], encoding).read_to_string(&mut decoded_head);
    let mut text = TextReader {
        format,
        header: options.header,
        delimiter: None,
        json_array: false,
        all_text: options.all_text,
    };
    match format {
        FileFormat::Csv => {
            let tab_separated = source
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("tsv"));
            text.delimiter = Some(options.delimiter.unwrap_or_else(|| {
                if tab_separated {
                    b'\t'
                } else {
                    sniff_delimiter(decoded_head.as_bytes())
                }
            }));
        }
        _ => {
            text.json_array = decoded_head
                .bytes()
                .find(|byte| !byte.is_ascii_whitespace())
                .is_some_and(|byte| byte == b'[');
        }
    }
    let open_text = || -> Result<Decoded<Tracked<File>>> { Ok(Decoded::new(open()?, encoding)) };
    let report = |retyped: Vec<String>| ImportReport {
        format,
        delimiter: text.delimiter,
        header: text.header,
        encoding,
        all_text: text.all_text,
        retyped,
    };

    read_text(source, &text, &open_text, &progress, &report)
        .map_err(|error| encoding_hint(error, encoding))
}

/// When text isn't valid in the encoding it was read as, says to choose another.
fn encoding_hint(error: anyhow::Error, encoding: TextEncoding) -> anyhow::Error {
    let invalid = error
        .chain()
        .any(|cause| cause.to_string().to_ascii_lowercase().contains("utf-8"));
    if invalid && encoding == TextEncoding::Utf8 {
        error.context(
            "this file isn't valid UTF-8; read it with another encoding, such as \
             Windows-1252 or UTF-16",
        )
    } else {
        error
    }
}

/// Reads a CSV or JSON file in two passes at most: with column types guessed
/// from the first records, and again with types chosen from every value when
/// a later value doesn't fit them.
fn read_text(
    source: &Path,
    text: &TextReader,
    open_text: &dyn Fn() -> Result<Decoded<Tracked<File>>>,
    progress: &Progress<'_>,
    report: &dyn Fn(Vec<String>) -> ImportReport,
) -> Result<ConvertedFile> {
    let format = text.format;
    // First pass: column types guessed from the first records.
    let guessed = text.infer(open_text()?, Some(MAX_INFER_ROWS), progress)?;
    let first = guarded_readers(format, || {
        Ok((Arc::clone(&guessed), text.batches(open_text()?, &guessed)?))
    })
    .and_then(|(schema, batches)| write_parquet(source, format, schema, batches, progress));
    let error = match first {
        Ok((path, folder)) => {
            return Ok(ConvertedFile {
                path,
                report: Some(report(Vec::new())),
                _folder: folder,
            });
        }
        Err(error) => error,
    };
    if error.downcast_ref::<ValueMismatch>().is_none() || text.all_text {
        return Err(error);
    }
    // A later value didn't fit: choose the types again from every value, and
    // read the file again with them.
    let every = text
        .infer(open_text()?, None, progress)
        .context("read every value to choose the column types")?;
    let retyped = every
        .fields()
        .iter()
        .filter(|field| {
            guessed
                .field_with_name(field.name())
                .map_or(true, |old| old.data_type() != field.data_type())
        })
        .map(|field| field.name().clone())
        .collect();
    let (path, folder) = guarded_readers(format, || {
        Ok((Arc::clone(&every), text.batches(open_text()?, &every)?))
    })
    .and_then(|(schema, batches)| write_parquet(source, format, schema, batches, progress))
    .map_err(|error| {
        if error.downcast_ref::<ValueMismatch>().is_some() {
            error.context(
                "a value doesn't fit the column types chosen from every value; \
                     reading every column as text avoids this",
            )
        } else {
            error
        }
    })?;
    Ok(ConvertedFile {
        path,
        report: Some(report(retyped)),
        _folder: folder,
    })
}

/// Where a conversion is, for its progress and cancellation.
struct Progress<'a> {
    position: &'a Rc<Cell<u64>>,
    total: u64,
    checkpoint: &'a dyn Fn(f64) -> bool,
    /// Set when a watched reader stopped because the work was cancelled.
    cancelled: Rc<Cell<bool>>,
}

impl Progress<'_> {
    fn stop(&self) -> bool {
        (self.checkpoint)(self.position.get() as f64 / self.total as f64)
    }

    fn watch<R: Read>(&self, inner: R) -> Watched<'_, R> {
        Watched {
            inner,
            position: Rc::clone(self.position),
            total: self.total,
            checkpoint: self.checkpoint,
            cancelled: Rc::clone(&self.cancelled),
            since: 0,
        }
    }

    /// An inference error, or [`Cancelled`] when a watched reader stopped it.
    fn error(&self, error: ArrowError, context: &'static str) -> anyhow::Error {
        if self.cancelled.get() {
            Cancelled.into()
        } else {
            anyhow::Error::new(error).context(context)
        }
    }
}

/// The arrow readers panic on some malformed input instead of returning an
/// error (found by the `other_formats` fuzz target); as for Parquet pages, a
/// panic becomes an error so callers outside the runtime are protected too.
fn guarded_readers(
    format: FileFormat,
    build: impl FnOnce() -> Result<(SchemaRef, Batches)>,
) -> Result<(SchemaRef, Batches)> {
    match catch_unwind(AssertUnwindSafe(build)) {
        Ok(readers) => readers,
        Err(payload) => bail!(
            "malformed {} data: {}",
            format.name(),
            panic_message(&*payload)
        ),
    }
}

/// Writes `batches` to a new temporary Parquet file. A value that doesn't fit
/// its column type ends it with a [`ValueMismatch`] error.
fn write_parquet(
    source: &Path,
    format: FileFormat,
    schema: SchemaRef,
    batches: Batches,
    progress: &Progress<'_>,
) -> Result<(PathBuf, TempFolder)> {
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
        if progress.stop() {
            return Err(Cancelled.into());
        }
        let batch = match batch {
            Ok(batch) => batch,
            Err(error)
                if inferred
                    && error
                        .downcast_ref::<ArrowError>()
                        .is_some_and(is_value_mismatch) =>
            {
                return Err(error.context(ValueMismatch));
            }
            Err(error) => return Err(error),
        };
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
    Ok((path, folder))
}

/// A parse error for one value (CSV), or a JSON value of another type than its
/// column's, or a JSON field the guessed columns don't have.
fn is_value_mismatch(error: &ArrowError) -> bool {
    match error {
        ArrowError::ParseError(_) => true,
        ArrowError::JsonError(message) => {
            message.contains("expected") || message.contains("missing from schema")
        }
        _ => false,
    }
}

/// Reads a CSV or JSON file's decoded text.
struct TextReader {
    format: FileFormat,
    header: bool,
    /// CSV only.
    delimiter: Option<u8>,
    /// JSON only: one array of objects rather than one object per line.
    json_array: bool,
    all_text: bool,
}

impl TextReader {
    fn csv_format(&self) -> arrow_csv::reader::Format {
        arrow_csv::reader::Format::default()
            .with_header(self.header)
            .with_delimiter(self.delimiter.unwrap_or(b','))
    }

    fn json_lines<R: Read + 'static>(&self, reader: R) -> Box<dyn BufRead> {
        if self.json_array {
            Box::new(BufReader::new(ArrayAsLines::new(reader)))
        } else {
            Box::new(BufReader::new(reader))
        }
    }

    /// The column types, guessed from the first `records` (all when `None`).
    fn infer<R: Read>(
        &self,
        reader: R,
        records: Option<usize>,
        progress: &Progress<'_>,
    ) -> Result<SchemaRef> {
        let reader = progress.watch(reader);
        let inferred = catch_unwind(AssertUnwindSafe(|| -> Result<Schema> {
            Ok(match self.format {
                FileFormat::Json => {
                    let lines: Box<dyn BufRead + '_> = if self.json_array {
                        Box::new(BufReader::new(ArrayAsLines::new(reader)))
                    } else {
                        Box::new(BufReader::new(reader))
                    };
                    arrow_json::reader::infer_json_schema(lines, records)
                        .map(|(schema, _)| schema)
                        .map_err(|error| {
                            progress.error(error, "read the JSON records and guess column types")
                        })?
                }
                _ => self
                    .csv_format()
                    .infer_schema(BufReader::new(reader), records)
                    .map(|(schema, _)| schema)
                    .map_err(|error| {
                        progress.error(error, "read the CSV header and guess column types")
                    })?,
            })
        }));
        let schema = match inferred {
            Ok(schema) => schema?,
            Err(payload) => bail!(
                "malformed {} data: {}",
                self.format.name(),
                panic_message(&*payload)
            ),
        };
        if !self.all_text {
            return Ok(Arc::new(schema));
        }
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                if field.data_type().is_nested() {
                    field.as_ref().clone()
                } else {
                    field.as_ref().clone().with_data_type(DataType::Utf8)
                }
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(Schema::new(fields)))
    }

    fn batches<R: Read + 'static>(&self, reader: R, schema: &SchemaRef) -> Result<Batches> {
        Ok(match self.format {
            FileFormat::Json => Box::new(
                arrow_json::ReaderBuilder::new(Arc::clone(schema))
                    .with_batch_size(BATCH_ROWS)
                    // A field the guessed columns lack is an error, so the
                    // types are chosen again from every record, instead of
                    // the field being left out.
                    .with_strict_mode(true)
                    // Numbers and booleans in a text column are read as text.
                    .with_coerce_primitive(true)
                    .build(self.json_lines(reader))?,
            ),
            _ => Box::new(
                arrow_csv::ReaderBuilder::new(Arc::clone(schema))
                    .with_format(self.csv_format())
                    .with_batch_size(BATCH_ROWS)
                    .build(BufReader::new(reader))?,
            ),
        })
    }
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

    /// A CSV file of `rows` numbers under the header `n`, then `last`.
    fn late_value(directory: &Path, name: &str, last: &str) -> PathBuf {
        let mut contents = b"n,label\n".to_vec();
        for value in 0..MAX_INFER_ROWS + 5 {
            writeln!(contents, "{value},x").unwrap();
        }
        writeln!(contents, "{last},y").unwrap();
        write(directory, name, &contents)
    }

    fn open_with(path: &Path, options: ImportOptions) -> Result<ParquetSource> {
        ParquetSource::open_with_options(path, None, &options, &|_| false)
    }

    #[test]
    fn retypes_columns_when_a_later_value_does_not_fit() {
        let directory = tempfile::tempdir().unwrap();
        // Text after 10,000 numbers: the column becomes text.
        let path = late_value(directory.path(), "late.csv", "oops");
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(source.row_count(), MAX_INFER_ROWS as u64 + 6);
        let last = source
            .read_window(source.row_count() - 1, 1, &Projection::all(2))
            .unwrap();
        assert_eq!(
            scalar_text(last.column(0).as_ref(), 0).unwrap().unwrap(),
            "oops"
        );
        let report = source.import_report().unwrap();
        assert_eq!(report.retyped, ["n"]);

        // A fraction after whole numbers: the column becomes a float.
        let path = late_value(directory.path(), "fraction.csv", "2.5");
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(source.schema().field(1).data_type(), &DataType::Utf8);

        // A whole number too large for 64 bits: read as text.
        let path = late_value(directory.path(), "huge.csv", "99999999999999999999999");
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn retypes_json_and_keeps_fields_that_appear_late() {
        let directory = tempfile::tempdir().unwrap();
        let mut contents = Vec::new();
        for value in 0..MAX_INFER_ROWS + 5 {
            writeln!(contents, "{{\"id\": {value}}}").unwrap();
        }
        contents.extend_from_slice(b"{\"id\": \"A-7\", \"extra\": true}\n");
        let path = write(directory.path(), "late.jsonl", &contents);
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.schema().field(0).data_type(), &DataType::Utf8);
        // A field first seen after the guessed records is kept, not dropped.
        let extra = source.schema().index_of("extra").unwrap();
        let last = source
            .read_window(source.row_count() - 1, 1, &Projection::all(2))
            .unwrap();
        assert_eq!(
            scalar_text(last.column(0).as_ref(), 0).unwrap().unwrap(),
            "A-7"
        );
        assert_eq!(
            scalar_text(last.column(extra).as_ref(), 0)
                .unwrap()
                .unwrap(),
            "true"
        );
        let mut retyped = source.import_report().unwrap().retyped.clone();
        retyped.sort();
        assert_eq!(retyped, ["extra", "id"]);
    }

    #[test]
    fn reads_csv_with_the_options_asked_for() {
        let directory = tempfile::tempdir().unwrap();
        let path = write(directory.path(), "plain.txt", b"1|a;b|2.5\n2|c|3\n");
        let options = ImportOptions {
            delimiter: Some(b'|'),
            header: false,
            ..ImportOptions::default()
        };
        let source =
            ParquetSource::open_with_options(&path, Some(FileFormat::Csv), &options, &|_| false)
                .unwrap();
        let names = source
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, ["column_1", "column_2", "column_3"]);
        assert_eq!(texts(&source, 1), ["a;b", "c"]);
        assert_eq!(source.schema().field(0).data_type(), &DataType::Int64);
        let report = source.import_report().unwrap();
        assert_eq!(report.delimiter, Some(b'|'));
        assert!(!report.header);
        assert_eq!(report.encoding, TextEncoding::Utf8);
        assert_eq!(report.options().delimiter, Some(b'|'));

        // Every column as text keeps leading zeros.
        let codes = write(directory.path(), "codes.csv", b"zip,n\n02134,1\n");
        let source = open_with(
            &codes,
            ImportOptions {
                all_text: true,
                ..ImportOptions::default()
            },
        )
        .unwrap();
        assert_eq!(texts(&source, 0), ["02134"]);
        assert_eq!(source.schema().field(1).data_type(), &DataType::Utf8);

        // The guesses are reported.
        let semicolons = write(directory.path(), "s.csv", b"a;b\n1;2\n");
        let report = ParquetSource::open(&semicolons)
            .unwrap()
            .import_report()
            .unwrap()
            .clone();
        assert_eq!(report.delimiter, Some(b';'));
        assert!(report.retyped.is_empty());
    }

    #[test]
    fn reads_csv_in_other_text_encodings() {
        let directory = tempfile::tempdir().unwrap();
        // UTF-8 with a byte order mark: the first column's name has no mark.
        let marked = write(
            directory.path(),
            "bom.csv",
            "\u{feff}id,name\n1,Zoë\n".as_bytes(),
        );
        let source = ParquetSource::open(&marked).unwrap();
        assert_eq!(source.schema().field(0).name(), "id");
        assert_eq!(texts(&source, 1), ["Zoë"]);

        // UTF-16 as Excel's "Unicode text" writes it: tab-separated, with a mark.
        let mut utf16 = vec![0xFF, 0xFE];
        for unit in "id\tname\n1\tZoë €\n".encode_utf16() {
            utf16.extend(unit.to_le_bytes());
        }
        let path = write(directory.path(), "unicode.txt", &utf16);
        let source = ParquetSource::open_with_options(
            &path,
            Some(FileFormat::Csv),
            &ImportOptions::default(),
            &|_| false,
        )
        .unwrap();
        assert_eq!(texts(&source, 1), ["Zoë €"]);
        let report = source.import_report().unwrap();
        assert_eq!(report.encoding, TextEncoding::Utf16Le);
        assert_eq!(report.delimiter, Some(b'\t'));

        // Windows-1252, detected because it isn't valid UTF-8, or asked for.
        let latin = write(directory.path(), "latin.csv", b"city\nM\xfcnchen\n\x80 5\n");
        let source = ParquetSource::open(&latin).unwrap();
        assert_eq!(texts(&source, 0), ["München", "€ 5"]);
        assert_eq!(
            source.import_report().unwrap().encoding,
            TextEncoding::Windows1252
        );
        let asked = open_with(
            &write(directory.path(), "ascii.csv", b"a\nx\n"),
            ImportOptions {
                encoding: Some(TextEncoding::Windows1252),
                ..ImportOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            asked.import_report().unwrap().encoding,
            TextEncoding::Windows1252
        );

        // Arrow and Parquet files have no report.
        assert!(asked.import_report().is_some());
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
