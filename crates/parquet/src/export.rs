//! Writers for exported rows: CSV, Parquet, JSON Lines and Arrow IPC.
//!
//! They write batch by batch to any [`Write`], so memory stays at one batch
//! (plus the Parquet writer's open row group). The app writes to a temporary
//! file and renames it; the CLI writes to standard output.

use std::io::Write;

use anyhow::{Result, bail};
use arrow_array::{Array, RecordBatch, cast::AsArray};
use arrow_schema::{DataType, SchemaRef};
use parquet::arrow::ArrowWriter;

use crate::value::{ScalarTexts, is_scalar_type, scalar_text};

/// Locks a partial output file (an export or `pavi-cli -o` written next to its
/// target, then renamed) for as long as `file` is open, so a later write to
/// the same target can tell one left by a killed process from one being
/// written (ADR 0017). Where the file system can't lock, nothing changes.
pub fn lock_partial_output(file: &std::fs::File) {
    let _ = file.try_lock();
}

/// Removes the partial output files a stopped export or `pavi-cli -o` left
/// next to `target`: `<name>.pavi-<n>.tmp` (the app) and
/// `<name>.pavi-cli-<n>.tmp` (the CLI) whose lock is free and that haven't
/// changed for a minute. Called before a new write to `target`.
pub fn remove_left_over_partial_outputs(target: &std::path::Path) {
    let Some(name) = target.file_name() else {
        return;
    };
    let prefix = format!("{}.pavi-", name.to_string_lossy());
    let folder = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => std::path::Path::new("."),
    };
    let Ok(entries) = std::fs::read_dir(folder) else {
        return;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let Some(middle) = file_name
            .strip_prefix(prefix.as_str())
            .and_then(|rest| rest.strip_suffix(".tmp"))
        else {
            continue;
        };
        let number = middle.strip_prefix("cli-").unwrap_or(middle);
        if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let old_enough = entry
            .metadata()
            .ok()
            .filter(std::fs::Metadata::is_file)
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| std::time::SystemTime::now().duration_since(modified).ok())
            .is_some_and(|age| age >= crate::spill::UNLOCKED_GRACE);
        if !old_enough {
            continue;
        }
        let path = entry.path();
        let unlocked = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .is_ok_and(|file| file.try_lock().is_ok());
        if unlocked {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Deepest nesting of lists, structs and maps written to JSON.
const MAX_JSON_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExportFormat {
    Csv,
    Parquet,
    /// One JSON object per line (JSON Lines).
    Json,
    /// Arrow IPC file format (Feather v2).
    Arrow,
}

impl ExportFormat {
    pub const ALL: [Self; 4] = [Self::Csv, Self::Parquet, Self::Json, Self::Arrow];

    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Parquet => "Parquet",
            Self::Json => "JSON Lines",
            Self::Arrow => "Arrow",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::Json => "jsonl",
            Self::Arrow => "arrow",
        }
    }
}

/// Writes batches of one schema in an [`ExportFormat`]. Call [`Self::finish`]
/// once at the end: the Parquet and Arrow formats end with a footer.
pub enum BatchWriter<W: Write + Send> {
    Csv(CsvWriter<W>),
    Parquet(Box<ArrowWriter<W>>),
    Json(JsonWriter<W>),
    Arrow(Box<arrow_ipc::writer::FileWriter<W>>),
}

impl<W: Write + Send> BatchWriter<W> {
    /// Starts the output (the CSV header line). Fails before writing anything
    /// when the format cannot hold `schema`, such as nested values in CSV.
    pub fn new(format: ExportFormat, output: W, schema: SchemaRef) -> Result<Self> {
        match format {
            ExportFormat::Csv => Ok(Self::Csv(CsvWriter::new(output, schema)?)),
            ExportFormat::Parquet => Ok(Self::Parquet(Box::new(ArrowWriter::try_new(
                output, schema, None,
            )?))),
            ExportFormat::Json => Ok(Self::Json(JsonWriter::new(output, schema))),
            ExportFormat::Arrow => Ok(Self::Arrow(Box::new(
                arrow_ipc::writer::FileWriter::try_new(output, &schema)?,
            ))),
        }
    }

    /// Tab-separated text: CSV's rules with a tab between fields (the CLI's
    /// `--format tsv`; not one of the app's export formats).
    pub fn tsv(output: W, schema: SchemaRef) -> Result<Self> {
        Ok(Self::Csv(CsvWriter::with_delimiter(output, schema, b'\t')?))
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::Csv(writer) => writer.write(batch),
            Self::Parquet(writer) => writer.write(batch).map_err(Into::into),
            Self::Json(writer) => writer.write(batch),
            Self::Arrow(writer) => writer.write(batch).map_err(Into::into),
        }
    }

    pub fn finish(self) -> Result<()> {
        match self {
            Self::Csv(mut writer) => writer.finish(),
            // `close` writes the footer and flushes the output; unlike
            // `into_inner` it keeps an I/O error as the error's source.
            Self::Parquet(writer) => {
                writer.close()?;
                Ok(())
            }
            Self::Json(mut writer) => writer.finish(),
            Self::Arrow(mut writer) => {
                writer.finish()?;
                writer.get_mut().flush()?;
                Ok(())
            }
        }
    }
}

/// JSON Lines: one object per row. Scalars use the shared value text, so they
/// read as the grid shows them: numbers and booleans as JSON numbers and
/// booleans, everything else as strings (binary as `0x` hex). Lists are arrays,
/// structs and maps are objects.
pub struct JsonWriter<W> {
    output: W,
    schema: SchemaRef,
    /// Each column's `"name":` (with the comma before it after the first),
    /// escaped once.
    keys: Vec<Vec<u8>>,
}

impl<W: Write> JsonWriter<W> {
    pub fn new(output: W, schema: SchemaRef) -> Self {
        let keys = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column, field)| {
                let mut key = Vec::new();
                if column > 0 {
                    key.push(b',');
                }
                // Writing a string to a Vec can't fail.
                let _ = serde_json::to_writer(&mut key, field.name());
                key.push(b':');
                key
            })
            .collect();
        Self {
            output,
            schema,
            keys,
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            bail!("export batch schema changed");
        }
        // Scalar columns are prepared once per batch; nested ones are walked
        // value by value.
        let columns = batch
            .columns()
            .iter()
            .map(|array| {
                let array = array.as_ref();
                Ok(if is_scalar_type(array.data_type()) {
                    JsonColumn::Scalar {
                        texts: ScalarTexts::new(array)?,
                        kind: json_kind(array.data_type()),
                        binary: is_binary(array.data_type()),
                    }
                } else {
                    JsonColumn::Nested(array)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut text = String::new();
        for row in 0..batch.num_rows() {
            self.output.write_all(b"{")?;
            for (column, key) in columns.iter().zip(&self.keys) {
                self.output.write_all(key)?;
                match column {
                    JsonColumn::Scalar {
                        texts,
                        kind,
                        binary,
                    } => {
                        text.clear();
                        if *binary {
                            text.push_str("0x");
                        }
                        if texts.write(row, &mut text)? {
                            write_json_scalar(&mut self.output, kind, &text)?;
                        } else {
                            self.output.write_all(b"null")?;
                        }
                    }
                    JsonColumn::Nested(array) => {
                        write_json_value(&mut self.output, *array, row, 0)?;
                    }
                }
            }
            self.output.write_all(b"}\n")?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.output.flush()?;
        Ok(())
    }
}

/// A batch column as [`JsonWriter`] writes it.
enum JsonColumn<'a> {
    Scalar {
        texts: ScalarTexts<'a>,
        kind: JsonKind,
        /// Binary text gets a `0x` before its hex digits.
        binary: bool,
    },
    Nested(&'a dyn Array),
}

/// A scalar's text as JSON: numbers that are JSON numbers and booleans bare,
/// everything else (NaN and infinity too) as a string.
fn write_json_scalar(output: &mut impl Write, kind: &JsonKind, text: &str) -> Result<()> {
    match kind {
        JsonKind::Number if is_json_number(text) => output.write_all(text.as_bytes())?,
        JsonKind::Boolean => output.write_all(text.as_bytes())?,
        JsonKind::Number | JsonKind::Text => serde_json::to_writer(&mut *output, text)?,
    }
    Ok(())
}

/// One value as JSON, as JSON Lines export writes it: numbers and booleans
/// bare, text quoted, nested values as arrays and objects, `null` for null.
pub fn json_value(array: &dyn Array, row: usize) -> Result<String> {
    let mut output = Vec::new();
    write_json_value(&mut output, array, row, 0)?;
    Ok(String::from_utf8(output)?)
}

fn write_json_value(
    output: &mut impl Write,
    array: &dyn Array,
    row: usize,
    depth: usize,
) -> Result<()> {
    if depth > MAX_JSON_DEPTH {
        bail!("JSON export supports values nested at most {MAX_JSON_DEPTH} levels deep");
    }
    if array.is_null(row) {
        output.write_all(b"null")?;
        return Ok(());
    }
    match array.data_type() {
        DataType::Struct(_) => {
            let values = array.as_struct();
            output.write_all(b"{")?;
            for (index, (field, column)) in values.fields().iter().zip(values.columns()).enumerate()
            {
                if index > 0 {
                    output.write_all(b",")?;
                }
                serde_json::to_writer(&mut *output, field.name())?;
                output.write_all(b":")?;
                write_json_value(output, column.as_ref(), row, depth + 1)?;
            }
            output.write_all(b"}")?;
            Ok(())
        }
        DataType::List(_) => {
            write_json_list(output, array.as_list::<i32>().value(row).as_ref(), depth)
        }
        DataType::LargeList(_) => {
            write_json_list(output, array.as_list::<i64>().value(row).as_ref(), depth)
        }
        DataType::FixedSizeList(..) => write_json_list(
            output,
            array.as_fixed_size_list().value(row).as_ref(),
            depth,
        ),
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            output.write_all(b"{")?;
            for entry in 0..entries.len() {
                if entry > 0 {
                    output.write_all(b",")?;
                }
                let key = scalar_text(entries.column(0).as_ref(), entry)?.unwrap_or_default();
                serde_json::to_writer(&mut *output, &key)?;
                output.write_all(b":")?;
                write_json_value(output, entries.column(1).as_ref(), entry, depth + 1)?;
            }
            output.write_all(b"}")?;
            Ok(())
        }
        data_type => {
            let Some(text) = scalar_text(array, row)? else {
                output.write_all(b"null")?;
                return Ok(());
            };
            let text = if is_binary(data_type) {
                format!("0x{text}")
            } else {
                text
            };
            write_json_scalar(output, &json_kind(data_type), &text)
        }
    }
}

fn write_json_list(output: &mut impl Write, items: &dyn Array, depth: usize) -> Result<()> {
    output.write_all(b"[")?;
    for item in 0..items.len() {
        if item > 0 {
            output.write_all(b",")?;
        }
        write_json_value(output, items, item, depth + 1)?;
    }
    output.write_all(b"]")?;
    Ok(())
}

enum JsonKind {
    Number,
    Boolean,
    Text,
}

fn json_kind(data_type: &DataType) -> JsonKind {
    match data_type {
        DataType::Boolean => JsonKind::Boolean,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => JsonKind::Number,
        DataType::Dictionary(_, values) => json_kind(values),
        DataType::RunEndEncoded(_, values) => json_kind(values.data_type()),
        _ => JsonKind::Text,
    }
}

/// `true` when `text` is a JSON number: `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
fn is_json_number(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut index = usize::from(bytes.first() == Some(&b'-'));
    let digits = |index: &mut usize| {
        let start = *index;
        while bytes.get(*index).is_some_and(u8::is_ascii_digit) {
            *index += 1;
        }
        *index - start
    };
    let integer_start = index;
    let integer = digits(&mut index);
    if integer == 0 || (integer > 1 && bytes[integer_start] == b'0') {
        return false;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        if digits(&mut index) == 0 {
            return false;
        }
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        if digits(&mut index) == 0 {
            return false;
        }
    }
    index == bytes.len()
}

/// CSV with a header line. Nulls are empty fields; fields holding a comma,
/// quote or line break are quoted.
pub struct CsvWriter<W> {
    output: W,
    schema: SchemaRef,
    /// `,` for CSV, a tab for TSV.
    delimiter: u8,
}

impl<W: Write> CsvWriter<W> {
    pub fn new(output: W, schema: SchemaRef) -> Result<Self> {
        Self::with_delimiter(output, schema, b',')
    }

    /// Delimited text with `delimiter` between fields, such as `b'\t'` for
    /// TSV. Fields holding the delimiter, a quote or a line break are quoted.
    pub fn with_delimiter(mut output: W, schema: SchemaRef, delimiter: u8) -> Result<Self> {
        if matches!(delimiter, b'"' | b'\n' | b'\r') || !delimiter.is_ascii() {
            bail!("{delimiter:?} can't separate delimited fields");
        }
        for field in schema.fields() {
            ensure_csv_type(field.data_type())?;
        }
        for (index, field) in schema.fields().iter().enumerate() {
            if index > 0 {
                output.write_all(&[delimiter])?;
            }
            write_csv_text(&mut output, field.name(), delimiter)?;
        }
        output.write_all(b"\n")?;
        Ok(Self {
            output,
            schema,
            delimiter,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            bail!("export batch schema changed");
        }
        // Each column is prepared once per batch; one buffer takes every value.
        let columns = batch
            .columns()
            .iter()
            .map(|array| {
                Ok((
                    ScalarTexts::new(array.as_ref())?,
                    is_binary(array.data_type()),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut text = String::new();
        for row in 0..batch.num_rows() {
            for (column, (texts, binary)) in columns.iter().enumerate() {
                if column > 0 {
                    self.output.write_all(&[self.delimiter])?;
                }
                text.clear();
                // Nulls are empty fields.
                if !texts.write(row, &mut text)? {
                    continue;
                }
                if *binary {
                    // `0x` and hex digits never need quotes.
                    self.output.write_all(b"0x")?;
                    self.output.write_all(text.as_bytes())?;
                } else {
                    write_csv_text(&mut self.output, &text, self.delimiter)?;
                }
            }
            self.output.write_all(b"\n")?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.output.flush()?;
        Ok(())
    }
}

fn ensure_csv_type(data_type: &DataType) -> Result<()> {
    if is_scalar_type(data_type) {
        Ok(())
    } else {
        bail!("CSV export does not support nested {data_type:?} values")
    }
}

fn is_binary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        DataType::Dictionary(_, values) => is_binary(values),
        DataType::RunEndEncoded(_, values) => is_binary(values.data_type()),
        _ => false,
    }
}

fn write_csv_text(output: &mut impl Write, value: &str, delimiter: u8) -> Result<()> {
    let quote = value
        .as_bytes()
        .iter()
        .any(|&byte| byte == delimiter || matches!(byte, b'"' | b'\n' | b'\r'));
    if quote {
        output.write_all(b"\"")?;
    }
    for fragment in value.split_inclusive('"') {
        output.write_all(fragment.as_bytes())?;
        if fragment.ends_with('"') {
            output.write_all(b"\"")?;
        }
    }
    if quote {
        output.write_all(b"\"")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, Float64Array, Int32Array, ListArray, RecordBatch, StringArray,
        StructArray, types::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn csv_and_json_lines_match_cell_by_cell_text_for_every_scalar_type() {
        let arrays = crate::value::tests::scalar_arrays();
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(index, array)| {
                    Field::new(format!("c{index} \"q\""), array.data_type().clone(), true)
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        // CSV, as it was written one `scalar_text` at a time.
        let mut expected = String::new();
        let header = schema
            .fields()
            .iter()
            .map(|field| format!("\"{}\"", field.name().replace('"', "\"\"")))
            .collect::<Vec<_>>();
        expected.push_str(&header.join(","));
        expected.push('\n');
        // Two batches: the second is the first again.
        for _ in 0..2 {
            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .map(|array| match scalar_text(array.as_ref(), row).unwrap() {
                        None => String::new(),
                        Some(text) if is_binary(array.data_type()) => format!("0x{text}"),
                        Some(text) if text.contains([',', '"', '\n', '\r']) => {
                            format!("\"{}\"", text.replace('"', "\"\""))
                        }
                        Some(text) => text,
                    })
                    .collect::<Vec<_>>();
                expected.push_str(&fields.join(","));
                expected.push('\n');
            }
        }
        let mut csv = Vec::new();
        let mut writer = CsvWriter::new(&mut csv, schema.clone()).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(String::from_utf8(csv).unwrap(), expected);

        // JSON Lines, against the per-value writer nested values still use.
        let mut expected = Vec::new();
        for row in 0..batch.num_rows() {
            expected.push(b'{');
            for (column, field) in schema.fields().iter().enumerate() {
                if column > 0 {
                    expected.push(b',');
                }
                serde_json::to_writer(&mut expected, field.name()).unwrap();
                expected.push(b':');
                write_json_value(&mut expected, batch.column(column).as_ref(), row, 0).unwrap();
            }
            expected.extend_from_slice(b"}\n");
        }
        let mut json = Vec::new();
        let mut writer = JsonWriter::new(&mut json, schema);
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(json).unwrap(),
            String::from_utf8(expected).unwrap()
        );
    }

    #[test]
    fn removes_only_unlocked_old_partial_outputs_of_the_same_target() {
        use std::fs::File;
        use std::time::{Duration, SystemTime};

        let folder = tempfile::tempdir().unwrap();
        let make = |name: &str, age_secs: u64| {
            let path = folder.path().join(name);
            let file = File::create(&path).unwrap();
            file.set_modified(SystemTime::now() - Duration::from_secs(age_secs))
                .unwrap();
            (path, file)
        };
        let (stopped_cli, file) = make("out.csv.pavi-cli-123.tmp", 120);
        drop(file);
        let (stopped_app, file) = make("out.csv.pavi-7.tmp", 120);
        drop(file);
        // A run still writing holds its lock, however old the file looks.
        let (writing, held) = make("out.csv.pavi-cli-456.tmp", 120);
        lock_partial_output(&held);
        // Under a minute old: may be a run that hasn't locked it yet.
        let (fresh, file) = make("out.csv.pavi-cli-789.tmp", 0);
        drop(file);
        // Not PAVI's partial output, or another target's.
        let (notes, file) = make("out.csv.pavi-notes.tmp", 120);
        drop(file);
        let (other, file) = make("other.csv.pavi-cli-1.tmp", 120);
        drop(file);
        let (target, file) = make("out.csv", 120);
        drop(file);

        remove_left_over_partial_outputs(&folder.path().join("out.csv"));
        assert!(!stopped_cli.exists(), "a killed CLI run's file is removed");
        assert!(!stopped_app.exists(), "a killed export's file is removed");
        assert!(writing.exists() && fresh.exists());
        assert!(notes.exists() && other.exists() && target.exists());
        drop(held);
    }

    #[test]
    fn writes_json_lines_with_the_grid_text_and_nested_values() {
        let point = StructArray::from(vec![(
            Arc::new(Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        )]);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec![Some("say \"hi\"\n"), None])) as ArrayRef,
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![1.5, f64::NAN])) as ArrayRef,
            ),
            (
                "bytes",
                Arc::new(BinaryArray::from(vec![b"\x01\xff".as_ref(), b"".as_ref()])) as ArrayRef,
            ),
            ("point", Arc::new(point) as ArrayRef),
            (
                "tags",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1), None]),
                    None,
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut output = Vec::new();
        let mut writer = JsonWriter::new(&mut output, batch.schema());
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(
            lines[0],
            r#"{"name":"say \"hi\"\n","score":1.5,"bytes":"0x01ff","point":{"x":1},"tags":[1,null]}"#
        );
        assert_eq!(
            lines[1],
            r#"{"name":null,"score":"NaN","bytes":"0x","point":{"x":null},"tags":null}"#
        );
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).unwrap();
        }
    }

    #[test]
    fn recognises_json_numbers() {
        for number in ["0", "-1", "1.5", "12.345", "1e300", "-2.5E-3"] {
            assert!(is_json_number(number), "{number}");
        }
        for text in ["", "-", "01", "1.", ".5", "+1", "NaN", "inf", "1e", "1x"] {
            assert!(!is_json_number(text), "{text}");
        }
    }

    #[test]
    fn rejects_nested_csv_values_before_writing_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        let mut output = Vec::new();
        assert!(BatchWriter::new(ExportFormat::Csv, &mut output, schema).is_err());
        assert!(output.is_empty());
    }

    #[test]
    fn writes_every_format_to_any_writer() {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )])
        .unwrap();
        for format in ExportFormat::ALL {
            let mut output = Vec::new();
            let mut writer = BatchWriter::new(format, &mut output, batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            match format {
                ExportFormat::Csv => assert_eq!(output, b"id\n1\n2\n"),
                ExportFormat::Json => assert_eq!(output, b"{\"id\":1}\n{\"id\":2}\n"),
                ExportFormat::Parquet => assert!(output.starts_with(b"PAR1")),
                ExportFormat::Arrow => assert!(output.starts_with(b"ARROW1")),
            }
        }
    }

    #[test]
    fn writes_tab_separated_text_quoting_what_needs_it() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec![
                    Some("a,b"),
                    Some("tab\there"),
                    Some("say \"hi\""),
                    None,
                ])) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut output = Vec::new();
        let mut writer = BatchWriter::tsv(&mut output, batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        // A comma needs no quotes in TSV; a tab and a quote do.
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "name\tid\na,b\t1\n\"tab\there\"\t2\n\"say \"\"hi\"\"\"\t3\n\t4\n"
        );
        assert!(CsvWriter::with_delimiter(Vec::new(), batch.schema(), b'"').is_err());
    }

    #[test]
    fn writes_readable_temporal_decimal_dictionary_and_binary_csv_values() {
        use arrow_array::{
            Date32Array, Decimal128Array, DictionaryArray, DurationSecondArray,
            TimestampMillisecondArray,
        };

        let dictionary: DictionaryArray<Int32Type> =
            vec![Some("red, green"), None].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "date",
                Arc::new(Date32Array::from(vec![Some(19_000), None])) as ArrayRef,
            ),
            (
                "naive",
                Arc::new(TimestampMillisecondArray::from(vec![Some(1_000), None])),
            ),
            (
                "zoned",
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(1_000), None])
                        .with_timezone("+01:00"),
                ),
            ),
            (
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345), None])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ),
            ("label", Arc::new(dictionary)),
            (
                "elapsed",
                Arc::new(DurationSecondArray::from(vec![Some(90), None])),
            ),
            (
                "bytes",
                Arc::new(BinaryArray::from(vec![Some(&[0xab_u8, 0x01][..]), None])),
            ),
        ])
        .unwrap();
        let mut output = Vec::new();
        let mut writer = CsvWriter::new(&mut output, batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            concat!(
                "date,naive,zoned,amount,label,elapsed,bytes\n",
                "2022-01-08,1970-01-01T00:00:01,1970-01-01T01:00:01+01:00,123.45,",
                "\"red, green\",PT90S,0xab01\n",
                ",,,,,,\n",
            )
        );
    }
}
