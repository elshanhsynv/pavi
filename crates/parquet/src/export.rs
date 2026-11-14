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

use crate::value::{is_scalar_type, scalar_text};

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
}

impl<W: Write> JsonWriter<W> {
    pub fn new(output: W, schema: SchemaRef) -> Self {
        Self { output, schema }
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            bail!("export batch schema changed");
        }
        for row in 0..batch.num_rows() {
            self.output.write_all(b"{")?;
            for (column, field) in self.schema.fields().iter().enumerate() {
                if column > 0 {
                    self.output.write_all(b",")?;
                }
                serde_json::to_writer(&mut self.output, field.name())?;
                self.output.write_all(b":")?;
                write_json_value(&mut self.output, batch.column(column).as_ref(), row, 0)?;
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
            match json_kind(data_type) {
                JsonKind::Number if is_json_number(&text) => output.write_all(text.as_bytes())?,
                JsonKind::Boolean => output.write_all(text.as_bytes())?,
                // NaN and infinity are not JSON numbers; they are written as text.
                JsonKind::Number | JsonKind::Text => {
                    let text = if is_binary(data_type) {
                        format!("0x{text}")
                    } else {
                        text
                    };
                    serde_json::to_writer(&mut *output, &text)?;
                }
            }
            Ok(())
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
}

impl<W: Write> CsvWriter<W> {
    pub fn new(mut output: W, schema: SchemaRef) -> Result<Self> {
        for field in schema.fields() {
            ensure_csv_type(field.data_type())?;
        }
        for (index, field) in schema.fields().iter().enumerate() {
            if index > 0 {
                output.write_all(b",")?;
            }
            write_csv_text(&mut output, field.name())?;
        }
        output.write_all(b"\n")?;
        Ok(Self { output, schema })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            bail!("export batch schema changed");
        }
        for row in 0..batch.num_rows() {
            for column in 0..batch.num_columns() {
                if column > 0 {
                    self.output.write_all(b",")?;
                }
                write_csv_value(&mut self.output, batch.column(column).as_ref(), row)?;
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

/// Writes one CSV field using the shared scalar text; nulls are empty fields and
/// binary values keep the `0x`-prefixed hex form.
fn write_csv_value(output: &mut impl Write, array: &dyn Array, row: usize) -> Result<()> {
    let Some(text) = scalar_text(array, row)? else {
        return Ok(());
    };
    if is_binary(array.data_type()) {
        output.write_all(b"0x")?;
        output.write_all(text.as_bytes())?;
        return Ok(());
    }
    write_csv_text(output, &text)
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

fn write_csv_text(output: &mut impl Write, value: &str) -> Result<()> {
    let quote = value
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'));
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
