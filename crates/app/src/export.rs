use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow_array::{Array, RecordBatch, cast::AsArray};
use arrow_schema::{DataType, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet_reader::value::{is_scalar_type, scalar_text};
use pavi_query::{LogicalPlan, QueryEngine, QueryPoll};
use pavi_runtime::{CancellationToken, GenerationId, RuntimeHandle};

const EVENT_CAPACITY: usize = 1;
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

pub enum ExportInput {
    Plan(LogicalPlan),
    Batch(RecordBatch),
}

pub enum ExportEvent {
    Progress {
        generation: GenerationId,
        rows: u64,
    },
    Finished {
        generation: GenerationId,
        rows: u64,
    },
    Cancelled {
        generation: GenerationId,
    },
    Failed {
        generation: GenerationId,
        error: String,
    },
}

pub struct ExportTask {
    receiver: Receiver<ExportEvent>,
    cancellation: CancellationToken,
    path: PathBuf,
}

impl ExportTask {
    pub fn start(
        input: ExportInput,
        path: PathBuf,
        format: ExportFormat,
        runtime: RuntimeHandle,
        generation: GenerationId,
    ) -> std::io::Result<Self> {
        let cancellation = CancellationToken::default();
        let worker_cancellation = cancellation.clone();
        let (sender, receiver) = mpsc::sync_channel(EVENT_CAPACITY);
        let worker_path = path.clone();
        thread::Builder::new()
            .name("pavi-export".to_string())
            .spawn(move || {
                let result = export(
                    input,
                    &worker_path,
                    format,
                    runtime,
                    generation,
                    &worker_cancellation,
                    &sender,
                );
                let event = match result {
                    Ok(_rows) if worker_cancellation.is_cancelled() => {
                        ExportEvent::Cancelled { generation }
                    }
                    Ok(rows) => ExportEvent::Finished { generation, rows },
                    Err(_) if worker_cancellation.is_cancelled() => {
                        ExportEvent::Cancelled { generation }
                    }
                    Err(error) => ExportEvent::Failed {
                        generation,
                        error: format!("{error:#}"),
                    },
                };
                let _ = sender.send(event);
            })?;
        Ok(Self {
            receiver,
            cancellation,
            path,
        })
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn try_recv(&self) -> Result<Option<ExportEvent>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => bail!("export worker stopped without a result"),
        }
    }
}

/// The app's current export, if any, and the generation that tags its events, so
/// events from a cancelled or replaced export are ignored.
pub(crate) struct ExportState {
    task: Option<ExportTask>,
    generation: GenerationId,
}

impl Default for ExportState {
    fn default() -> Self {
        Self {
            task: None,
            generation: GenerationId(0),
        }
    }
}

impl ExportState {
    pub(crate) fn is_running(&self) -> bool {
        self.task.is_some()
    }

    /// Cancels the running export, if any, and moves to a new generation.
    /// Returns whether an export was running.
    pub(crate) fn cancel(&mut self) -> bool {
        let running = self.task.take().inspect(ExportTask::cancel).is_some();
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        running
    }

    /// Cancels any running export and starts a new one with a fresh generation.
    pub(crate) fn start(
        &mut self,
        start: impl FnOnce(GenerationId) -> std::io::Result<ExportTask>,
    ) -> std::io::Result<()> {
        self.cancel();
        self.task = Some(start(self.generation)?);
        Ok(())
    }

    /// Handles the next event of the current export and returns the status text to
    /// show, if any. Finished, cancelled and failed exports end the task; events
    /// from older generations are ignored.
    pub(crate) fn poll(&mut self) -> Option<String> {
        let task = self.task.as_ref()?;
        let event = match task.try_recv() {
            Ok(Some(event)) => event,
            Ok(None) => return None,
            Err(error) => {
                self.task = None;
                return Some(format!("Export: {error:#}"));
            }
        };
        let generation = match &event {
            ExportEvent::Progress { generation, .. }
            | ExportEvent::Finished { generation, .. }
            | ExportEvent::Cancelled { generation }
            | ExportEvent::Failed { generation, .. } => *generation,
        };
        if generation != self.generation {
            return None;
        }
        Some(match event {
            ExportEvent::Progress { rows, .. } => format!("Exporting: {rows} rows written"),
            ExportEvent::Finished { rows, .. } => {
                let path = self
                    .task
                    .take()
                    .map(|task| task.path().display().to_string())
                    .unwrap_or_default();
                format!("Export complete: {rows} rows written to {path}")
            }
            ExportEvent::Cancelled { .. } => {
                self.task = None;
                "Export cancelled".to_string()
            }
            ExportEvent::Failed { error, .. } => {
                self.task = None;
                format!("Export failed: {error}")
            }
        })
    }
}

fn export(
    input: ExportInput,
    path: &Path,
    format: ExportFormat,
    runtime: RuntimeHandle,
    generation: GenerationId,
    cancellation: &CancellationToken,
    sender: &SyncSender<ExportEvent>,
) -> Result<u64> {
    let temporary = temporary_path(path, generation);
    let result = export_temporary(
        input,
        &temporary,
        format,
        runtime,
        generation,
        cancellation,
        sender,
    );
    if result.is_err() || cancellation.is_cancelled() {
        let _ = fs::remove_file(&temporary);
        return result;
    }
    fs::rename(&temporary, path).with_context(|| format!("finalize export {}", path.display()))?;
    result
}

fn export_temporary(
    input: ExportInput,
    temporary: &Path,
    format: ExportFormat,
    runtime: RuntimeHandle,
    generation: GenerationId,
    cancellation: &CancellationToken,
    sender: &SyncSender<ExportEvent>,
) -> Result<u64> {
    let (schema, mut batches): (SchemaRef, Box<dyn BatchSource>) = match input {
        ExportInput::Plan(plan) => {
            let engine = QueryEngine::from_handle(runtime);
            let schema = engine.output_schema(&plan)?;
            (
                schema,
                Box::new(QueryBatches::new(engine.execute(&plan, generation)?)),
            )
        }
        ExportInput::Batch(batch) => {
            let schema = batch.schema();
            (schema, Box::new(OneBatch(Some(batch))))
        }
    };
    let file = File::create(temporary)
        .with_context(|| format!("create export {}", temporary.display()))?;
    let mut writer = OutputWriter::new(format, BufWriter::new(file), schema)?;
    let mut rows = 0_u64;
    while let Some(batch) = batches.next(cancellation)? {
        if cancellation.is_cancelled() {
            bail!("export cancelled");
        }
        writer.write(&batch)?;
        rows = rows.saturating_add(batch.num_rows() as u64);
        if !send_progress(sender, generation, rows) {
            bail!("export receiver dropped");
        }
    }
    if cancellation.is_cancelled() {
        bail!("export cancelled");
    }
    writer.finish()?;
    Ok(rows)
}

fn temporary_path(path: &Path, generation: GenerationId) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("export");
    path.with_file_name(format!("{name}.pavi-{}.tmp", generation.0))
}

fn send_progress(sender: &SyncSender<ExportEvent>, generation: GenerationId, rows: u64) -> bool {
    match sender.try_send(ExportEvent::Progress { generation, rows }) {
        Ok(()) | Err(TrySendError::Full(_)) => true,
        Err(TrySendError::Disconnected(_)) => false,
    }
}

trait BatchSource: Send {
    fn next(&mut self, cancellation: &CancellationToken) -> Result<Option<RecordBatch>>;
}

struct OneBatch(Option<RecordBatch>);

impl BatchSource for OneBatch {
    fn next(&mut self, cancellation: &CancellationToken) -> Result<Option<RecordBatch>> {
        if cancellation.is_cancelled() {
            bail!("export cancelled");
        }
        Ok(self.0.take())
    }
}

struct QueryBatches(pavi_query::QueryExecution);

impl QueryBatches {
    fn new(execution: pavi_query::QueryExecution) -> Self {
        Self(execution)
    }
}

impl BatchSource for QueryBatches {
    fn next(&mut self, cancellation: &CancellationToken) -> Result<Option<RecordBatch>> {
        loop {
            if cancellation.is_cancelled() {
                self.0.cancel();
                bail!("export cancelled");
            }
            match self.0.poll_next_batch()? {
                QueryPoll::Batch(batch) => return Ok(Some(batch.batch)),
                QueryPoll::Finished => return Ok(None),
                QueryPoll::Pending => thread::sleep(Duration::from_millis(5)),
            }
        }
    }
}

enum OutputWriter {
    Csv(CsvWriter<BufWriter<File>>),
    Parquet(Box<ArrowWriter<BufWriter<File>>>),
    Json(JsonWriter<BufWriter<File>>),
    Arrow(Box<arrow_ipc::writer::FileWriter<BufWriter<File>>>),
}

impl OutputWriter {
    fn new(format: ExportFormat, writer: BufWriter<File>, schema: SchemaRef) -> Result<Self> {
        match format {
            ExportFormat::Csv => Ok(Self::Csv(CsvWriter::new(writer, schema)?)),
            ExportFormat::Parquet => Ok(Self::Parquet(Box::new(ArrowWriter::try_new(
                writer, schema, None,
            )?))),
            ExportFormat::Json => Ok(Self::Json(JsonWriter::new(writer, schema))),
            ExportFormat::Arrow => Ok(Self::Arrow(Box::new(
                arrow_ipc::writer::FileWriter::try_new(writer, &schema)?,
            ))),
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::Csv(writer) => writer.write(batch),
            Self::Parquet(writer) => writer.write(batch).map_err(Into::into),
            Self::Json(writer) => writer.write(batch),
            Self::Arrow(writer) => writer.write(batch).map_err(Into::into),
        }
    }

    fn finish(self) -> Result<()> {
        match self {
            Self::Csv(mut writer) => writer.finish(),
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
struct JsonWriter<W> {
    output: W,
    schema: SchemaRef,
}

impl<W: Write> JsonWriter<W> {
    fn new(output: W, schema: SchemaRef) -> Self {
        Self { output, schema }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
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

    fn finish(&mut self) -> Result<()> {
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

struct CsvWriter<W> {
    output: W,
    schema: SchemaRef,
}

impl<W: Write> CsvWriter<W> {
    fn new(mut output: W, schema: SchemaRef) -> Result<Self> {
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

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
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

    fn finish(&mut self) -> Result<()> {
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
mod json_tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, Float64Array, Int32Array, ListArray, RecordBatch, StringArray,
        StructArray, types::Int32Type,
    };
    use arrow_schema::{DataType, Field};

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
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use parquet_reader::ParquetSource;
    use pavi_runtime::{Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;

    fn source(rows: usize, values: Vec<Option<&str>>) -> (TempDir, Arc<ParquetSource>) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("input.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
        ]));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(2))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from_iter((0..rows).map(|row| Some(row as i32)))),
                        Arc::new(StringArray::from(values)),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        (directory, Arc::new(ParquetSource::open(path).unwrap()))
    }

    fn wait(task: &ExportTask) -> ExportEvent {
        for _ in 0..2_000 {
            if let Some(event) = task.try_recv().unwrap()
                && !matches!(event, ExportEvent::Progress { .. })
            {
                return event;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("export did not finish")
    }

    fn export_plan(source: Arc<ParquetSource>, path: PathBuf, format: ExportFormat) -> ExportEvent {
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let task = ExportTask::start(
            ExportInput::Plan(LogicalPlan::scan(source)),
            path,
            format,
            runtime.handle(),
            GenerationId(1),
        )
        .unwrap();
        wait(&task)
    }

    #[test]
    fn writes_escaped_csv_with_nulls_and_multiple_batches() {
        let (_directory, source) = source(
            5,
            vec![
                Some("a,b"),
                Some("say \"hi\""),
                None,
                Some("line\nbreak"),
                Some("ok"),
            ],
        );
        assert_eq!(source.row_groups().len(), 3);
        let output = tempfile::NamedTempFile::new().unwrap();
        let event = export_plan(source, output.path().to_path_buf(), ExportFormat::Csv);
        assert!(matches!(event, ExportEvent::Finished { rows: 5, .. }));
        assert_eq!(
            fs::read_to_string(output.path()).unwrap(),
            "id,text\n0,\"a,b\"\n1,\"say \"\"hi\"\"\"\n2,\n3,\"line\nbreak\"\n4,ok\n"
        );
    }

    #[test]
    fn writes_parquet_with_the_input_schema_and_order() {
        let (_directory, source) = source(3, vec![Some("a"), None, Some("c")]);
        let output = tempfile::NamedTempFile::new().unwrap();
        let event = export_plan(source, output.path().to_path_buf(), ExportFormat::Parquet);
        assert!(matches!(event, ExportEvent::Finished { rows: 3, .. }));
        let exported = ParquetSource::open(output.path()).unwrap();
        assert_eq!(exported.schema().fields()[0].name(), "id");
        assert_eq!(exported.schema().fields()[1].name(), "text");
        assert_eq!(
            exported
                .read_page(0, &parquet_reader::Projection::all(2))
                .unwrap()
                .window
                .row_count,
            3
        );
    }

    #[test]
    fn writes_json_lines_and_arrow_that_open_again() {
        let (_directory, source) = source(3, vec![Some("a"), None, Some("c")]);
        let output = tempfile::tempdir().unwrap();
        for format in [ExportFormat::Json, ExportFormat::Arrow] {
            let path = output.path().join(format!("out.{}", format.extension()));
            let event = export_plan(Arc::clone(&source), path.clone(), format);
            assert!(
                matches!(event, ExportEvent::Finished { rows: 3, .. }),
                "{format:?}"
            );
            let exported = ParquetSource::open(&path).unwrap();
            assert_eq!(exported.row_count(), 3, "{format:?}");
            let batch = exported
                .read_window(0, 3, &parquet_reader::Projection::all(2))
                .unwrap();
            let text = (0..3)
                .map(|row| scalar_text(batch.column(1).as_ref(), row).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                text,
                [Some("a".to_string()), None, Some("c".to_string())],
                "{format:?}"
            );
        }
        // JSON Lines, one object per row.
        assert_eq!(
            fs::read_to_string(output.path().join("out.jsonl")).unwrap(),
            "{\"id\":0,\"text\":\"a\"}\n{\"id\":1,\"text\":null}\n{\"id\":2,\"text\":\"c\"}\n"
        );
    }

    #[test]
    fn writes_empty_query_headers_and_filtered_results() {
        let (_directory, source) = source(3, vec![Some("a"), Some("b"), Some("c")]);
        let output = tempfile::NamedTempFile::new().unwrap();
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let plan = LogicalPlan::scan(source).filter(pavi_query::Filter::parse("id > 9").unwrap());
        let task = ExportTask::start(
            ExportInput::Plan(plan),
            output.path().to_path_buf(),
            ExportFormat::Csv,
            runtime.handle(),
            GenerationId(3),
        )
        .unwrap();
        assert!(matches!(wait(&task), ExportEvent::Finished { rows: 0, .. }));
        assert_eq!(fs::read_to_string(output.path()).unwrap(), "id,text\n");
    }

    #[test]
    fn exports_sql_plans_and_reports_bounded_progress() {
        let (_directory, source) = source(3, vec![Some("a"), Some("b"), Some("c")]);
        let output = tempfile::NamedTempFile::new().unwrap();
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let plan = pavi_query::SqlAst::parse("SELECT text FROM dataset WHERE id >= 1")
            .unwrap()
            .to_logical_plan(source)
            .unwrap();
        let task = ExportTask::start(
            ExportInput::Plan(plan),
            output.path().to_path_buf(),
            ExportFormat::Csv,
            runtime.handle(),
            GenerationId(7),
        )
        .unwrap();
        let progress = loop {
            if let Some(ExportEvent::Progress { generation, rows }) = task.try_recv().unwrap() {
                break (generation, rows);
            }
            thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(progress, (GenerationId(7), 2));
        assert!(matches!(wait(&task), ExportEvent::Finished { rows: 2, .. }));
        assert_eq!(fs::read_to_string(output.path()).unwrap(), "text\nb\nc\n");
        assert_eq!(EVENT_CAPACITY, 1);
    }

    #[test]
    fn cancellation_and_write_failure_remove_incomplete_output() {
        let (_directory, large_source) = source(1, vec![Some("x")]);
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("cancel.csv");
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let task = ExportTask::start(
            ExportInput::Plan(LogicalPlan::scan(large_source)),
            path.clone(),
            ExportFormat::Csv,
            runtime.handle(),
            GenerationId(4),
        )
        .unwrap();
        task.cancel();
        assert!(matches!(wait(&task), ExportEvent::Cancelled { .. }));
        assert!(!path.exists());
        assert!(!temporary_path(&path, GenerationId(4)).exists());

        let missing = directory.path().join("missing").join("fail.csv");
        let (_directory, source) = source(1, vec![Some("x")]);
        assert!(matches!(
            export_plan(source, missing.clone(), ExportFormat::Csv),
            ExportEvent::Failed { .. }
        ));
        assert!(!temporary_path(&missing, GenerationId(1)).exists());
    }

    #[test]
    fn exports_a_selected_batch_without_replaying_the_dataset() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("picked")]))],
        )
        .unwrap();
        let output = tempfile::NamedTempFile::new().unwrap();
        let runtime = Runtime::new(RuntimeConfig::default()).unwrap();
        let task = ExportTask::start(
            ExportInput::Batch(batch),
            output.path().to_path_buf(),
            ExportFormat::Csv,
            runtime.handle(),
            GenerationId(5),
        )
        .unwrap();
        assert!(matches!(wait(&task), ExportEvent::Finished { rows: 1, .. }));
        assert_eq!(fs::read_to_string(output.path()).unwrap(), "text\npicked\n");
    }

    #[test]
    fn rejects_nested_csv_values_before_writing_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        assert!(CsvWriter::new(Vec::new(), schema).is_err());
    }

    #[test]
    fn writes_readable_temporal_decimal_dictionary_and_binary_csv_values() {
        use arrow_array::{
            BinaryArray, Date32Array, Decimal128Array, DictionaryArray, DurationSecondArray,
            TimestampMillisecondArray, types::Int32Type,
        };

        let dictionary: DictionaryArray<Int32Type> =
            vec![Some("red, green"), None].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "date",
                Arc::new(Date32Array::from(vec![Some(19_000), None])) as arrow_array::ArrayRef,
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
