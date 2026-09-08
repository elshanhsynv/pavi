use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef};
use parquet::arrow::ArrowWriter;
use pavi_query::{LogicalPlan, QueryEngine, QueryPoll};
use pavi_runtime::{CancellationToken, GenerationId, RuntimeHandle};

const EVENT_CAPACITY: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExportFormat {
    Csv,
    Parquet,
}

impl ExportFormat {
    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Parquet => "Parquet",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Parquet => "parquet",
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
}

impl OutputWriter {
    fn new(format: ExportFormat, writer: BufWriter<File>, schema: SchemaRef) -> Result<Self> {
        match format {
            ExportFormat::Csv => Ok(Self::Csv(CsvWriter::new(writer, schema)?)),
            ExportFormat::Parquet => Ok(Self::Parquet(Box::new(ArrowWriter::try_new(
                writer, schema, None,
            )?))),
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::Csv(writer) => writer.write(batch),
            Self::Parquet(writer) => writer.write(batch).map_err(Into::into),
        }
    }

    fn finish(self) -> Result<()> {
        match self {
            Self::Csv(mut writer) => writer.finish(),
            Self::Parquet(writer) => {
                writer.close()?;
                Ok(())
            }
        }
    }
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
    if matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    ) {
        Ok(())
    } else {
        bail!("CSV export does not support {data_type:?}")
    }
}

fn write_csv_value(output: &mut impl Write, array: &dyn Array, row: usize) -> Result<()> {
    if array.is_null(row) {
        return Ok(());
    }
    macro_rules! number {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .context("read CSV scalar")?
                .value(row)
                .to_string()
        };
    }
    let value = match array.data_type() {
        DataType::Boolean => number!(BooleanArray),
        DataType::Int8 => number!(Int8Array),
        DataType::Int16 => number!(Int16Array),
        DataType::Int32 => number!(Int32Array),
        DataType::Int64 => number!(Int64Array),
        DataType::UInt8 => number!(UInt8Array),
        DataType::UInt16 => number!(UInt16Array),
        DataType::UInt32 => number!(UInt32Array),
        DataType::UInt64 => number!(UInt64Array),
        DataType::Float32 => number!(Float32Array),
        DataType::Float64 => number!(Float64Array),
        DataType::Date32 => number!(Date32Array),
        DataType::Date64 => number!(Date64Array),
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => number!(TimestampSecondArray),
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            number!(TimestampMillisecondArray)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            number!(TimestampMicrosecondArray)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            number!(TimestampNanosecondArray)
        }
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("read CSV string")?
                .value(row);
            return write_csv_text(output, value);
        }
        DataType::LargeUtf8 => {
            let value = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("read CSV string")?
                .value(row);
            return write_csv_text(output, value);
        }
        DataType::Binary => {
            let value = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context("read CSV binary")?
                .value(row);
            return write_hex(output, value);
        }
        DataType::LargeBinary => {
            let value = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .context("read CSV binary")?
                .value(row);
            return write_hex(output, value);
        }
        other => return Err(anyhow!("CSV export does not support {other:?}")),
    };
    output.write_all(value.as_bytes())?;
    Ok(())
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

fn write_hex(output: &mut impl Write, value: &[u8]) -> Result<()> {
    output.write_all(b"0x")?;
    for byte in value {
        write!(output, "{byte:02x}")?;
    }
    Ok(())
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
    fn rejects_unsupported_csv_values_before_writing_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration",
            DataType::Duration(arrow_schema::TimeUnit::Second),
            true,
        )]));
        assert!(CsvWriter::new(Vec::new(), schema).is_err());
    }
}
