use std::{
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet_reader::export::BatchWriter;
pub use parquet_reader::export::ExportFormat;
use pavi_query::{LogicalPlan, QueryEngine, QueryPoll};
use pavi_runtime::{CancellationToken, GenerationId, RuntimeHandle};

const EVENT_CAPACITY: usize = 1;

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
    let mut writer = BatchWriter::new(format, BufWriter::new(file), schema)?;
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

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use parquet_reader::{ParquetSource, value::scalar_text};
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
}
