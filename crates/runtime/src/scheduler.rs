use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
};

use parquet_reader::{FilterExpr, ParquetSource, Projection};

use crate::{
    CancellationToken, GenerationId, OpenOutcome, OpenResponse, OpenTask, PageOutcome,
    PageResponse, PageTask, TaskId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub worker_count: usize,
    pub queue_capacity: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_count: thread::available_parallelism()
                .map_or(1, usize::from)
                .max(1),
            queue_capacity: 32,
        }
    }
}

#[derive(Debug)]
pub enum RuntimeConfigError {
    ZeroWorkers,
    ZeroQueueCapacity,
    WorkerSpawn(std::io::Error),
}

impl fmt::Display for RuntimeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroWorkers => f.write_str("runtime needs at least one worker"),
            Self::ZeroQueueCapacity => f.write_str("runtime queue capacity must be positive"),
            Self::WorkerSpawn(error) => write!(f, "start runtime worker: {error}"),
        }
    }
}

impl std::error::Error for RuntimeConfigError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitError {
    QueueFull,
    Shutdown,
    TaskIdExhausted,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => f.write_str("runtime request queue is full"),
            Self::Shutdown => f.write_str("runtime has shut down"),
            Self::TaskIdExhausted => f.write_str("runtime task IDs are exhausted"),
        }
    }
}

impl std::error::Error for SubmitError {}

enum ReadOperation {
    Page {
        page_index: u64,
    },
    Window {
        first_row: u64,
        row_count: usize,
    },
    FilteredWindow {
        filter: FilterExpr,
        first_match_offset: u64,
        row_count: usize,
    },
}

struct PageRequest {
    task_id: TaskId,
    generation_id: GenerationId,
    source: Arc<ParquetSource>,
    operation: ReadOperation,
    projection: Projection,
    cancellation: CancellationToken,
    response: SyncSender<PageResponse>,
    #[cfg(test)]
    before_read: Option<Box<dyn FnOnce() + Send>>,
}

struct OpenRequest {
    task_id: TaskId,
    generation_id: GenerationId,
    path: PathBuf,
    cancellation: CancellationToken,
    response: SyncSender<OpenResponse>,
}

enum Request {
    Open(OpenRequest),
    Read(PageRequest),
}

/// A small, fixed worker pool for background Parquet page reads.
pub struct Runtime {
    submissions: Arc<Mutex<Option<SyncSender<Request>>>>,
    shutdown: Arc<AtomicBool>,
    next_task_id: Arc<AtomicU64>,
    workers: Vec<JoinHandle<()>>,
}

/// Cloneable submission endpoint that does not own runtime worker threads.
#[derive(Clone)]
pub struct RuntimeHandle {
    submissions: Arc<Mutex<Option<SyncSender<Request>>>>,
    next_task_id: Arc<AtomicU64>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeConfigError> {
        if config.worker_count == 0 {
            return Err(RuntimeConfigError::ZeroWorkers);
        }
        if config.queue_capacity == 0 {
            return Err(RuntimeConfigError::ZeroQueueCapacity);
        }

        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(config.worker_count);
        for index in 0..config.worker_count {
            let receiver = Arc::clone(&receiver);
            let worker_shutdown = Arc::clone(&shutdown);
            match thread::Builder::new()
                .name(format!("pavi-page-{index}"))
                .spawn(move || worker_loop(receiver, worker_shutdown))
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    shutdown.store(true, Ordering::Release);
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(RuntimeConfigError::WorkerSpawn(error));
                }
            }
        }

        Ok(Self {
            submissions: Arc::new(Mutex::new(Some(sender))),
            shutdown,
            next_task_id: Arc::new(AtomicU64::new(1)),
            workers,
        })
    }

    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle {
            submissions: Arc::clone(&self.submissions),
            next_task_id: Arc::clone(&self.next_task_id),
        }
    }

    pub fn submit_page(
        &self,
        source: Arc<ParquetSource>,
        page_index: u64,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.handle().submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::Page { page_index },
            None,
        )
    }

    pub fn submit_open(
        &self,
        path: impl Into<PathBuf>,
        generation_id: GenerationId,
    ) -> Result<OpenTask, SubmitError> {
        self.handle().submit_open(path, generation_id)
    }

    pub fn submit_window(
        &self,
        source: Arc<ParquetSource>,
        first_row: u64,
        row_count: usize,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.handle().submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::Window {
                first_row,
                row_count,
            },
            None,
        )
    }

    pub fn submit_filtered_window(
        &self,
        source: Arc<ParquetSource>,
        filter: FilterExpr,
        first_match_offset: u64,
        row_count: usize,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.handle().submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::FilteredWindow {
                filter,
                first_match_offset,
                row_count,
            },
            None,
        )
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Ok(mut submissions) = self.submissions.lock() {
            submissions.take();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }

    #[cfg(test)]
    fn submit_page_blocked_before_read(
        &self,
        source: Arc<ParquetSource>,
        page_index: u64,
        projection: Projection,
        generation_id: GenerationId,
        before_read: impl FnOnce() + Send + 'static,
    ) -> Result<PageTask, SubmitError> {
        self.handle().submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::Page { page_index },
            Some(Box::new(before_read)),
        )
    }
}

impl RuntimeHandle {
    pub fn submit_page(
        &self,
        source: Arc<ParquetSource>,
        page_index: u64,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::Page { page_index },
            None,
        )
    }

    pub fn submit_open(
        &self,
        path: impl Into<PathBuf>,
        generation_id: GenerationId,
    ) -> Result<OpenTask, SubmitError> {
        let sender = self.sender()?;
        let task_id = self.next_task_id()?;
        let cancellation = CancellationToken::default();
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        let request = OpenRequest {
            task_id,
            generation_id,
            path: path.into(),
            cancellation: cancellation.clone(),
            response: response_sender,
        };

        match sender.try_send(Request::Open(request)) {
            Ok(()) => Ok(OpenTask::new(
                task_id,
                generation_id,
                cancellation,
                response_receiver,
            )),
            Err(TrySendError::Full(_)) => Err(SubmitError::QueueFull),
            Err(TrySendError::Disconnected(_)) => Err(SubmitError::Shutdown),
        }
    }

    pub fn submit_window(
        &self,
        source: Arc<ParquetSource>,
        first_row: u64,
        row_count: usize,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::Window {
                first_row,
                row_count,
            },
            None,
        )
    }

    pub fn submit_filtered_window(
        &self,
        source: Arc<ParquetSource>,
        filter: FilterExpr,
        first_match_offset: u64,
        row_count: usize,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.submit_read(
            source,
            projection,
            generation_id,
            ReadOperation::FilteredWindow {
                filter,
                first_match_offset,
                row_count,
            },
            None,
        )
    }

    fn submit_read(
        &self,
        source: Arc<ParquetSource>,
        projection: Projection,
        generation_id: GenerationId,
        operation: ReadOperation,
        #[cfg(test)] before_read: Option<Box<dyn FnOnce() + Send>>,
        #[cfg(not(test))] _before_read: Option<()>,
    ) -> Result<PageTask, SubmitError> {
        let sender = self.sender()?;
        let task_id = self.next_task_id()?;
        let cancellation = CancellationToken::default();
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        let request = PageRequest {
            task_id,
            generation_id,
            source,
            operation,
            projection,
            cancellation: cancellation.clone(),
            response: response_sender,
            #[cfg(test)]
            before_read,
        };

        match sender.try_send(Request::Read(request)) {
            Ok(()) => Ok(PageTask::new(
                task_id,
                generation_id,
                cancellation,
                response_receiver,
            )),
            Err(TrySendError::Full(_)) => Err(SubmitError::QueueFull),
            Err(TrySendError::Disconnected(_)) => Err(SubmitError::Shutdown),
        }
    }

    fn sender(&self) -> Result<SyncSender<Request>, SubmitError> {
        self.submissions
            .lock()
            .map_err(|_| SubmitError::Shutdown)?
            .as_ref()
            .cloned()
            .ok_or(SubmitError::Shutdown)
    }

    fn next_task_id(&self) -> Result<TaskId, SubmitError> {
        self.next_task_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map(TaskId)
            .map_err(|_| SubmitError::TaskIdExhausted)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_loop(receiver: Arc<Mutex<Receiver<Request>>>, shutdown: Arc<AtomicBool>) {
    loop {
        let request = match receiver.lock() {
            Ok(receiver) => receiver.recv(),
            Err(_) => return,
        };
        let Ok(request) = request else {
            return;
        };
        match request {
            Request::Open(request) => execute_open(request, shutdown.load(Ordering::Acquire)),
            Request::Read(request) => execute(request, shutdown.load(Ordering::Acquire)),
        }
    }
}

fn execute_open(request: OpenRequest, shutting_down: bool) {
    let outcome = if shutting_down || request.cancellation.is_cancelled() {
        OpenOutcome::Cancelled
    } else {
        match ParquetSource::open(&request.path) {
            Ok(_) if request.cancellation.is_cancelled() => OpenOutcome::Cancelled,
            Ok(source) => OpenOutcome::Opened(Arc::new(source)),
            Err(error) => OpenOutcome::OpenFailed(error),
        }
    };
    let _ = request.response.send(OpenResponse {
        task_id: request.task_id,
        generation_id: request.generation_id,
        outcome,
    });
}

fn execute(request: PageRequest, shutting_down: bool) {
    let PageRequest {
        task_id,
        generation_id,
        source,
        operation,
        projection,
        cancellation,
        response,
        #[cfg(test)]
        before_read,
    } = request;

    #[cfg(test)]
    if let Some(before_read) = before_read {
        before_read();
    }

    let outcome = if shutting_down || cancellation.is_cancelled() {
        PageOutcome::Cancelled
    } else {
        match read_source(&source, operation, &projection) {
            Ok(_) if cancellation.is_cancelled() => PageOutcome::Cancelled,
            Ok(outcome) => outcome,
            Err(error) => PageOutcome::ReadFailed(error),
        }
    };
    let _ = response.send(PageResponse {
        task_id,
        generation_id,
        outcome,
    });
}

fn read_source(
    source: &ParquetSource,
    operation: ReadOperation,
    projection: &Projection,
) -> anyhow::Result<PageOutcome> {
    match operation {
        ReadOperation::Page { page_index } => source
            .read_page(page_index, projection)
            .map(PageOutcome::Loaded),
        ReadOperation::Window {
            first_row,
            row_count,
        } => source
            .read_window(first_row, row_count, projection)
            .map(PageOutcome::Batch),
        ReadOperation::FilteredWindow {
            filter,
            first_match_offset,
            row_count,
        } => source
            .read_filtered_window(&filter, first_match_offset, row_count, projection)
            .map(PageOutcome::Batch),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        sync::{Arc, Barrier},
    };

    use arrow_array::{Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::TempDir;

    use super::*;

    fn test_source() -> (TempDir, Arc<ParquetSource>, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("runtime.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(3))
                    .build(),
            ),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int32Array::from_iter_values(0..6)),
                        Arc::new(Int32Array::from_iter_values((0..6).map(|value| value * 10))),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();
        let source = Arc::new(ParquetSource::open(&path).unwrap());
        (dir, source, path)
    }

    fn runtime(workers: usize, capacity: usize) -> Runtime {
        Runtime::new(RuntimeConfig {
            worker_count: workers,
            queue_capacity: capacity,
        })
        .unwrap()
    }

    fn loaded(task: PageTask) -> PageResponse {
        let response = task.recv().unwrap();
        assert!(matches!(response.outcome, PageOutcome::Loaded(_)));
        response
    }

    #[test]
    fn executes_page_reads_and_delivers_projected_pages() {
        let (_dir, source, _) = test_source();
        let runtime = runtime(1, 2);
        let task = runtime
            .submit_page(
                source,
                0,
                Projection::columns(vec![1], 2).unwrap(),
                GenerationId(7),
            )
            .unwrap();
        let response = loaded(task);
        assert_eq!(response.generation_id, GenerationId(7));
        let PageOutcome::Loaded(page) = response.outcome else {
            unreachable!()
        };
        assert_eq!(page.batches[0].num_columns(), 1);
        assert_eq!(
            page.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 10, 20, 30, 40, 50]
        );
    }

    #[test]
    fn opens_sources_on_the_worker_pool() {
        let (_dir, _source, path) = test_source();
        let runtime = runtime(1, 1);
        let response = runtime
            .submit_open(path, GenerationId(7))
            .unwrap()
            .recv()
            .unwrap();

        assert_eq!(response.generation_id, GenerationId(7));
        let OpenOutcome::Opened(source) = response.outcome else {
            panic!("source open did not succeed")
        };
        assert_eq!(source.row_count(), 6);
    }

    #[test]
    fn runs_multiple_workers() {
        let (_dir, source, _) = test_source();
        let runtime = runtime(2, 2);
        let started = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let first = runtime
            .submit_page_blocked_before_read(
                source.clone(),
                0,
                Projection::all(2),
                GenerationId(1),
                {
                    let started = Arc::clone(&started);
                    let release = Arc::clone(&release);
                    move || {
                        started.wait();
                        release.wait();
                    }
                },
            )
            .unwrap();
        let second = runtime
            .submit_page_blocked_before_read(source, 0, Projection::all(2), GenerationId(1), {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                move || {
                    started.wait();
                    release.wait();
                }
            })
            .unwrap();
        started.wait();
        release.wait();
        loaded(first);
        loaded(second);
    }

    #[test]
    fn reports_full_queue_and_cancels_queued_work() {
        let (_dir, source, _) = test_source();
        let runtime = runtime(1, 1);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let running = runtime
            .submit_page_blocked_before_read(
                source.clone(),
                0,
                Projection::all(2),
                GenerationId(1),
                {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    move || {
                        entered.wait();
                        release.wait();
                    }
                },
            )
            .unwrap();
        entered.wait();
        let queued = runtime
            .submit_page(source.clone(), 0, Projection::all(2), GenerationId(2))
            .unwrap();
        assert!(matches!(
            runtime.submit_page(source, 0, Projection::all(2), GenerationId(3)),
            Err(SubmitError::QueueFull)
        ));
        queued.cancel();
        release.wait();
        loaded(running);
        assert!(matches!(
            queued.recv().unwrap().outcome,
            PageOutcome::Cancelled
        ));
    }

    #[test]
    fn preserves_generations_for_stale_result_handling() {
        let (_dir, source, _) = test_source();
        let runtime = runtime(1, 2);
        let response = loaded(
            runtime
                .submit_page(source, 0, Projection::all(2), GenerationId(4))
                .unwrap(),
        );
        assert!(response.is_stale_for(GenerationId(5)));
        assert!(!response.is_stale_for(GenerationId(4)));
    }

    #[test]
    fn cancellation_before_execution_and_repeated_cancellation_are_safe() {
        let (_dir, source, _) = test_source();
        let runtime = runtime(1, 1);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let running = runtime
            .submit_page_blocked_before_read(
                source.clone(),
                0,
                Projection::all(2),
                GenerationId(1),
                {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    move || {
                        entered.wait();
                        release.wait();
                    }
                },
            )
            .unwrap();
        entered.wait();
        let task = runtime
            .submit_page(source, 0, Projection::all(2), GenerationId(1))
            .unwrap();
        task.cancel();
        task.cancel();
        release.wait();
        loaded(running);
        assert!(matches!(
            task.recv().unwrap().outcome,
            PageOutcome::Cancelled
        ));
    }

    #[test]
    fn propagates_read_failures_and_ignores_dropped_receivers() {
        let (_dir, source, path) = test_source();
        let (_valid_dir, valid_source, _) = test_source();
        fs::remove_file(path).unwrap();
        let runtime = runtime(1, 2);
        let task = runtime
            .submit_page(source.clone(), 0, Projection::all(2), GenerationId(1))
            .unwrap();
        assert!(matches!(
            task.recv().unwrap().outcome,
            PageOutcome::ReadFailed(_)
        ));
        drop(
            runtime
                .submit_page(valid_source.clone(), 0, Projection::all(2), GenerationId(2))
                .unwrap(),
        );
        loaded(
            runtime
                .submit_page(valid_source, 0, Projection::all(2), GenerationId(3))
                .unwrap(),
        );
    }

    #[test]
    fn shutdown_cancels_pending_work_without_deadlocking() {
        let (_dir, source, _) = test_source();
        let runtime = runtime(1, 1);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let running = runtime
            .submit_page_blocked_before_read(
                source.clone(),
                0,
                Projection::all(2),
                GenerationId(1),
                {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    move || {
                        entered.wait();
                        release.wait();
                    }
                },
            )
            .unwrap();
        entered.wait();
        let queued = runtime
            .submit_page(source, 0, Projection::all(2), GenerationId(2))
            .unwrap();
        let shutdown = std::thread::spawn(move || {
            let mut runtime = runtime;
            runtime.shutdown();
        });
        release.wait();
        shutdown.join().unwrap();
        let _ = running.recv().unwrap();
        assert!(matches!(
            queued.recv().unwrap().outcome,
            PageOutcome::Cancelled
        ));
    }

    #[test]
    fn handles_do_not_hold_shutdown_open() {
        let runtime = runtime(1, 1);
        let handle = runtime.handle();
        let mut runtime = runtime;

        runtime.shutdown();
        assert!(matches!(
            handle.submit_open("missing.parquet", GenerationId(1)),
            Err(SubmitError::Shutdown)
        ));
    }
}
