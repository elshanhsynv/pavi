use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
};

use parquet_reader::{ParquetSource, Projection};

use crate::{CancellationToken, GenerationId, PageOutcome, PageResponse, PageTask, TaskId};

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

struct PageRequest {
    task_id: TaskId,
    generation_id: GenerationId,
    source: Arc<ParquetSource>,
    page_index: u64,
    projection: Projection,
    cancellation: CancellationToken,
    response: SyncSender<PageResponse>,
    #[cfg(test)]
    before_read: Option<Box<dyn FnOnce() + Send>>,
}

/// A small, fixed worker pool for background Parquet page reads.
pub struct Runtime {
    sender: Option<SyncSender<PageRequest>>,
    shutdown: Arc<AtomicBool>,
    next_task_id: AtomicU64,
    workers: Vec<JoinHandle<()>>,
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
            sender: Some(sender),
            shutdown,
            next_task_id: AtomicU64::new(1),
            workers,
        })
    }

    pub fn submit_page(
        &self,
        source: Arc<ParquetSource>,
        page_index: u64,
        projection: Projection,
        generation_id: GenerationId,
    ) -> Result<PageTask, SubmitError> {
        self.submit_page_inner(source, page_index, projection, generation_id, None)
    }

    fn submit_page_inner(
        &self,
        source: Arc<ParquetSource>,
        page_index: u64,
        projection: Projection,
        generation_id: GenerationId,
        #[cfg(test)] before_read: Option<Box<dyn FnOnce() + Send>>,
        #[cfg(not(test))] _before_read: Option<()>,
    ) -> Result<PageTask, SubmitError> {
        let Some(sender) = &self.sender else {
            return Err(SubmitError::Shutdown);
        };
        let task_id = self
            .next_task_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map(TaskId)
            .map_err(|_| SubmitError::TaskIdExhausted)?;
        let cancellation = CancellationToken::default();
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        let request = PageRequest {
            task_id,
            generation_id,
            source,
            page_index,
            projection,
            cancellation: cancellation.clone(),
            response: response_sender,
            #[cfg(test)]
            before_read,
        };

        match sender.try_send(request) {
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

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.sender.take();
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
        self.submit_page_inner(
            source,
            page_index,
            projection,
            generation_id,
            Some(Box::new(before_read)),
        )
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_loop(receiver: Arc<Mutex<Receiver<PageRequest>>>, shutdown: Arc<AtomicBool>) {
    loop {
        let request = match receiver.lock() {
            Ok(receiver) => receiver.recv(),
            Err(_) => return,
        };
        let Ok(request) = request else {
            return;
        };
        execute(request, shutdown.load(Ordering::Acquire));
    }
}

fn execute(request: PageRequest, shutting_down: bool) {
    let PageRequest {
        task_id,
        generation_id,
        source,
        page_index,
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
        match source.read_page(page_index, &projection) {
            Ok(_) if cancellation.is_cancelled() => PageOutcome::Cancelled,
            Ok(page) => PageOutcome::Loaded(page),
            Err(error) => PageOutcome::ReadFailed(error),
        }
    };
    let _ = response.send(PageResponse {
        task_id,
        generation_id,
        outcome,
    });
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
}
