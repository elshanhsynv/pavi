//! Reproducible bounded pipeline measurements for PAVI.
//!
//! Usage: `cargo run -p pavi_bench -- <file.parquet> [sequential-pages]`.

use std::{env, path::Path, sync::Arc, time::Instant};

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use parquet_reader::{DataPage, PAGE_ROWS, PageCacheLimits, ParquetSource, Projection};
use pavi_query::{LogicalPlan, QueryEngine};
use pavi_runtime::{GenerationId, OpenOutcome, PageOutcome, Runtime, RuntimeConfig};

const DEFAULT_SEQUENTIAL_PAGES: u64 = 8;

fn main() -> Result<()> {
    let mut arguments = env::args_os();
    let executable = arguments.next().unwrap_or_default();
    let Some(path) = arguments.next() else {
        bail!(
            "usage: {} <file.parquet> [sequential-pages]",
            executable.display()
        );
    };
    let pages = arguments
        .next()
        .map(|value| value.to_string_lossy().parse())
        .transpose()
        .context("parse sequential-pages")?
        .unwrap_or(DEFAULT_SEQUENTIAL_PAGES);
    if arguments.next().is_some() || pages == 0 {
        bail!("supply one Parquet file and a positive sequential page count");
    }

    let path = Path::new(&path);
    let mut runtime = Runtime::new(RuntimeConfig {
        worker_count: 2,
        queue_capacity: 32,
    })?;
    println!("metric,value");
    let source = open_through_runtime(&runtime, path)?;
    let columns = source.column_count();
    let all = Projection::all(columns);
    let first_column = Projection::columns(vec![0], columns)?;
    let file_bytes = std::fs::metadata(path)
        .with_context(|| format!("read metadata for {}", path.display()))?
        .len();

    println!("file_bytes,{file_bytes}");
    println!("rows,{}", source.row_count());
    println!("columns,{columns}");
    println!("row_groups,{}", source.row_groups().len());

    source.set_cache_limits(PageCacheLimits::default())?;
    let (all_page, all_cold) = read_page(&runtime, Arc::clone(&source), 0, all.clone())?;
    println!("first_page_all_cold_ms,{}", millis(all_cold));
    println!("first_page_all_bytes,{}", all_page.byte_size);
    drop(all_page);
    let (all_page, all_warm) = read_page(&runtime, Arc::clone(&source), 0, all)?;
    println!("first_page_all_warm_ms,{}", millis(all_warm));
    println!("first_page_all_warm_bytes,{}", all_page.byte_size);
    drop(all_page);
    print_cache("all_page_cache", &source)?;

    source.set_cache_limits(PageCacheLimits::default())?;
    let (projected_page, projected_cold) =
        read_page(&runtime, Arc::clone(&source), 0, first_column)?;
    println!("first_page_projected_cold_ms,{}", millis(projected_cold));
    println!("first_page_projected_bytes,{}", projected_page.byte_size);
    drop(projected_page);
    print_cache("projected_page_cache", &source)?;

    source.set_cache_limits(PageCacheLimits::default())?;
    let available_pages = source.row_count().div_ceil(PAGE_ROWS);
    let sequential_pages = pages.min(available_pages);
    let started = Instant::now();
    let mut sequential_rows = 0_usize;
    for page in 0..sequential_pages {
        let (loaded, _) = read_page(
            &runtime,
            Arc::clone(&source),
            page,
            Projection::all(columns),
        )?;
        sequential_rows += loaded.window.row_count;
    }
    println!("sequential_pages,{sequential_pages}");
    println!("sequential_rows,{sequential_rows}");
    println!("sequential_scroll_ms,{}", millis(started.elapsed()));
    print_cache("sequential_cache", &source)?;

    source.set_cache_limits(PageCacheLimits::default())?;
    let jump_page = available_pages.saturating_sub(1);
    let (jump, jump_time) = read_page(
        &runtime,
        Arc::clone(&source),
        jump_page,
        Projection::all(columns),
    )?;
    println!("large_jump_page,{jump_page}");
    println!("large_jump_rows,{}", jump.window.row_count);
    println!("large_jump_ms,{}", millis(jump_time));
    println!("large_jump_bytes,{}", jump.byte_size);
    drop(jump);
    print_cache("jump_cache", &source)?;

    source.set_cache_limits(PageCacheLimits::default())?;
    let started = Instant::now();
    let mut execution = QueryEngine::new(&runtime).execute(
        &LogicalPlan::scan(Arc::clone(&source)).limit(PAGE_ROWS as usize),
        GenerationId(1),
    )?;
    let query_batch = execution
        .next_batch()?
        .context("limited scan unexpectedly returned no rows")?;
    println!("query_first_batch_ms,{}", millis(started.elapsed()));
    println!("query_first_batch_rows,{}", query_batch.batch.num_rows());
    println!(
        "query_first_batch_bytes,{}",
        batch_bytes(&query_batch.batch)
    );
    println!("query_scheduled_reads,{}", execution.scheduled_reads());
    print_cache("query_cache", &source)?;

    runtime.shutdown();
    Ok(())
}

fn open_through_runtime(runtime: &Runtime, path: &Path) -> Result<Arc<ParquetSource>> {
    let started = Instant::now();
    let task = runtime.submit_open(path.to_path_buf(), GenerationId(1))?;
    let response = task.recv().context("receive source open response")?;
    let source = match response.outcome {
        OpenOutcome::Opened(source) => source,
        OpenOutcome::Cancelled => bail!("source open was cancelled"),
        OpenOutcome::OpenFailed(error) => return Err(error).context("open source through runtime"),
    };
    println!("open_metadata_ms,{}", millis(started.elapsed()));
    Ok(source)
}

fn read_page(
    runtime: &Runtime,
    source: Arc<ParquetSource>,
    page: u64,
    projection: Projection,
) -> Result<(DataPage, std::time::Duration)> {
    let started = Instant::now();
    let task = runtime.submit_page(source, page, projection, GenerationId(1))?;
    let response = task.recv().context("receive page response")?;
    let elapsed = started.elapsed();
    match response.outcome {
        PageOutcome::Loaded(page) => Ok((page, elapsed)),
        PageOutcome::Cancelled => bail!("page {page} was cancelled"),
        PageOutcome::ReadFailed(error) => Err(error).context(format!("read page {page}")),
        PageOutcome::Batch(_) => bail!("page {page} unexpectedly returned a record batch"),
        PageOutcome::Batches(_) => bail!("page {page} unexpectedly returned record batches"),
    }
}

fn print_cache(prefix: &str, source: &ParquetSource) -> Result<()> {
    let stats = source.cache_stats()?;
    println!("{prefix}_hits,{}", stats.hits);
    println!("{prefix}_misses,{}", stats.misses);
    println!("{prefix}_entries,{}", stats.entries);
    println!("{prefix}_bytes,{}", stats.bytes);
    Ok(())
}

fn batch_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

fn millis(duration: std::time::Duration) -> u128 {
    duration.as_micros() / 1_000
}
