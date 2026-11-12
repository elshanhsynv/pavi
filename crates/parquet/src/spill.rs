//! External merge sort: sorted runs written to temporary Parquet files, then
//! merged a batch at a time (ADR 0005).
//!
//! Every row carries a sequence number as the last sort key, so no two rows are
//! equal and the merge gives exactly the order a stable in-memory sort would.

use std::{
    collections::VecDeque,
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, bail};
use arrow_array::{RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{SchemaRef, SortOptions};
use arrow_select::{concat::concat_batches, take::take_record_batch};
use parquet::{
    arrow::{
        ArrowWriter, arrow_reader::ParquetRecordBatchReader,
        arrow_reader::ParquetRecordBatchReaderBuilder,
    },
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{Cancelled, NullOrder, SortDirection, SortSpec, page::PAGE_ROWS};

/// Most temporary disk space one sort may use.
pub const MAX_SPILL_BYTES: u64 = 32 * 1024 * 1024 * 1024;
/// Runs merged at once; more runs are first merged into longer runs.
pub const MERGE_FAN_IN: usize = 16;
/// Spill directories older than this are left over from a crash and removed.
const STALE_SPILL_AGE: Duration = Duration::from_secs(24 * 60 * 60);
/// Name prefix of spill directories in the temp folder.
const SPILL_PREFIX: &str = "pavi-sort-";

/// Which columns of a batch are sort keys, and how they sort.
#[derive(Clone, Debug)]
pub(crate) struct SortKeys {
    /// Batch column and ordering of each key, most significant first.
    pub(crate) keys: Vec<(usize, SortSpec)>,
    /// The sequence-number column, the final tie-breaker.
    pub(crate) sequence: usize,
}

impl SortKeys {
    fn columns(&self, batch: &RecordBatch) -> Vec<SortColumn> {
        self.keys
            .iter()
            .map(|(position, spec)| SortColumn {
                values: batch.column(*position).clone(),
                options: Some(SortOptions {
                    descending: spec.direction == SortDirection::Descending,
                    nulls_first: spec.nulls == NullOrder::First,
                }),
            })
            .chain([SortColumn {
                values: batch.column(self.sequence).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            }])
            .collect()
    }

    /// Row order of `batch`, keeping only the first `limit` rows if given.
    pub(crate) fn indices(&self, batch: &RecordBatch, limit: Option<usize>) -> Result<UInt32Array> {
        lexsort_to_indices(&self.columns(batch), limit).context("sort supported Arrow values")
    }

    /// Sorts `batches` into one batch, keeping only the first `limit` rows if given.
    pub(crate) fn sort(
        &self,
        batches: &[RecordBatch],
        limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let input = concat(batches)?;
        let indices = self.indices(&input, limit)?;
        Ok(take_record_batch(&input, &indices)?)
    }
}

fn concat(batches: &[RecordBatch]) -> Result<RecordBatch> {
    match batches {
        [] => bail!("nothing to sort"),
        [batch] => Ok(batch.clone()),
        _ => Ok(concat_batches(&batches[0].schema(), batches)?),
    }
}

/// A folder in the temp directory for one sort's runs or one converted file,
/// removed when dropped.
#[derive(Debug)]
pub(crate) struct TempFolder {
    pub(crate) path: PathBuf,
}

impl TempFolder {
    /// Creates `<temp>/<prefix><pid>-<n>/`, first removing folders with the same
    /// prefix that a crashed PAVI left behind.
    pub(crate) fn create(prefix: &str) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let temp = std::env::temp_dir();
        remove_stale_dirs(&temp, prefix);
        let path = temp.join(format!(
            "{prefix}{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create temporary folder {}", path.display()))?;
        Ok(Self { path })
    }
}

impl Drop for TempFolder {
    fn drop(&mut self) {
        // Best effort: a file still open elsewhere is cleaned up by a later run.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Removes `prefix` folders a crashed PAVI left behind (older than a day).
fn remove_stale_dirs(temp: &Path, prefix: &str) {
    let Ok(entries) = std::fs::read_dir(temp) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry.file_name().to_string_lossy().starts_with(prefix)
            && entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|modified| SystemTime::now().duration_since(modified).ok())
                .is_some_and(|age| age > STALE_SPILL_AGE);
        if stale {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// One sorted run on disk.
#[derive(Debug)]
struct Run {
    path: PathBuf,
}

/// Writes sorted runs and keeps count of the disk space they take.
pub(crate) struct RunWriter {
    dir: Arc<TempFolder>,
    runs: Vec<Run>,
    bytes: u64,
    next: usize,
}

impl RunWriter {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            dir: Arc::new(TempFolder::create(SPILL_PREFIX)?),
            runs: Vec::new(),
            bytes: 0,
            next: 0,
        })
    }

    /// Starts a new run file.
    fn start(&mut self, schema: SchemaRef) -> Result<(PathBuf, ArrowWriter<File>)> {
        let path = self.dir.path.join(format!("run-{}.parquet", self.next));
        self.next += 1;
        let file = File::create(&path)
            .with_context(|| format!("create sort spill file {}", path.display()))?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_row_count(Some(64 * PAGE_ROWS as usize))
            .build();
        Ok((path, ArrowWriter::try_new(file, schema, Some(properties))?))
    }

    /// Finishes a run file and counts its size against [`MAX_SPILL_BYTES`].
    fn finish(&mut self, path: PathBuf, writer: ArrowWriter<File>) -> Result<Run> {
        writer.close().context("finish sort spill file")?;
        let size = std::fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        self.bytes = self.bytes.saturating_add(size);
        if self.bytes > MAX_SPILL_BYTES {
            bail!(
                "this sort needs more than {} GB of temporary disk space; \
                 filter the data or add a LIMIT",
                MAX_SPILL_BYTES / (1024 * 1024 * 1024)
            );
        }
        Ok(Run { path })
    }

    /// Writes one sorted batch as a run.
    pub(crate) fn write(&mut self, sorted: &RecordBatch) -> Result<()> {
        let (path, mut writer) = self.start(sorted.schema())?;
        writer.write(sorted).context("write sort spill file")?;
        let run = self.finish(path, writer)?;
        self.runs.push(run);
        Ok(())
    }

    /// Merges runs in groups of [`MERGE_FAN_IN`] until at most that many are
    /// left, then returns the merge that hands out the final order.
    pub(crate) fn into_merge(
        mut self,
        keys: SortKeys,
        output_positions: Vec<usize>,
        limit: Option<usize>,
        checkpoint: &dyn Fn() -> bool,
    ) -> Result<SortMerge> {
        while self.runs.len() > MERGE_FAN_IN {
            let mut merged = Vec::new();
            let runs = std::mem::take(&mut self.runs);
            let mut runs = runs.into_iter().peekable();
            while runs.peek().is_some() {
                let group = runs.by_ref().take(MERGE_FAN_IN).collect::<Vec<_>>();
                if group.len() == 1 {
                    merged.extend(group);
                    continue;
                }
                let mut cursors = Merger::open(&group, keys.clone())?;
                let mut output = None;
                while let Some(batch) = cursors.round()? {
                    if checkpoint() {
                        return Err(Cancelled.into());
                    }
                    let (_, writer) = match &mut output {
                        Some(output) => output,
                        None => output.insert(self.start(batch.schema())?),
                    };
                    writer.write(&batch).context("write sort spill file")?;
                }
                drop(cursors);
                for run in &group {
                    let _ = std::fs::remove_file(&run.path);
                }
                if let Some((path, writer)) = output {
                    merged.push(self.finish(path, writer)?);
                }
            }
            self.runs = merged;
        }
        let merger = Merger::open(&self.runs, keys)?;
        Ok(SortMerge {
            _dir: self.dir,
            merger,
            output_positions,
            pending: VecDeque::new(),
            remaining: limit,
            finished: false,
        })
    }
}

/// Reads one run a batch at a time.
struct Cursor {
    reader: ParquetRecordBatchReader,
    /// Rows of the current batch not merged yet.
    buffer: Option<RecordBatch>,
    done: bool,
}

impl Cursor {
    /// Makes sure `buffer` holds rows, unless the run is used up.
    fn fill(&mut self) -> Result<()> {
        while !self.done
            && self
                .buffer
                .as_ref()
                .is_none_or(|batch| batch.num_rows() == 0)
        {
            match self
                .reader
                .next()
                .transpose()
                .context("read sort spill file")?
            {
                Some(batch) => self.buffer = Some(batch),
                None => {
                    self.done = true;
                    self.buffer = None;
                }
            }
        }
        Ok(())
    }
}

/// Merges sorted runs. Each round emits every buffered row that is no greater
/// than the smallest "last buffered row" of any run: no unread row can come
/// before those.
struct Merger {
    cursors: Vec<Cursor>,
    keys: SortKeys,
}

impl Merger {
    fn open(runs: &[Run], keys: SortKeys) -> Result<Self> {
        let cursors = runs
            .iter()
            .map(|run| {
                let file = File::open(&run.path)
                    .with_context(|| format!("open sort spill file {}", run.path.display()))?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(PAGE_ROWS as usize)
                    .build()?;
                Ok(Cursor {
                    reader,
                    buffer: None,
                    done: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { cursors, keys })
    }

    /// The next rows in sort order, or `None` when every run is used up.
    fn round(&mut self) -> Result<Option<RecordBatch>> {
        for cursor in &mut self.cursors {
            cursor.fill()?;
        }
        let active = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| {
                cursor.buffer.as_ref().map(|batch| (index, batch.clone()))
            })
            .collect::<Vec<_>>();
        if active.is_empty() {
            return Ok(None);
        }
        let batches = active
            .iter()
            .map(|(_, batch)| batch.clone())
            .collect::<Vec<_>>();
        let combined = concat(&batches)?;
        let order = self.keys.indices(&combined, None)?;
        // Where each run's rows start in `combined`, and its last row there.
        let mut starts = Vec::with_capacity(active.len());
        let mut start = 0_usize;
        for (_, batch) in &active {
            starts.push(start);
            start += batch.num_rows();
        }
        let last_rows = active
            .iter()
            .zip(&starts)
            .map(|((_, batch), start)| start + batch.num_rows() - 1)
            .collect::<Vec<_>>();
        // The first "last row" in sort order is the boundary: everything up to it
        // is final.
        let boundary = order
            .values()
            .iter()
            .position(|row| last_rows.contains(&(*row as usize)))
            .context("merge lost its boundary row")?;
        let taken = UInt32Array::from(order.values()[..=boundary].to_vec());
        let mut used = vec![0_usize; active.len()];
        for row in taken.values() {
            let row = *row as usize;
            let run = starts.partition_point(|start| *start <= row) - 1;
            used[run] += 1;
        }
        for ((index, batch), used) in active.iter().zip(used) {
            // Each run's buffer is sorted, so the rows it gave are a prefix.
            self.cursors[*index].buffer = Some(batch.slice(used, batch.num_rows() - used));
        }
        Ok(Some(take_record_batch(&combined, &taken)?))
    }
}

/// The final merge of a spilled sort, handing out rows as they are asked for.
/// Dropping it deletes the spill files.
pub struct SortMerge {
    _dir: Arc<TempFolder>,
    merger: Merger,
    /// Batch columns that make up the output, in output order.
    output_positions: Vec<usize>,
    /// Merged and projected rows not handed out yet.
    pending: VecDeque<RecordBatch>,
    /// Rows still to hand out under a LIMIT.
    remaining: Option<usize>,
    finished: bool,
}

impl std::fmt::Debug for SortMerge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SortMerge")
            .field("runs", &self.merger.cursors.len())
            .field("finished", &self.finished)
            .finish()
    }
}

impl SortMerge {
    /// Up to about `rows` more rows in sort order, in page-sized batches, and
    /// whether the sort has handed out every row.
    pub fn next_rows(
        &mut self,
        rows: usize,
        checkpoint: &dyn Fn() -> bool,
    ) -> Result<(Vec<RecordBatch>, bool)> {
        let mut batches = Vec::new();
        let mut count = 0_usize;
        while count < rows && self.remaining != Some(0) {
            if let Some(batch) = self.pending.pop_front() {
                let batch = match &mut self.remaining {
                    Some(remaining) => {
                        let batch = batch.slice(0, batch.num_rows().min(*remaining));
                        *remaining -= batch.num_rows();
                        batch
                    }
                    None => batch,
                };
                count += batch.num_rows();
                batches.push(batch);
                continue;
            }
            if self.finished {
                break;
            }
            if checkpoint() {
                return Err(Cancelled.into());
            }
            match self.merger.round()? {
                Some(merged) => {
                    let projected = merged.project(&self.output_positions)?;
                    let page = PAGE_ROWS as usize;
                    for first in (0..projected.num_rows()).step_by(page) {
                        self.pending.push_back(
                            projected.slice(first, (projected.num_rows() - first).min(page)),
                        );
                    }
                }
                None => self.finished = true,
            }
        }
        let done = self.remaining == Some(0) || (self.finished && self.pending.is_empty());
        Ok((batches, done))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Int32Array, UInt64Array};

    use super::*;

    /// `values` with a sequence column, as the sort builds them.
    fn batch(values: &[Option<i32>], first: u64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("v", Arc::new(Int32Array::from(values.to_vec())) as ArrayRef),
            (
                "__pavi_sequence",
                Arc::new(UInt64Array::from_iter_values(
                    first..first + values.len() as u64,
                )),
            ),
        ])
        .unwrap()
    }

    fn keys(direction: SortDirection) -> SortKeys {
        SortKeys {
            keys: vec![(0, SortSpec::new(0, direction, NullOrder::Last))],
            sequence: 1,
        }
    }

    fn values(batches: &[RecordBatch]) -> Vec<Option<i32>> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..array.len())
                    .map(|row| array.is_valid(row).then(|| array.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn merges_runs_into_one_stable_order_and_cleans_up() {
        // 40 runs (more than the fan-in, so one extra pass) of pseudo-random values.
        let mut writer = RunWriter::new().unwrap();
        let dir = writer.dir.path.clone();
        let keys = keys(SortDirection::Ascending);
        let mut all = Vec::new();
        let mut sequence = 0;
        for run in 0..40_i32 {
            // Many equal values and some nulls, so ties and null placement matter.
            let values = (0..250)
                .map(|row| (row % 50 != 7).then_some((run * 31 + row * 17) % 97))
                .collect::<Vec<_>>();
            let run_batch = batch(&values, sequence);
            sequence += values.len() as u64;
            all.push(run_batch.clone());
            writer
                .write(&keys.sort(&[run_batch], None).unwrap())
                .unwrap();
        }
        let mut merge = writer
            .into_merge(keys.clone(), vec![0, 1], None, &|| false)
            .unwrap();
        let mut merged = Vec::new();
        loop {
            let (batches, finished) = merge.next_rows(3_000, &|| false).unwrap();
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.num_rows() <= PAGE_ROWS as usize)
            );
            merged.extend(batches);
            if finished {
                break;
            }
        }
        let expected = keys.sort(&all, None).unwrap();
        assert_eq!(values(&merged), values(std::slice::from_ref(&expected)));
        // Equal values keep file order: the sequence column comes out the same.
        let sequence_of = |batches: &[RecordBatch]| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(sequence_of(&merged), sequence_of(&[expected]));
        assert!(dir.exists());
        drop(merge);
        assert!(!dir.exists(), "the spill folder is removed with the merge");
        assert_eq!(
            merged
                .iter()
                .map(|batch| batch.column(0).len())
                .sum::<usize>(),
            10_000
        );
    }

    #[test]
    fn merges_descending_with_nulls_and_stops_when_asked() {
        let keys = keys(SortDirection::Descending);
        let mut writer = RunWriter::new().unwrap();
        for (first, run) in [
            (0, vec![Some(5), None, Some(1)]),
            (3, vec![Some(4), Some(5), None]),
        ] {
            writer
                .write(&keys.sort(&[batch(&run, first)], None).unwrap())
                .unwrap();
        }
        let mut merge = writer
            .into_merge(keys.clone(), vec![0], None, &|| false)
            .unwrap();
        let (batches, finished) = merge.next_rows(100, &|| false).unwrap();
        assert!(finished);
        assert_eq!(
            values(&batches),
            [Some(5), Some(5), Some(4), Some(1), None, None]
        );
        assert_eq!(batches[0].num_columns(), 1, "only the output columns");

        let mut writer = RunWriter::new().unwrap();
        writer.write(&batch(&[Some(1)], 0)).unwrap();
        let mut merge = writer
            .into_merge(keys_for_cancel(), vec![0], None, &|| false)
            .unwrap();
        assert!(merge.next_rows(10, &|| true).unwrap_err().is::<Cancelled>());
    }

    fn keys_for_cancel() -> SortKeys {
        keys(SortDirection::Ascending)
    }

    #[test]
    fn removes_only_old_leftover_spill_folders() {
        let temp = tempfile::TempDir::new().unwrap();
        let fresh = temp.path().join(format!("{SPILL_PREFIX}1-1"));
        let other = temp.path().join("not-pavi");
        std::fs::create_dir_all(&fresh).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        remove_stale_dirs(temp.path(), SPILL_PREFIX);
        assert!(
            fresh.exists() && other.exists(),
            "a fresh folder may belong to a running sort"
        );
    }
}
