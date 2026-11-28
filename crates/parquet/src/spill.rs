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
use arrow_ord::{
    ord::make_comparator,
    sort::{SortColumn, lexsort_to_indices},
};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{SchemaRef, SortOptions};
use arrow_select::{concat::concat_batches, take::take_record_batch};
use parquet::{
    arrow::{
        ArrowWriter, arrow_reader::ParquetRecordBatchReader,
        arrow_reader::ParquetRecordBatchReaderBuilder,
    },
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
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

    /// Compares rows of `left` with rows of `right` in sort order, the sequence
    /// number last, so no two rows compare equal.
    fn comparator(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
    ) -> Result<impl Fn(usize, usize) -> std::cmp::Ordering + use<>> {
        let columns = self
            .columns(left)
            .into_iter()
            .zip(self.columns(right))
            .map(|(left, right)| {
                make_comparator(
                    left.values.as_ref(),
                    right.values.as_ref(),
                    left.options.unwrap_or_default(),
                )
                .context("compare supported Arrow values")
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(move |left: usize, right: usize| {
            columns
                .iter()
                .map(|compare| compare(left, right))
                .find(|ordering| ordering.is_ne())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }

    /// Row order of `batch`, keeping only the first `limit` rows if given.
    ///
    /// One key, or keys that are all plain numbers, dates or times, are
    /// sorted column-wise (`lexsort_to_indices`, with typed comparisons,
    /// faster for them). Two or more keys with one of text, binary or another
    /// non-primitive type are first encoded into Arrow's row format, whose rows compare as bytes in exactly the keys'
    /// order (the sequence number last), so each comparison is one `memcmp`
    /// instead of a call per key column. A type the row format can't encode
    /// falls back to the column-wise sort.
    pub(crate) fn indices(&self, batch: &RecordBatch, limit: Option<usize>) -> Result<UInt32Array> {
        let columns = self.columns(batch);
        let key_types = self
            .keys
            .iter()
            .map(|(position, _)| batch.column(*position).data_type())
            .collect::<Vec<_>>();
        if sorts_by_rows(&key_types)
            && let Some(order) = row_order(&columns, limit)?
        {
            return Ok(order);
        }
        lexsort_to_indices(&columns, limit).context("sort supported Arrow values")
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

/// Whether keys of these types are sorted through the row format (see
/// [`SortKeys::indices`]): two or more keys, one of them text, binary or
/// another type that isn't a plain number, date or time. Measured on a
/// million rows, the row format sorts a text key then a number in 250 ms
/// against 392; a single key, or plain ones only, compare faster typed (a
/// float 116 ms against 307, four-valued text 174 against 325).
pub(crate) fn sorts_by_rows(key_types: &[&arrow_schema::DataType]) -> bool {
    key_types.len() > 1 && key_types.iter().any(|data_type| !data_type.is_primitive())
}

/// Memory the row format takes per row and key, besides the keys' own bytes:
/// a null marker, a value's fixed width or a string's block padding.
pub(crate) const ROW_ENCODING_BYTES_PER_KEY: usize = 16;

/// The order of the rows of `columns` (all keys, the sequence number last,
/// so no two rows are equal) by their row-format encoding, keeping only the
/// first `limit` if given; `None` when the row format can't encode a type.
fn row_order(columns: &[SortColumn], limit: Option<usize>) -> Result<Option<UInt32Array>> {
    let fields = columns
        .iter()
        .map(|column| {
            SortField::new_with_options(
                column.values.data_type().clone(),
                column.options.unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    if !RowConverter::supports_fields(&fields) {
        return Ok(None);
    }
    let converter = RowConverter::new(fields).context("encode sort keys")?;
    let rows = converter
        .convert_columns(
            &columns
                .iter()
                .map(|column| Arc::clone(&column.values))
                .collect::<Vec<_>>(),
        )
        .context("encode sort keys")?;
    let count = u32::try_from(rows.num_rows()).context("sort run has too many rows")?;
    let mut order = (0..count).collect::<Vec<u32>>();
    let compare =
        |left: &u32, right: &u32| rows.row(*left as usize).cmp(&rows.row(*right as usize));
    // Every row is unique, so an unstable sort gives the one order.
    match limit {
        Some(limit) if limit < order.len() => {
            if limit > 0 {
                order.select_nth_unstable_by(limit - 1, compare);
            }
            order.truncate(limit);
            order.sort_unstable_by(compare);
        }
        _ => order.sort_unstable_by(compare),
    }
    Ok(Some(UInt32Array::from(order)))
}

/// A folder in the temp directory for one sort's runs or one converted file,
/// removed when dropped. It holds [`LOCK_FILE`], locked for as long as the
/// folder is in use: the operating system releases the lock when the process
/// ends, however it ends (Ctrl+C, a crash, a kill), so another PAVI can tell a
/// left-over folder from one in use and remove it.
#[derive(Debug)]
pub(crate) struct TempFolder {
    pub(crate) path: PathBuf,
    /// The locked [`LOCK_FILE`]; `None` where the file system can't lock.
    lock: Option<File>,
}

impl TempFolder {
    /// Creates `<temp>/<prefix><pid>-<n>/` and locks it, first removing
    /// folders a stopped PAVI left behind ([`remove_leftover_temp_folders`]).
    pub(crate) fn create(prefix: &str) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let temp = std::env::temp_dir();
        remove_leftovers(&temp, prefix, UNLOCKED_GRACE);
        let path = temp.join(format!(
            "{prefix}{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create temporary folder {}", path.display()))?;
        let lock_path = path.join(LOCK_FILE);
        let lock = File::create(&lock_path)
            .ok()
            .filter(|file| file.try_lock().is_ok());
        if lock.is_none() {
            // No lock: the folder is told apart by its age, as before.
            let _ = std::fs::remove_file(&lock_path);
        }
        Ok(Self { path, lock })
    }
}

impl Drop for TempFolder {
    fn drop(&mut self) {
        // Unlock and close first: Windows can't delete an open file.
        drop(self.lock.take());
        // Best effort: a file still open elsewhere is cleaned up by a later run.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The file in every temporary folder that its PAVI keeps locked.
const LOCK_FILE: &str = ".pavi-lock";
/// Name start of every temporary folder PAVI makes (sorts, opened CSV/JSON/Arrow
/// files, downloads, prepared query tables).
const TEMP_FOLDER_PREFIX: &str = "pavi-";
/// An unlocked folder younger than this may be one another PAVI is just
/// creating (between making the folder and locking it), so it is kept.
pub(crate) const UNLOCKED_GRACE: Duration = Duration::from_secs(60);

/// Removes the temporary folders other PAVI processes left behind: folders
/// whose lock is free (their process has ended) and that are more than a
/// minute old. Folders in use are kept. Run when the app or the CLI starts,
/// on a background thread, and before each new temporary folder is made.
pub fn remove_leftover_temp_folders() {
    remove_leftovers(&std::env::temp_dir(), "", UNLOCKED_GRACE);
}

/// [`remove_leftover_temp_folders`] in `temp`; folders with `legacy_prefix`
/// and no lock file (made by an older PAVI, or where locks don't work) are
/// removed once they are more than a day old, as before. Unlocked folders
/// younger than `grace` are kept.
fn remove_leftovers(temp: &Path, legacy_prefix: &str, grace: Duration) {
    let Ok(entries) = std::fs::read_dir(temp) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(TEMP_FOLDER_PREFIX) {
            continue;
        }
        let age = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| SystemTime::now().duration_since(modified).ok());
        let path = entry.path();
        let lock_path = path.join(LOCK_FILE);
        if lock_path.is_file() {
            if age.is_none_or(|age| age < grace) {
                continue;
            }
            let unlocked = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
                .is_ok_and(|file| file.try_lock().is_ok());
            if unlocked {
                remove_folder(&path, &lock_path);
            }
        } else if !legacy_prefix.is_empty()
            && name.starts_with(legacy_prefix)
            && age.is_some_and(|age| age > STALE_SPILL_AGE)
        {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

/// Removes a left-over folder, its lock file last, so a removal cut short
/// (the process ends) is finished by the next one.
fn remove_folder(path: &Path, lock_path: &Path) {
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path == lock_path {
                continue;
            }
            let _ = if entry_path.is_dir() {
                std::fs::remove_dir_all(&entry_path)
            } else {
                std::fs::remove_file(&entry_path)
            };
        }
    }
    let _ = std::fs::remove_file(lock_path);
    let _ = std::fs::remove_dir(path);
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
        // A run is read once, in order: no dictionaries or statistics to
        // build, which made writing it about 2.5x slower.
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
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
/// before those. Only the rows a round emits are sorted, so each row is sorted
/// once.
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
        // The boundary: the first, in sort order, of the runs' last buffered rows.
        let lasts = active
            .iter()
            .map(|(_, batch)| batch.slice(batch.num_rows() - 1, 1))
            .collect::<Vec<_>>();
        let lasts = concat(&lasts)?;
        let first_last = self.keys.indices(&lasts, Some(1))?;
        let boundary_run = first_last
            .values()
            .first()
            .map(|row| *row as usize)
            .context("merge lost its boundary row")?;
        let boundary = lasts.slice(boundary_run, 1);
        // Each run's buffer is sorted, so the rows up to the boundary are a prefix.
        let mut prefixes = Vec::with_capacity(active.len());
        for (position, (index, batch)) in active.iter().enumerate() {
            let used = if position == boundary_run {
                batch.num_rows()
            } else {
                let compare = self.keys.comparator(batch, &boundary)?;
                partition_point(batch.num_rows(), |row| compare(row, 0).is_lt())
            };
            if used > 0 {
                prefixes.push(batch.slice(0, used));
            }
            self.cursors[*index].buffer = Some(batch.slice(used, batch.num_rows() - used));
        }
        Ok(Some(self.keys.sort(&prefixes, None)?))
    }
}

/// The number of leading rows of `0..rows` for which `before` holds, when it
/// holds for a prefix and not after.
fn partition_point(rows: usize, before: impl Fn(usize) -> bool) -> usize {
    let (mut low, mut high) = (0, rows);
    while low < high {
        let middle = low + (high - low) / 2;
        if before(middle) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
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
            self.merge_round()?;
        }
        let done = self.remaining == Some(0) || (self.finished && self.pending.is_empty());
        Ok((batches, done))
    }

    /// Drops the next `rows` rows in sort order without handing them out, for
    /// a jump ahead in the result, and says how many were dropped and whether
    /// the sort has handed out every row. Rows past a LIMIT are not counted.
    /// Holds at most one merge round, as [`Self::next_rows`] does.
    pub fn skip_rows(&mut self, rows: u64, checkpoint: &dyn Fn() -> bool) -> Result<(u64, bool)> {
        let mut skipped = 0_u64;
        while skipped < rows && self.remaining != Some(0) {
            if let Some(batch) = self.pending.pop_front() {
                let count = batch.num_rows();
                let mut take = usize::try_from(rows - skipped)
                    .unwrap_or(usize::MAX)
                    .min(count);
                if let Some(remaining) = &mut self.remaining {
                    take = take.min(*remaining);
                    *remaining -= take;
                }
                skipped += take as u64;
                if take < count {
                    self.pending.push_front(batch.slice(take, count - take));
                }
                continue;
            }
            if self.finished {
                break;
            }
            if checkpoint() {
                return Err(Cancelled.into());
            }
            self.merge_round()?;
        }
        let done = self.remaining == Some(0) || (self.finished && self.pending.is_empty());
        Ok((skipped, done))
    }

    /// Merges one round into `pending`, in page-sized batches of the output
    /// columns, or marks the merge finished.
    fn merge_round(&mut self) -> Result<()> {
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
        Ok(())
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
    fn merges_long_runs_over_many_rounds_by_two_keys() {
        // Runs of 10,000 rows, longer than a merge round's 4,096-row buffers,
        // with ranges that overlap only partly, so some runs give nothing for
        // several rounds.
        let keys = SortKeys {
            keys: vec![
                (
                    0,
                    SortSpec::new(0, SortDirection::Descending, NullOrder::First),
                ),
                (
                    1,
                    SortSpec::new(1, SortDirection::Ascending, NullOrder::Last),
                ),
            ],
            sequence: 2,
        };
        let mut writer = RunWriter::new().unwrap();
        let mut all = Vec::new();
        let mut sequence = 0_u64;
        for run in 0..5_i32 {
            let rows = 10_000_i32;
            let first = (0..rows)
                .map(|row| (row % 997 != 3).then_some(run * 4_000 + (row * 7) % 6_000))
                .collect::<Vec<_>>();
            let second = (0..rows)
                .map(|row| (row % 13 != 0).then_some((row * 31 + run) % 5))
                .collect::<Vec<_>>();
            let run_batch = RecordBatch::try_from_iter(vec![
                ("a", Arc::new(Int32Array::from(first)) as ArrayRef),
                ("b", Arc::new(Int32Array::from(second))),
                (
                    "__pavi_sequence",
                    Arc::new(UInt64Array::from_iter_values(
                        sequence..sequence + rows as u64,
                    )),
                ),
            ])
            .unwrap();
            sequence += rows as u64;
            all.push(run_batch.clone());
            writer
                .write(&keys.sort(&[run_batch], None).unwrap())
                .unwrap();
        }
        let mut merge = writer
            .into_merge(keys.clone(), vec![0, 1, 2], None, &|| false)
            .unwrap();
        let mut merged = Vec::new();
        loop {
            let (batches, finished) = merge.next_rows(PAGE_ROWS as usize, &|| false).unwrap();
            merged.extend(batches);
            if finished {
                break;
            }
        }
        let merged = concat_batches(&merged[0].schema(), &merged).unwrap();
        let expected = keys.sort(&all, None).unwrap();
        assert_eq!(merged.num_rows(), 50_000);
        for column in 0..3 {
            assert_eq!(
                merged.column(column).as_ref(),
                expected.column(column).as_ref(),
                "column {column}"
            );
        }
    }

    #[test]
    fn skips_merged_rows_exactly_and_within_a_limit() {
        let keys = keys(SortDirection::Ascending);
        let merged = |limit: Option<usize>, skip: u64, first_read: usize| {
            let mut writer = RunWriter::new().unwrap();
            let mut sequence = 0;
            for run in 0..3_i32 {
                let values = (0..9_000)
                    .map(|row| Some((row * 3 + run) % 20_000))
                    .collect::<Vec<_>>();
                let run_batch = batch(&values, sequence);
                sequence += values.len() as u64;
                writer
                    .write(&keys.sort(&[run_batch], None).unwrap())
                    .unwrap();
            }
            let mut merge = writer
                .into_merge(keys.clone(), vec![0, 1], limit, &|| false)
                .unwrap();
            let mut out = Vec::new();
            if first_read > 0 {
                let (batches, _) = merge.next_rows(first_read, &|| false).unwrap();
                out.extend(values(&batches));
            }
            let (skipped, done) = merge.skip_rows(skip, &|| false).unwrap();
            let mut rest = Vec::new();
            loop {
                let (batches, finished) = merge.next_rows(5_000, &|| false).unwrap();
                rest.extend(values(&batches));
                if finished {
                    break;
                }
            }
            (out, skipped, done, rest)
        };
        let (_, _, _, all) = merged(None, 0, 0);
        assert_eq!(all.len(), 27_000);
        for (skip, first_read) in [
            (1, 0),
            (4_095, 0),
            (4_097, 0),
            (20_000, 0),
            (5_000, 4_096),
            (30_000, 0),
        ] {
            let (out, skipped, done, rest) = merged(None, skip, first_read);
            let start = out.len() + skipped as usize;
            assert_eq!(skipped, skip.min(27_000 - out.len() as u64), "skip {skip}");
            assert_eq!(rest, all[start..], "skip {skip} after {first_read}");
            assert_eq!(done, start == 27_000);
        }
        // Under a LIMIT, rows past it are neither skipped nor handed out.
        let (_, skipped, done, rest) = merged(Some(10_000), 9_000, 0);
        assert_eq!((skipped, done), (9_000, false));
        assert_eq!(rest, all[9_000..10_000]);
        let (_, skipped, done, rest) = merged(Some(10_000), 20_000, 0);
        assert_eq!((skipped, done, rest.len()), (10_000, true, 0));
    }

    #[test]
    fn row_format_sorts_like_the_column_sort_for_every_type() {
        use arrow_ord::sort::lexsort_to_indices;
        use arrow_select::take::take;

        // Every scalar type, each with a null; repeated so ties need the next key.
        let arrays = crate::value::tests::scalar_arrays()
            .into_iter()
            .map(|array| {
                let data_type = crate::types::read_type(array.data_type());
                arrow_cast::cast(array.as_ref(), &data_type).unwrap()
            })
            .collect::<Vec<_>>();
        let pick = UInt32Array::from_iter_values((0..60_u32).map(|row| (row * 7 + row / 5) % 3));
        let columns = arrays
            .iter()
            .map(|array| take(array.as_ref(), &pick, None).unwrap())
            .collect::<Vec<_>>();
        let sequence = Arc::new(UInt64Array::from_iter_values(0..60)) as ArrayRef;
        let mut checked = 0;
        for (first, second) in (0..columns.len()).map(|index| (index, (index + 5) % columns.len()))
        {
            for (direction, nulls) in [
                (SortDirection::Ascending, NullOrder::Last),
                (SortDirection::Descending, NullOrder::First),
            ] {
                let batch = RecordBatch::try_from_iter(vec![
                    ("a", Arc::clone(&columns[first])),
                    ("b", Arc::clone(&columns[second])),
                    ("__pavi_sequence", Arc::clone(&sequence)),
                ])
                .unwrap();
                for keys in [vec![0], vec![0, 1], vec![1, 0]] {
                    let keys = SortKeys {
                        keys: keys
                            .into_iter()
                            .map(|position| (position, SortSpec::new(position, direction, nulls)))
                            .collect(),
                        sequence: 2,
                    };
                    let columns = keys.columns(&batch);
                    for limit in [None, Some(7)] {
                        let expected = lexsort_to_indices(&columns, limit).unwrap();
                        let Some(rows) = row_order(&columns, limit).unwrap() else {
                            continue;
                        };
                        assert_eq!(
                            rows,
                            expected,
                            "{:?} then {:?}, {direction:?}, limit {limit:?}",
                            batch.column(0).data_type(),
                            batch.column(1).data_type()
                        );
                        assert_eq!(keys.indices(&batch, limit).unwrap(), expected);
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 200, "{checked} orders checked by the row format");
        // One key, or plain numeric keys, keep the typed column sort; text
        // with another key doesn't.
        assert!(!sorts_by_rows(&[&arrow_schema::DataType::Float64]));
        assert!(!sorts_by_rows(&[&arrow_schema::DataType::Utf8]));
        assert!(!sorts_by_rows(&[
            &arrow_schema::DataType::Int32,
            &arrow_schema::DataType::Date32
        ]));
        assert!(sorts_by_rows(&[
            &arrow_schema::DataType::Int32,
            &arrow_schema::DataType::Utf8
        ]));
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
    fn removes_unlocked_leftover_folders_and_keeps_those_in_use() {
        let temp = tempfile::TempDir::new().unwrap();
        let folder = |name: &str, locked: bool| {
            let path = temp.path().join(name);
            std::fs::create_dir_all(path.join("nested")).unwrap();
            std::fs::write(path.join("run-0.parquet"), b"rows").unwrap();
            std::fs::write(path.join("nested").join("part"), b"rows").unwrap();
            let lock = File::create(path.join(LOCK_FILE)).unwrap();
            if locked {
                lock.try_lock().unwrap();
            }
            (path, lock)
        };
        // A sort still running holds its lock; one stopped by Ctrl+C doesn't.
        let (in_use, _held) = folder(&format!("{SPILL_PREFIX}1-0"), true);
        let (stopped, released) = folder(&format!("{SPILL_PREFIX}2-0"), false);
        drop(released);
        let (other_kind, released) = folder("pavi-open-3-0", false);
        drop(released);
        // Not PAVI's, even with a lock file.
        let (foreign, released) = folder("someone-else-4-0", false);
        drop(released);

        // Within the grace time nothing unlocked is touched: it may be a
        // folder another PAVI is just creating.
        remove_leftovers(temp.path(), SPILL_PREFIX, UNLOCKED_GRACE);
        assert!(in_use.exists() && stopped.exists() && other_kind.exists());

        remove_leftovers(temp.path(), SPILL_PREFIX, Duration::ZERO);
        assert!(
            in_use.join("run-0.parquet").exists(),
            "a locked folder is in use"
        );
        assert!(!stopped.exists(), "a stopped sort's folder is removed");
        assert!(!other_kind.exists(), "every kind of PAVI folder is cleaned");
        assert!(foreign.exists());

        // A removal cut short keeps the lock file, so it is finished later.
        let (half, released) = folder(&format!("{SPILL_PREFIX}5-0"), false);
        drop(released);
        std::fs::remove_file(half.join("run-0.parquet")).unwrap();
        remove_leftovers(temp.path(), SPILL_PREFIX, Duration::ZERO);
        assert!(!half.exists());
    }

    #[test]
    fn temp_folders_are_locked_while_in_use_and_removed_when_dropped() {
        let folder = TempFolder::create("pavi-test-").unwrap();
        let lock_path = folder.path.join(LOCK_FILE);
        assert!(lock_path.is_file());
        // Another handle can't take the lock while the folder is in use.
        let other = File::options()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        assert!(other.try_lock().is_err());
        drop(other);
        let path = folder.path.clone();
        drop(folder);
        assert!(!path.exists());
    }

    #[test]
    fn removes_only_old_leftover_spill_folders() {
        let temp = tempfile::TempDir::new().unwrap();
        let fresh = temp.path().join(format!("{SPILL_PREFIX}1-1"));
        let other = temp.path().join("not-pavi");
        std::fs::create_dir_all(&fresh).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        remove_leftovers(temp.path(), SPILL_PREFIX, UNLOCKED_GRACE);
        assert!(
            fresh.exists() && other.exists(),
            "a fresh folder may belong to a running sort"
        );
    }
}
