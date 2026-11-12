use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fs::File,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
    Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, RecordBatchOptions, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::{concat::concat_batches, filter::filter_record_batch};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
        RowSelector,
    },
};
use parquet::file::{metadata::ParquetMetaData, statistics::Statistics};

use crate::{
    AggregateExpr, AggregateFunction, AggregateSpec, DataPage, DatasetMetadata, GroupBudget,
    PageCache, PageCacheLimits, PageCacheStats, PageKey, Projection, RowGroupInfo, RowWindow,
    SortBudget, SortSpec,
    convert::{self, ConvertedFile, FileFormat},
    dataset::{
        self, MAX_DATASET_METADATA_BYTES, PartitionColumn, constant_array, partition_statistics,
    },
    filter::FilterExpr,
    metadata::ColumnStatistics,
    page::PAGE_ROWS,
    sort::validate_sort_keys,
    spill::{RunWriter, SortKeys, SortMerge},
    statistics::{leaf_columns, metadata_statistics},
    types::{
        is_comparable, is_decimal, is_float, is_signed_integer, is_summable, is_unsigned_integer,
    },
};

const BATCH_SIZE: usize = 4096;

/// A scan stops adding row groups to one reader's run past this many rows, so the
/// up-front dictionary checks of a run stay small when a filtered page ends early.
const SCAN_RUN_ROWS: u64 = 1 << 20;

/// Loads the footer and the Arrow schema.
///
/// Writers may embed an Arrow schema (`ARROW:schema`) as a hint for exact Arrow types.
/// The hint is optional: if it is corrupt, load again without it and derive the schema
/// from the Parquet schema alone, so the file still opens. If that fails too, report the
/// original error. (Before arrow-rs 60, corrupt hints panicked; found by the `parquet_file`
/// fuzz target.)
fn load_arrow_metadata(file: &File) -> Result<ArrowReaderMetadata> {
    match ArrowReaderMetadata::load(file, ArrowReaderOptions::new()) {
        Ok(metadata) => Ok(metadata),
        Err(error) => ArrowReaderMetadata::load(
            file,
            ArrowReaderOptions::new().with_skip_arrow_metadata(true),
        )
        .map_err(|_| error.into()),
    }
}

/// Pulls record batches from the Parquet decoder and turns a decoder panic into an error.
///
/// arrow-rs panics on some malformed pages instead of returning an error (found by the
/// `parquet_file` fuzz target; open upstream as apache/arrow-rs#12129 and #6255, list in
/// docs/FUZZING.md). Guarding here protects every caller, including the CLI and bench, which do
/// not run inside the runtime's worker guard. After a panic the decoder's state is unknown,
/// so the reader stops.
pub(crate) struct GuardedReader<R> {
    inner: Option<R>,
}

impl<R> GuardedReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self { inner: Some(inner) }
    }
}

impl<R, E> Iterator for GuardedReader<R>
where
    R: Iterator<Item = std::result::Result<RecordBatch, E>>,
    E: Into<anyhow::Error>,
{
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.inner.as_mut()?;
        match catch_unwind(AssertUnwindSafe(|| reader.next())) {
            Ok(item) => item.map(|batch| batch.map_err(Into::into)),
            Err(payload) => {
                self.inner = None;
                Some(Err(anyhow!(
                    "malformed Parquet data: {}",
                    panic_message(&*payload)
                )))
            }
        }
    }
}

/// Largest dictionary page PAVI decodes (uncompressed). Writers default to 1 MiB.
const MAX_DICTIONARY_PAGE_BYTES: i64 = 256 * 1024 * 1024;

/// Page headers are usually under 100 bytes; the read starts here and grows if needed.
const PAGE_HEADER_FIRST_READ_BYTES: u64 = 1024;

/// Checks the first page of a column chunk if it is a dictionary page.
/// Anything this can't read or decode is left for the parquet crate to report.
fn check_dictionary_page(
    mut file: &File,
    column: &parquet::file::metadata::ColumnChunkMetaData,
) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let offset = column
        .dictionary_page_offset()
        .unwrap_or(column.data_page_offset());
    let (Ok(offset), Ok(chunk)) = (
        u64::try_from(offset),
        u64::try_from(column.compressed_size()),
    ) else {
        return Ok(());
    };
    // Read more until the header decodes, up to the whole chunk, so padding a header with
    // unknown fields can't push it past this check.
    let limit = chunk.min(file.metadata()?.len().saturating_sub(offset));
    let mut len = PAGE_HEADER_FIRST_READ_BYTES.min(limit);
    let sizes = loop {
        let mut header = vec![0_u8; usize::try_from(len)?];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut header)?;
        if let Some(sizes) = crate::footer::page_header_sizes(&header) {
            break sizes;
        }
        if len >= limit {
            return Ok(());
        }
        len = len.saturating_mul(16).min(limit);
    };
    if sizes.page_type != Some(crate::footer::DICTIONARY_PAGE) {
        return Ok(());
    }
    let (Some(values), Some(bytes)) = (sizes.dictionary_values, sizes.uncompressed_size) else {
        return Ok(());
    };
    if bytes > MAX_DICTIONARY_PAGE_BYTES {
        bail!(
            "dictionary page is {bytes} bytes, over PAVI's {MAX_DICTIONARY_PAGE_BYTES}-byte limit"
        );
    }
    // Smallest PLAIN encoding of one dictionary value, in bits.
    let bits_per_value: i64 = match column.column_type() {
        parquet::basic::Type::BOOLEAN => 1,
        parquet::basic::Type::INT32 | parquet::basic::Type::FLOAT => 32,
        parquet::basic::Type::INT64 | parquet::basic::Type::DOUBLE => 64,
        parquet::basic::Type::INT96 => 96,
        parquet::basic::Type::BYTE_ARRAY => 32, // 4-byte length prefix
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => {
            8 * i64::from(column.column_descr().type_length().max(1))
        }
    };
    let needed_bits = values.saturating_mul(bits_per_value);
    if values < 0 || bytes < 0 || needed_bits > bytes.saturating_mul(8) {
        bail!("dictionary page claims {values} values but holds only {bytes} bytes");
    }
    Ok(())
}

pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("decoder panicked")
}

/// One Parquet file of a source: the only file, or one file of a folder dataset.
struct SourceFile {
    path: PathBuf,
    bytes: u64,
    metadata: Arc<ParquetMetaData>,
    arrow_metadata: ArrowReaderMetadata,
    /// The Parquet leaf column of each file column; `None` when nested.
    leaf_columns: Vec<Option<usize>>,
    /// Source-wide index of this file's first row group.
    first_row_group: usize,
    /// This file's value of each partition column.
    partition_values: Vec<Option<String>>,
    /// Per row group of this file, statistics of each partition column.
    partition_statistics: Vec<Vec<Statistics>>,
}

/// A Parquet file, or a folder of Parquet files read as one table (ADR 0006).
///
/// Row groups are numbered across all files, in file order; columns are the files'
/// columns followed by the partition columns taken from `key=value` folder names.
pub struct ParquetSource {
    path: PathBuf,
    file_info: std::fs::Metadata,
    compression: String,
    dataset_metadata: DatasetMetadata,
    files: Vec<SourceFile>,
    /// For each row group: its file and its index within that file.
    row_group_files: Vec<(usize, usize)>,
    /// Columns stored in the files; partition columns come after them.
    file_columns: usize,
    partition_columns: Vec<PartitionColumn>,
    cache: Mutex<PageCache>,
    /// `(row group, leaf column)` chunks whose dictionary page header passed
    /// [`ParquetSource::check_dictionary_pages`].
    checked_dictionaries: Mutex<HashSet<(usize, usize)>>,
    /// The format of the file opened; not Parquet when it was converted.
    format: FileFormat,
    /// The temporary Parquet copy of a CSV, JSON or Arrow file (ADR 0007),
    /// deleted with the source.
    converted: Option<ConvertedFile>,
}

/// Reads one file's footer.
fn open_file(path: &Path) -> Result<(u64, ArrowReaderMetadata)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let bytes = file.metadata().context("read file properties")?.len();
    crate::footer::check_footer(&file)
        .with_context(|| format!("read Parquet metadata from {}", path.display()))?;
    let arrow_metadata = load_arrow_metadata(&file)
        .with_context(|| format!("read Parquet metadata from {}", path.display()))?;
    Ok((bytes, arrow_metadata))
}

impl ParquetSource {
    /// Opens a Parquet file, a CSV, JSON or Arrow file (read into a temporary
    /// Parquet file, ADR 0007), or every Parquet file in a folder as one table.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_cancellable(path, &|_| false)
    }

    /// Like [`Self::open`]. `checkpoint` is called with the share of the work done
    /// (0 to 1) between the files of a folder and between the batches of a
    /// conversion; when it returns `true` the open ends with a [`Cancelled`] error.
    pub fn open_cancellable(
        path: impl AsRef<Path>,
        checkpoint: &dyn Fn(f64) -> bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file_info =
            std::fs::metadata(&path).with_context(|| format!("open {}", path.display()))?;
        let format = FileFormat::from_path(&path);
        if !file_info.is_dir() && format != FileFormat::Parquet {
            let converted = convert::convert(&path, format, checkpoint)
                .with_context(|| format!("open {} as {}", path.display(), format.name()))?;
            let (bytes, arrow_metadata) = open_file(&converted.path)?;
            let mut source = Self::from_files(
                path,
                file_info,
                vec![(converted.path.clone(), bytes, arrow_metadata, Vec::new())],
                Vec::new(),
            )?;
            source.compression = format.name().to_string();
            source.format = format;
            source.converted = Some(converted);
            return Ok(source);
        }
        if !file_info.is_dir() {
            let (bytes, arrow_metadata) = open_file(&path)?;
            return Self::from_files(
                path.clone(),
                file_info,
                vec![(path, bytes, arrow_metadata, Vec::new())],
                Vec::new(),
            );
        }
        let found = dataset::find_files(&path, &|| checkpoint(0.0))?;
        let file_count = found.len().max(1);
        let partition_columns = dataset::partition_columns(&found);
        let mut files = Vec::with_capacity(found.len());
        let mut metadata_bytes = 0_usize;
        for (index, file) in found.into_iter().enumerate() {
            if checkpoint(index as f64 / file_count as f64) {
                return Err(Cancelled.into());
            }
            let (bytes, arrow_metadata) = open_file(&file.path)?;
            metadata_bytes = metadata_bytes.saturating_add(arrow_metadata.metadata().memory_size());
            if metadata_bytes > MAX_DATASET_METADATA_BYTES {
                bail!(
                    "the footers of this folder's files need more than {} MB of memory; \
                     open a smaller folder",
                    MAX_DATASET_METADATA_BYTES / (1024 * 1024)
                );
            }
            let values = file
                .partitions
                .into_iter()
                .map(|(_, value)| value)
                .collect();
            files.push((file.path, bytes, arrow_metadata, values));
        }
        Self::from_files(path, file_info, files, partition_columns)
    }

    /// Builds the source from opened files, which must have the same columns.
    fn from_files(
        path: PathBuf,
        file_info: std::fs::Metadata,
        opened: Vec<(PathBuf, u64, ArrowReaderMetadata, Vec<Option<String>>)>,
        partition_columns: Vec<PartitionColumn>,
    ) -> Result<Self> {
        let Some((first_path, _, first_metadata, _)) = opened.first() else {
            bail!("{} holds no Parquet files", path.display());
        };
        let first_schema = first_metadata.schema().clone();
        // A column is nullable when it is in any file.
        let mut fields = first_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        for (other_path, _, other, _) in &opened[1..] {
            let other_fields = other.schema().fields();
            let mismatch = if other_fields.len() != fields.len() {
                Some(format!(
                    "it has {} columns, not {}",
                    other_fields.len(),
                    fields.len()
                ))
            } else {
                fields
                    .iter()
                    .zip(other_fields.iter())
                    .find_map(|(field, other)| {
                        if field.name() != other.name() {
                            Some(format!(
                                "it has column {} where the first file has {}",
                                other.name(),
                                field.name()
                            ))
                        } else if field.data_type() != other.data_type() {
                            Some(format!(
                                "its column {} is {} there but {} here",
                                field.name(),
                                field.data_type(),
                                other.data_type()
                            ))
                        } else {
                            None
                        }
                    })
            };
            if let Some(mismatch) = mismatch {
                bail!(
                    "{} does not have the same columns as {}: {mismatch}",
                    other_path.display(),
                    first_path.display()
                );
            }
            for (field, other) in fields.iter_mut().zip(other_fields.iter()) {
                if other.is_nullable() && !field.is_nullable() {
                    *field = field.clone().with_nullable(true);
                }
            }
        }
        let file_columns = fields.len();
        let file_schema =
            Schema::new_with_metadata(fields.clone(), first_schema.metadata().clone());
        // A partition key the files also store as a column (Polars writes both) is
        // read from the files; its folders add no column.
        let keep = partition_columns
            .iter()
            .map(|column| !fields.iter().any(|field| field.name() == &column.name))
            .collect::<Vec<_>>();
        let partition_columns = partition_columns
            .into_iter()
            .zip(&keep)
            .filter_map(|(column, keep)| keep.then_some(column))
            .collect::<Vec<_>>();
        for column in &partition_columns {
            fields.push(Field::new(
                column.name.clone(),
                column.data_type.clone(),
                true,
            ));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            first_schema.metadata().clone(),
        ));

        let mut codecs = std::collections::BTreeSet::new();
        let mut row_group_counts = Vec::new();
        let mut row_group_files = Vec::new();
        let mut statistics = vec![Vec::new(); schema.fields().len()];
        let mut files = Vec::with_capacity(opened.len());
        for (index, (file_path, bytes, arrow_metadata, partition_values)) in
            opened.into_iter().enumerate()
        {
            let partition_values = partition_values
                .into_iter()
                .zip(&keep)
                .filter_map(|(value, keep)| keep.then_some(value))
                .collect::<Vec<_>>();
            let metadata = arrow_metadata.metadata().clone();
            codecs.extend(
                metadata
                    .row_groups()
                    .iter()
                    .flat_map(|group| group.columns())
                    .map(|column| column.compression().to_string()),
            );
            let first_row_group = row_group_counts.len();
            let mut partition_stats = Vec::with_capacity(metadata.num_row_groups());
            for local in 0..metadata.num_row_groups() {
                let rows = metadata.row_group(local).num_rows().max(0) as u64;
                row_group_counts.push(rows);
                row_group_files.push((index, local));
                partition_stats.push(
                    partition_columns
                        .iter()
                        .zip(&partition_values)
                        .map(|(column, value)| {
                            partition_statistics(&column.data_type, value.as_deref(), rows)
                        })
                        .collect(),
                );
                for (offset, value) in partition_values.iter().enumerate() {
                    statistics[file_columns + offset].push(Some(ColumnStatistics {
                        min: value.clone(),
                        max: value.clone(),
                        null_count: Some(if value.is_some() { 0 } else { rows }),
                        distinct_count: Some(u64::from(value.is_some())),
                    }));
                }
            }
            for (column, file_statistics) in metadata_statistics(&metadata, &file_schema)
                .into_iter()
                .enumerate()
            {
                statistics[column].extend(file_statistics);
            }
            files.push(SourceFile {
                path: file_path,
                bytes,
                leaf_columns: leaf_columns(&metadata, file_columns),
                metadata,
                arrow_metadata,
                first_row_group,
                partition_values,
                partition_statistics: partition_stats,
            });
        }
        let compression = if codecs.is_empty() {
            "Unavailable".to_string()
        } else {
            codecs.into_iter().collect::<Vec<_>>().join(", ")
        };
        let dataset_metadata = DatasetMetadata::new(schema, row_group_counts)
            .with_context(|| format!("build row-group index for {}", path.display()))?
            .with_column_statistics(statistics);
        Ok(Self {
            path,
            file_info,
            compression,
            dataset_metadata,
            files,
            row_group_files,
            file_columns,
            partition_columns,
            cache: Mutex::new(PageCache::default()),
            checked_dictionaries: Mutex::new(HashSet::new()),
            format: FileFormat::Parquet,
            converted: None,
        })
    }

    /// The file or folder this source was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `true` when this source is a folder of files.
    pub fn is_folder(&self) -> bool {
        self.file_info.is_dir()
    }

    /// How many Parquet files the source reads.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Size of the file opened, or of all the Parquet files of a folder together.
    pub fn total_bytes(&self) -> u64 {
        if self.converted.is_some() {
            return self.file_info.len();
        }
        self.files.iter().map(|file| file.bytes).sum()
    }

    /// The format of the file opened. Not Parquet means it was read into a
    /// temporary Parquet file.
    pub fn format(&self) -> FileFormat {
        self.format
    }

    /// Where the temporary Parquet copy of a converted file is.
    #[cfg(test)]
    pub(crate) fn converted_path(&self) -> Option<&Path> {
        self.converted
            .as_ref()
            .map(|converted| converted.path.as_path())
    }

    /// Names of the columns made from `key=value` folders, in column order.
    pub fn partition_column_names(&self) -> Vec<&str> {
        self.partition_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect()
    }

    /// The file holding source row group `row_group`, and its index in that file.
    fn locate(&self, row_group: usize) -> Result<(&SourceFile, usize)> {
        let (file, local) = self
            .row_group_files
            .get(row_group)
            .copied()
            .ok_or_else(|| anyhow!("row group {row_group} out of range"))?;
        let file = self
            .files
            .get(file)
            .ok_or_else(|| anyhow!("file {file} out of range"))?;
        Ok((file, local))
    }

    pub fn metadata(&self) -> &DatasetMetadata {
        &self.dataset_metadata
    }

    pub fn file_info(&self) -> &std::fs::Metadata {
        &self.file_info
    }

    pub fn compression(&self) -> &str {
        &self.compression
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.dataset_metadata.schema.clone()
    }

    pub fn row_count(&self) -> u64 {
        self.dataset_metadata.row_count
    }

    pub fn column_count(&self) -> usize {
        self.dataset_metadata.column_count
    }

    pub fn row_groups(&self) -> &[RowGroupInfo] {
        &self.dataset_metadata.row_groups
    }

    pub fn set_cache_limits(&self, limits: PageCacheLimits) -> Result<()> {
        *self
            .cache
            .lock()
            .map_err(|_| anyhow!("page cache lock poisoned"))? = PageCache::new(limits);
        Ok(())
    }

    pub fn cache_stats(&self) -> Result<PageCacheStats> {
        Ok(self
            .cache
            .lock()
            .map_err(|_| anyhow!("page cache lock poisoned"))?
            .stats())
    }

    pub fn validate_filter(&self, filter: &FilterExpr) -> Result<()> {
        filter.validate_schema(&self.dataset_metadata.schema)
    }

    pub fn read_window(
        &self,
        first_row: u64,
        row_count: usize,
        projection: &Projection,
    ) -> Result<RecordBatch> {
        let batches = self.read_window_batches(first_row, row_count, projection)?;
        self.concat_or_empty(projection, batches)
    }

    pub fn read_page(&self, page_index: u64, projection: &Projection) -> Result<DataPage> {
        let key = PageKey::new(page_index, projection.clone());
        if let Some(page) = self
            .cache
            .lock()
            .map_err(|_| anyhow!("page cache lock poisoned"))?
            .get(&key)
        {
            return Ok(page);
        }

        let window = RowWindow::for_page(page_index, self.row_count());
        let batches = self.read_window_batches(window.first_row, window.row_count, projection)?;
        let page = DataPage::new(key, window, batches);
        self.cache
            .lock()
            .map_err(|_| anyhow!("page cache lock poisoned"))?
            .insert(page.clone());
        Ok(page)
    }

    /// Reads up to `row_count` rows matching `filter`, after skipping the first
    /// `first_match_offset` matches. This always scans from the start of the file;
    /// to page through results, use [`Self::read_filtered_from`].
    pub fn read_filtered_window(
        &self,
        filter: &FilterExpr,
        first_match_offset: u64,
        row_count: usize,
        projection: &Projection,
    ) -> Result<RecordBatch> {
        Ok(self
            .scan_filtered(
                filter,
                0,
                first_match_offset,
                row_count,
                projection,
                &|_| false,
            )?
            .batch)
    }

    /// Reads up to `row_count` rows matching `filter`, starting at source row
    /// `start_row`, and returns where to resume. Paging with the returned
    /// [`FilteredWindow::next_row`] visits each row once, so reading a whole
    /// filtered result stays linear in the file size.
    ///
    /// `checkpoint` is called before each decoded batch with the number of source
    /// rows scanned so far (a position in the file, so skipped row groups count);
    /// when it returns `true` the read ends with a [`Cancelled`] error.
    pub fn read_filtered_from(
        &self,
        filter: &FilterExpr,
        start_row: u64,
        row_count: usize,
        projection: &Projection,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<FilteredWindow> {
        self.scan_filtered(filter, start_row, 0, row_count, projection, checkpoint)
    }

    fn scan_filtered(
        &self,
        filter: &FilterExpr,
        start_row: u64,
        mut skip_matches: u64,
        row_count: usize,
        projection: &Projection,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<FilteredWindow> {
        let total_rows = self.row_count();
        if start_row > total_rows {
            bail!("filter start row {start_row} is past row count {total_rows}");
        }
        if row_count == 0 {
            return Ok(FilteredWindow {
                batch: self.empty_batch(projection),
                next_row: start_row,
            });
        }

        let mut read_columns = projection.as_slice().to_vec();
        for column in filter.columns(&self.dataset_metadata.schema)? {
            if !read_columns.contains(&column) {
                read_columns.push(column);
            }
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let output_positions = projection
            .as_slice()
            .iter()
            .map(|column| projected_position(&read_projection, *column, "output"))
            .collect::<Result<Vec<_>>>()?;

        let mut remaining = row_count;
        let mut batches = Vec::new();
        let mut next_group = 0;
        let keep = |row_group: &RowGroupInfo| self.row_group_might_match(filter, row_group);
        while let Some(run) = self.next_scan_run(&mut next_group, start_row, &keep)? {
            let (file, mut reader, mut batch_first_row) =
                self.run_reader(&run, &read_projection, start_row)?;
            while let Some(batch) = reader.next().transpose()? {
                if checkpoint(batch_first_row) {
                    return Err(Cancelled.into());
                }
                let batch = self.reorder_batch(batch, &read_projection, file)?;
                let batch_rows = batch.num_rows() as u64;
                let mask = self.evaluate_filter(filter, &batch, &read_projection)?;
                let matches = mask.true_count();
                if matches as u64 <= skip_matches {
                    skip_matches -= matches as u64;
                    batch_first_row += batch_rows;
                    continue;
                }
                let first = skip_matches as usize;
                skip_matches = 0;
                let take = remaining.min(matches - first);
                let filtered = filter_record_batch(&batch, &mask)?;
                batches.push(project_batch(
                    &filtered.slice(first, take),
                    projection,
                    &output_positions,
                )?);
                remaining -= take;
                if remaining == 0 {
                    // Resume right after the source row of the last match taken.
                    let last = mask
                        .values()
                        .set_indices()
                        .nth(first + take - 1)
                        .ok_or_else(|| anyhow!("filter mask lost a match"))?;
                    return Ok(FilteredWindow {
                        batch: self.concat_or_empty(projection, batches)?,
                        next_row: batch_first_row + last as u64 + 1,
                    });
                }
                batch_first_row += batch_rows;
            }
        }

        Ok(FilteredWindow {
            batch: self.concat_or_empty(projection, batches)?,
            next_row: total_rows,
        })
    }

    /// Reads, sorts, and returns 4,096-row output batches for one supported sort key.
    ///
    /// Rows that pass `filter` must fit in `budget`. With `limit` (at most
    /// `budget.max_rows()`), only the best `limit` rows are kept while scanning, so
    /// `ORDER BY … LIMIT n` works on files of any size. Ties keep file order.
    /// PAVI has no external sort for larger inputs. `checkpoint` is called before each
    /// decoded batch with the number of source rows scanned so far; when it returns
    /// `true` the read ends with a [`Cancelled`] error.
    pub fn read_sorted(
        &self,
        filter: Option<&FilterExpr>,
        projection: &Projection,
        sort: &[SortSpec],
        budget: SortBudget,
        limit: Option<usize>,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<SortOutput> {
        self.validate_sort(sort)?;
        if let Some(filter) = filter {
            self.validate_filter(filter)?;
        }
        if limit == Some(0) {
            return Ok(SortOutput::InMemory(Vec::new()));
        }
        let top = limit.filter(|limit| *limit <= budget.max_rows());

        let filter_columns = filter
            .map(|filter| filter.columns(&self.dataset_metadata.schema))
            .transpose()?
            .unwrap_or_default();
        let mut read_columns = projection.as_slice().to_vec();
        for key in sort {
            if !read_columns.contains(&key.column) {
                read_columns.push(key.column);
            }
        }
        // Columns kept with each row: the output, then sort keys not in it.
        let kept_columns = read_columns.clone();
        for column in filter_columns {
            if !read_columns.contains(&column) {
                read_columns.push(column);
            }
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let kept_positions = kept_columns
            .iter()
            .map(|column| projected_position(&read_projection, *column, "sort"))
            .collect::<Result<Vec<_>>>()?;
        let position_in_kept = |column: usize| {
            kept_columns
                .iter()
                .position(|kept| *kept == column)
                .ok_or_else(|| anyhow!("sort column {column} was not kept"))
        };
        let order = SortKeys {
            keys: sort
                .iter()
                .map(|key| Ok((position_in_kept(key.column)?, *key)))
                .collect::<Result<Vec<_>>>()?,
            sequence: kept_columns.len(),
        };
        let output_positions = projection
            .as_slice()
            .iter()
            .map(|column| position_in_kept(*column))
            .collect::<Result<Vec<_>>>()?;

        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut sequence = 0_u64;
        let mut retained = Vec::new();
        let mut runs: Option<RunWriter> = None;
        let mut scanned_total = 0_u64;
        let mut next_group = 0;
        let keep = |row_group: &RowGroupInfo| {
            filter.is_none_or(|filter| self.row_group_might_match(filter, row_group))
        };
        while let Some(run) = self.next_scan_run(&mut next_group, 0, &keep)? {
            let (file, mut reader, mut scanned) = self.run_reader(&run, &read_projection, 0)?;
            while let Some(batch) = reader.next().transpose()? {
                if checkpoint(scanned) {
                    return Err(Cancelled.into());
                }
                scanned += batch.num_rows() as u64;
                scanned_total = scanned;
                let batch = self.reorder_batch(batch, &read_projection, file)?;
                let batch = match filter {
                    Some(filter) => filter_record_batch(
                        &batch,
                        &self.evaluate_filter(filter, &batch, &read_projection)?,
                    )?,
                    None => batch,
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                let batch = batch.project(&kept_positions)?;
                let batch_rows = batch.num_rows();
                let batch = with_sequence(batch, sequence)?;
                sequence += batch_rows as u64;
                rows = rows.saturating_add(batch_rows);
                bytes = bytes.saturating_add(batch.get_array_memory_size());
                retained.push(batch);
                if let Some(top) = top
                    && rows >= top.saturating_add(8 * BATCH_SIZE)
                {
                    let kept = order.sort(&retained, Some(top))?;
                    rows = kept.num_rows();
                    bytes = kept.get_array_memory_size();
                    retained = vec![kept];
                }
                let over_budget = (top.is_none() && rows > budget.max_rows())
                    || bytes
                        .saturating_mul(3)
                        .saturating_add(rows.saturating_mul(8))
                        > budget.max_bytes();
                if over_budget {
                    if top.is_some() && rows <= budget.max_rows() {
                        // A top-N whose kept rows alone are too large: no spill helps.
                        bail!(
                            "the rows kept for this LIMIT exceed the in-memory sort budget of \
                             {} bytes; select fewer columns or a smaller LIMIT",
                            budget.max_bytes()
                        );
                    }
                    // Too many rows to sort in memory: write them out as a sorted run.
                    let writer = match &mut runs {
                        Some(writer) => writer,
                        None => runs.insert(RunWriter::new()?),
                    };
                    writer.write(&order.sort(&retained, None)?)?;
                    retained.clear();
                    rows = 0;
                    bytes = 0;
                }
            }
        }

        match runs {
            Some(mut writer) => {
                if !retained.is_empty() {
                    writer.write(&order.sort(&retained, None)?)?;
                }
                drop(retained);
                let merge = writer.into_merge(order, output_positions, limit, &|| {
                    checkpoint(scanned_total)
                })?;
                Ok(SortOutput::Spilled(merge))
            }
            None if retained.is_empty() => Ok(SortOutput::InMemory(Vec::new())),
            None => {
                let sorted = order.sort(&retained, top)?.project(&output_positions)?;
                Ok(SortOutput::InMemory(
                    (0..sorted.num_rows())
                        .step_by(PAGE_ROWS as usize)
                        .map(|first_row| {
                            sorted.slice(
                                first_row,
                                (sorted.num_rows() - first_row).min(PAGE_ROWS as usize),
                            )
                        })
                        .collect(),
                ))
            }
        }
    }

    /// Computes aggregates incrementally over decoded batches without retaining input rows.
    /// `checkpoint` is called before each decoded batch with the number of source
    /// rows scanned so far; when it returns `true` the read ends with a
    /// [`Cancelled`] error.
    pub fn read_aggregated(
        &self,
        filter: Option<&FilterExpr>,
        aggregate: &AggregateSpec,
        budget: GroupBudget,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<RecordBatch> {
        self.validate_aggregate(aggregate)?;
        if let Some(filter) = filter {
            self.validate_filter(filter)?;
        }

        let filter_columns = filter
            .map(|filter| filter.columns(&self.dataset_metadata.schema))
            .transpose()?
            .unwrap_or_default();
        let mut read_columns = aggregate.group_by().to_vec();
        for expression in aggregate.expressions() {
            if let Some(column) = expression.column()
                && !read_columns.contains(&column)
            {
                read_columns.push(column);
            }
        }
        for column in filter_columns {
            if !read_columns.contains(&column) {
                read_columns.push(column);
            }
        }
        if read_columns.is_empty() {
            let mut groups = GroupCollector::new(aggregate, &self.dataset_metadata.schema, budget)?;
            groups.update_count_all(self.row_count())?;
            return groups.finish();
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let group_positions = aggregate
            .group_by()
            .iter()
            .map(|column| projected_position(&read_projection, *column, "group"))
            .collect::<Result<Vec<_>>>()?;
        let expression_positions = aggregate
            .expressions()
            .iter()
            .map(|expression| {
                expression
                    .column()
                    .map(|column| projected_position(&read_projection, column, "aggregate"))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        let mut groups = GroupCollector::new(aggregate, &self.dataset_metadata.schema, budget)?;
        let mut next_group = 0;
        let keep = |row_group: &RowGroupInfo| {
            filter.is_none_or(|filter| self.row_group_might_match(filter, row_group))
        };
        while let Some(run) = self.next_scan_run(&mut next_group, 0, &keep)? {
            let (file, mut reader, mut scanned) = self.run_reader(&run, &read_projection, 0)?;
            while let Some(batch) = reader.next().transpose()? {
                if checkpoint(scanned) {
                    return Err(Cancelled.into());
                }
                scanned += batch.num_rows() as u64;
                let batch = self.reorder_batch(batch, &read_projection, file)?;
                let batch = match filter {
                    Some(filter) => filter_record_batch(
                        &batch,
                        &self.evaluate_filter(filter, &batch, &read_projection)?,
                    )?,
                    None => batch,
                };
                groups.update_batch(&batch, &group_positions, &expression_positions)?;
            }
        }
        groups.finish()
    }

    pub fn validate_aggregate(&self, aggregate: &AggregateSpec) -> Result<()> {
        let schema = &self.dataset_metadata.schema;
        for &group in aggregate.group_by() {
            let field = schema
                .fields()
                .get(group)
                .ok_or_else(|| anyhow!("group column {group} out of range"))?;
            if !is_comparable(field.data_type()) {
                bail!(
                    "GROUP BY is not supported for {:?} columns",
                    field.data_type()
                );
            }
        }
        for expression in aggregate.expressions() {
            let field = expression
                .column()
                .map(|column| {
                    schema
                        .fields()
                        .get(column)
                        .ok_or_else(|| anyhow!("aggregate column {column} out of range"))
                })
                .transpose()?;
            match (expression.function(), field) {
                (AggregateFunction::Count, _) => {}
                (AggregateFunction::Sum | AggregateFunction::Avg, Some(field))
                    if is_summable(field.data_type()) => {}
                (AggregateFunction::Min | AggregateFunction::Max, Some(field))
                    if is_comparable(field.data_type()) => {}
                (AggregateFunction::Sum | AggregateFunction::Avg, Some(field)) => bail!(
                    "{:?} is not supported for {:?} columns",
                    expression.function(),
                    field.data_type()
                ),
                (AggregateFunction::Min | AggregateFunction::Max, Some(field)) => bail!(
                    "{:?} is not supported for {:?} columns",
                    expression.function(),
                    field.data_type()
                ),
                (_, None) => bail!("only COUNT may use *"),
            }
        }
        Ok(())
    }

    /// Returns the schema produced by a validated aggregate without scanning data.
    pub fn aggregate_schema(&self, aggregate: &AggregateSpec) -> Result<Arc<Schema>> {
        self.validate_aggregate(aggregate)?;
        Ok(GroupCollector::new(
            aggregate,
            &self.dataset_metadata.schema,
            GroupBudget::default(),
        )?
        .output_schema())
    }

    pub fn validate_sort(&self, sort: &[SortSpec]) -> Result<()> {
        validate_sort_keys(sort)?;
        for key in sort {
            let field = self
                .dataset_metadata
                .schema
                .fields()
                .get(key.column)
                .ok_or_else(|| anyhow!("sort column {} out of range", key.column))?;
            if !is_comparable(field.data_type()) {
                bail!(
                    "sorting is not supported for {:?} columns",
                    field.data_type()
                );
            }
        }
        Ok(())
    }

    /// The next run of row groups a scan reads with one reader, searching from
    /// `next_group` and advancing it: consecutive row groups of one file that end
    /// after `start_row` and that `keep` accepts, up to [`SCAN_RUN_ROWS`] rows.
    /// The run's rows are contiguous in the source, so a scan's position is the
    /// run's first row plus the rows it has decoded.
    fn next_scan_run(
        &self,
        next_group: &mut usize,
        start_row: u64,
        keep: &dyn Fn(&RowGroupInfo) -> bool,
    ) -> Result<Option<ScanRun>> {
        let row_groups = self.row_groups();
        let mut run: Option<ScanRun> = None;
        while let Some(row_group) = row_groups.get(*next_group) {
            let (file, local) = self
                .row_group_files
                .get(row_group.index)
                .copied()
                .ok_or_else(|| anyhow!("row group {} out of range", row_group.index))?;
            if let Some(run) = &run
                && (file != run.file || run.row_count >= SCAN_RUN_ROWS)
            {
                break;
            }
            *next_group += 1;
            let wanted = row_group.first_row + row_group.row_count > start_row && keep(row_group);
            match (&mut run, wanted) {
                (Some(run), true) => {
                    run.local_row_groups.push(local);
                    run.row_count += row_group.row_count;
                }
                // A skipped row group ends the run: the next one is not contiguous.
                (Some(_), false) => break,
                (None, true) => {
                    run = Some(ScanRun {
                        file,
                        local_row_groups: vec![local],
                        first_row: row_group.first_row,
                        row_count: row_group.row_count,
                    });
                }
                (None, false) => {}
            }
        }
        Ok(run)
    }

    /// A reader over `run` from `start_row` on, with its file and the source row of
    /// its first decoded row.
    fn run_reader(
        &self,
        run: &ScanRun,
        projection: &Projection,
        start_row: u64,
    ) -> Result<(
        &SourceFile,
        GuardedReader<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
        u64,
    )> {
        let file = self
            .files
            .get(run.file)
            .ok_or_else(|| anyhow!("file {} out of range", run.file))?;
        // Only the run containing `start_row` starts part-way through.
        let skip = start_row.saturating_sub(run.first_row).min(run.row_count);
        let selection = if skip > 0 {
            Some(RowSelection::from(vec![
                RowSelector::skip(usize::try_from(skip)?),
                RowSelector::select(usize::try_from(run.row_count - skip)?),
            ]))
        } else {
            None
        };
        let reader = self.reader_for(file, projection, run.local_row_groups.clone(), selection)?;
        Ok((file, reader, run.first_row + skip))
    }

    fn read_window_batches(
        &self,
        first_row: u64,
        row_count: usize,
        projection: &Projection,
    ) -> Result<Vec<RecordBatch>> {
        let row_count = self
            .dataset_metadata
            .validate_window(first_row, row_count)
            .with_context(|| {
                format!("validate row window first_row={first_row} row_count={row_count}")
            })?;
        if row_count == 0 {
            return Ok(Vec::new());
        }

        let row_group_indexes = self
            .dataset_metadata
            .overlapping_row_group_indexes(first_row, row_count);
        if row_group_indexes.is_empty() {
            return Ok(Vec::new());
        }

        let end_row = first_row + row_count as u64;
        let mut batches = Vec::new();
        // Consecutive row groups of one file are read with one reader.
        let mut start = 0;
        while start < row_group_indexes.len() {
            let file_index = self
                .row_group_files
                .get(row_group_indexes[start])
                .map(|(file, _)| *file);
            let end = start
                + row_group_indexes[start..]
                    .iter()
                    .take_while(|index| {
                        self.row_group_files.get(**index).map(|(file, _)| *file) == file_index
                    })
                    .count();
            let groups = &row_group_indexes[start..end.max(start + 1)];
            start = end.max(start + 1);

            let (first_group, last_group) = match (groups.first(), groups.last()) {
                (Some(first), Some(last)) => (
                    self.row_groups()
                        .get(*first)
                        .ok_or_else(|| anyhow!("row group {first} out of range"))?,
                    self.row_groups()
                        .get(*last)
                        .ok_or_else(|| anyhow!("row group {last} out of range"))?,
                ),
                _ => continue,
            };
            let chunk_start = first_group.first_row;
            let chunk_end = last_group.first_row + last_group.row_count;
            let select_from = first_row.max(chunk_start);
            let select_to = end_row.min(chunk_end);
            if select_to <= select_from {
                continue;
            }
            let mut selectors = Vec::new();
            let skip_before = usize::try_from(select_from - chunk_start)?;
            if skip_before > 0 {
                selectors.push(RowSelector::skip(skip_before));
            }
            selectors.push(RowSelector::select(usize::try_from(
                select_to - select_from,
            )?));

            let (file, _) = self.locate(groups[0])?;
            let local = groups
                .iter()
                .map(|group| self.locate(*group).map(|(_, local)| local))
                .collect::<Result<Vec<_>>>()?;
            let mut reader =
                self.reader_for(file, projection, local, Some(RowSelection::from(selectors)))?;
            while let Some(batch) = reader.next().transpose()? {
                if batch.num_rows() > 0 {
                    batches.push(self.reorder_batch(batch, projection, file)?);
                }
            }
        }

        Ok(batches)
    }

    /// The file columns decoded for `projection`, in file order. Partition columns
    /// are not stored; when only they are wanted, the first column is decoded so
    /// batches still carry their row counts.
    fn decoded_columns(&self, projection: &Projection) -> Vec<usize> {
        let mut columns = projection
            .parquet_columns()
            .into_iter()
            .filter(|column| *column < self.file_columns)
            .collect::<Vec<_>>();
        if columns.is_empty() && self.file_columns > 0 {
            columns.push(0);
        }
        columns
    }

    /// A reader over `row_groups` (indexes within `source_file`).
    fn reader_for(
        &self,
        source_file: &SourceFile,
        projection: &Projection,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
    ) -> Result<GuardedReader<parquet::arrow::arrow_reader::ParquetRecordBatchReader>> {
        let file = File::open(&source_file.path)
            .with_context(|| format!("open {}", source_file.path.display()))?;
        let parquet_columns = self.decoded_columns(projection);
        self.check_dictionary_pages(&file, source_file, &parquet_columns, &row_groups)?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            file,
            source_file.arrow_metadata.clone(),
        );
        let projection = ProjectionMask::roots(builder.parquet_schema(), parquet_columns);
        let mut builder = builder
            .with_batch_size(BATCH_SIZE)
            .with_projection(projection)
            .with_row_groups(row_groups);

        if let Some(selection) = selection {
            builder = builder.with_row_selection(selection);
        }

        Ok(GuardedReader::new(builder.build()?))
    }

    /// Rejects dictionary pages whose header claims more values than the page can hold.
    ///
    /// The parquet crate sizes the dictionary from the header's `num_values` before decoding
    /// (`DictDecoder::set_dict`), so a 20-byte page claiming 1.5 billion values allocates
    /// gigabytes, and a failed allocation aborts the process. Each chunk is checked once.
    fn check_dictionary_pages(
        &self,
        file: &File,
        source_file: &SourceFile,
        root_columns: &[usize],
        row_groups: &[usize],
    ) -> Result<()> {
        let schema = source_file.metadata.file_metadata().schema_descr();
        let leaves: Vec<usize> = (0..schema.num_columns())
            .filter(|&leaf| root_columns.contains(&schema.get_column_root_idx(leaf)))
            .collect();
        for &group in row_groups {
            let Some(row_group) = source_file.metadata.row_groups().get(group) else {
                continue;
            };
            for &leaf in &leaves {
                let key = (source_file.first_row_group + group, leaf);
                let already = self
                    .checked_dictionaries
                    .lock()
                    .map_err(|_| anyhow!("dictionary check lock poisoned"))?
                    .contains(&key);
                if already {
                    continue;
                }
                if let Some(column) = row_group.columns().get(leaf) {
                    check_dictionary_page(file, column).with_context(|| {
                        format!(
                            "malformed Parquet data in column {} of row group {group}",
                            column.column_path()
                        )
                    })?;
                }
                self.checked_dictionaries
                    .lock()
                    .map_err(|_| anyhow!("dictionary check lock poisoned"))?
                    .insert(key);
            }
        }
        Ok(())
    }

    /// Evaluates `filter` on a batch read with `read_projection`, which holds every
    /// column the filter names.
    fn evaluate_filter(
        &self,
        filter: &FilterExpr,
        batch: &RecordBatch,
        read_projection: &Projection,
    ) -> Result<arrow_array::BooleanArray> {
        filter.evaluate(batch, &self.dataset_metadata.schema, &|column| {
            projected_position(read_projection, column, "filter")
        })
    }

    /// `false` only when the row group's statistics prove no row can pass `filter`.
    fn row_group_might_match(&self, filter: &FilterExpr, row_group: &RowGroupInfo) -> bool {
        let schema = &self.dataset_metadata.schema;
        let Ok((file, local)) = self.locate(row_group.index) else {
            return true;
        };
        let chunks = file.metadata.row_group(local).columns();
        filter.might_match(schema, &|column| {
            let data_type = schema.fields().get(column)?.data_type();
            if column >= self.file_columns {
                // Every row of the file holds its folder's value.
                let statistics = file
                    .partition_statistics
                    .get(local)?
                    .get(column - self.file_columns)?;
                return Some((Some(statistics), data_type));
            }
            // A nested column has no single min/max; it never rules a group out.
            let leaf = file.leaf_columns.get(column).copied().flatten()?;
            chunks
                .get(leaf)
                .map(|chunk| (chunk.statistics(), data_type))
        })
    }

    fn concat_or_empty(
        &self,
        projection: &Projection,
        mut batches: Vec<RecordBatch>,
    ) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(self.empty_batch(projection));
        }

        if batches.len() == 1
            && let Some(batch) = batches.pop()
        {
            return Ok(batch);
        }

        Ok(concat_batches(&batches[0].schema(), &batches)?)
    }

    fn empty_batch(&self, projection: &Projection) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(self.projected_schema(projection)))
    }

    fn projected_schema(&self, projection: &Projection) -> Schema {
        let fields: Vec<Field> = projection
            .as_slice()
            .iter()
            .map(|column| self.dataset_metadata.schema.field(*column).clone())
            .collect();
        Schema::new(fields)
    }

    /// Puts a decoded batch of `file` into `projection` order, adding partition
    /// columns.
    fn reorder_batch(
        &self,
        batch: RecordBatch,
        projection: &Projection,
        file: &SourceFile,
    ) -> Result<RecordBatch> {
        let decoded = self.decoded_columns(projection);
        let rows = batch.num_rows();
        let arrays = projection
            .as_slice()
            .iter()
            .map(|&column| {
                if column >= self.file_columns {
                    let partition = column - self.file_columns;
                    let data_type = self
                        .partition_columns
                        .get(partition)
                        .map(|partition| &partition.data_type)
                        .ok_or_else(|| anyhow!("partition column {column} out of range"))?;
                    let value = file.partition_values.get(partition).cloned().flatten();
                    return constant_array(data_type, value.as_deref(), rows);
                }
                decoded
                    .iter()
                    .position(|read_column| *read_column == column)
                    .map(|position| batch.column(position).clone())
                    .ok_or_else(|| anyhow!("projected column {column} was not decoded"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            Arc::new(self.projected_schema(projection)),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)
    }
}

/// Consecutive row groups of one file that a scan reads with one reader.
struct ScanRun {
    /// Index into [`ParquetSource::files`].
    file: usize,
    local_row_groups: Vec<usize>,
    /// Source row of the run's first row.
    first_row: u64,
    row_count: u64,
}

/// The error a long read returns when its `checkpoint` callback asks it to end early.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("read cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// Matching rows from [`ParquetSource::read_filtered_from`] and where to resume.
#[derive(Clone, Debug)]
pub struct FilteredWindow {
    pub batch: RecordBatch,
    /// Source row to continue scanning from; equal to the row count at the end.
    pub next_row: u64,
}

/// Column positions and direction for sorting buffered rows. `sequence` holds each
/// row's position in the filtered input and breaks ties so sorting is stable.
/// The result of [`ParquetSource::read_sorted`].
#[derive(Debug)]
pub enum SortOutput {
    /// Everything fit in memory: the sorted rows, in page-sized batches.
    InMemory(Vec<RecordBatch>),
    /// The sort spilled to disk; rows come from the merge as they are asked for.
    Spilled(SortMerge),
}

/// Appends a sequence-number column starting at `first`.
fn with_sequence(batch: RecordBatch, first: u64) -> Result<RecordBatch> {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "__pavi_sequence",
        DataType::UInt64,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from_iter_values(
        first..first + batch.num_rows() as u64,
    )));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn project_batch(
    batch: &RecordBatch,
    projection: &Projection,
    output_positions: &[usize],
) -> Result<RecordBatch> {
    let fields: Vec<Field> = projection
        .as_slice()
        .iter()
        .enumerate()
        .map(|(index, _)| batch.schema().field(output_positions[index]).clone())
        .collect();
    let arrays = output_positions
        .iter()
        .map(|position| batch.column(*position).clone())
        .collect();

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum Scalar {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    Utf8(String),
    Date32(i32),
    Date64(i64),
    Timestamp(i64),
    /// Time of day in the column's unit.
    Time(i64),
    /// Unscaled decimal; one column always has one scale.
    Decimal(i128),
}

impl Scalar {
    fn bytes(&self) -> usize {
        match self {
            Self::Utf8(value) => 16 + value.len(),
            _ => 16,
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    fn f64(&self) -> Result<f64> {
        match self {
            Self::F64(value) => Ok(f64::from_bits(*value)),
            Self::I64(value) => Ok(*value as f64),
            Self::U64(value) => Ok(*value as f64),
            // Unscaled: AVG divides by the scale (`Accumulator::Avg::divisor`).
            Self::Decimal(value) => Ok(*value as f64),
            _ => bail!("aggregate encountered an incompatible numeric value"),
        }
    }
}

enum Accumulator {
    Count(u64),
    SumI64(Option<i64>),
    SumU64(Option<u64>),
    SumF64(Option<f64>),
    /// Unscaled decimal sum.
    SumDecimal(Option<i128>),
    Avg {
        sum: f64,
        count: u64,
        /// 10^scale for decimals, so the mean comes out in the column's units.
        divisor: f64,
    },
    Min(Option<Scalar>),
    Max(Option<Scalar>),
}

impl Accumulator {
    fn new(expression: AggregateExpr, data_type: Option<&DataType>) -> Result<Self> {
        match expression.function() {
            AggregateFunction::Count => Ok(Self::Count(0)),
            AggregateFunction::Sum => match data_type {
                Some(data_type) if is_decimal(data_type) => Ok(Self::SumDecimal(None)),
                Some(data_type) if is_signed_integer(data_type) => Ok(Self::SumI64(None)),
                Some(data_type) if is_unsigned_integer(data_type) => Ok(Self::SumU64(None)),
                Some(DataType::Float32 | DataType::Float64) => Ok(Self::SumF64(None)),
                _ => bail!("SUM needs a numeric column"),
            },
            AggregateFunction::Avg => Ok(Self::Avg {
                sum: 0.0,
                count: 0,
                divisor: data_type.map_or(1.0, decimal_divisor),
            }),
            AggregateFunction::Min => Ok(Self::Min(None)),
            AggregateFunction::Max => Ok(Self::Max(None)),
        }
    }

    fn update(&mut self, expression: AggregateExpr, value: Option<Scalar>) -> Result<()> {
        match self {
            Self::Count(count) => {
                if expression.column().is_none()
                    || value.as_ref().is_some_and(|value| !value.is_null())
                {
                    *count = count.checked_add(1).context("COUNT overflow")?;
                }
            }
            Self::SumI64(sum) => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                let Scalar::I64(value) = value else {
                    bail!("SUM encountered an incompatible signed value");
                };
                *sum = Some(
                    sum.unwrap_or(0)
                        .checked_add(value)
                        .context("SUM overflow for signed integer")?,
                );
            }
            Self::SumU64(sum) => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                let Scalar::U64(value) = value else {
                    bail!("SUM encountered an incompatible unsigned value");
                };
                *sum = Some(
                    sum.unwrap_or(0)
                        .checked_add(value)
                        .context("SUM overflow for unsigned integer")?,
                );
            }
            Self::SumDecimal(sum) => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                let Scalar::Decimal(value) = value else {
                    bail!("SUM encountered an incompatible decimal value");
                };
                *sum = Some(
                    sum.unwrap_or(0)
                        .checked_add(value)
                        .filter(|sum| sum.unsigned_abs() < DECIMAL128_LIMIT)
                        .context("SUM overflow for decimal (more than 38 digits)")?,
                );
            }
            Self::SumF64(sum) => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                let value = value.f64()?;
                let next = sum.unwrap_or(0.0) + value;
                if !next.is_finite() {
                    bail!("SUM overflow for floating-point value");
                }
                *sum = Some(next);
            }
            Self::Avg { sum, count, .. } => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                let next = *sum + value.f64()?;
                if !next.is_finite() {
                    bail!("AVG overflow for numeric value");
                }
                *sum = next;
                *count = count.checked_add(1).context("AVG count overflow")?;
            }
            Self::Min(current) => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                if current
                    .as_ref()
                    .is_none_or(|current| scalar_compare(&value, current).is_lt())
                {
                    *current = Some(value);
                }
            }
            Self::Max(current) => {
                let Some(value) = value.filter(|value| !value.is_null()) else {
                    return Ok(());
                };
                if current
                    .as_ref()
                    .is_none_or(|current| scalar_compare(&value, current).is_gt())
                {
                    *current = Some(value);
                }
            }
        }
        Ok(())
    }

    fn value(&self) -> Option<Scalar> {
        match self {
            Self::Count(value) => Some(Scalar::U64(*value)),
            Self::SumI64(value) => value.map(Scalar::I64),
            Self::SumU64(value) => value.map(Scalar::U64),
            Self::SumF64(value) => value.map(|value| Scalar::F64(value.to_bits())),
            Self::SumDecimal(value) => value.map(Scalar::Decimal),
            Self::Avg {
                sum,
                count,
                divisor,
            } if *count > 0 => Some(Scalar::F64((sum / *count as f64 / divisor).to_bits())),
            Self::Avg { .. } => None,
            Self::Min(value) | Self::Max(value) => value.clone(),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Min(Some(value)) | Self::Max(Some(value)) => value.bytes(),
            _ => 16,
        }
    }
}

struct GroupState {
    /// One value per GROUP BY column; empty for the one overall group.
    key: Vec<Scalar>,
    accumulators: Vec<Accumulator>,
}

impl GroupState {
    fn new(
        key: Vec<Scalar>,
        expressions: &[AggregateExpr],
        input_types: &[Option<DataType>],
    ) -> Result<Self> {
        let accumulators = expressions
            .iter()
            .zip(input_types)
            .map(|(expression, data_type)| Accumulator::new(*expression, data_type.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { key, accumulators })
    }

    fn bytes(&self) -> usize {
        64 + self.key.iter().map(Scalar::bytes).sum::<usize>()
            + self
                .accumulators
                .iter()
                .map(Accumulator::bytes)
                .sum::<usize>()
    }
}

struct GroupCollector {
    expressions: Vec<AggregateExpr>,
    input_types: Vec<Option<DataType>>,
    input_names: Vec<Option<String>>,
    output_types: Vec<DataType>,
    /// Name and type of each GROUP BY column.
    group_by: Vec<(String, DataType)>,
    groups: Vec<GroupState>,
    indexes: HashMap<Vec<Scalar>, usize>,
    bytes: usize,
    budget: GroupBudget,
}

impl GroupCollector {
    fn new(aggregate: &AggregateSpec, schema: &Schema, budget: GroupBudget) -> Result<Self> {
        let expressions = aggregate.expressions().to_vec();
        let input_types = expressions
            .iter()
            .map(|expression| {
                expression
                    .column()
                    .map(|column| {
                        schema
                            .fields()
                            .get(column)
                            .map(|field| field.data_type().clone())
                            .ok_or_else(|| anyhow!("aggregate column {column} out of range"))
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let output_types = expressions
            .iter()
            .zip(&input_types)
            .map(|(expression, data_type)| aggregate_output_type(*expression, data_type.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let input_names = expressions
            .iter()
            .map(|expression| {
                expression
                    .column()
                    .map(|column| {
                        schema
                            .fields()
                            .get(column)
                            .map(|field| field.name().to_owned())
                            .ok_or_else(|| anyhow!("aggregate column {column} out of range"))
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let group_by = aggregate
            .group_by()
            .iter()
            .map(|column| {
                schema
                    .fields()
                    .get(*column)
                    .map(|field| (field.name().to_owned(), field.data_type().clone()))
                    .ok_or_else(|| anyhow!("group column {column} out of range"))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut collector = Self {
            expressions,
            input_types,
            input_names,
            output_types,
            group_by,
            groups: Vec::new(),
            indexes: HashMap::new(),
            bytes: 0,
            budget,
        };
        if collector.group_by.is_empty() {
            collector.ensure_group(Vec::new())?;
        }
        Ok(collector)
    }

    fn update_batch(
        &mut self,
        batch: &RecordBatch,
        group_positions: &[usize],
        expression_positions: &[Option<usize>],
    ) -> Result<()> {
        if group_positions.len() != self.group_by.len() {
            bail!("aggregate group projection was inconsistent");
        }
        for row in 0..batch.num_rows() {
            let key = self
                .group_by
                .iter()
                .zip(group_positions)
                .map(|((_, data_type), position)| {
                    scalar_at(batch.column(*position).as_ref(), row, data_type)
                })
                .collect::<Result<Vec<_>>>()?;
            let index = self.ensure_group(key)?;
            let before = self.groups[index].bytes();
            for (expression_index, ((expression, input_type), position)) in self
                .expressions
                .iter()
                .zip(&self.input_types)
                .zip(expression_positions)
                .enumerate()
            {
                let value = match (input_type, position) {
                    (Some(_), Some(position))
                        if expression.function() == AggregateFunction::Count =>
                    {
                        Some(if batch.column(*position).is_null(row) {
                            Scalar::Null
                        } else {
                            Scalar::Bool(true)
                        })
                    }
                    (Some(data_type), Some(position)) => {
                        Some(scalar_at(batch.column(*position).as_ref(), row, data_type)?)
                    }
                    (None, None) => None,
                    _ => bail!("aggregate input projection was inconsistent"),
                };
                let accumulator = self.groups[index]
                    .accumulators
                    .get_mut(expression_index)
                    .ok_or_else(|| anyhow!("aggregate accumulator was lost"))?;
                accumulator.update(*expression, value)?;
            }
            let after = self.groups[index].bytes();
            self.bytes = self.bytes.saturating_sub(before).saturating_add(after);
            if self.bytes > self.budget.max_bytes() {
                bail!(
                    "grouped aggregate exceeds the in-memory group budget of {} bytes",
                    self.budget.max_bytes()
                );
            }
        }
        Ok(())
    }

    fn update_count_all(&mut self, rows: u64) -> Result<()> {
        for accumulator in &mut self.groups[0].accumulators {
            let Accumulator::Count(count) = accumulator else {
                bail!("COUNT(*) fast path received a non-count aggregate");
            };
            *count = count.checked_add(rows).context("COUNT overflow")?;
        }
        Ok(())
    }

    fn ensure_group(&mut self, key: Vec<Scalar>) -> Result<usize> {
        if let Some(index) = self.indexes.get(&key) {
            return Ok(*index);
        }
        if self.groups.len() >= self.budget.max_groups() {
            bail!(
                "grouped aggregate exceeds the in-memory group budget of {} groups",
                self.budget.max_groups()
            );
        }
        let state = GroupState::new(key.clone(), &self.expressions, &self.input_types)?;
        let next_bytes = self.bytes.saturating_add(state.bytes());
        if next_bytes > self.budget.max_bytes() {
            bail!(
                "grouped aggregate exceeds the in-memory group budget of {} bytes",
                self.budget.max_bytes()
            );
        }
        let index = self.groups.len();
        self.bytes = next_bytes;
        self.indexes.insert(key, index);
        self.groups.push(state);
        Ok(index)
    }

    fn finish(self) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (index, (_, data_type)) in self.group_by.iter().enumerate() {
            arrays.push(build_array(
                data_type,
                &self
                    .groups
                    .iter()
                    .map(|group| group.key.get(index).cloned())
                    .collect::<Vec<_>>(),
            )?);
        }
        for (index, (_expression, output_type)) in
            self.expressions.iter().zip(&self.output_types).enumerate()
        {
            arrays.push(build_array(
                output_type,
                &self
                    .groups
                    .iter()
                    .map(|group| group.accumulators[index].value())
                    .collect::<Vec<_>>(),
            )?);
        }
        RecordBatch::try_new(self.output_schema(), arrays).map_err(Into::into)
    }

    fn output_schema(&self) -> Arc<Schema> {
        let mut fields = Vec::new();
        for (name, data_type) in &self.group_by {
            fields.push(Field::new(name, data_type.clone(), true));
        }
        for (index, (expression, output_type)) in
            self.expressions.iter().zip(&self.output_types).enumerate()
        {
            fields.push(Field::new(
                aggregate_name(*expression, &self.input_names[index]),
                output_type.clone(),
                expression.function() != AggregateFunction::Count,
            ));
        }
        Arc::new(Schema::new(fields))
    }
}

fn aggregate_output_type(expression: AggregateExpr, input: Option<&DataType>) -> Result<DataType> {
    match expression.function() {
        AggregateFunction::Count => Ok(DataType::UInt64),
        AggregateFunction::Sum if input.is_some_and(is_signed_integer) => Ok(DataType::Int64),
        // A sum keeps the scale and gets the widest decimal precision.
        AggregateFunction::Sum => match input {
            Some(
                DataType::Decimal32(_, scale)
                | DataType::Decimal64(_, scale)
                | DataType::Decimal128(_, scale),
            ) => Ok(DataType::Decimal128(38, *scale)),
            _ if input.is_some_and(is_unsigned_integer) => Ok(DataType::UInt64),
            _ if input.is_some_and(is_float) => Ok(DataType::Float64),
            _ => bail!("aggregate expression has an unsupported input type"),
        },
        AggregateFunction::Avg if input.is_some_and(is_summable) => Ok(DataType::Float64),
        AggregateFunction::Min | AggregateFunction::Max => {
            input.cloned().context("MIN/MAX needs a source column")
        }
        _ => bail!("aggregate expression has an unsupported input type"),
    }
}

fn aggregate_name(expression: AggregateExpr, input: &Option<String>) -> String {
    let function = match expression.function() {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Avg => "AVG",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    };
    match (expression.function(), expression.column(), input) {
        (AggregateFunction::Count, None, _) => "COUNT(*)".to_string(),
        (_, Some(_), Some(column)) => format!("{function}({column})"),
        _ => function.to_string(),
    }
}

fn scalar_at(array: &dyn Array, row: usize, data_type: &DataType) -> Result<Scalar> {
    if array.is_null(row) {
        return Ok(Scalar::Null);
    }
    match data_type {
        DataType::Boolean => Ok(Scalar::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("read boolean aggregate value")?
                .value(row),
        )),
        DataType::Int8 => Ok(Scalar::I64(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .context("read int8 aggregate value")?
                .value(row) as i64,
        )),
        DataType::Int16 => Ok(Scalar::I64(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .context("read int16 aggregate value")?
                .value(row) as i64,
        )),
        DataType::Int32 => Ok(Scalar::I64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("read int32 aggregate value")?
                .value(row) as i64,
        )),
        DataType::Int64 => Ok(Scalar::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("read int64 aggregate value")?
                .value(row),
        )),
        DataType::UInt8 => Ok(Scalar::U64(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .context("read uint8 aggregate value")?
                .value(row) as u64,
        )),
        DataType::UInt16 => Ok(Scalar::U64(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .context("read uint16 aggregate value")?
                .value(row) as u64,
        )),
        DataType::UInt32 => Ok(Scalar::U64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .context("read uint32 aggregate value")?
                .value(row) as u64,
        )),
        DataType::UInt64 => Ok(Scalar::U64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("read uint64 aggregate value")?
                .value(row),
        )),
        DataType::Float32 => Ok(Scalar::F64(
            (array
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("read float32 aggregate value")?
                .value(row) as f64)
                .to_bits(),
        )),
        DataType::Float64 => Ok(Scalar::F64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("read float64 aggregate value")?
                .value(row)
                .to_bits(),
        )),
        DataType::Utf8 => Ok(Scalar::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("read string aggregate value")?
                .value(row)
                .to_owned(),
        )),
        DataType::LargeUtf8 => Ok(Scalar::Utf8(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("read large string aggregate value")?
                .value(row)
                .to_owned(),
        )),
        DataType::Utf8View => Ok(Scalar::Utf8(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .context("read string view aggregate value")?
                .value(row)
                .to_owned(),
        )),
        DataType::Date32 => Ok(Scalar::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .context("read date32 aggregate value")?
                .value(row),
        )),
        DataType::Date64 => Ok(Scalar::Date64(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .context("read date64 aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("read second timestamp aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("read millisecond timestamp aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("read microsecond timestamp aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("read nanosecond timestamp aggregate value")?
                .value(row),
        )),
        DataType::Time32(TimeUnit::Second) => Ok(Scalar::Time(
            array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .context("read time aggregate value")?
                .value(row)
                .into(),
        )),
        DataType::Time32(TimeUnit::Millisecond) => Ok(Scalar::Time(
            array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .context("read time aggregate value")?
                .value(row)
                .into(),
        )),
        DataType::Time64(TimeUnit::Microsecond) => Ok(Scalar::Time(
            array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .context("read time aggregate value")?
                .value(row),
        )),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(Scalar::Time(
            array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .context("read time aggregate value")?
                .value(row),
        )),
        DataType::Decimal32(_, _) => Ok(Scalar::Decimal(
            array
                .as_any()
                .downcast_ref::<Decimal32Array>()
                .context("read decimal aggregate value")?
                .value(row)
                .into(),
        )),
        DataType::Decimal64(_, _) => Ok(Scalar::Decimal(
            array
                .as_any()
                .downcast_ref::<Decimal64Array>()
                .context("read decimal aggregate value")?
                .value(row)
                .into(),
        )),
        DataType::Decimal128(_, _) => Ok(Scalar::Decimal(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .context("read decimal aggregate value")?
                .value(row),
        )),
        _ => bail!("aggregate cannot read unsupported {data_type:?} values"),
    }
}

fn scalar_compare(left: &Scalar, right: &Scalar) -> std::cmp::Ordering {
    match (left, right) {
        (Scalar::Bool(left), Scalar::Bool(right)) => left.cmp(right),
        (Scalar::I64(left), Scalar::I64(right)) => left.cmp(right),
        (Scalar::U64(left), Scalar::U64(right)) => left.cmp(right),
        (Scalar::F64(left), Scalar::F64(right)) => {
            f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
        }
        (Scalar::Utf8(left), Scalar::Utf8(right)) => left.cmp(right),
        (Scalar::Date32(left), Scalar::Date32(right)) => left.cmp(right),
        (Scalar::Date64(left), Scalar::Date64(right)) => left.cmp(right),
        (Scalar::Timestamp(left), Scalar::Timestamp(right)) => left.cmp(right),
        (Scalar::Time(left), Scalar::Time(right)) => left.cmp(right),
        (Scalar::Decimal(left), Scalar::Decimal(right)) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    }
}

fn build_array(data_type: &DataType, values: &[Option<Scalar>]) -> Result<ArrayRef> {
    macro_rules! values {
        ($convert:expr) => {
            values
                .iter()
                .map(|value| match value {
                    None | Some(Scalar::Null) => Ok(None),
                    Some(value) => Ok(Some($convert(value)?)),
                })
                .collect::<Result<Vec<_>>>()?
        };
    }
    Ok(match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(values!(|value: &Scalar| match value {
            Scalar::Bool(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Int8 => Arc::new(Int8Array::from(values!(|value: &Scalar| match value {
            Scalar::I64(value) => Ok(*value as i8),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Int16 => Arc::new(Int16Array::from(values!(|value: &Scalar| match value {
            Scalar::I64(value) => Ok(*value as i16),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Int32 => Arc::new(Int32Array::from(values!(|value: &Scalar| match value {
            Scalar::I64(value) => Ok(*value as i32),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Int64 => Arc::new(Int64Array::from(values!(|value: &Scalar| match value {
            Scalar::I64(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::UInt8 => Arc::new(UInt8Array::from(values!(|value: &Scalar| match value {
            Scalar::U64(value) => Ok(*value as u8),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::UInt16 => Arc::new(UInt16Array::from(values!(|value: &Scalar| match value {
            Scalar::U64(value) => Ok(*value as u16),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::UInt32 => Arc::new(UInt32Array::from(values!(|value: &Scalar| match value {
            Scalar::U64(value) => Ok(*value as u32),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::UInt64 => Arc::new(UInt64Array::from(values!(|value: &Scalar| match value {
            Scalar::U64(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Float32 => Arc::new(Float32Array::from(values!(|value: &Scalar| {
            Ok::<_, anyhow::Error>(value.f64()? as f32)
        }))),
        DataType::Float64 => Arc::new(Float64Array::from(values!(|value: &Scalar| {
            value.f64()
        }))),
        DataType::Utf8 => Arc::new(StringArray::from(values!(|value: &Scalar| match value {
            Scalar::Utf8(value) => Ok(value.clone()),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(values!(
            |value: &Scalar| match value {
                Scalar::Utf8(value) => Ok(value.clone()),
                _ => bail!("aggregate output type mismatch"),
            }
        ))),
        DataType::Utf8View => Arc::new(StringViewArray::from(values!(
            |value: &Scalar| match value {
                Scalar::Utf8(value) => Ok(value.clone()),
                _ => bail!("aggregate output type mismatch"),
            }
        ))),
        DataType::Date32 => Arc::new(Date32Array::from(values!(|value: &Scalar| match value {
            Scalar::Date32(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Date64 => Arc::new(Date64Array::from(values!(|value: &Scalar| match value {
            Scalar::Date64(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Timestamp(TimeUnit::Second, timezone) => Arc::new(
            TimestampSecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_timezone_opt(timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => Arc::new(
            TimestampMillisecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_timezone_opt(timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
            TimestampMicrosecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_timezone_opt(timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => Arc::new(
            TimestampNanosecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_timezone_opt(timezone.clone()),
        ),
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(values!(
            |value: &Scalar| match value {
                Scalar::Time(value) => i32::try_from(*value).context("time out of range"),
                _ => bail!("aggregate output type mismatch"),
            }
        ))),
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(Time32MillisecondArray::from(values!(
            |value: &Scalar| match value {
                Scalar::Time(value) => i32::try_from(*value).context("time out of range"),
                _ => bail!("aggregate output type mismatch"),
            }
        ))),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(values!(
            |value: &Scalar| match value {
                Scalar::Time(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }
        ))),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondArray::from(values!(
            |value: &Scalar| match value {
                Scalar::Time(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }
        ))),
        DataType::Decimal32(precision, scale) => Arc::new(
            Decimal32Array::from(values!(|value: &Scalar| match value {
                Scalar::Decimal(value) => i32::try_from(*value).context("decimal out of range"),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Decimal64(precision, scale) => Arc::new(
            Decimal64Array::from(values!(|value: &Scalar| match value {
                Scalar::Decimal(value) => i64::try_from(*value).context("decimal out of range"),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from(values!(|value: &Scalar| match value {
                Scalar::Decimal(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }))
            .with_precision_and_scale(*precision, *scale)?,
        ),
        _ => bail!("aggregate output is not supported for {data_type:?}"),
    })
}

/// One more than the largest unscaled `Decimal128(38, _)`: 10^38.
const DECIMAL128_LIMIT: u128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

/// What an AVG divides its sum by to undo a decimal's scale; 1 for other types.
fn decimal_divisor(data_type: &DataType) -> f64 {
    match data_type {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale) => 10_f64.powi((*scale).into()),
        _ => 1.0,
    }
}

fn projected_position(projection: &Projection, column: usize, purpose: &str) -> Result<usize> {
    projection
        .as_slice()
        .iter()
        .position(|projected| *projected == column)
        .ok_or_else(|| anyhow!("{purpose} column {column} was not projected"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, Date32Array, Int32Array, LargeListArray, ListArray, RecordBatch,
        StringArray, StructArray, TimestampMillisecondArray,
        builder::{Int32Builder, MapBuilder, StringBuilder},
        types::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::TempDir;

    use super::*;
    use crate::{NullOrder, SortDirection};

    impl ParquetSource {
        /// [`ParquetSource::read_sorted`], with a spilled merge read to the end.
        fn read_sorted_all(
            &self,
            filter: Option<&FilterExpr>,
            projection: &Projection,
            sort: &[SortSpec],
            budget: SortBudget,
            limit: Option<usize>,
            checkpoint: &dyn Fn(u64) -> bool,
        ) -> Result<Vec<RecordBatch>> {
            self.read_sorted(filter, projection, sort, budget, limit, checkpoint)
                .map(drain)
        }
    }

    /// Every row a sort gives, draining a spilled merge.
    fn drain(output: SortOutput) -> Vec<RecordBatch> {
        match output {
            SortOutput::InMemory(batches) => batches,
            SortOutput::Spilled(mut merge) => {
                let mut batches = Vec::new();
                loop {
                    let (more, finished) = merge.next_rows(100_000, &|| false).unwrap();
                    batches.extend(more);
                    if finished {
                        return batches;
                    }
                }
            }
        }
    }

    fn test_file() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.parquet");
        let file = File::create(&path).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        for start in [0, 3] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(start..start + 3)),
                    Arc::new(Int32Array::from_iter_values(
                        (start..start + 3).map(|value| value * 10),
                    )),
                ],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }

        writer.close().unwrap();
        (dir, path)
    }

    /// A 9-byte unknown footer field, `list<bool>` claiming i32::MAX elements, made the parquet
    /// crate spin for ~5 s per field while skipping it (found by the `parquet_file` fuzz target).
    #[test]
    fn rejects_footer_collections_claiming_more_elements_than_bytes() {
        let (_dir, path) = test_file();
        assert!(ParquetSource::open(&path).is_ok());

        let bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        let footer_len = u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap()) as usize;
        let mut footer = bytes[n - 8 - footer_len..n - 8].to_vec();
        assert_eq!(footer.pop(), Some(0x00), "FileMetaData stop byte");
        // Unknown field 100 (long form), list<bool> with i32::MAX elements, then stop.
        footer.extend([0x09, 0xC8, 0x01, 0xF1, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x00]);
        let mut forged = bytes[..n - 8 - footer_len].to_vec();
        forged.extend(&footer);
        forged.extend((footer.len() as u32).to_le_bytes());
        forged.extend(b"PAR1");
        std::fs::write(&path, forged).unwrap();

        let started = std::time::Instant::now();
        let error = ParquetSource::open(&path).err().unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "took {:?}",
            started.elapsed()
        );
        assert!(
            format!("{error:#}").contains("claims 2147483647 elements"),
            "{error:#}"
        );
    }

    /// The parquet crate sizes a dictionary from its page header's `num_values` before decoding;
    /// fuzzing found 20-byte pages claiming 1.5 billion values (a 5.9 GB allocation).
    #[test]
    fn rejects_dictionary_pages_claiming_more_values_than_bytes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("dictionary.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![7, 8, 7, 8]));
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![values]).unwrap())
            .unwrap();
        writer.close().unwrap();
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(
            source
                .read_window(0, 4, &Projection::all(1))
                .unwrap()
                .num_rows(),
            4
        );

        // PageHeader … 7: DictionaryPageHeader { 1: num_values = 2 } is `4C 15 04`. Claim 63
        // values instead (zigzag 0x7E keeps the byte length) in a page that holds 8 bytes.
        let column = source.files[0].metadata.row_group(0).column(0);
        let offset = column.dictionary_page_offset().unwrap() as usize;
        let mut bytes = std::fs::read(&path).unwrap();
        let at = offset
            + bytes[offset..offset + 32]
                .windows(3)
                .position(|window| window == [0x4C, 0x15, 0x04])
                .unwrap();
        bytes[at + 2] = 0x7E;
        std::fs::write(&path, bytes).unwrap();

        let source = ParquetSource::open(&path).unwrap();
        let error = source.read_window(0, 4, &Projection::all(1)).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("dictionary page claims 63 values but holds only 8 bytes"),
            "{message}"
        );
    }

    #[test]
    fn decoder_panics_become_errors_and_stop_the_reader() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let mut calls = 0;
        let decoder = std::iter::from_fn(move || {
            calls += 1;
            match calls {
                1 => Some(Ok::<_, anyhow::Error>(batch.clone())),
                2 => panic!("Decoder for dict should have been set"),
                _ => Some(Ok(batch.clone())),
            }
        });
        let mut reader = GuardedReader::new(decoder);

        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
        let error = reader.next().unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "malformed Parquet data: Decoder for dict should have been set"
        );
        assert!(
            reader.next().is_none(),
            "a panicked decoder is not polled again"
        );
    }

    /// Regression for a `parquet_file` fuzz crash: an embedded Arrow schema whose integer
    /// width is invalid ("Int type with bit width of 524288"). arrow-rs 59 panicked on it;
    /// 60 returns an error. Either way PAVI opens the file using the Parquet schema.
    #[test]
    fn opens_files_whose_embedded_arrow_schema_is_corrupt() {
        use arrow_array::Int16Array;
        use base64::{Engine as _, engine::general_purpose::STANDARD};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("hint.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int16, false)]));
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), None).unwrap();
        let values: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3]));
        writer
            .write(&RecordBatch::try_new(schema, vec![values]).unwrap())
            .unwrap();
        writer.close().unwrap();

        let original = std::fs::read(&path).unwrap();
        let metadata =
            ArrowReaderMetadata::load(&File::open(&path).unwrap(), ArrowReaderOptions::new())
                .unwrap();
        let hint = metadata
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|entry| entry.key == "ARROW:schema")
            .and_then(|entry| entry.value.clone())
            .unwrap();
        let ipc = STANDARD.decode(&hint).unwrap();
        let hint_at = original
            .windows(hint.len())
            .position(|window| window == hint.as_bytes())
            .unwrap();

        // Set the Int16 column's bit width to the value the fuzzer found. The hint keeps its
        // byte length, so the footer stays valid. Keep the patch that arrow-rs rejects (it
        // must return an error, not panic).
        let hint_rejected = (0..ipc.len().saturating_sub(3))
            .filter(|&at| ipc[at..at + 4] == 16_i32.to_le_bytes())
            .any(|at| {
                let mut forged = ipc.clone();
                forged[at..at + 4].copy_from_slice(&524_288_i32.to_le_bytes());
                let mut bytes = original.clone();
                bytes[hint_at..hint_at + hint.len()]
                    .copy_from_slice(STANDARD.encode(&forged).as_bytes());
                std::fs::write(&path, &bytes).unwrap();
                ArrowReaderMetadata::load(&File::open(&path).unwrap(), ArrowReaderOptions::new())
                    .is_err()
            });
        assert!(hint_rejected, "no patch made arrow-rs reject the hint");

        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.schema().field(0).data_type(), &DataType::Int16);
        let batch = source.read_window(0, 3, &Projection::all(1)).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn filters_and_reports_statistics_for_large_unsigned_values() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("unsigned.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("u32", DataType::UInt32, false),
            Field::new("u64", DataType::UInt64, false),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let high = (1_u64 << 63) + 5;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![
                    3_000_000_000,
                    3_000_000_001,
                    5,
                    6,
                    5,
                    3_000_000_002,
                ])),
                Arc::new(UInt64Array::from(vec![high, high + 1, 5, 6, 5, high + 2])),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(path).unwrap();
        assert_eq!(source.row_groups().len(), 3);
        let projection = Projection::all(source.column_count());
        let count = |expression: &str| {
            source
                .read_filtered_window(&FilterExpr::parse(expression).unwrap(), 0, 100, &projection)
                .unwrap()
                .num_rows()
        };
        assert_eq!(count("u32 > 2999999999"), 3);
        assert_eq!(count("u32 == 3000000001"), 1);
        assert_eq!(count("u32 < 10"), 3);
        assert_eq!(count(&format!("u64 > {}", 1_u64 << 63)), 3);
        assert_eq!(count(&format!("u64 == {}", high + 1)), 1);
        assert_eq!(count("u64 < 10"), 3);

        let statistics = |column: usize, row_group: usize| {
            source.metadata().columns()[column].row_group_statistics[row_group]
                .clone()
                .unwrap()
        };
        assert_eq!(statistics(0, 0).min.as_deref(), Some("3000000000"));
        assert_eq!(statistics(0, 0).max.as_deref(), Some("3000000001"));
        assert_eq!(statistics(0, 2).min.as_deref(), Some("5"));
        assert_eq!(statistics(0, 2).max.as_deref(), Some("3000000002"));
        assert_eq!(statistics(1, 0).min, Some(high.to_string()));
        assert_eq!(statistics(1, 2).max, Some((high + 2).to_string()));
    }

    #[test]
    fn reports_readable_statistics_for_dates_timestamps_and_decimals() {
        use arrow_array::Decimal128Array;

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("typed-stats.parquet");
        let batch = RecordBatch::try_from_iter(vec![
            (
                "day",
                Arc::new(Date32Array::from(vec![19_000, 19_010])) as ArrayRef,
            ),
            (
                "at",
                Arc::new(
                    TimestampMillisecondArray::from(vec![1_000, 61_000]).with_timezone("+01:00"),
                ),
            ),
            (
                // Precision 10 is stored as INT64.
                "price",
                Arc::new(
                    Decimal128Array::from(vec![-5, 12_345])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ),
            (
                // Precision 25 is stored as a fixed-length byte array.
                "big",
                Arc::new(
                    Decimal128Array::from(vec![-12_345, 10_i128.pow(20)])
                        .with_precision_and_scale(25, 3)
                        .unwrap(),
                ),
            ),
        ])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(path).unwrap();
        let range = |column: usize| {
            let statistics = source.metadata().columns()[column].row_group_statistics[0]
                .clone()
                .unwrap();
            (statistics.min.unwrap(), statistics.max.unwrap())
        };
        assert_eq!(range(0), ("2022-01-08".into(), "2022-01-18".into()));
        assert_eq!(
            range(1),
            (
                "1970-01-01T01:00:01+01:00".into(),
                "1970-01-01T01:01:01+01:00".into()
            )
        );
        assert_eq!(range(2), ("-0.05".into(), "123.45".into()));
        assert_eq!(
            range(3),
            ("-12.345".into(), "100000000000000000.000".into())
        );
    }

    #[test]
    fn sorts_aggregates_and_filters_every_comparable_type() {
        use arrow_array::{BooleanArray, LargeStringArray, StringViewArray};

        use crate::value::format_cell;

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("comparable.parquet");
        let start = 1_700_000_000_000_i64; // 2023-11-14T23:13:20+01:00
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(LargeStringArray::from(vec!["b", "a", "b", "a", "c"])) as ArrayRef,
            ),
            (
                "view",
                Arc::new(StringViewArray::from(vec![
                    "kiwi", "apple", "banana", "cherry", "date",
                ])),
            ),
            (
                "day",
                Arc::new(Date32Array::from(vec![
                    19_000, 19_001, 19_002, 19_003, 19_004,
                ])),
            ),
            (
                "at",
                Arc::new(
                    TimestampMillisecondArray::from_iter_values(
                        (0..5).map(|hour| start + hour * 3_600_000),
                    )
                    .with_timezone("Europe/Berlin"),
                ),
            ),
            (
                "flag",
                Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            ),
        ])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let source = ParquetSource::open(path).unwrap();
        assert_eq!(source.schema().field(0).data_type(), &DataType::LargeUtf8);
        assert_eq!(source.schema().field(1).data_type(), &DataType::Utf8View);
        let all = Projection::all(source.column_count());
        let cells = |batch: &RecordBatch, column: usize| {
            (0..batch.num_rows())
                .map(|row| format_cell(batch.column(column).as_ref(), row))
                .collect::<Vec<_>>()
        };

        // GROUP BY a LargeUtf8 column with MIN over a zoned timestamp and MAX over Utf8View.
        let grouped = source
            .read_aggregated(
                None,
                &AggregateSpec::grouped(
                    0,
                    vec![
                        AggregateExpr::count_all(),
                        AggregateExpr::min(3),
                        AggregateExpr::max(1),
                    ],
                )
                .unwrap(),
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(cells(&grouped, 0), ["b", "a", "c"]);
        assert_eq!(cells(&grouped, 1), ["2", "2", "1"]);
        assert_eq!(
            cells(&grouped, 2),
            [
                "2023-11-14T23:13:20+01:00",
                "2023-11-15T00:13:20+01:00",
                "2023-11-15T03:13:20+01:00"
            ]
        );
        assert_eq!(cells(&grouped, 3), ["kiwi", "cherry", "date"]);
        let minimum = source
            .read_aggregated(
                None,
                &AggregateSpec::new(vec![AggregateExpr::min(0)]).unwrap(),
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(minimum.schema().field(0).data_type(), &DataType::LargeUtf8);
        assert_eq!(cells(&minimum, 0), ["a"]);

        // Sorting by Utf8View and by a zoned timestamp.
        let sort = |column: usize, direction: SortDirection| {
            let batches = source
                .read_sorted_all(
                    None,
                    &Projection::columns(vec![column], source.column_count()).unwrap(),
                    &[SortSpec::new(column, direction, NullOrder::Last)],
                    SortBudget::default(),
                    None,
                    &|_| false,
                )
                .unwrap();
            cells(&batches[0], 0)
        };
        assert_eq!(
            sort(1, SortDirection::Ascending),
            ["apple", "banana", "cherry", "date", "kiwi"]
        );
        assert_eq!(
            sort(3, SortDirection::Descending)[0],
            "2023-11-15T03:13:20+01:00"
        );

        // Filters on dates, zoned timestamps (local time unless an offset is given) and Utf8View.
        let count = |expression: &str| {
            source
                .read_filtered_window(&FilterExpr::parse(expression).unwrap(), 0, 100, &all)
                .unwrap()
                .num_rows()
        };
        assert_eq!(count("day >= 2022-01-10"), 3);
        assert_eq!(count("day == 2022-01-08"), 1);
        assert_eq!(count("day > 2030-01-01"), 0);
        assert_eq!(count("at < 2023-11-15T01:00:00"), 2);
        assert_eq!(count("at == 2023-11-15T00:13:20+01:00"), 1);
        assert_eq!(count("at >= 2023-11-14T22:13:20Z"), 5);
        assert_eq!(count("view contains an"), 1);
        assert_eq!(count("view > c"), 3);
        assert_eq!(count("name == b"), 2);

        let error = |expression: &str| {
            source
                .validate_filter(&FilterExpr::parse(expression).unwrap())
                .unwrap_err()
                .to_string()
        };
        assert!(error("day == tomorrow").contains("is not a date"));
        assert!(error("at > soon").contains("is not a timestamp"));
        assert!(error("day contains 2022").contains("only supported for text"));
        assert!(error("flag > true").contains("only support == and !="));
    }

    fn numbered_file(rows: i32, row_group_rows: usize) -> (TempDir, ParquetSource) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("numbered.parquet");
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..rows)) as ArrayRef,
            ),
            (
                "bucket",
                Arc::new(Int32Array::from_iter_values((0..rows).map(|id| id % 7))),
            ),
        ])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_rows))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        (directory, ParquetSource::open(path).unwrap())
    }

    /// Writes ids `range` as one Parquet file at `root/relative`.
    fn write_ids(root: &Path, relative: &str, range: std::ops::Range<i32>, nullable: bool) {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            nullable,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(range))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2048))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn texts(batch: &RecordBatch, column: usize) -> Vec<String> {
        (0..batch.num_rows())
            .map(|row| {
                crate::value::scalar_text(batch.column(column).as_ref(), row)
                    .unwrap()
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect()
    }

    #[test]
    fn reads_a_partitioned_folder_as_one_table() {
        let directory = TempDir::new().unwrap();
        let root = directory.path();
        write_ids(root, "year=2023/region=east/a.parquet", 0..5000, false);
        write_ids(root, "year=2023/region=west/b.parquet", 5000..6000, true);
        write_ids(root, "year=2024/region=east/c.parquet", 6000..9000, false);
        write_ids(
            root,
            "year=2024/region=__HIVE_DEFAULT_PARTITION__/d.parquet",
            9000..9100,
            false,
        );
        std::fs::write(root.join("_SUCCESS"), b"").unwrap();
        let source = ParquetSource::open(root).unwrap();

        // Files in name order: a, b, d (`_` sorts before `e`), c.
        assert!(source.is_folder());
        assert_eq!(source.file_count(), 4);
        assert_eq!(source.row_count(), 9100);
        assert_eq!(source.partition_column_names(), ["year", "region"]);
        let schema = source.schema();
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable(), "nullable in one file");
        assert_eq!(source.row_groups().len(), 3 + 1 + 1 + 2);

        // A window across two files, and one ending in the null partition.
        let all = Projection::all(3);
        let window = source.read_window(4998, 4, &all).unwrap();
        assert_eq!(texts(&window, 0), ["4998", "4999", "5000", "5001"]);
        assert_eq!(texts(&window, 2), ["east", "east", "west", "west"]);
        let window = source.read_window(5998, 4, &all).unwrap();
        assert_eq!(texts(&window, 0), ["5998", "5999", "9000", "9001"]);
        assert_eq!(texts(&window, 1), ["2023", "2023", "2024", "2024"]);
        assert_eq!(texts(&window, 2), ["west", "west", "null", "null"]);
        // Only a partition column.
        let page = source
            .read_window(6098, 4, &Projection::columns(vec![2], 3).unwrap())
            .unwrap();
        assert_eq!(texts(&page, 0), ["null", "null", "east", "east"]);
        assert_eq!(
            source.metadata().columns()[1].row_group_statistics[0]
                .as_ref()
                .and_then(|statistics| statistics.min.clone()),
            Some("2023".to_string())
        );

        // A filter on a partition column reads only that partition's file.
        let scanned = std::cell::RefCell::new(Vec::new());
        let west = source
            .read_filtered_from(
                &FilterExpr::parse("region == west").unwrap(),
                0,
                10_000,
                &all,
                &|row| {
                    scanned.borrow_mut().push(row);
                    false
                },
            )
            .unwrap();
        assert_eq!(west.batch.num_rows(), 1000);
        // Read: `west` (rows 5000..6000) and the null region (6000..6100), whose
        // statistics have no min/max. Skipped: every `east` file.
        assert!(
            scanned
                .borrow()
                .iter()
                .all(|row| (5000..6100).contains(row))
        );
        let late = source
            .read_filtered_window(
                &FilterExpr::parse("year == 2024 AND id < 9050").unwrap(),
                0,
                10_000,
                &all,
            )
            .unwrap();
        assert_eq!(late.num_rows(), 3000 + 50);
        let unknown = source
            .read_filtered_window(
                &FilterExpr::parse("region IS NULL").unwrap(),
                0,
                10_000,
                &all,
            )
            .unwrap();
        assert_eq!(unknown.num_rows(), 100);

        // Sort and group by partition columns across files.
        let sorted = source
            .read_sorted_all(
                None,
                &all,
                &[
                    SortSpec::new(1, SortDirection::Descending, NullOrder::Last),
                    SortSpec::new(0, SortDirection::Ascending, NullOrder::Last),
                ],
                SortBudget::default(),
                Some(2),
                &|_| false,
            )
            .unwrap();
        assert_eq!(texts(&sorted[0], 0), ["6000", "6001"]);
        let grouped = source
            .read_aggregated(
                None,
                &AggregateSpec::grouped(1, vec![AggregateExpr::count_all()]).unwrap(),
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(texts(&grouped, 0), ["2023", "2024"]);
        assert_eq!(texts(&grouped, 1), ["6000", "3100"]);
    }

    #[test]
    fn rejects_folders_whose_files_differ() {
        let directory = TempDir::new().unwrap();
        let root = directory.path();
        write_ids(root, "a.parquet", 0..10, false);
        let path = root.join("b.parquet");
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        )])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let error = format!("{:#}", ParquetSource::open(root).err().unwrap());
        assert!(error.contains("does not have the same columns"), "{error}");
        assert!(error.contains("Int32"), "{error}");

        // A partition key the files also store (as Polars writes them) is read
        // from the files, not added again.
        let directory = TempDir::new().unwrap();
        write_ids(directory.path(), "id=1/a.parquet", 0..10, false);
        let polars = ParquetSource::open(directory.path()).unwrap();
        assert_eq!(polars.column_count(), 1);
        assert!(polars.partition_column_names().is_empty());
        assert_eq!(
            texts(&polars.read_window(8, 2, &Projection::all(1)).unwrap(), 0),
            ["8", "9"]
        );

        // Cancelled between files.
        assert!(
            ParquetSource::open_cancellable(root, &|_| true)
                .err()
                .unwrap()
                .downcast_ref::<Cancelled>()
                .is_some()
        );
    }

    #[test]
    fn pages_filtered_rows_with_a_resume_cursor() {
        let (_directory, source) = numbered_file(30_000, 10_000);
        assert_eq!(source.row_groups().len(), 3);
        let projection = Projection::columns(vec![0], 2).unwrap();
        let filter = FilterExpr::parse("bucket == 3").unwrap();
        let expected = (0..30_000).filter(|id| id % 7 == 3).collect::<Vec<_>>();

        let mut seen = Vec::new();
        let mut next_row = 0;
        loop {
            let window = source
                .read_filtered_from(&filter, next_row, 1_000, &projection, &|_| false)
                .unwrap();
            assert!(window.next_row > next_row || window.batch.num_rows() == 0);
            seen.extend(ids(&window.batch));
            next_row = window.next_row;
            if next_row == source.row_count() {
                break;
            }
        }
        assert_eq!(seen, expected);

        // Same rows as the offset-based reader.
        let by_offset = source
            .read_filtered_window(&filter, 1_500, 10, &projection)
            .unwrap();
        assert_eq!(ids(&by_offset), expected[1_500..1_510]);

        // Starting part-way through a row group, and after a pruned prefix.
        let middle = source
            .read_filtered_from(&filter, 15_001, 2, &projection, &|_| false)
            .unwrap();
        assert_eq!(ids(&middle.batch), [15_004, 15_011]);
        assert_eq!(middle.next_row, 15_012);
        let late = source
            .read_filtered_from(
                &FilterExpr::parse("id >= 25000").unwrap(),
                0,
                3,
                &projection,
                &|_| false,
            )
            .unwrap();
        assert_eq!(ids(&late.batch), [25_000, 25_001, 25_002]);
        assert_eq!(late.next_row, 25_003);
        assert!(
            source
                .read_filtered_from(&filter, 30_001, 1, &projection, &|_| false)
                .is_err()
        );
    }

    #[test]
    fn keeps_the_top_rows_with_stable_ties_and_counts_only_filtered_rows() {
        let (_directory, source) = numbered_file(30_000, 4_000);
        let projection = Projection::all(2);
        let tiny = SortBudget::new(100, SortBudget::DEFAULT_MAX_BYTES).unwrap();
        let by_bucket = SortSpec::new(1, SortDirection::Descending, NullOrder::Last);

        // 30,000 rows exceed the 100-row budget, but LIMIT 5 keeps only the top 5;
        // equal buckets keep file order.
        let top = source
            .read_sorted_all(None, &projection, &[by_bucket], tiny, Some(5), &|_| false)
            .unwrap();
        assert_eq!(ids(&top[0]), [6, 13, 20, 27, 34]);

        // Without a LIMIT (or with one past the budget) the sort spills to disk
        // and still gives every row, in the same stable order.
        let all = source
            .read_sorted_all(None, &projection, &[by_bucket], tiny, None, &|_| false)
            .unwrap();
        let all = all.iter().flat_map(ids).collect::<Vec<_>>();
        let mut expected = (0..30_000).collect::<Vec<i32>>();
        expected.sort_by_key(|id| std::cmp::Reverse(id % 7));
        assert_eq!(all, expected);
        let limited = source
            .read_sorted_all(None, &projection, &[by_bucket], tiny, Some(101), &|_| false)
            .unwrap();
        assert_eq!(
            limited.iter().flat_map(ids).collect::<Vec<_>>(),
            expected[..101]
        );

        // The budget counts rows that pass the filter, not the whole file.
        let filtered = source
            .read_sorted_all(
                Some(&FilterExpr::parse("id < 50").unwrap()),
                &projection,
                &[SortSpec::new(0, SortDirection::Descending, NullOrder::Last)],
                tiny,
                None,
                &|_| false,
            )
            .unwrap();
        assert_eq!(ids(&filtered[0]), (0..50).rev().collect::<Vec<_>>());
        assert!(
            source
                .read_sorted_all(None, &projection, &[by_bucket], tiny, Some(0), &|_| false)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn long_reads_stop_between_batches_when_asked() {
        use std::cell::Cell;

        // 30,000 rows in 4,096-row batches: about nine batches per full scan.
        let (_directory, source) = numbered_file(30_000, 10_000);
        let projection = Projection::all(2);
        let is_cancelled = |error: anyhow::Error| error.downcast_ref::<Cancelled>().is_some();

        let calls = Cell::new(0);
        let stop_on_third = |_| {
            calls.set(calls.get() + 1);
            calls.get() >= 3
        };
        // Asking for more matches than exist forces a scan of every batch.
        let filter = FilterExpr::parse("bucket == 3").unwrap();
        let error = source
            .read_filtered_from(&filter, 0, 30_000, &projection, &stop_on_third)
            .unwrap_err();
        assert!(is_cancelled(error));
        assert_eq!(calls.get(), 3);

        calls.set(0);
        let error = source
            .read_sorted_all(
                None,
                &projection,
                &[SortSpec::new(0, SortDirection::Descending, NullOrder::Last)],
                SortBudget::default(),
                Some(5),
                &stop_on_third,
            )
            .unwrap_err();
        assert!(is_cancelled(error));
        assert_eq!(calls.get(), 3);

        calls.set(0);
        let error = source
            .read_aggregated(
                None,
                &AggregateSpec::new(vec![AggregateExpr::sum(0)]).unwrap(),
                GroupBudget::default(),
                &stop_on_third,
            )
            .unwrap_err();
        assert!(is_cancelled(error));
        assert_eq!(calls.get(), 3);
    }

    #[test]
    fn long_reads_report_how_far_they_have_scanned() {
        use std::cell::RefCell;

        // Three row groups of 10,000 rows, read in 4,096-row batches.
        let (_directory, source) = numbered_file(30_000, 10_000);
        let projection = Projection::all(2);
        let seen = RefCell::new(Vec::new());
        let record = |rows: u64| {
            seen.borrow_mut().push(rows);
            false
        };
        let check = |what: &str| {
            let positions = seen.take();
            assert_eq!(
                positions.first(),
                Some(&0),
                "{what} starts at the first row"
            );
            assert!(
                positions.is_sorted(),
                "{what} only moves forward: {positions:?}"
            );
            let last = positions.last().copied().unwrap_or_default();
            // The last call comes before the final batch of the last row group.
            assert!((20_000..30_000).contains(&last), "{what} ended at {last}");
        };

        let filter = FilterExpr::parse("bucket == 3").unwrap();
        source
            .read_filtered_from(&filter, 0, 30_000, &projection, &record)
            .unwrap();
        check("filter");

        source
            .read_sorted_all(
                None,
                &projection,
                &[SortSpec::new(0, SortDirection::Descending, NullOrder::Last)],
                SortBudget::default(),
                Some(5),
                &record,
            )
            .unwrap();
        check("sort");

        source
            .read_aggregated(
                None,
                &AggregateSpec::new(vec![AggregateExpr::sum(0)]).unwrap(),
                GroupBudget::default(),
                &record,
            )
            .unwrap();
        check("aggregate");

        // A filtered read that resumes part-way reports positions in the file, not
        // rows since it started.
        source
            .read_filtered_from(&filter, 15_000, 30_000, &projection, &record)
            .unwrap();
        assert!(seen.take().iter().all(|rows| *rows >= 15_000));
    }

    /// Every position a scan passes to `checkpoint`.
    fn scan_positions(source: &ParquetSource, filter: Option<&str>) -> [Vec<u64>; 3] {
        use std::cell::RefCell;

        let seen = RefCell::new(Vec::new());
        let record = |rows: u64| {
            seen.borrow_mut().push(rows);
            false
        };
        let filter = filter.map(|filter| FilterExpr::parse(filter).unwrap());
        let projection = Projection::all(source.column_count());
        let all = FilterExpr::parse("id >= 0").unwrap();
        source
            .read_filtered_from(
                filter.as_ref().unwrap_or(&all),
                0,
                usize::MAX,
                &projection,
                &record,
            )
            .unwrap();
        let filtered = seen.take();
        source
            .read_sorted_all(
                filter.as_ref(),
                &projection,
                &[SortSpec::new(0, SortDirection::Descending, NullOrder::Last)],
                SortBudget::default(),
                None,
                &record,
            )
            .unwrap();
        let sorted = seen.take();
        source
            .read_aggregated(
                filter.as_ref(),
                &AggregateSpec::new(vec![AggregateExpr::sum(0)]).unwrap(),
                GroupBudget::default(),
                &record,
            )
            .unwrap();
        [filtered, sorted, seen.take()]
    }

    #[test]
    fn scans_report_exact_positions_across_row_group_runs() {
        // Thirty 1,000-row groups read as one run: batches span row groups.
        let (_directory, source) = numbered_file(30_000, 1_000);
        assert_eq!(source.row_groups().len(), 30);
        let every_batch = (0..30_000).step_by(BATCH_SIZE).collect::<Vec<u64>>();
        for positions in scan_positions(&source, None) {
            assert_eq!(positions, every_batch);
        }

        // Pruned row groups split the scan into runs that start at their own rows.
        for positions in scan_positions(&source, Some("id < 3000 OR id >= 25500")) {
            assert_eq!(positions, [0, 25_000, 29_096]);
        }

        // Each file of a folder is its own run.
        let directory = TempDir::new().unwrap();
        write_ids(directory.path(), "a.parquet", 0..5_000, false);
        write_ids(directory.path(), "b.parquet", 5_000..9_000, false);
        let folder = ParquetSource::open(directory.path()).unwrap();
        assert_eq!(folder.row_groups().len(), 5);
        for positions in scan_positions(&folder, None) {
            assert_eq!(positions, [0, 4_096, 5_000]);
        }
        for positions in scan_positions(&folder, Some("id >= 4500")) {
            assert_eq!(positions, [4_096, 5_000]);
        }
    }

    #[test]
    fn resume_cursors_stay_exact_across_row_group_runs() {
        let (_directory, source) = numbered_file(30_000, 1_000);
        let projection = Projection::columns(vec![0], 2).unwrap();
        let filter = FilterExpr::parse("bucket == 3").unwrap();
        let expected = (0..30_000).filter(|id| id % 7 == 3).collect::<Vec<_>>();

        // Pages end in the middle of runs and batches; each resumes after its last match.
        let mut seen = Vec::new();
        let mut next_row = 0;
        while next_row < source.row_count() {
            let window = source
                .read_filtered_from(&filter, next_row, 333, &projection, &|_| false)
                .unwrap();
            let page = ids(&window.batch);
            if let Some(last) = page.last()
                && window.next_row < source.row_count()
            {
                assert_eq!(window.next_row, *last as u64 + 1);
            }
            seen.extend(page);
            next_row = window.next_row;
        }
        assert_eq!(seen, expected);

        // Starting inside a row group and inside a batch of a later run.
        let middle = source
            .read_filtered_from(&filter, 15_001, 2, &projection, &|_| false)
            .unwrap();
        assert_eq!(ids(&middle.batch), [15_004, 15_011]);
        assert_eq!(middle.next_row, 15_012);
        let split = FilterExpr::parse("id < 3000 OR id >= 25500").unwrap();
        let after_gap = source
            .read_filtered_from(&split, 2_999, 3, &projection, &|_| false)
            .unwrap();
        assert_eq!(ids(&after_gap.batch), [2_999, 25_500, 25_501]);
        assert_eq!(after_gap.next_row, 25_502);
        let resumed = source
            .read_filtered_from(&split, 26_000, 2, &projection, &|_| false)
            .unwrap();
        assert_eq!(ids(&resumed.batch), [26_000, 26_001]);
        assert_eq!(resumed.next_row, 26_002);
    }

    #[test]
    fn filters_on_several_columns_across_row_groups() {
        // Three row groups of 10,000 rows; `bucket` is `id % 7`.
        let (_directory, source) = numbered_file(30_000, 10_000);
        // Only `id` is read out: the filter columns are added to the read and dropped.
        let projection = Projection::columns(vec![0], 2).unwrap();
        let matches = |filter: &str| {
            let filter = FilterExpr::parse(filter).unwrap();
            source.validate_filter(&filter).unwrap();
            let window = source
                .read_filtered_from(&filter, 0, 30_000, &projection, &|_| false)
                .unwrap();
            assert_eq!(window.batch.num_columns(), 1);
            ids(&window.batch)
        };

        // One match in each of the last two row groups.
        assert_eq!(matches("id IN (5, 10005, 25000)"), [5, 10_005, 25_000]);
        // AND over two columns, spanning a row-group boundary.
        assert_eq!(
            matches("id BETWEEN 9990 AND 10010 AND bucket == 3"),
            (9_990..=10_010)
                .filter(|id| id % 7 == 3)
                .collect::<Vec<_>>()
        );
        // An OR must keep row groups that only one side can match.
        assert_eq!(
            matches("id == 3 OR (id > 29990 AND bucket == 0)"),
            [3, 29_995]
        );
        assert_eq!(matches("id < 3 AND bucket NOT IN (0, 1)"), [2]);
        assert_eq!(matches("id IS NULL"), Vec::<i32>::new());
        assert_eq!(matches("id IS NOT NULL AND id < 2"), [0, 1]);

        // The same filter narrows a sort and an aggregate.
        let filter = FilterExpr::parse("id >= 29990 AND (bucket == 1 OR bucket == 2)").unwrap();
        let sorted = source
            .read_sorted_all(
                Some(&filter),
                &projection,
                &[SortSpec::new(0, SortDirection::Descending, NullOrder::Last)],
                SortBudget::default(),
                None,
                &|_| false,
            )
            .unwrap();
        assert_eq!(
            sorted.iter().flat_map(ids).collect::<Vec<_>>(),
            [29_997, 29_996, 29_990]
        );
        let counted = source
            .read_aggregated(
                Some(&filter),
                &AggregateSpec::new(vec![AggregateExpr::count_all()]).unwrap(),
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(
            crate::value::format_cell(counted.column(0).as_ref(), 0),
            "3"
        );
    }

    /// 40 rows in row groups of 10: `id`; `tags` (a list, so the file is nested);
    /// `price` Decimal128(9, 2) = id - 20 + 0.25 (INT32 storage); `big`
    /// Decimal128(30, 2) (byte-array storage); `at` Time64(µs) = 08:00:00 + id
    /// minutes; `alarm` Time32(s), null for odd ids.
    fn typed_file() -> (TempDir, ParquetSource) {
        use arrow_array::{
            Decimal128Array, Time32SecondArray, Time64MicrosecondArray, builder::ListBuilder,
        };
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("typed.parquet");
        let ids = 0..40_i32;
        let mut tags = ListBuilder::new(Int32Builder::new());
        for id in ids.clone() {
            tags.values().append_value(id);
            tags.append(true);
        }
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from_iter_values(ids.clone())) as ArrayRef,
            ),
            ("tags", Arc::new(tags.finish())),
            (
                "price",
                Arc::new(
                    Decimal128Array::from_iter_values(
                        ids.clone().map(|id| i128::from(id - 20) * 100 + 25),
                    )
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
                ),
            ),
            (
                "big",
                Arc::new(
                    Decimal128Array::from_iter_values(
                        ids.clone().map(|id| 10_i128.pow(27) + i128::from(id)),
                    )
                    .with_precision_and_scale(30, 2)
                    .unwrap(),
                ),
            ),
            (
                "at",
                Arc::new(Time64MicrosecondArray::from_iter_values(
                    ids.clone()
                        .map(|id| (8 * 3_600 + i64::from(id) * 60) * 1_000_000),
                )),
            ),
            (
                "alarm",
                Arc::new(Time32SecondArray::from_iter(
                    ids.clone()
                        .map(|id| (id % 2 == 0).then_some(6 * 3_600 + id)),
                )),
            ),
        ])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        (directory, ParquetSource::open(path).unwrap())
    }

    #[test]
    fn filters_sorts_and_aggregates_decimals_and_times() {
        let (_directory, source) = typed_file();
        let projection = Projection::columns(vec![0], 6).unwrap();
        let matches = |filter: &str| {
            let filter = FilterExpr::parse(filter).unwrap();
            source.validate_filter(&filter).unwrap();
            ids(&source
                .read_filtered_from(&filter, 0, 100, &projection, &|_| false)
                .unwrap()
                .batch)
        };
        // price is id - 19.75.
        assert_eq!(matches("price > 18"), [38, 39]);
        assert_eq!(matches("price == -0.75"), [19]);
        assert_eq!(matches("price == -0.750"), [19], "trailing zeros are exact");
        assert_eq!(
            matches("price == 0.251"),
            Vec::<i32>::new(),
            "between two cents"
        );
        assert_eq!(matches("price > 0.251 AND price < 2"), [21]);
        assert_eq!(matches("price BETWEEN -1 AND 1"), [19, 20]);
        assert_eq!(matches("big >= 10000000000000000000000000.38"), [38, 39]);
        // at is 08:00 + id minutes; alarm is 06:00:00 + id seconds, null for odd ids.
        assert_eq!(matches("at >= 08:38:00"), [38, 39]);
        assert_eq!(matches("at == 08:05:00.000000"), [5]);
        assert_eq!(matches("alarm < 06:00:04"), [0, 2]);
        assert_eq!(matches("alarm IS NULL AND id < 4"), [1, 3]);
        for (filter, message) in [
            ("price == ten", "not a number"),
            ("at > noon", "not a time of day"),
            ("at contains 08", "contains is only supported for text"),
        ] {
            let error = source
                .validate_filter(&FilterExpr::parse(filter).unwrap())
                .unwrap_err()
                .to_string();
            assert!(error.contains(message), "{filter}: {error}");
        }

        // Pruning uses decimal and time statistics, and skips the right groups.
        let groups = source.row_groups().to_vec();
        let might = |filter: &str| {
            let filter = FilterExpr::parse(filter).unwrap();
            groups
                .iter()
                .map(|group| source.row_group_might_match(&filter, group))
                .collect::<Vec<_>>()
        };
        assert_eq!(might("price > 18"), [false, false, false, true]);
        assert_eq!(might("at < 08:10:00"), [true, false, false, false]);
        // Byte-array decimal statistics are not read as text: nothing is skipped.
        assert_eq!(might("big < 0"), [true, true, true, true]);

        let sorted = source
            .read_sorted_all(
                None,
                &projection,
                &[SortSpec::new(2, SortDirection::Descending, NullOrder::Last)],
                SortBudget::default(),
                Some(3),
                &|_| false,
            )
            .unwrap();
        assert_eq!(
            sorted.iter().flat_map(ids).collect::<Vec<_>>(),
            [39, 38, 37]
        );
        let sorted = source
            .read_sorted_all(
                None,
                &projection,
                &[SortSpec::new(5, SortDirection::Ascending, NullOrder::Last)],
                SortBudget::default(),
                Some(2),
                &|_| false,
            )
            .unwrap();
        assert_eq!(sorted.iter().flat_map(ids).collect::<Vec<_>>(), [0, 2]);

        let aggregate = AggregateSpec::new(vec![
            AggregateExpr::sum(2),
            AggregateExpr::avg(2),
            AggregateExpr::min(2),
            AggregateExpr::max(4),
            AggregateExpr::min(5),
            AggregateExpr::sum(3),
        ])
        .unwrap();
        source.validate_aggregate(&aggregate).unwrap();
        let result = source
            .read_aggregated(None, &aggregate, GroupBudget::default(), &|_| false)
            .unwrap();
        let text = |column: usize| crate::value::format_cell(result.column(column).as_ref(), 0);
        // Sum of id - 19.75 over 0..40: 780 - 790 = -10.00.
        assert_eq!(text(0), "-10.00");
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
        assert_eq!(text(1), "-0.25");
        assert_eq!(text(2), "-19.75");
        assert_eq!(text(3), "08:39:00");
        assert_eq!(text(4), "06:00:00");
        assert_eq!(text(5), "400000000000000000000000007.80");
        // Times can be compared but not summed.
        assert!(
            source
                .validate_aggregate(&AggregateSpec::new(vec![AggregateExpr::sum(4)]).unwrap())
                .is_err()
        );

        // GROUP BY a decimal keeps its type.
        let grouped = source
            .read_aggregated(
                Some(&FilterExpr::parse("id < 2").unwrap()),
                &AggregateSpec::grouped(2, vec![AggregateExpr::count_all()]).unwrap(),
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(
            crate::value::format_cell(grouped.column(0).as_ref(), 1),
            "-18.75"
        );
    }

    #[test]
    fn nested_files_keep_statistics_and_pruning_for_their_plain_columns() {
        let (_directory, source) = typed_file();
        let columns = source.metadata().columns();
        let stats = |column: usize| columns[column].row_group_statistics.clone();
        // The list column has none; the columns after it still have theirs.
        assert!(stats(1).iter().all(Option::is_none));
        let price = stats(2)[3].clone().unwrap();
        assert_eq!(price.min.as_deref(), Some("10.25"));
        assert_eq!(price.max.as_deref(), Some("19.25"));
        let at = stats(4)[0].clone().unwrap();
        assert_eq!(at.max.as_deref(), Some("08:09:00"));
        assert_eq!(stats(0)[1].clone().unwrap().min.as_deref(), Some("10"));
        assert_eq!(stats(5)[0].clone().unwrap().null_count, Some(5));

        let groups = source.row_groups().to_vec();
        let filter = FilterExpr::parse("id >= 35").unwrap();
        assert_eq!(
            groups
                .iter()
                .map(|group| source.row_group_might_match(&filter, group))
                .collect::<Vec<_>>(),
            [false, false, false, true]
        );
        // A filter on the list column itself never rules a group out.
        let filter = FilterExpr::parse("tags IS NULL").unwrap();
        assert!(
            groups
                .iter()
                .all(|group| source.row_group_might_match(&filter, group))
        );
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn builds_row_group_ranges() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();

        assert_eq!(source.row_count(), 6);
        assert_eq!(
            source.row_groups(),
            &[
                RowGroupInfo {
                    index: 0,
                    first_row: 0,
                    row_count: 3
                },
                RowGroupInfo {
                    index: 1,
                    first_row: 3,
                    row_count: 3
                }
            ]
        );
    }

    #[test]
    fn exposes_top_level_column_metadata_and_available_statistics() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let columns = source.metadata().columns();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert!(!columns[0].nullable);
        assert_eq!(columns[0].row_group_statistics.len(), 2);
        let statistics = columns[0].row_group_statistics[0].as_ref().unwrap();
        assert_eq!(statistics.min.as_deref(), Some("0"));
        assert_eq!(statistics.max.as_deref(), Some("2"));
    }

    #[test]
    fn reads_window_within_one_row_group() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::columns(vec![0], source.column_count()).unwrap();
        let batch = source.read_window(1, 2, &projection).unwrap();

        assert_eq!(ids(&batch), vec![1, 2]);
    }

    #[test]
    fn reads_window_spanning_row_groups() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::columns(vec![0], source.column_count()).unwrap();
        let batch = source.read_window(2, 3, &projection).unwrap();

        assert_eq!(ids(&batch), vec![2, 3, 4]);
    }

    #[test]
    fn projects_requested_columns() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::columns(vec![1], source.column_count()).unwrap();
        let batch = source.read_window(0, 2, &projection).unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "value");
    }

    #[test]
    fn preserves_projected_order() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::columns(vec![1, 0], source.column_count()).unwrap();
        let batch = source.read_window(0, 2, &projection).unwrap();

        assert_eq!(batch.schema().field(0).name(), "value");
        assert_eq!(batch.schema().field(1).name(), "id");
    }

    #[test]
    fn reads_filtered_window_without_materializing_all_matches() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let filter = FilterExpr::parse("id >= 2").unwrap();
        let projection = Projection::columns(vec![0], source.column_count()).unwrap();
        let batch = source
            .read_filtered_window(&filter, 1, 2, &projection)
            .unwrap();

        assert_eq!(ids(&batch), vec![3, 4]);
    }

    #[test]
    fn filters_with_a_projected_column_before_its_filter_column() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let filter = FilterExpr::parse("id >= 3").unwrap();
        let projection = Projection::columns(vec![1], source.column_count()).unwrap();
        let batch = source
            .read_filtered_window(&filter, 0, 2, &projection)
            .unwrap();

        assert_eq!(ids(&batch), vec![30, 40]);
    }

    #[test]
    fn reads_fixed_pages() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::all(source.column_count());
        let page = source.read_page(0, &projection).unwrap();

        assert_eq!(page.window.first_row, 0);
        assert_eq!(page.window.row_count, 6);
        assert_eq!(
            page.batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            6
        );
    }

    #[test]
    fn rejects_windows_starting_past_end() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::all(source.column_count());

        assert!(source.read_window(7, 1, &projection).is_err());
    }

    #[test]
    fn reports_malformed_parquet_without_panicking() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("malformed.parquet");
        std::fs::write(&path, b"not a parquet file").unwrap();

        let error = ParquetSource::open(path).err().unwrap();
        assert!(error.to_string().contains("read Parquet metadata"));
    }

    #[test]
    fn opens_and_reads_a_wide_schema() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("wide.parquet");
        let schema = Arc::new(Schema::new(
            (0..64)
                .map(|column| Field::new(format!("column_{column}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<ArrayRef> = (0..64)
            .map(|column| Arc::new(Int32Array::from(vec![column])) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(path).unwrap();
        let page = source
            .read_page(0, &Projection::all(source.column_count()))
            .unwrap();
        assert_eq!(source.column_count(), 64);
        assert_eq!(source.metadata().columns().len(), 64);
        assert_eq!(page.batches[0].num_columns(), 64);
    }

    #[test]
    fn reads_projected_top_level_nested_columns_without_flattening() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("nested.parquet");
        let list =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1), Some(2)]), None]);
        let large_list = LargeListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(3)]),
            Some(vec![]),
        ]);
        let structure = StructArray::from(vec![
            (
                Arc::new(Field::new("code", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("label", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ),
        ]);
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("first");
        map.values().append_value(1);
        map.append(true).unwrap();
        map.keys().append_value("second");
        map.values().append_value(2);
        map.append(true).unwrap();
        let map = map.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("items", list.data_type().clone(), true),
            Field::new("large_items", large_list.data_type().clone(), true),
            Field::new("payload", structure.data_type().clone(), true),
            Field::new("attributes", map.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(list),
                Arc::new(large_list),
                Arc::new(structure),
                Arc::new(map),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::columns(vec![3, 0], source.column_count()).unwrap();
        let nested = source.read_window(0, 2, &projection).unwrap();

        assert_eq!(source.metadata().columns().len(), 4);
        assert!(matches!(
            source.metadata().columns()[2].data_type,
            DataType::Struct(_)
        ));
        assert!(matches!(
            source.metadata().columns()[3].data_type,
            DataType::Map(_, _)
        ));
        assert!(matches!(nested.column(0).data_type(), DataType::Map(_, _)));
        assert!(matches!(nested.column(1).data_type(), DataType::List(_)));
        assert_eq!(nested.num_rows(), 2);

        let sort_error = source
            .validate_sort(&[SortSpec::new(0, SortDirection::Ascending, NullOrder::Last)])
            .unwrap_err();
        assert!(sort_error.to_string().contains("sorting is not supported"));
        let aggregate_error = source
            .validate_aggregate(&AggregateSpec::new(vec![AggregateExpr::sum(0)]).unwrap())
            .unwrap_err();
        assert!(aggregate_error.to_string().contains("not supported"));
    }

    #[test]
    fn sorts_rows_with_explicit_null_order_and_a_budget() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("sorted.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(1),
                Some(2),
            ]))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::all(source.column_count());
        let ascending = source
            .read_sorted_all(
                None,
                &projection,
                &[SortSpec::new(0, SortDirection::Ascending, NullOrder::First)],
                SortBudget::new(10, 1_000_000).unwrap(),
                None,
                &|_| false,
            )
            .unwrap();
        let ascending = ascending[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            ascending.iter().collect::<Vec<_>>(),
            vec![None, Some(1), Some(2), Some(3)]
        );

        let descending = source
            .read_sorted_all(
                None,
                &projection,
                &[SortSpec::new(0, SortDirection::Descending, NullOrder::Last)],
                SortBudget::new(10, 1_000_000).unwrap(),
                None,
                &|_| false,
            )
            .unwrap();
        let descending = descending[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            descending.iter().collect::<Vec<_>>(),
            vec![Some(3), Some(2), Some(1), None]
        );

        // Over a 3-row budget the sort spills, and nulls still come last.
        let spilled = source
            .read_sorted(
                None,
                &projection,
                &[SortSpec::new(0, SortDirection::Ascending, NullOrder::Last)],
                SortBudget::new(3, 1_000_000).unwrap(),
                None,
                &|_| false,
            )
            .unwrap();
        assert!(matches!(spilled, SortOutput::Spilled(_)));
        let spilled = drain(spilled);
        let spilled = spilled[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            spilled.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3), None]
        );
    }

    #[test]
    fn sorts_by_several_keys_in_memory_and_spilled() {
        // bucket = id % 7, id 0..30,000: sort by bucket ascending, then id descending.
        let (_directory, source) = numbered_file(30_000, 4_000);
        let projection = Projection::all(2);
        let keys = [
            SortSpec::new(1, SortDirection::Ascending, NullOrder::Last),
            SortSpec::new(0, SortDirection::Descending, NullOrder::Last),
        ];
        let mut expected = (0..30_000).collect::<Vec<i32>>();
        expected.sort_by_key(|id| (id % 7, std::cmp::Reverse(*id)));
        for budget in [
            SortBudget::default(),
            SortBudget::new(1_000, 1_000_000_000).unwrap(),
        ] {
            let rows = source
                .read_sorted_all(None, &projection, &keys, budget, None, &|_| false)
                .unwrap();
            assert_eq!(rows.iter().flat_map(ids).collect::<Vec<_>>(), expected);
        }
        // Only the output columns come back, even when a key is not one of them.
        let only_ids = Projection::columns(vec![0], 2).unwrap();
        let rows = source
            .read_sorted_all(
                None,
                &only_ids,
                &keys,
                SortBudget::new(1_000, 1_000_000_000).unwrap(),
                Some(3),
                &|_| false,
            )
            .unwrap();
        assert_eq!(rows[0].num_columns(), 1);
        assert_eq!(
            rows.iter().flat_map(ids).collect::<Vec<_>>(),
            [29_995, 29_988, 29_981]
        );
        for (keys, message) in [
            (vec![], "at least one column"),
            (vec![keys[0], keys[0]], "same column twice"),
        ] {
            let error = source.validate_sort(&keys).unwrap_err().to_string();
            assert!(error.contains(message), "{error}");
        }
    }

    #[test]
    fn groups_by_several_columns() {
        let (_directory, source) = numbered_file(70, 70);
        // Grouping by (bucket, id): every row of id < 14 is its own group.
        let aggregate =
            AggregateSpec::grouped_by(vec![1, 0], vec![AggregateExpr::count_all()]).unwrap();
        source.validate_aggregate(&aggregate).unwrap();
        let result = source
            .read_aggregated(
                Some(&FilterExpr::parse("id < 14").unwrap()),
                &aggregate,
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(result.num_columns(), 3, "two group columns and the count");
        assert_eq!(result.schema().field(0).name(), "bucket");
        assert_eq!(result.schema().field(1).name(), "id");
        assert_eq!(result.num_rows(), 14);
        let bucket_and_id = AggregateSpec::grouped_by(vec![1, 1], vec![AggregateExpr::count_all()]);
        assert!(
            bucket_and_id
                .unwrap_err()
                .to_string()
                .contains("same column twice")
        );

        // One key: 70 ids fall into 7 buckets of 10.
        let result = source
            .read_aggregated(
                None,
                &AggregateSpec::grouped_by(vec![1], vec![AggregateExpr::count_all()]).unwrap(),
                GroupBudget::default(),
                &|_| false,
            )
            .unwrap();
        assert_eq!(result.num_rows(), 7);
        assert_eq!(
            crate::value::format_cell(result.column(1).as_ref(), 0),
            "10"
        );
    }

    #[test]
    fn sorts_dates_and_timestamps() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("temporal-sort.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("date", DataType::Date32, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2])),
                Arc::new(Date32Array::from(vec![3, 1, 2])),
                Arc::new(TimestampMillisecondArray::from(vec![20, 10, 30])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let source = ParquetSource::open(path).unwrap();
        let projection = Projection::all(source.column_count());
        for (column, direction, expected) in [
            (1, SortDirection::Ascending, vec![1, 2, 0]),
            (2, SortDirection::Descending, vec![2, 0, 1]),
        ] {
            let sorted = source
                .read_sorted_all(
                    None,
                    &projection,
                    &[SortSpec::new(column, direction, NullOrder::Last)],
                    SortBudget::new(10, 1_000_000).unwrap(),
                    None,
                    &|_| false,
                )
                .unwrap();
            assert_eq!(ids(&sorted[0]), expected);
        }
    }
}
