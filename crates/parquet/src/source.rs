use std::{
    any::Any,
    collections::HashMap,
    fs::File,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SortOptions, TimeUnit};
use arrow_select::{concat::concat_batches, filter::filter_record_batch, take::take_record_batch};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
        RowSelector,
    },
};
use parquet::file::metadata::ParquetMetaData;

use crate::{
    AggregateExpr, AggregateFunction, AggregateSpec, DataPage, DatasetMetadata, GroupBudget,
    NullOrder, PageCache, PageCacheLimits, PageCacheStats, PageKey, Projection, RowGroupInfo,
    RowWindow, SortBudget, SortDirection, SortSpec,
    filter::FilterExpr,
    page::PAGE_ROWS,
    statistics::metadata_statistics,
    types::{is_comparable, is_numeric, is_signed_integer, is_unsigned_integer},
};

const BATCH_SIZE: usize = 4096;

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
struct GuardedReader<R> {
    inner: Option<R>,
}

impl<R> GuardedReader<R> {
    fn new(inner: R) -> Self {
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

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("decoder panicked")
}

pub struct ParquetSource {
    path: PathBuf,
    file_info: std::fs::Metadata,
    compression: String,
    dataset_metadata: DatasetMetadata,
    metadata: Arc<ParquetMetaData>,
    arrow_metadata: ArrowReaderMetadata,
    cache: Mutex<PageCache>,
}

impl ParquetSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let file_info = file.metadata().context("read file properties")?;
        let arrow_metadata = load_arrow_metadata(&file)
            .with_context(|| format!("read Parquet metadata from {}", path.display()))?;
        let schema = arrow_metadata.schema().clone();
        let metadata = arrow_metadata.metadata().clone();
        let codecs = metadata
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .map(|column| column.compression().to_string())
            .collect::<std::collections::BTreeSet<_>>();
        let compression = if codecs.is_empty() {
            "Unavailable".to_string()
        } else {
            codecs.into_iter().collect::<Vec<_>>().join(", ")
        };
        let row_group_counts =
            (0..metadata.num_row_groups()).map(|index| metadata.row_group(index).num_rows() as u64);
        let dataset_metadata = DatasetMetadata::new(schema.clone(), row_group_counts)
            .with_context(|| format!("build row-group index for {}", path.display()))?
            .with_column_statistics(metadata_statistics(&metadata, &schema));

        Ok(Self {
            path,
            file_info,
            compression,
            dataset_metadata,
            metadata,
            arrow_metadata,
            cache: Mutex::new(PageCache::default()),
        })
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
                &|| false,
            )?
            .batch)
    }

    /// Reads up to `row_count` rows matching `filter`, starting at source row
    /// `start_row`, and returns where to resume. Paging with the returned
    /// [`FilteredWindow::next_row`] visits each row once, so reading a whole
    /// filtered result stays linear in the file size.
    ///
    /// `stop` is checked before each decoded batch; when it returns `true` the read
    /// ends with a [`Cancelled`] error.
    pub fn read_filtered_from(
        &self,
        filter: &FilterExpr,
        start_row: u64,
        row_count: usize,
        projection: &Projection,
        stop: &dyn Fn() -> bool,
    ) -> Result<FilteredWindow> {
        self.scan_filtered(filter, start_row, 0, row_count, projection, stop)
    }

    fn scan_filtered(
        &self,
        filter: &FilterExpr,
        start_row: u64,
        mut skip_matches: u64,
        row_count: usize,
        projection: &Projection,
        stop: &dyn Fn() -> bool,
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

        let filter_column = filter.column_index(&self.dataset_metadata.schema)?;
        let mut read_columns = projection.as_slice().to_vec();
        if !read_columns.contains(&filter_column) {
            read_columns.push(filter_column);
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let filter_position = projected_position(&read_projection, filter_column, "filter")?;
        let output_positions = projection
            .as_slice()
            .iter()
            .map(|column| projected_position(&read_projection, *column, "output"))
            .collect::<Result<Vec<_>>>()?;

        let mut remaining = row_count;
        let mut batches = Vec::new();
        for row_group in self.row_groups() {
            if row_group.first_row + row_group.row_count <= start_row
                || !self.row_group_might_match(filter, row_group, filter_column)
            {
                continue;
            }
            // Only the row group containing `start_row` starts part-way through.
            let skip = start_row.saturating_sub(row_group.first_row);
            let selection = (skip > 0).then(|| {
                RowSelection::from(vec![
                    RowSelector::skip(skip as usize),
                    RowSelector::select((row_group.row_count - skip) as usize),
                ])
            });
            let mut reader = self.reader_for(&read_projection, vec![row_group.index], selection)?;
            let mut batch_first_row = row_group.first_row + skip;
            while let Some(batch) = reader.next().transpose()? {
                if stop() {
                    return Err(Cancelled.into());
                }
                let batch = self.reorder_batch(batch, &read_projection)?;
                let batch_rows = batch.num_rows() as u64;
                let mask = filter.evaluate_batch(&batch, filter_position)?;
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
    /// PAVI has no external sort for larger inputs. `stop` is checked before each
    /// decoded batch; when it returns `true` the read ends with a [`Cancelled`] error.
    pub fn read_sorted(
        &self,
        filter: Option<&FilterExpr>,
        projection: &Projection,
        sort: SortSpec,
        budget: SortBudget,
        limit: Option<usize>,
        stop: &dyn Fn() -> bool,
    ) -> Result<Vec<RecordBatch>> {
        self.validate_sort(sort)?;
        if let Some(filter) = filter {
            self.validate_filter(filter)?;
        }
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let top = limit.filter(|limit| *limit <= budget.max_rows());

        let filter_column = filter
            .map(|filter| filter.column_index(&self.dataset_metadata.schema))
            .transpose()?;
        let mut read_columns = projection.as_slice().to_vec();
        if !read_columns.contains(&sort.column) {
            read_columns.push(sort.column);
        }
        if let Some(filter_column) = filter_column
            && !read_columns.contains(&filter_column)
        {
            read_columns.push(filter_column);
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let sort_position = projected_position(&read_projection, sort.column, "sort")?;
        let sequence_position = read_projection.as_slice().len();
        let filter_position = filter_column
            .map(|column| projected_position(&read_projection, column, "filter"))
            .transpose()?;
        let output_positions = projection
            .as_slice()
            .iter()
            .map(|column| projected_position(&read_projection, *column, "output"))
            .collect::<Result<Vec<_>>>()?;
        let order = SortKeys {
            value: sort_position,
            sequence: sequence_position,
            sort,
        };

        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut sequence = 0_u64;
        let mut retained = Vec::new();
        for row_group in self.row_groups() {
            if let (Some(filter), Some(filter_column)) = (filter, filter_column)
                && !self.row_group_might_match(filter, row_group, filter_column)
            {
                continue;
            }
            let mut reader = self.reader_for(&read_projection, vec![row_group.index], None)?;
            while let Some(batch) = reader.next().transpose()? {
                if stop() {
                    return Err(Cancelled.into());
                }
                let batch = self.reorder_batch(batch, &read_projection)?;
                let batch = match (filter, filter_position) {
                    (Some(filter), Some(filter_position)) => filter_record_batch(
                        &batch,
                        &filter.evaluate_batch(&batch, filter_position)?,
                    )?,
                    (Some(_), None) => bail!("filter column was not projected for sorting"),
                    (None, _) => batch,
                };
                if batch.num_rows() == 0 {
                    continue;
                }
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
                if (top.is_none() && rows > budget.max_rows())
                    || bytes
                        .saturating_mul(3)
                        .saturating_add(rows.saturating_mul(8))
                        > budget.max_bytes()
                {
                    bail!(
                        "sort input exceeds the in-memory sort budget of {} rows / {} bytes; \
                         filter the data or add a LIMIT of at most {} rows (external sorting is unavailable)",
                        budget.max_rows(),
                        budget.max_bytes(),
                        budget.max_rows()
                    );
                }
            }
        }

        if retained.is_empty() {
            return Ok(Vec::new());
        }
        let sorted = order.sort(&retained, top)?;
        let fields: Vec<Field> = output_positions
            .iter()
            .map(|position| sorted.schema().field(*position).clone())
            .collect();
        let arrays = output_positions
            .iter()
            .map(|position| sorted.column(*position).clone())
            .collect();
        let sorted = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        Ok((0..sorted.num_rows())
            .step_by(PAGE_ROWS as usize)
            .map(|first_row| {
                sorted.slice(
                    first_row,
                    (sorted.num_rows() - first_row).min(PAGE_ROWS as usize),
                )
            })
            .collect())
    }

    /// Computes aggregates incrementally over decoded batches without retaining input rows.
    /// `stop` is checked before each decoded batch; when it returns `true` the read
    /// ends with a [`Cancelled`] error.
    pub fn read_aggregated(
        &self,
        filter: Option<&FilterExpr>,
        aggregate: &AggregateSpec,
        budget: GroupBudget,
        stop: &dyn Fn() -> bool,
    ) -> Result<RecordBatch> {
        self.validate_aggregate(aggregate)?;
        if let Some(filter) = filter {
            self.validate_filter(filter)?;
        }

        let filter_column = filter
            .map(|filter| filter.column_index(&self.dataset_metadata.schema))
            .transpose()?;
        let mut read_columns = Vec::new();
        if let Some(group) = aggregate.group_by() {
            read_columns.push(group);
        }
        for expression in aggregate.expressions() {
            if let Some(column) = expression.column()
                && !read_columns.contains(&column)
            {
                read_columns.push(column);
            }
        }
        if let Some(column) = filter_column
            && !read_columns.contains(&column)
        {
            read_columns.push(column);
        }
        if read_columns.is_empty() {
            let mut groups = GroupCollector::new(aggregate, &self.dataset_metadata.schema, budget)?;
            groups.update_count_all(self.row_count())?;
            return groups.finish();
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let group_position = aggregate
            .group_by()
            .map(|column| projected_position(&read_projection, column, "group"))
            .transpose()?;
        let filter_position = filter_column
            .map(|column| projected_position(&read_projection, column, "filter"))
            .transpose()?;
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
        for row_group in self.row_groups() {
            if let (Some(filter), Some(filter_column)) = (filter, filter_column)
                && !self.row_group_might_match(filter, row_group, filter_column)
            {
                continue;
            }
            let mut reader = self.reader_for(&read_projection, vec![row_group.index], None)?;
            while let Some(batch) = reader.next().transpose()? {
                if stop() {
                    return Err(Cancelled.into());
                }
                let batch = self.reorder_batch(batch, &read_projection)?;
                let batch = match (filter, filter_position) {
                    (Some(filter), Some(position)) => {
                        filter_record_batch(&batch, &filter.evaluate_batch(&batch, position)?)?
                    }
                    (Some(_), None) => bail!("filter column was not projected for aggregation"),
                    (None, _) => batch,
                };
                groups.update_batch(&batch, group_position, &expression_positions)?;
            }
        }
        groups.finish()
    }

    pub fn validate_aggregate(&self, aggregate: &AggregateSpec) -> Result<()> {
        let schema = &self.dataset_metadata.schema;
        if let Some(group) = aggregate.group_by() {
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
                    if is_numeric(field.data_type()) => {}
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

    pub fn validate_sort(&self, sort: SortSpec) -> Result<()> {
        let field = self
            .dataset_metadata
            .schema
            .fields()
            .get(sort.column)
            .ok_or_else(|| anyhow!("sort column {} out of range", sort.column))?;
        if !is_comparable(field.data_type()) {
            bail!(
                "sorting is not supported for {:?} columns",
                field.data_type()
            );
        }
        Ok(())
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

        let first_selected_row = row_group_indexes
            .first()
            .and_then(|index| self.row_groups().get(*index))
            .map(|group| group.first_row)
            .ok_or_else(|| anyhow!("no selected row groups"))?;
        let skip_before = usize::try_from(first_row - first_selected_row)?;
        let mut selectors = Vec::new();
        if skip_before > 0 {
            selectors.push(RowSelector::skip(skip_before));
        }
        selectors.push(RowSelector::select(row_count));

        let mut reader = self.reader_for(
            projection,
            row_group_indexes,
            Some(RowSelection::from(selectors)),
        )?;
        let mut batches = Vec::new();

        while let Some(batch) = reader.next().transpose()? {
            if batch.num_rows() > 0 {
                batches.push(self.reorder_batch(batch, projection)?);
            }
        }

        Ok(batches)
    }

    fn reader_for(
        &self,
        projection: &Projection,
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
    ) -> Result<GuardedReader<parquet::arrow::arrow_reader::ParquetRecordBatchReader>> {
        let file =
            File::open(&self.path).with_context(|| format!("open {}", self.path.display()))?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.arrow_metadata.clone());
        let parquet_columns = projection.parquet_columns();
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

    fn row_group_might_match(
        &self,
        filter: &FilterExpr,
        row_group: &RowGroupInfo,
        filter_column: usize,
    ) -> bool {
        if self.column_count() != self.metadata.file_metadata().schema_descr().num_columns() {
            return true;
        }

        let data_type = self
            .dataset_metadata
            .schema
            .field(filter_column)
            .data_type();
        self.metadata
            .row_group(row_group.index)
            .columns()
            .get(filter_column)
            .is_none_or(|column| filter.might_match_statistics(column.statistics(), data_type))
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

    fn reorder_batch(&self, batch: RecordBatch, projection: &Projection) -> Result<RecordBatch> {
        let parquet_columns = projection.parquet_columns();
        let output_positions: Vec<_> = projection
            .as_slice()
            .iter()
            .map(|column| {
                parquet_columns
                    .iter()
                    .position(|read_column| read_column == column)
                    .ok_or_else(|| anyhow!("projected column {column} was not decoded"))
            })
            .collect::<Result<_>>()?;

        project_batch(&batch, projection, &output_positions)
    }
}

/// The error a long read returns when its `stop` check asks it to end early.
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
struct SortKeys {
    value: usize,
    sequence: usize,
    sort: SortSpec,
}

impl SortKeys {
    /// Sorts `batches` into one batch, keeping only the first `limit` rows if given.
    fn sort(&self, batches: &[RecordBatch], limit: Option<usize>) -> Result<RecordBatch> {
        let input = if batches.len() == 1 {
            batches[0].clone()
        } else {
            concat_batches(&batches[0].schema(), batches)?
        };
        let indices = lexsort_to_indices(
            &[
                SortColumn {
                    values: input.column(self.value).clone(),
                    options: Some(SortOptions {
                        descending: self.sort.direction == SortDirection::Descending,
                        nulls_first: self.sort.nulls == NullOrder::First,
                    }),
                },
                SortColumn {
                    values: input.column(self.sequence).clone(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
            ],
            limit,
        )
        .context("sort supported Arrow values")?;
        Ok(take_record_batch(&input, &indices)?)
    }
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
            _ => bail!("aggregate encountered an incompatible numeric value"),
        }
    }
}

enum Accumulator {
    Count(u64),
    SumI64(Option<i64>),
    SumU64(Option<u64>),
    SumF64(Option<f64>),
    Avg { sum: f64, count: u64 },
    Min(Option<Scalar>),
    Max(Option<Scalar>),
}

impl Accumulator {
    fn new(expression: AggregateExpr, data_type: Option<&DataType>) -> Result<Self> {
        match expression.function() {
            AggregateFunction::Count => Ok(Self::Count(0)),
            AggregateFunction::Sum => match data_type {
                Some(data_type) if is_signed_integer(data_type) => Ok(Self::SumI64(None)),
                Some(data_type) if is_unsigned_integer(data_type) => Ok(Self::SumU64(None)),
                Some(DataType::Float32 | DataType::Float64) => Ok(Self::SumF64(None)),
                _ => bail!("SUM needs a numeric column"),
            },
            AggregateFunction::Avg => Ok(Self::Avg { sum: 0.0, count: 0 }),
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
            Self::Avg { sum, count } => {
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
            Self::Avg { sum, count } if *count > 0 => {
                Some(Scalar::F64((sum / *count as f64).to_bits()))
            }
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
    key: Scalar,
    accumulators: Vec<Accumulator>,
}

impl GroupState {
    fn new(
        key: Scalar,
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
        64 + self.key.bytes()
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
    group_by: Option<(String, DataType)>,
    groups: Vec<GroupState>,
    indexes: HashMap<Scalar, usize>,
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
            .map(|column| {
                schema
                    .fields()
                    .get(column)
                    .map(|field| (field.name().to_owned(), field.data_type().clone()))
                    .ok_or_else(|| anyhow!("group column {column} out of range"))
            })
            .transpose()?;
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
        if collector.group_by.is_none() {
            collector.ensure_group(Scalar::Null)?;
        }
        Ok(collector)
    }

    fn update_batch(
        &mut self,
        batch: &RecordBatch,
        group_position: Option<usize>,
        expression_positions: &[Option<usize>],
    ) -> Result<()> {
        for row in 0..batch.num_rows() {
            let key = match (&self.group_by, group_position) {
                (Some((_, data_type)), Some(position)) => {
                    scalar_at(batch.column(position).as_ref(), row, data_type)?
                }
                (None, None) => Scalar::Null,
                _ => bail!("aggregate group projection was inconsistent"),
            };
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

    fn ensure_group(&mut self, key: Scalar) -> Result<usize> {
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
        if let Some((_, data_type)) = &self.group_by {
            arrays.push(build_array(
                data_type,
                &self
                    .groups
                    .iter()
                    .map(|group| Some(group.key.clone()))
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
        if let Some((name, data_type)) = &self.group_by {
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
        AggregateFunction::Sum if input.is_some_and(is_unsigned_integer) => Ok(DataType::UInt64),
        AggregateFunction::Sum | AggregateFunction::Avg
            if input.is_some_and(|data_type| {
                matches!(data_type, DataType::Float32 | DataType::Float64)
            }) =>
        {
            Ok(DataType::Float64)
        }
        AggregateFunction::Avg if input.is_some_and(is_numeric) => Ok(DataType::Float64),
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
        _ => bail!("aggregate output is not supported for {data_type:?}"),
    })
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
                &|| false,
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
                &|| false,
            )
            .unwrap();
        assert_eq!(minimum.schema().field(0).data_type(), &DataType::LargeUtf8);
        assert_eq!(cells(&minimum, 0), ["a"]);

        // Sorting by Utf8View and by a zoned timestamp.
        let sort = |column: usize, direction: SortDirection| {
            let batches = source
                .read_sorted(
                    None,
                    &Projection::columns(vec![column], source.column_count()).unwrap(),
                    SortSpec::new(column, direction, NullOrder::Last),
                    SortBudget::default(),
                    None,
                    &|| false,
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
                .read_filtered_from(&filter, next_row, 1_000, &projection, &|| false)
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
            .read_filtered_from(&filter, 15_001, 2, &projection, &|| false)
            .unwrap();
        assert_eq!(ids(&middle.batch), [15_004, 15_011]);
        assert_eq!(middle.next_row, 15_012);
        let late = source
            .read_filtered_from(
                &FilterExpr::parse("id >= 25000").unwrap(),
                0,
                3,
                &projection,
                &|| false,
            )
            .unwrap();
        assert_eq!(ids(&late.batch), [25_000, 25_001, 25_002]);
        assert_eq!(late.next_row, 25_003);
        assert!(
            source
                .read_filtered_from(&filter, 30_001, 1, &projection, &|| false)
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
            .read_sorted(None, &projection, by_bucket, tiny, Some(5), &|| false)
            .unwrap();
        assert_eq!(ids(&top[0]), [6, 13, 20, 27, 34]);

        let error = source
            .read_sorted(None, &projection, by_bucket, tiny, None, &|| false)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("add a LIMIT of at most 100 rows")
        );
        assert!(
            source
                .read_sorted(None, &projection, by_bucket, tiny, Some(101), &|| false)
                .is_err()
        );

        // The budget counts rows that pass the filter, not the whole file.
        let filtered = source
            .read_sorted(
                Some(&FilterExpr::parse("id < 50").unwrap()),
                &projection,
                SortSpec::new(0, SortDirection::Descending, NullOrder::Last),
                tiny,
                None,
                &|| false,
            )
            .unwrap();
        assert_eq!(ids(&filtered[0]), (0..50).rev().collect::<Vec<_>>());
        assert!(
            source
                .read_sorted(None, &projection, by_bucket, tiny, Some(0), &|| false)
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
        let stop_on_third = || {
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
            .read_sorted(
                None,
                &projection,
                SortSpec::new(0, SortDirection::Descending, NullOrder::Last),
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
            .validate_sort(SortSpec::new(0, SortDirection::Ascending, NullOrder::Last))
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
            .read_sorted(
                None,
                &projection,
                SortSpec::new(0, SortDirection::Ascending, NullOrder::First),
                SortBudget::new(10, 1_000_000).unwrap(),
                None,
                &|| false,
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
            .read_sorted(
                None,
                &projection,
                SortSpec::new(0, SortDirection::Descending, NullOrder::Last),
                SortBudget::new(10, 1_000_000).unwrap(),
                None,
                &|| false,
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

        let error = source
            .read_sorted(
                None,
                &projection,
                SortSpec::new(0, SortDirection::Ascending, NullOrder::Last),
                SortBudget::new(3, 1_000_000).unwrap(),
                None,
                &|| false,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("external sorting is unavailable")
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
                .read_sorted(
                    None,
                    &projection,
                    SortSpec::new(column, direction, NullOrder::Last),
                    SortBudget::new(10, 1_000_000).unwrap(),
                    None,
                    &|| false,
                )
                .unwrap();
            assert_eq!(ids(&sorted[0]), expected);
        }
    }
}
