use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SortOptions, TimeUnit};
use arrow_select::{concat::concat_batches, filter::filter_record_batch, take::take};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{
        ArrowReaderMetadata, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
    },
};
use parquet::file::metadata::ParquetMetaData;

use crate::{
    AggregateExpr, AggregateFunction, AggregateSpec, DataPage, DatasetMetadata, GroupBudget,
    NullOrder, PageCache, PageCacheLimits, PageCacheStats, PageKey, Projection, RowGroupInfo,
    RowWindow, SortBudget, SortDirection, SortSpec, filter::FilterExpr, page::PAGE_ROWS,
};

const BATCH_SIZE: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRequest {
    pub first_row: u64,
    pub row_count: usize,
    pub projection: Projection,
}

pub struct ParquetSource {
    path: PathBuf,
    dataset_metadata: DatasetMetadata,
    metadata: Arc<ParquetMetaData>,
    arrow_metadata: ArrowReaderMetadata,
    cache: Mutex<PageCache>,
}

impl ParquetSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let arrow_metadata = ArrowReaderMetadata::load(&file, Default::default())
            .with_context(|| format!("read Parquet metadata from {}", path.display()))?;
        let schema = arrow_metadata.schema().clone();
        let metadata = arrow_metadata.metadata().clone();
        let row_group_counts =
            (0..metadata.num_row_groups()).map(|index| metadata.row_group(index).num_rows() as u64);
        let dataset_metadata = DatasetMetadata::new(schema, row_group_counts)
            .with_context(|| format!("build row-group index for {}", path.display()))?;

        Ok(Self {
            path,
            dataset_metadata,
            metadata,
            arrow_metadata,
            cache: Mutex::new(PageCache::default()),
        })
    }

    pub fn metadata(&self) -> &DatasetMetadata {
        &self.dataset_metadata
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

    pub fn head(&self, rows: usize) -> Result<RecordBatch> {
        self.read_window(0, rows, &Projection::all(self.column_count()))
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

    pub fn read_filtered_window(
        &self,
        filter: &FilterExpr,
        first_match_offset: u64,
        row_count: usize,
        projection: &Projection,
    ) -> Result<RecordBatch> {
        if row_count == 0 {
            return Ok(self.empty_batch(projection));
        }

        let filter_column = filter.column_index(&self.dataset_metadata.schema)?;
        let mut read_columns = projection.as_slice().to_vec();
        if !read_columns.contains(&filter_column) {
            read_columns.push(filter_column);
        }
        let read_projection = Projection::columns(read_columns, self.column_count())?;
        let filter_position = read_projection
            .as_slice()
            .iter()
            .position(|column| *column == filter_column)
            .ok_or_else(|| anyhow!("filter column was not projected"))?;
        let output_positions: Vec<_> = projection
            .as_slice()
            .iter()
            .map(|column| {
                read_projection
                    .as_slice()
                    .iter()
                    .position(|read_column| read_column == column)
                    .ok_or_else(|| anyhow!("output column was not projected"))
            })
            .collect::<Result<_>>()?;

        let mut skipped_matches = 0_u64;
        let mut remaining = row_count;
        let mut batches = Vec::new();

        for row_group in self.row_groups() {
            if !self.row_group_might_match(filter, row_group, filter_column) {
                continue;
            }

            let mut reader = self.reader_for(&read_projection, vec![row_group.index], None)?;
            while let Some(batch) = reader.next().transpose()? {
                let batch = self.reorder_batch(batch, &read_projection)?;
                let mask = filter.evaluate_batch(&batch, filter_position)?;
                let filtered = filter_record_batch(&batch, &mask)?;
                if filtered.num_rows() == 0 {
                    continue;
                }

                if skipped_matches + filtered.num_rows() as u64 <= first_match_offset {
                    skipped_matches += filtered.num_rows() as u64;
                    continue;
                }

                let start = first_match_offset.saturating_sub(skipped_matches) as usize;
                let take = remaining.min(filtered.num_rows() - start);
                let page = filtered.slice(start, take);
                batches.push(project_batch(&page, projection, &output_positions)?);
                remaining -= take;
                skipped_matches += filtered.num_rows() as u64;

                if remaining == 0 {
                    return self.concat_or_empty(projection, batches);
                }
            }
        }

        self.concat_or_empty(projection, batches)
    }

    /// Reads, sorts, and returns bounded output batches for one supported sort key.
    ///
    /// This intentionally rejects inputs beyond `budget`; PAVI has no external sort yet.
    pub fn read_sorted(
        &self,
        filter: Option<&FilterExpr>,
        projection: &Projection,
        sort: SortSpec,
        budget: SortBudget,
    ) -> Result<Vec<RecordBatch>> {
        self.validate_sort(sort)?;
        if let Some(filter) = filter {
            self.validate_filter(filter)?;
        }
        if self.row_count() > budget.max_rows() as u64 {
            bail!(
                "sort input has {} rows, exceeding the in-memory sort budget of {} rows; external sorting is unavailable",
                self.row_count(),
                budget.max_rows()
            );
        }

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
        let filter_position = filter_column
            .map(|column| projected_position(&read_projection, column, "filter"))
            .transpose()?;
        let output_positions = projection
            .as_slice()
            .iter()
            .map(|column| projected_position(&read_projection, *column, "output"))
            .collect::<Result<Vec<_>>>()?;

        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut batches = Vec::new();
        for row_group in self.row_groups() {
            if let (Some(filter), Some(filter_column)) = (filter, filter_column)
                && !self.row_group_might_match(filter, row_group, filter_column)
            {
                continue;
            }
            let mut reader = self.reader_for(&read_projection, vec![row_group.index], None)?;
            while let Some(batch) = reader.next().transpose()? {
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
                let next_rows = rows.saturating_add(batch.num_rows());
                let next_bytes = bytes.saturating_add(batch.get_array_memory_size());
                if next_rows > budget.max_rows()
                    || next_bytes
                        .saturating_mul(3)
                        .saturating_add(next_rows.saturating_mul(8))
                        > budget.max_bytes()
                {
                    bail!(
                        "sort input exceeds the in-memory sort budget of {} rows / {} bytes; external sorting is unavailable",
                        budget.max_rows(),
                        budget.max_bytes()
                    );
                }
                rows = next_rows;
                bytes = next_bytes;
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let input = self.concat_or_empty(&read_projection, batches)?;
        let tie_breaker = UInt64Array::from_iter_values(0..input.num_rows() as u64);
        let indices = lexsort_to_indices(
            &[
                SortColumn {
                    values: input.column(sort_position).clone(),
                    options: Some(SortOptions {
                        descending: sort.direction == SortDirection::Descending,
                        nulls_first: sort.nulls == NullOrder::First,
                    }),
                },
                SortColumn {
                    values: std::sync::Arc::new(tie_breaker),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
            ],
            None,
        )
        .context("sort supported Arrow values")?;
        let fields: Vec<Field> = output_positions
            .iter()
            .map(|position| input.schema().field(*position).clone())
            .collect();
        let arrays = output_positions
            .iter()
            .map(|position| take(input.column(*position).as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let sorted = RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), arrays)?;
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
    pub fn read_aggregated(
        &self,
        filter: Option<&FilterExpr>,
        aggregate: &AggregateSpec,
        budget: GroupBudget,
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
            if !supports_grouping(field.data_type()) {
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
                    if supports_numeric(field.data_type()) => {}
                (AggregateFunction::Min | AggregateFunction::Max, Some(field))
                    if supports_min_max(field.data_type()) => {}
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

    pub fn validate_sort(&self, sort: SortSpec) -> Result<()> {
        let field = self
            .dataset_metadata
            .schema
            .fields()
            .get(sort.column)
            .ok_or_else(|| anyhow!("sort column {} out of range", sort.column))?;
        match field.data_type() {
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
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _) => Ok(()),
            data_type => bail!("sorting is not supported for {data_type:?} columns"),
        }
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
    ) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader> {
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

        Ok(builder.build()?)
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

        self.metadata
            .row_group(row_group.index)
            .columns()
            .get(filter_column)
            .is_none_or(|column| filter.might_match_statistics(column.statistics()))
    }

    fn concat_or_empty(
        &self,
        projection: &Projection,
        batches: Vec<RecordBatch>,
    ) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(self.empty_batch(projection));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
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
                Some(data_type) if is_signed(data_type) => Ok(Self::SumI64(None)),
                Some(data_type) if is_unsigned(data_type) => Ok(Self::SumU64(None)),
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
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        if let Some((name, data_type)) = &self.group_by {
            fields.push(Field::new(name, data_type.clone(), true));
            arrays.push(build_array(
                data_type,
                &self
                    .groups
                    .iter()
                    .map(|group| Some(group.key.clone()))
                    .collect::<Vec<_>>(),
            )?);
        }
        for (index, (expression, output_type)) in
            self.expressions.iter().zip(&self.output_types).enumerate()
        {
            fields.push(Field::new(
                aggregate_name(*expression, &self.input_names[index]),
                output_type.clone(),
                expression.function() != AggregateFunction::Count,
            ));
            arrays.push(build_array(
                output_type,
                &self
                    .groups
                    .iter()
                    .map(|group| group.accumulators[index].value())
                    .collect::<Vec<_>>(),
            )?);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(Into::into)
    }
}

fn supports_numeric(data_type: &DataType) -> bool {
    is_signed(data_type)
        || is_unsigned(data_type)
        || matches!(data_type, DataType::Float32 | DataType::Float64)
}

fn is_signed(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn is_unsigned(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}

fn supports_min_max(data_type: &DataType) -> bool {
    supports_numeric(data_type)
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, None)
        )
}

fn supports_grouping(data_type: &DataType) -> bool {
    supports_min_max(data_type)
}

fn aggregate_output_type(expression: AggregateExpr, input: Option<&DataType>) -> Result<DataType> {
    match expression.function() {
        AggregateFunction::Count => Ok(DataType::UInt64),
        AggregateFunction::Sum if input.is_some_and(is_signed) => Ok(DataType::Int64),
        AggregateFunction::Sum if input.is_some_and(is_unsigned) => Ok(DataType::UInt64),
        AggregateFunction::Sum | AggregateFunction::Avg
            if input.is_some_and(|data_type| {
                matches!(data_type, DataType::Float32 | DataType::Float64)
            }) =>
        {
            Ok(DataType::Float64)
        }
        AggregateFunction::Avg if input.is_some_and(supports_numeric) => Ok(DataType::Float64),
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
        DataType::Timestamp(TimeUnit::Second, None) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .context("read second timestamp aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .context("read millisecond timestamp aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Scalar::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("read microsecond timestamp aggregate value")?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok(Scalar::Timestamp(
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
        DataType::Date32 => Arc::new(Date32Array::from(values!(|value: &Scalar| match value {
            Scalar::Date32(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Date64 => Arc::new(Date64Array::from(values!(|value: &Scalar| match value {
            Scalar::Date64(value) => Ok(*value),
            _ => bail!("aggregate output type mismatch"),
        }))),
        DataType::Timestamp(TimeUnit::Second, None) => Arc::new(TimestampSecondArray::from(
            values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            }),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Arc::new(
            TimestampMillisecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            })),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(
            TimestampMicrosecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            })),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::new(
            TimestampNanosecondArray::from(values!(|value: &Scalar| match value {
                Scalar::Timestamp(value) => Ok(*value),
                _ => bail!("aggregate output type mismatch"),
            })),
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
        Array, ArrayRef, Date32Array, Int32Array, RecordBatch, TimestampMillisecondArray,
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
        assert_eq!(page.batches[0].num_columns(), 64);
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
                )
                .unwrap();
            assert_eq!(ids(&sorted[0]), expected);
        }
    }
}
