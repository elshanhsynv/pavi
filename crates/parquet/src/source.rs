use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use arrow_select::{concat::concat_batches, filter::filter_record_batch};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
};
use parquet::file::metadata::ParquetMetaData;

use crate::{
    DataPage, DatasetMetadata, PageCache, PageCacheLimits, PageKey, Projection, RowGroupInfo,
    RowWindow, filter::FilterExpr,
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
    cache: Mutex<PageCache>,
}

impl ParquetSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("read Parquet metadata from {}", path.display()))?;
        let schema = builder.schema().clone();
        let metadata = builder.metadata().clone();
        let row_group_counts =
            (0..metadata.num_row_groups()).map(|index| metadata.row_group(index).num_rows() as u64);
        let dataset_metadata = DatasetMetadata::new(schema, row_group_counts)
            .with_context(|| format!("build row-group index for {}", path.display()))?;

        Ok(Self {
            path,
            dataset_metadata,
            metadata,
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
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("create Parquet reader for {}", self.path.display()))?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
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
}
