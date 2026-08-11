use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow, bail};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use arrow_select::{concat::concat_batches, filter::filter_record_batch};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
};
use parquet::file::metadata::ParquetMetaData;

use crate::{
    cache::{CacheKey, WindowCache},
    filter::FilterExpr,
};

const BATCH_SIZE: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRequest {
    pub first_row: u64,
    pub row_count: usize,
    pub columns: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowGroupInfo {
    pub index: usize,
    pub first_row: u64,
    pub row_count: u64,
}

pub struct ParquetSource {
    path: PathBuf,
    schema: Arc<Schema>,
    metadata: Arc<ParquetMetaData>,
    row_count: u64,
    row_groups: Vec<RowGroupInfo>,
    cache: Mutex<WindowCache>,
}

impl ParquetSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let metadata = builder.metadata().clone();
        let row_count = metadata.file_metadata().num_rows() as u64;

        let mut first_row = 0;
        let row_groups = (0..metadata.num_row_groups())
            .map(|index| {
                let row_count = metadata.row_group(index).num_rows() as u64;
                let info = RowGroupInfo {
                    index,
                    first_row,
                    row_count,
                };
                first_row += row_count;
                info
            })
            .collect();

        Ok(Self {
            path,
            schema,
            metadata,
            row_count,
            row_groups,
            cache: Mutex::new(WindowCache::default()),
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn column_count(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn row_groups(&self) -> &[RowGroupInfo] {
        &self.row_groups
    }

    pub fn head(&self, rows: usize) -> Result<RecordBatch> {
        let columns: Vec<_> = (0..self.column_count()).collect();
        self.read_window(0, rows, &columns)
    }

    pub fn read_window(
        &self,
        first_row: u64,
        row_count: usize,
        columns: &[usize],
    ) -> Result<RecordBatch> {
        let columns = self.normalize_columns(columns)?;
        let row_count = self.clamped_row_count(first_row, row_count);
        let key = CacheKey::new(first_row, row_count, &columns);

        if let Some(batch) = self
            .cache
            .lock()
            .map_err(|_| anyhow!("window cache lock poisoned"))?
            .get(&key)
        {
            return Ok(batch);
        }

        let batch = self.read_window_uncached(first_row, row_count, &columns)?;
        self.cache
            .lock()
            .map_err(|_| anyhow!("window cache lock poisoned"))?
            .insert(key, batch.clone());
        Ok(batch)
    }

    pub fn read_filtered_window(
        &self,
        filter: &FilterExpr,
        first_match_offset: u64,
        row_count: usize,
        columns: &[usize],
    ) -> Result<RecordBatch> {
        let output_columns = self.normalize_columns(columns)?;
        if row_count == 0 {
            return Ok(self.empty_batch(&output_columns));
        }

        let filter_column = filter.column_index(&self.schema)?;
        let mut read_columns = output_columns.clone();
        if !read_columns.contains(&filter_column) {
            read_columns.push(filter_column);
            read_columns.sort_unstable();
        }

        let filter_position = read_columns
            .iter()
            .position(|column| *column == filter_column)
            .ok_or_else(|| anyhow!("filter column was not projected"))?;
        let output_positions: Vec<_> = output_columns
            .iter()
            .map(|column| {
                read_columns
                    .iter()
                    .position(|read_column| read_column == column)
                    .ok_or_else(|| anyhow!("output column was not projected"))
            })
            .collect::<Result<_>>()?;

        let mut skipped_matches = 0_u64;
        let mut remaining = row_count;
        let mut batches = Vec::new();

        for row_group in &self.row_groups {
            if !self.row_group_might_match(filter, row_group, filter_column) {
                continue;
            }

            let mut reader = self.reader_for(&read_columns, vec![row_group.index], None)?;
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
                batches.push(project_batch(&page, &output_columns, &output_positions)?);
                remaining -= take;
                skipped_matches += filtered.num_rows() as u64;

                if remaining == 0 {
                    return self.concat_or_empty(&output_columns, batches);
                }
            }
        }

        self.concat_or_empty(&output_columns, batches)
    }

    fn read_window_uncached(
        &self,
        first_row: u64,
        row_count: usize,
        columns: &[usize],
    ) -> Result<RecordBatch> {
        if row_count == 0 || first_row >= self.row_count {
            return Ok(self.empty_batch(columns));
        }

        let selected = self.overlapping_row_groups(first_row, row_count);
        if selected.is_empty() {
            return Ok(self.empty_batch(columns));
        }

        let first_selected_row = selected
            .first()
            .map(|group| group.first_row)
            .ok_or_else(|| anyhow!("no selected row groups"))?;
        let skip_before = usize::try_from(first_row - first_selected_row)?;
        let mut selectors = Vec::new();
        if skip_before > 0 {
            selectors.push(RowSelector::skip(skip_before));
        }
        selectors.push(RowSelector::select(row_count));

        let row_group_indexes = selected.iter().map(|group| group.index).collect();
        let mut reader = self.reader_for(
            columns,
            row_group_indexes,
            Some(RowSelection::from(selectors)),
        )?;
        let mut batches = Vec::new();

        while let Some(batch) = reader.next().transpose()? {
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        self.concat_or_empty(columns, batches)
    }

    fn reader_for(
        &self,
        columns: &[usize],
        row_groups: Vec<usize>,
        selection: Option<RowSelection>,
    ) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader> {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let projection = ProjectionMask::roots(builder.parquet_schema(), columns.iter().copied());
        let mut builder = builder
            .with_batch_size(BATCH_SIZE)
            .with_projection(projection)
            .with_row_groups(row_groups);

        if let Some(selection) = selection {
            builder = builder.with_row_selection(selection);
        }

        Ok(builder.build()?)
    }

    fn overlapping_row_groups(&self, first_row: u64, row_count: usize) -> Vec<&RowGroupInfo> {
        let end_row = first_row
            .saturating_add(row_count as u64)
            .min(self.row_count);
        let start = self
            .row_groups
            .partition_point(|group| group.first_row + group.row_count <= first_row);

        self.row_groups[start..]
            .iter()
            .take_while(|group| group.first_row < end_row)
            .collect()
    }

    fn normalize_columns(&self, columns: &[usize]) -> Result<Vec<usize>> {
        let mut columns = if columns.is_empty() {
            (0..self.column_count()).collect()
        } else {
            columns.to_vec()
        };

        columns.sort_unstable();
        columns.dedup();

        if let Some(column) = columns
            .iter()
            .find(|column| **column >= self.column_count())
        {
            bail!("column index {column} out of range");
        }

        Ok(columns)
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

    fn clamped_row_count(&self, first_row: u64, row_count: usize) -> usize {
        if first_row >= self.row_count {
            return 0;
        }

        row_count.min((self.row_count - first_row) as usize)
    }

    fn concat_or_empty(&self, columns: &[usize], batches: Vec<RecordBatch>) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(self.empty_batch(columns));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        Ok(concat_batches(&batches[0].schema(), &batches)?)
    }

    fn empty_batch(&self, columns: &[usize]) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(self.projected_schema(columns)))
    }

    fn projected_schema(&self, columns: &[usize]) -> Schema {
        let fields: Vec<Field> = columns
            .iter()
            .map(|column| self.schema.field(*column).clone())
            .collect();
        Schema::new(fields)
    }
}

fn project_batch(
    batch: &RecordBatch,
    output_columns: &[usize],
    output_positions: &[usize],
) -> Result<RecordBatch> {
    let fields: Vec<Field> = output_columns
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
        let batch = source.read_window(1, 2, &[0]).unwrap();

        assert_eq!(ids(&batch), vec![1, 2]);
    }

    #[test]
    fn reads_window_spanning_row_groups() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let batch = source.read_window(2, 3, &[0]).unwrap();

        assert_eq!(ids(&batch), vec![2, 3, 4]);
    }

    #[test]
    fn projects_requested_columns() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let batch = source.read_window(0, 2, &[1]).unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "value");
    }

    #[test]
    fn reads_filtered_window_without_materializing_all_matches() {
        let (_dir, path) = test_file();
        let source = ParquetSource::open(path).unwrap();
        let filter = FilterExpr::parse("id >= 2").unwrap();
        let batch = source.read_filtered_window(&filter, 1, 2, &[0]).unwrap();

        assert_eq!(ids(&batch), vec![3, 4]);
    }
}
