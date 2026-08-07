use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use arrow_schema::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub struct ParquetSource {
    path: PathBuf,
    schema: Arc<Schema>,
    row_count: u64,
}

impl ParquetSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();

        let file = File::open(&path)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        let schema = builder.schema().clone();

        let row_count = builder.metadata().file_metadata().num_rows() as u64;

        Ok(Self {
            path,
            schema,
            row_count,
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}
