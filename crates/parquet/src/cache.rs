use std::collections::VecDeque;

use arrow_array::RecordBatch;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheKey {
    pub first_row: u64,
    pub row_count: usize,
    pub columns: Vec<usize>,
}

impl CacheKey {
    pub fn new(first_row: u64, row_count: usize, columns: &[usize]) -> Self {
        Self {
            first_row,
            row_count,
            columns: columns.to_vec(),
        }
    }
}

pub struct WindowCache {
    max_entries: usize,
    entries: VecDeque<(CacheKey, RecordBatch)>,
}

impl WindowCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            entries: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &CacheKey) -> Option<RecordBatch> {
        let index = self.entries.iter().position(|(entry, _)| entry == key)?;
        let (key, batch) = self.entries.remove(index)?;
        self.entries.push_back((key, batch.clone()));
        Some(batch)
    }

    pub fn insert(&mut self, key: CacheKey, batch: RecordBatch) {
        if let Some(index) = self.entries.iter().position(|(entry, _)| entry == &key) {
            self.entries.remove(index);
        }

        self.entries.push_back((key, batch));

        while self.entries.len() > self.max_entries {
            self.entries.pop_front();
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for WindowCache {
    fn default() -> Self {
        Self::new(3)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(value: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))]).unwrap()
    }

    #[test]
    fn evicts_after_max_entries() {
        let mut cache = WindowCache::new(3);

        for row in 0..4 {
            cache.insert(CacheKey::new(row, 1, &[0]), batch(row as i32));
        }

        assert_eq!(cache.len(), 3);
        assert!(cache.get(&CacheKey::new(0, 1, &[0])).is_none());
        assert!(cache.get(&CacheKey::new(3, 1, &[0])).is_some());
    }
}
