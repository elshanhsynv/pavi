use std::collections::VecDeque;

use crate::{DataPage, PageKey};

pub const DEFAULT_MAX_PAGES: usize = 8;
pub const DEFAULT_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageCacheLimits {
    pub max_pages: usize,
    pub max_bytes: usize,
}

pub struct PageCache {
    limits: PageCacheLimits,
    bytes: usize,
    entries: VecDeque<DataPage>,
}

impl PageCache {
    pub fn new(limits: PageCacheLimits) -> Self {
        Self {
            limits: PageCacheLimits {
                max_pages: limits.max_pages.max(1),
                max_bytes: limits.max_bytes,
            },
            bytes: 0,
            entries: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &PageKey) -> Option<DataPage> {
        let index = self.entries.iter().position(|entry| &entry.key == key)?;
        let page = self.entries.remove(index)?;
        self.entries.push_back(page.clone());
        Some(page)
    }

    pub fn insert(&mut self, page: DataPage) -> bool {
        if page.byte_size > self.limits.max_bytes {
            self.remove(&page.key);
            return false;
        }

        self.remove(&page.key);
        self.bytes += page.byte_size;
        self.entries.push_back(page);
        self.evict();
        true
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&mut self, key: &PageKey) {
        if let Some(index) = self.entries.iter().position(|entry| &entry.key == key) {
            if let Some(page) = self.entries.remove(index) {
                self.bytes = self.bytes.saturating_sub(page.byte_size);
            }
        }
    }

    fn evict(&mut self) {
        while self.entries.len() > self.limits.max_pages || self.bytes > self.limits.max_bytes {
            let Some(page) = self.entries.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(page.byte_size);
        }
    }
}

impl Default for PageCache {
    fn default() -> Self {
        Self::new(PageCacheLimits {
            max_pages: DEFAULT_MAX_PAGES,
            max_bytes: DEFAULT_MAX_BYTES,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{DataPage, PageKey, Projection, RowWindow};

    use super::*;

    fn page(index: u64, bytes: usize, projection: Projection) -> DataPage {
        DataPage {
            key: PageKey::new(index, projection),
            window: RowWindow {
                first_row: index * 4096,
                row_count: 1,
            },
            batches: Vec::new(),
            byte_size: bytes,
        }
    }

    #[test]
    fn evicts_after_max_pages() {
        let projection = Projection::all(1);
        let mut cache = PageCache::new(PageCacheLimits {
            max_pages: 2,
            max_bytes: 100,
        });

        cache.insert(page(0, 1, projection.clone()));
        cache.insert(page(1, 1, projection.clone()));
        cache.insert(page(2, 1, projection.clone()));

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&PageKey::new(0, projection.clone())).is_none());
        assert!(cache.get(&PageKey::new(2, projection)).is_some());
    }

    #[test]
    fn evicts_by_byte_budget() {
        let projection = Projection::all(1);
        let mut cache = PageCache::new(PageCacheLimits {
            max_pages: 8,
            max_bytes: 10,
        });

        cache.insert(page(0, 6, projection.clone()));
        cache.insert(page(1, 6, projection.clone()));

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.bytes(), 6);
        assert!(cache.get(&PageKey::new(0, projection)).is_none());
    }

    #[test]
    fn skips_oversized_pages() {
        let projection = Projection::all(1);
        let mut cache = PageCache::new(PageCacheLimits {
            max_pages: 8,
            max_bytes: 10,
        });

        assert!(!cache.insert(page(0, 11, projection.clone())));
        assert!(cache.get(&PageKey::new(0, projection)).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn isolates_projection_keys() {
        let mut cache = PageCache::new(PageCacheLimits {
            max_pages: 8,
            max_bytes: 100,
        });
        let first = Projection::columns(vec![0], 2).unwrap();
        let second = Projection::columns(vec![1], 2).unwrap();

        cache.insert(page(0, 1, first.clone()));

        assert!(cache.get(&PageKey::new(0, first)).is_some());
        assert!(cache.get(&PageKey::new(0, second)).is_none());
    }
}
