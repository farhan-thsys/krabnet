//! Custom buffer pool with graph-aware eviction ordering.
//!
//! Provides a page-level memory allocator backed by a contiguous pre-allocated
//! buffer. Pages are fixed-size and can be allocated, freed, read, and written.
//! Graph-aware eviction respects frame temperature tiers: Cold pages are evicted
//! first, then Warm, and Hot pages are never evicted.
//!
//! # Usage
//!
//! ```
//! use krabnet::buffer_pool::{BufferPool, PageMeta, PageHandle};
//! use krabnet::FrameTier;
//!
//! let mut pool = BufferPool::new(4096, 256); // 16 pages of 256 bytes
//!
//! let handle = pool.alloc(PageMeta { frame_id: Some(1), tier: FrameTier::Warm }).unwrap();
//! pool.write(handle, 0, &[1, 2, 3, 4]);
//! let data = pool.read(handle, 0, 4);
//! assert_eq!(data, &[1, 2, 3, 4]);
//!
//! pool.free(handle);
//! assert_eq!(pool.free_page_count(), 16); // all pages free again
//! ```

use std::collections::HashMap;

use crate::types::FrameTier;

/// Metadata for an allocated page.
#[derive(Debug, Clone)]
pub struct PageMeta {
    /// Which frame owns this page (if any).
    pub frame_id: Option<u64>,
    /// Tier of the owning frame at allocation time.
    pub tier: FrameTier,
}

/// Handle returned by alloc, used for read/write/free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageHandle(pub usize);

/// A custom buffer pool with fixed-size pages and graph-aware eviction.
///
/// The pool manages a contiguous backing buffer divided into fixed-size pages.
/// Pages are allocated from a free list (stack) for O(1) allocation, and
/// freed back to the free list. Graph-aware eviction respects frame temperature
/// tiers for memory pressure relief.
pub struct BufferPool {
    /// Contiguous backing buffer.
    buffer: Vec<u8>,
    /// Size of each page in bytes.
    page_size: usize,
    /// Total number of pages.
    page_count: usize,
    /// Free list: indices of available pages (used as stack).
    free_list: Vec<usize>,
    /// Allocated pages: maps page_index -> PageMeta.
    allocated: HashMap<usize, PageMeta>,
}

impl BufferPool {
    /// Creates a new buffer pool with the given total size and page size.
    ///
    /// # Arguments
    ///
    /// * `total_bytes` - Total size of the backing buffer in bytes.
    /// * `page_size` - Size of each page in bytes. Must be > 0.
    ///
    /// # Panics
    ///
    /// Panics if `page_size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::buffer_pool::BufferPool;
    ///
    /// // 256 MB with 4 KB pages = 65,536 pages
    /// let pool = BufferPool::new(256 * 1024 * 1024, 4096);
    /// assert_eq!(pool.total_page_count(), 65536);
    ///
    /// // Small pool for testing
    /// let pool = BufferPool::new(4096, 256);
    /// assert_eq!(pool.total_page_count(), 16);
    /// ```
    pub fn new(total_bytes: usize, page_size: usize) -> Self {
        assert!(page_size > 0, "page_size must be > 0");
        let page_count = total_bytes / page_size;
        let buffer = vec![0u8; page_count * page_size];
        let free_list: Vec<usize> = (0..page_count).collect();

        Self {
            buffer,
            page_size,
            page_count,
            free_list,
            allocated: HashMap::new(),
        }
    }

    /// Allocates a page from the pool with the given metadata.
    ///
    /// Returns `None` if no free pages are available.
    pub fn alloc(&mut self, meta: PageMeta) -> Option<PageHandle> {
        let index = self.free_list.pop()?;
        self.allocated.insert(index, meta);
        Some(PageHandle(index))
    }

    /// Frees a previously allocated page, returning it to the free list.
    ///
    /// # Panics
    ///
    /// Debug-asserts if the handle is not currently allocated.
    pub fn free(&mut self, handle: PageHandle) {
        debug_assert!(
            self.allocated.contains_key(&handle.0),
            "Attempted to free unallocated page handle: {:?}",
            handle
        );
        self.allocated.remove(&handle.0);
        self.free_list.push(handle.0);
    }

    /// Reads `len` bytes from a page starting at the given offset.
    ///
    /// # Panics
    ///
    /// Panics if `offset + len > page_size` or if the handle is not allocated.
    pub fn read(&self, handle: PageHandle, offset: usize, len: usize) -> &[u8] {
        assert!(
            self.allocated.contains_key(&handle.0),
            "Read from unallocated page handle: {:?}",
            handle
        );
        assert!(
            offset + len <= self.page_size,
            "Read exceeds page boundary: offset={offset}, len={len}, page_size={}",
            self.page_size
        );
        let page_start = handle.0 * self.page_size;
        &self.buffer[page_start + offset..page_start + offset + len]
    }

    /// Writes data to a page starting at the given offset.
    ///
    /// # Panics
    ///
    /// Panics if `offset + data.len() > page_size` or if the handle is not allocated.
    pub fn write(&mut self, handle: PageHandle, offset: usize, data: &[u8]) {
        assert!(
            self.allocated.contains_key(&handle.0),
            "Write to unallocated page handle: {:?}",
            handle
        );
        assert!(
            offset + data.len() <= self.page_size,
            "Write exceeds page boundary: offset={offset}, len={}, page_size={}",
            data.len(),
            self.page_size
        );
        let page_start = handle.0 * self.page_size;
        self.buffer[page_start + offset..page_start + offset + data.len()].copy_from_slice(data);
    }

    /// Returns the number of free pages available for allocation.
    pub fn free_page_count(&self) -> usize {
        self.free_list.len()
    }

    /// Returns the number of currently allocated pages.
    pub fn allocated_page_count(&self) -> usize {
        self.allocated.len()
    }

    /// Returns the total number of pages in the pool.
    pub fn total_page_count(&self) -> usize {
        self.page_count
    }

    /// Evicts all allocated pages with the given tier, freeing them.
    ///
    /// Returns the handles of the evicted pages.
    pub fn evict_by_tier(&mut self, tier: FrameTier) -> Vec<PageHandle> {
        let to_evict: Vec<usize> = self
            .allocated
            .iter()
            .filter(|(_, meta)| meta.tier == tier)
            .map(|(idx, _)| *idx)
            .collect();

        let mut handles = Vec::with_capacity(to_evict.len());
        for idx in to_evict {
            self.allocated.remove(&idx);
            self.free_list.push(idx);
            handles.push(PageHandle(idx));
        }
        handles
    }

    /// Evicts up to `count` pages in priority order: Cold first, then Warm, never Hot.
    ///
    /// Collects Cold pages first. If not enough, adds Warm pages. Never touches
    /// Hot pages. Frees each evicted page and returns their handles.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::buffer_pool::{BufferPool, PageMeta};
    /// use krabnet::FrameTier;
    ///
    /// let mut pool = BufferPool::new(1024, 256); // 4 pages
    /// pool.alloc(PageMeta { frame_id: Some(1), tier: FrameTier::Cold });
    /// pool.alloc(PageMeta { frame_id: Some(2), tier: FrameTier::Warm });
    /// pool.alloc(PageMeta { frame_id: Some(3), tier: FrameTier::Hot });
    ///
    /// let evicted = pool.evict_coldest(2);
    /// assert_eq!(evicted.len(), 2); // Cold + Warm evicted, Hot untouched
    /// ```
    pub fn evict_coldest(&mut self, count: usize) -> Vec<PageHandle> {
        let mut evicted = Vec::with_capacity(count);

        // Phase 1: Collect Cold pages
        let cold_indices: Vec<usize> = self
            .allocated
            .iter()
            .filter(|(_, meta)| meta.tier == FrameTier::Cold)
            .map(|(idx, _)| *idx)
            .collect();

        for idx in cold_indices {
            if evicted.len() >= count {
                break;
            }
            self.allocated.remove(&idx);
            self.free_list.push(idx);
            evicted.push(PageHandle(idx));
        }

        // Phase 2: If not enough, collect Warm pages
        if evicted.len() < count {
            let warm_indices: Vec<usize> = self
                .allocated
                .iter()
                .filter(|(_, meta)| meta.tier == FrameTier::Warm)
                .map(|(idx, _)| *idx)
                .collect();

            for idx in warm_indices {
                if evicted.len() >= count {
                    break;
                }
                self.allocated.remove(&idx);
                self.free_list.push(idx);
                evicted.push(PageHandle(idx));
            }
        }

        // Never touch Hot pages

        evicted
    }

    /// Updates the tier of a page's metadata.
    ///
    /// Used when a frame's tier changes (e.g., after hysteresis tier change).
    ///
    /// # Panics
    ///
    /// Panics if the handle is not currently allocated.
    pub fn update_tier(&mut self, handle: PageHandle, new_tier: FrameTier) {
        let meta = self
            .allocated
            .get_mut(&handle.0)
            .expect("update_tier on unallocated page handle");
        meta.tier = new_tier;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TEST-29: Allocate all pages, free half, reallocate, verify no corruption
    /// by writing distinct patterns to each page and reading them back.
    #[test]
    fn test_alloc_free() {
        let page_size = 256;
        let total_bytes = 4096; // 16 pages
        let mut pool = BufferPool::new(total_bytes, page_size);
        let page_count = pool.total_page_count();
        assert_eq!(page_count, 16);

        // Allocate all pages, writing distinct patterns
        let mut handles: Vec<PageHandle> = Vec::new();
        for i in 0..page_count {
            let handle = pool
                .alloc(PageMeta {
                    frame_id: Some(i as u64),
                    tier: FrameTier::Warm,
                })
                .expect("Should be able to allocate");

            // Write a distinct pattern: fill page with (i as u8)
            let pattern = vec![i as u8; page_size];
            pool.write(handle, 0, &pattern);
            handles.push(handle);
        }

        assert_eq!(pool.free_page_count(), 0);
        assert_eq!(pool.allocated_page_count(), page_count);

        // Verify all data is intact
        for (i, handle) in handles.iter().enumerate() {
            let data = pool.read(*handle, 0, page_size);
            let expected = vec![i as u8; page_size];
            assert_eq!(data, &expected[..], "Data corruption detected at page {i}");
        }

        // Free the first half
        for handle in handles.iter().take(page_count / 2) {
            pool.free(*handle);
        }
        assert_eq!(pool.free_page_count(), page_count / 2);
        assert_eq!(pool.allocated_page_count(), page_count / 2);

        // Verify remaining pages still have correct data
        for (i, handle) in handles.iter().enumerate().skip(page_count / 2) {
            let data = pool.read(*handle, 0, page_size);
            let expected = vec![i as u8; page_size];
            assert_eq!(
                data,
                &expected[..],
                "Data corruption detected at page {i} after freeing first half"
            );
        }

        // Reallocate the freed pages with new patterns
        let mut new_handles: Vec<PageHandle> = Vec::new();
        for i in 0..(page_count / 2) {
            let handle = pool
                .alloc(PageMeta {
                    frame_id: Some((100 + i) as u64),
                    tier: FrameTier::Cold,
                })
                .expect("Should be able to reallocate freed pages");

            // Write a new distinct pattern
            let pattern = vec![(200 + i) as u8; page_size];
            pool.write(handle, 0, &pattern);
            new_handles.push(handle);
        }

        assert_eq!(pool.free_page_count(), 0);

        // Verify new pages have correct data
        for (i, handle) in new_handles.iter().enumerate() {
            let data = pool.read(*handle, 0, page_size);
            let expected = vec![(200 + i) as u8; page_size];
            assert_eq!(
                data,
                &expected[..],
                "Data corruption detected at reallocated page {i}"
            );
        }

        // Verify old remaining pages still intact
        for (i, handle) in handles.iter().enumerate().skip(page_count / 2) {
            let data = pool.read(*handle, 0, page_size);
            let expected = vec![i as u8; page_size];
            assert_eq!(
                data,
                &expected[..],
                "Data corruption detected at original page {i} after reallocation"
            );
        }
    }

    /// TEST-30: Allocate pages with Cold, Warm, and Hot tiers.
    /// Call evict_coldest(N). Verify Cold evicted first, then Warm, Hot never evicted.
    #[test]
    fn test_eviction_order() {
        let mut pool = BufferPool::new(2560, 256); // 10 pages

        // Allocate 3 Cold, 3 Warm, 3 Hot pages
        let mut cold_handles = Vec::new();
        let mut warm_handles = Vec::new();
        let mut hot_handles = Vec::new();

        for i in 0..3 {
            let h = pool
                .alloc(PageMeta {
                    frame_id: Some(i),
                    tier: FrameTier::Cold,
                })
                .unwrap();
            cold_handles.push(h);
        }
        for i in 3..6 {
            let h = pool
                .alloc(PageMeta {
                    frame_id: Some(i),
                    tier: FrameTier::Warm,
                })
                .unwrap();
            warm_handles.push(h);
        }
        for i in 6..9 {
            let h = pool
                .alloc(PageMeta {
                    frame_id: Some(i),
                    tier: FrameTier::Hot,
                })
                .unwrap();
            hot_handles.push(h);
        }

        assert_eq!(pool.allocated_page_count(), 9);
        assert_eq!(pool.free_page_count(), 1);

        // Evict 5 pages: should get all 3 Cold + 2 Warm
        let evicted = pool.evict_coldest(5);
        assert_eq!(evicted.len(), 5, "Should evict exactly 5 pages");

        // All Cold pages should have been evicted
        for h in &cold_handles {
            assert!(
                evicted.contains(h),
                "Cold page {:?} should be evicted",
                h
            );
        }

        // Some Warm pages should have been evicted (2 of 3)
        let warm_evicted_count = warm_handles.iter().filter(|h| evicted.contains(h)).count();
        assert_eq!(
            warm_evicted_count, 2,
            "Exactly 2 Warm pages should be evicted"
        );

        // No Hot pages should have been evicted
        for h in &hot_handles {
            assert!(
                !evicted.contains(h),
                "Hot page {:?} should NOT be evicted",
                h
            );
        }

        assert_eq!(pool.allocated_page_count(), 4); // 1 Warm + 3 Hot
        assert_eq!(pool.free_page_count(), 6); // 1 original + 5 evicted
    }

    #[test]
    fn test_alloc_returns_none_when_full() {
        let mut pool = BufferPool::new(512, 256); // 2 pages

        let h1 = pool.alloc(PageMeta {
            frame_id: None,
            tier: FrameTier::Cold,
        });
        assert!(h1.is_some());

        let h2 = pool.alloc(PageMeta {
            frame_id: None,
            tier: FrameTier::Cold,
        });
        assert!(h2.is_some());

        // Pool is full
        let h3 = pool.alloc(PageMeta {
            frame_id: None,
            tier: FrameTier::Cold,
        });
        assert!(h3.is_none(), "Should return None when pool is full");
    }

    #[test]
    fn test_read_write_roundtrip() {
        let mut pool = BufferPool::new(1024, 256);
        let handle = pool
            .alloc(PageMeta {
                frame_id: Some(42),
                tier: FrameTier::Warm,
            })
            .unwrap();

        // Write some bytes at offset 10
        let data = b"Hello, BufferPool!";
        pool.write(handle, 10, data);

        // Read back
        let read_back = pool.read(handle, 10, data.len());
        assert_eq!(read_back, data);

        // Verify bytes before offset are still zero
        let zeros = pool.read(handle, 0, 10);
        assert_eq!(zeros, &[0u8; 10]);
    }

    #[test]
    fn test_update_tier() {
        let mut pool = BufferPool::new(1024, 256);

        // Allocate as Cold
        let handle = pool
            .alloc(PageMeta {
                frame_id: Some(1),
                tier: FrameTier::Cold,
            })
            .unwrap();

        // Update to Hot
        pool.update_tier(handle, FrameTier::Hot);

        // Verify evict_coldest skips it
        let evicted = pool.evict_coldest(10);
        assert!(
            evicted.is_empty(),
            "Updated-to-Hot page should not be evicted"
        );

        // Page should still be allocated
        assert_eq!(pool.allocated_page_count(), 1);
    }
}
