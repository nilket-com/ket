use std::sync::Arc;
pub use wendelon::network::pool::{BufPool, PooledBuf};

/// Toron-specific memory manager, wrapping Wendelon's bounded buffer pool.
pub struct MemoryManager {
    pool: Arc<BufPool>,
}

impl MemoryManager {
    pub fn new(max_retained: usize, max_cap_bytes: usize) -> Self {
        Self {
            pool: BufPool::new(max_retained, max_cap_bytes),
        }
    }

    pub fn acquire(&self) -> PooledBuf {
        self.pool.acquire()
    }

    pub fn acquire_at_least(&self, min_capacity: usize) -> PooledBuf {
        self.pool.acquire_at_least(min_capacity)
    }

    pub fn available(&self) -> usize {
        self.pool.available()
    }
}
