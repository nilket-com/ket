//! Reusable `Vec<u8>` pool for HRFT framing.
//!
//! The wire path touches one `Vec<u8>` per payload section on every
//! inbound frame (meta, optional arrow, optional trailing) and one
//! more on every outbound postcard encode. At sustained Raft
//! heartbeat rates across a multi-node control ring that is tens of
//! thousands of small allocations per second — all with very similar
//! sizes, and all short-lived.
//!
//! True zero-copy over quinn's internal receive buffers isn't
//! possible without reaching below quinn's public API, but keeping
//! the `Vec<u8>` buffers in a free list gets steady-state allocator
//! churn on the hot path down to zero. Amortized, a frame round trip
//! goes: pool pop → `read_exact` fills → postcard borrows for decode
//! → buffer reset → postcard writes response bytes → quinn sends →
//! pool push.
//!
//! ## Bounding
//!
//! The pool has two limits. `max_retained` caps the free list length
//! so idle memory is bounded. `max_cap_bytes` caps the Vec capacity
//! we're willing to hold onto — an oversize buffer from a rare giant
//! frame is dropped rather than retained as cache-resident pressure.
//!
//! ## Drop semantics
//!
//! [`PooledBuf`] holds a `Weak<BufPool>`, so a buffer outliving the
//! pool (tests tearing down, shutdown races) just drops its Vec
//! normally. No lifetimes leak into downstream types: callers see
//! `PooledBuf` as "a `Vec<u8>` that knows where to go home."

use std::num::NonZeroU32;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};

/// Bounded free list of reusable `Vec<u8>` buffers. Safe to share
/// across tokio tasks via `Arc`.
pub struct BufPool {
    bufs: Mutex<Vec<Vec<u8>>>,
    max_retained: usize,
    max_cap_bytes: usize,
}

impl BufPool {
    /// Build a pool that keeps at most `max_retained` buffers and
    /// refuses to retain any individual buffer whose capacity has
    /// grown past `max_cap_bytes`.
    pub fn new(max_retained: usize, max_cap_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            bufs: Mutex::new(Vec::with_capacity(max_retained)),
            max_retained,
            max_cap_bytes,
        })
    }

    /// Acquire a reusable buffer. The returned `PooledBuf` has
    /// `len() == 0` but may carry non-zero capacity from prior use.
    pub fn acquire(self: &Arc<Self>) -> PooledBuf {
        let buf = self.bufs.lock().unwrap().pop().unwrap_or_default();
        PooledBuf {
            buf,
            pool: Some(Arc::downgrade(self)),
            lkey: None,
        }
    }

    /// Diagnostic: current free-list depth. Exposed for tests and
    /// future metrics surfaces.
    pub fn available(&self) -> usize {
        self.bufs.lock().unwrap().len()
    }

    fn release(&self, mut buf: Vec<u8>) {
        if buf.capacity() > self.max_cap_bytes {
            return;
        }
        buf.clear();
        let mut pool = self.bufs.lock().unwrap();
        if pool.len() < self.max_retained {
            pool.push(buf);
        }
    }
}

/// RAII guard around a `Vec<u8>` drawn from a [`BufPool`]. Dropping
/// the guard returns the buffer to the pool (subject to the pool's
/// bounds).
///
/// Derefs to `[u8]` so functions taking `&[u8]` — including
/// [`write_frame`][crate::network::core::write_frame] and
/// `postcard::from_bytes` — accept a `&PooledBuf` transparently.
///
/// ## RDMA LKey slot
///
/// `lkey` carries the NIC's Local Key for this buffer's backing
/// allocation when a
/// [`PoolRegistrar`][crate::network::transport::PoolRegistrar]
/// (Tier 0 / RDMA) has pinned the memory. QUIC callers leave it as
/// `None`; the data plane reads it directly to post zero-copy work
/// requests without a side-table lookup.
pub struct PooledBuf {
    buf: Vec<u8>,
    pool: Option<Weak<BufPool>>,
    lkey: Option<NonZeroU32>,
}

impl PooledBuf {
    /// A `PooledBuf` detached from any pool. Useful for tests and
    /// one-off frames where the pooling infrastructure isn't in
    /// scope; the inner Vec just drops normally.
    pub fn orphan(buf: Vec<u8>) -> Self {
        Self {
            buf,
            pool: None,
            lkey: None,
        }
    }

    /// Borrow the inner Vec for writes. Exposed so callers like
    /// [`crate::network::wire::encode_into`] and `read_frame` can
    /// resize / append in place.
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    pub fn as_vec(&self) -> &Vec<u8> {
        &self.buf
    }

    /// NIC-registered local key for this buffer's allocation, if
    /// any. Populated by a [`PoolRegistrar`][crate::network::transport::PoolRegistrar].
    pub fn lkey(&self) -> Option<NonZeroU32> {
        self.lkey
    }

    /// Record an LKey returned by a registrar. The caller owns the
    /// invariant that the key matches the buffer's current backing
    /// allocation — if the Vec later reallocates, the RDMA path
    /// must clear or refresh this before the next DMA.
    pub fn set_lkey(&mut self, lkey: Option<NonZeroU32>) {
        self.lkey = lkey;
    }
}

impl std::fmt::Debug for PooledBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledBuf")
            .field("len", &self.buf.len())
            .field("cap", &self.buf.capacity())
            .finish()
    }
}

impl Deref for PooledBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buf
    }
}

impl DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }
}

impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(weak) = self.pool.take() {
            if let Some(pool) = weak.upgrade() {
                let taken = std::mem::take(&mut self.buf);
                pool.release(taken);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_returns_buffer_to_pool() {
        let pool = BufPool::new(4, 1024);
        assert_eq!(pool.available(), 0);

        {
            let mut buf = pool.acquire();
            buf.as_mut_vec().extend_from_slice(&[1, 2, 3, 4]);
            assert_eq!(&*buf, &[1, 2, 3, 4]);
        }
        assert_eq!(pool.available(), 1, "buffer should return on drop");

        let reused = pool.acquire();
        assert!(reused.is_empty(), "reused buffer must be cleared");
        assert!(
            reused.as_vec().capacity() >= 4,
            "reused buffer retains its grown capacity"
        );
    }

    #[test]
    fn oversize_buffer_is_not_retained() {
        let pool = BufPool::new(4, 16);
        {
            let mut buf = pool.acquire();
            buf.as_mut_vec().extend_from_slice(&[0u8; 64]);
        }
        assert_eq!(
            pool.available(),
            0,
            "buffer over max_cap_bytes must be dropped, not retained"
        );
    }

    #[test]
    fn free_list_length_is_bounded() {
        let pool = BufPool::new(2, 1024);
        let b1 = pool.acquire();
        let b2 = pool.acquire();
        let b3 = pool.acquire();
        drop(b1);
        drop(b2);
        drop(b3);
        assert_eq!(pool.available(), 2, "free list is capped at max_retained");
    }

    #[test]
    fn orphan_has_no_pool_interaction() {
        let pool = BufPool::new(4, 1024);
        {
            let _orphan = PooledBuf::orphan(vec![7; 8]);
        }
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn dropping_pool_before_buffer_is_safe() {
        let pool = BufPool::new(4, 1024);
        let buf = pool.acquire();
        drop(pool); // strong count → 0; weak stays live
        // PooledBuf drop tries to upgrade the weak handle, fails,
        // and the Vec drops normally. No panic, no leak.
        drop(buf);
    }
}
