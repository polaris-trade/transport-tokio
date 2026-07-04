//! Bounded slab pool with a fixed slot array and a parking_lot free list.
//!
//! Each slab is a `Vec<u8>` behind `UnsafeCell`. The free list gates
//! exclusive slot ownership: `acquire` pops an index, `VecSlab::drop`
//! pushes it back, so at most one `VecSlab` ever holds a given index.
//! `BufferPool` is implemented on the [`SharedVecPool`] newtype so each
//! `VecSlab` can carry an owned back-reference for `Drop`-based reclaim.

use std::cell::UnsafeCell;
use std::sync::Arc;

use parking_lot::Mutex;
use transport_core::BufferPool;

pub struct VecPool {
    slabs: Box<[UnsafeCell<Vec<u8>>]>,
    free: Mutex<Vec<u32>>,
    slab_size: usize,
}

// SAFETY: slot access gated by the free list. `acquire` pops an index and
// hands it to a fresh `VecSlab`; `VecSlab::drop` pushes it back. No two
// `VecSlab`s ever hold the same index, so `UnsafeCell` interior mutation
// stays single-owner even though `VecPool` is shared across threads.
unsafe impl Sync for VecPool {}

impl VecPool {
    fn build(capacity: usize, slab_size: usize) -> Self {
        assert!(capacity > 0, "capacity must be non-zero");
        assert!(slab_size > 0, "slab_size must be non-zero");
        assert!(capacity <= u32::MAX as usize, "capacity exceeds u32::MAX");
        let slabs = (0..capacity)
            .map(|_| UnsafeCell::new(Vec::with_capacity(slab_size)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free: Vec<u32> = (0..capacity as u32).rev().collect();
        Self {
            slabs,
            free: Mutex::new(free),
            slab_size,
        }
    }

    pub fn slab_size(&self) -> usize {
        self.slab_size
    }
}

/// Reference-counted handle around [`VecPool`]. Implements [`BufferPool`] so
/// backends can share it across tasks and hand out [`VecSlab`] frames whose
/// `Drop` returns the slot to the free list.
#[derive(Clone)]
pub struct SharedVecPool(Arc<VecPool>);

impl SharedVecPool {
    pub fn new(capacity: usize, slab_size: usize) -> Self {
        Self(Arc::new(VecPool::build(capacity, slab_size)))
    }

    pub fn slab_size(&self) -> usize {
        self.0.slab_size
    }
}

pub struct VecSlab {
    pool: Arc<VecPool>,
    index: u32,
    len: usize,
}

impl VecSlab {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl AsRef<[u8]> for VecSlab {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: single-owner index guarantee, read-only view.
        let vec = unsafe { &*self.pool.slabs[self.index as usize].get() };
        let end = self.len.min(vec.len());
        &vec[..end]
    }
}

impl Drop for VecSlab {
    fn drop(&mut self) {
        self.pool.free.lock().push(self.index);
    }
}

impl BufferPool for SharedVecPool {
    type Slab = VecSlab;

    fn acquire(&self, len: usize) -> Option<VecSlab> {
        if len > self.0.slab_size {
            return None;
        }
        let index = self.0.free.lock().pop()?;
        Some(VecSlab {
            pool: Arc::clone(&self.0),
            index,
            len: 0,
        })
    }

    fn capacity(&self) -> usize {
        self.0.slabs.len()
    }

    fn in_use(&self) -> usize {
        self.0.slabs.len() - self.0.free.lock().len()
    }
}
