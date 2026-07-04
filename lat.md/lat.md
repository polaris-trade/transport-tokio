# transport-tokio

Tokio-based backend for `transport_core`. Ships a `BufferPool` implementation today; UDP and TCP `Transport` impls arrive alongside the recv path in follow-up work.

## Pool

Bounded slab pool: fixed slot array plus a free list, cheap `Drop`-based reclaim, backpressure via `acquire` returning `None`.

[[crates/transport_tokio/src/pool.rs#SharedVecPool]] is the reference-counted handle backends share across tasks; it wraps [[crates/transport_tokio/src/pool.rs#VecPool]] which owns a fixed slot array of `UnsafeCell<Vec<u8>>` plus a `parking_lot::Mutex<Vec<u32>>` free list. `Sync` is asserted manually because slot access is gated by the free list, not the compiler.

[[crates/transport_tokio/src/pool.rs#VecSlab]] is the owned slab handle. It carries `Arc<VecPool>`, a slot index, and a length. `Drop` returns the index to the free list, so the pool self-heals on task cancellation. `AsRef<[u8]>` returns the filled slice up to `len`.

`SharedVecPool::acquire(len)` returns `None` when `len` exceeds `slab_size` or when the free list is empty, giving backends a natural backpressure signal.
